/*
 * mdadm - manage Linux "md" devices aka RAID arrays.
 *
 * Copyright (C) 2001-2002 Neil Brown <neilb@cse.unsw.edu.au>
 *
 *
 *    This program is free software; you can redistribute it and/or modify
 *    it under the terms of the GNU General Public License as published by
 *    the Free Software Foundation; either version 2 of the License, or
 *    (at your option) any later version.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU General Public License for more details.
 *
 *    You should have received a copy of the GNU General Public License
 *    along with this program; if not, write to the Free Software
 *    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 *    Author: Neil Brown
 *    Email: <neilb@cse.unsw.edu.au>
 *    Paper: Neil Brown
 *           School of Computer Science and Engineering
 *           The University of New South Wales
 *           Sydney, 2052
 *           Australia
 */

#include	"mdadm.h"
#include	"dlink.h"
#include	<sys/dir.h>
#include	<glob.h>
#include	<fnmatch.h>
#include	<ctype.h>

/*
 * Read the config file
 *
 * conf_get_uuids gets a list of devicename+uuid pairs
 * conf_get_devs gets device names after expanding wildcards
 *
 * Each keeps the returned list and frees it when asked to make
 * a new list.
 *
 * The format of the config file needs to be fairly extensible.
 * Now, arrays only have names and uuids and devices merely are.
 * But later arrays might want names, and devices might want superblock
 * versions, and who knows what else.
 * I like free format, abhore backslash line continuation, adore
 *   indentation for structure and am ok about # comments.
 *
 * So, each line that isn't blank or a #comment must either start
 *  with a key word, and not be indented, or must start with a
 *  non-key-word and must be indented.
 *
 * Keywords are DEVICE and ARRAY
 * DEV{ICE} introduces some devices that might contain raid components.
 * e.g.
 *   DEV style=0 /dev/sda* /dev/hd*
 *   DEV style=1 /dev/sd[b-f]*
 * ARR{AY} describes an array giving md device and attributes like uuid=whatever
 * e.g.
 *   ARRAY /dev/md0 uuid=whatever name=something
 * Spaces separate words on each line.  Quoting, with "" or '' protects them,
 * but may not wrap over lines
 *
 */

#ifndef CONFFILE
#define CONFFILE "/etc/mdadm.conf"
#endif
#ifndef CONFFILE2
/* for Debian compatibility .... */
#define CONFFILE2 "/etc/mdadm/mdadm.conf"
#endif
char DefaultConfFile[] = CONFFILE;
char DefaultAltConfFile[] = CONFFILE2;

char *keywords[] = { "device", "array", "mailaddr", "program", NULL };

/*
 * match_keyword returns an index into the keywords array, or -1 for no match
 * case is ignored, and at least three characters must be given
 */

int match_keyword(char *word)
{
	int len = strlen(word);
	int n;
    
	if (len < 3) return -1;
	for (n=0; keywords[n]; n++) {
		if (strncasecmp(word, keywords[n], len)==0)
			return n;
	}
	return -1;
}

/* conf_word gets one word from the conf file.
 * if "allow_key", then accept words at the start of a line,
 * otherwise stop when such a word is found.
 * We assume that the file pointer is at the end of a word, so the
 * next character is a space, or a newline.  If not, it is the start of a line.
 */

char *conf_word(FILE *file, int allow_key)
{
	int wsize = 100;
	int len = 0;
	int c;
	int quote;
	int wordfound = 0;
	char *word = malloc(wsize);

	if (!word) abort();

	while (wordfound==0) {
		/* at the end of a word.. */
		c = getc(file);
		if (c == '#')
			while (c != EOF && c != '\n')
				c = getc(file);
		if (c == EOF) break;
		if (c == '\n') continue;

		if (c != ' ' && c != '\t' && ! allow_key) {
			ungetc(c, file);
			break;
		}
		/* looks like it is safe to get a word here, if there is one */
		quote = 0;
		/* first, skip any spaces */
		while (c == ' ' || c == '\t')
			c = getc(file);
		if (c != EOF && c != '\n' && c != '#') {
			/* we really have a character of a word, so start saving it */
			while (c != EOF && c != '\n' && (quote || (c!=' ' && c != '\t'))) {
				wordfound = 1;
				if (quote && c == quote) quote = 0;
				else if (quote == 0 && (c == '\'' || c == '"'))
					quote = c;
				else {
					if (len == wsize-1) {
						wsize += 100;
						word = realloc(word, wsize);
						if (!word) abort();
					}
					word[len++] = c;
				}
				c = getc(file);
			}
		}
		if (c != EOF) ungetc(c, file);
	}
	word[len] = 0;
/*    printf("word is <%s>\n", word); */
	if (!wordfound) {
		free(word);
		word = NULL;
	}
	return word;
}
	
/*
 * conf_line reads one logical line from the conffile.
 * It skips comments and continues until it finds a line that starts
 * with a non blank/comment.  This character is pushed back for the next call
 * A doubly linked list of words is returned.
 * the first word will be a keyword.  Other words will have had quotes removed.
 */

char *conf_line(FILE *file)
{
	char *w;
	char *list;

	w = conf_word(file, 1);
	if (w == NULL) return NULL;

	list = dl_strdup(w);
	free(w);
	dl_init(list);

	while ((w = conf_word(file,0))){
		char *w2 = dl_strdup(w);
		free(w);
		dl_add(list, w2);
	}
/*    printf("got a line\n");*/
	return list;
}

void free_line(char *line)
{
	char *w;
	for (w=dl_next(line); w != line; w=dl_next(line)) {
		dl_del(w);
		dl_free(w);
	}
	dl_free(line);
}


struct conf_dev {
    struct conf_dev *next;
    char *name;
} *cdevlist = NULL;

mddev_dev_t load_partitions(void)
{
	FILE *f = fopen("/proc/partitions", "r");
	char buf[1024];
	mddev_dev_t rv = NULL;
	if (f == NULL) {
		fprintf(stderr, Name ": cannot open /proc/partitions\n");
		return NULL;
	}
	while (fgets(buf, 1024, f)) {
		int major, minor;
		char *name, *mp;
		mddev_dev_t d;

		buf[1023] = '\0';
		if (buf[0] != ' ')
			continue;
		major = strtoul(buf, &mp, 10);
		if (mp == buf || *mp != ' ') 
			continue;
		minor = strtoul(mp, NULL, 10);

		name = map_dev(major, minor);
		if (!name) {
			snprintf(buf, 1024, "%d:%d", major, minor);
			name = buf;
		}

		d = malloc(sizeof(*d));
		d->devname = strdup(name);
		d->next = rv;
		rv = d;
	}
	fclose(f);
	return rv;
}


void devline(char *line) 
{
	char *w;
	struct conf_dev *cd;

	for (w=dl_next(line); w != line; w=dl_next(w)) {
		if (w[0] == '/' || strcasecmp(w, "partitions") == 0) {
			cd = malloc(sizeof(*cd));
			cd->name = strdup(w);
			cd->next = cdevlist;
			cdevlist = cd;
		} else {
			fprintf(stderr, Name ": unreconised word on DEVICE line: %s\n",
				w);
		}
	}
}

mddev_ident_t mddevlist = NULL;
mddev_ident_t *mddevlp = &mddevlist;

void arrayline(char *line)
{
	char *w;

	struct mddev_ident_s mis;
	mddev_ident_t mi;

	mis.uuid_set = 0;
	mis.super_minor = UnSet;
	mis.level = UnSet;
	mis.raid_disks = UnSet;
	mis.spare_disks = UnSet;
	mis.devices = NULL;
	mis.devname = NULL;
	mis.spare_group = NULL;
	mis.autof = 0;
	mis.next = NULL;
	mis.st = NULL;
	mis.bitmap_fd = -1;
	mis.name[0] = 0;

	for (w=dl_next(line); w!=line; w=dl_next(w)) {
		if (w[0] == '/') {
			if (mis.devname)
				fprintf(stderr, Name ": only give one device per ARRAY line: %s and %s\n",
					mis.devname, w);
			else mis.devname = w;
		} else if (strncasecmp(w, "uuid=", 5)==0 ) {
			if (mis.uuid_set)
				fprintf(stderr, Name ": only specify uuid once, %s ignored.\n",
					w);
			else {
				if (parse_uuid(w+5, mis.uuid))
					mis.uuid_set = 1;
				else
					fprintf(stderr, Name ": bad uuid: %s\n", w);
			}
		} else if (strncasecmp(w, "super-minor=", 12)==0 ) {
			if (mis.super_minor != UnSet)
				fprintf(stderr, Name ": only specify super-minor once, %s ignored.\n",
					w);
			else {
				char *endptr;
				mis.super_minor= strtol(w+12, &endptr, 10);
				if (w[12]==0 || endptr[0]!=0 || mis.super_minor < 0) {
					fprintf(stderr, Name ": invalid super-minor number: %s\n",
						w);
					mis.super_minor = UnSet;
				}
			}
		} else if (strncasecmp(w, "name=", 5)==0) {
			if (mis.name[0])
				fprintf(stderr, Name ": only specify name once, %s ignored.\n",
					w);
			else if (strlen(w+5) > 32)
				fprintf(stderr, Name ": name too long, ignoring %s\n", w);
			else
				strcpy(mis.name, w+5);

		} else if (strncasecmp(w, "devices=", 8 ) == 0 ) {
			if (mis.devices)
				fprintf(stderr, Name ": only specify devices once (use a comma separated list). %s ignored\n",
					w);
			else
				mis.devices = strdup(w+8);
		} else if (strncasecmp(w, "spare-group=", 12) == 0 ) {
			if (mis.spare_group)
				fprintf(stderr, Name ": only specify one spare group per array. %s ignored.\n",
					w);
			else
				mis.spare_group = strdup(w+12);
		} else if (strncasecmp(w, "level=", 6) == 0 ) {
			/* this is mainly for compatability with --brief output */
			mis.level = map_name(pers, w+6);
		} else if (strncasecmp(w, "disks=", 6) == 0 ) {
			/* again, for compat */
			mis.raid_disks = atoi(w+6);
		} else if (strncasecmp(w, "num-devices=", 12) == 0 ) {
			/* again, for compat */
			mis.raid_disks = atoi(w+12);
		} else if (strncasecmp(w, "spares=", 7) == 0 ) {
			/* for warning if not all spares present */
			mis.spare_disks = atoi(w+7);
		} else if (strncasecmp(w, "metadata=", 9) == 0) {
			/* style of metadata on the devices. */
			int i;
			
			for(i=0; superlist[i] && !mis.st; i++)
				mis.st = superlist[i]->match_metadata_desc(w+9);

			if (!mis.st)
				fprintf(stderr, Name ": metadata format %s unknown, ignored.\n", w+9);
		} else if (strncasecmp(w, "auto=", 5) == 0 ) {
			/* whether to create device special files as needed */
			if (strcasecmp(w+5, "no")==0)
				mis.autof = 0;
			else if (strcasecmp(w+5,"yes")==0 || strcasecmp(w+5,"md")==0)
				mis.autof = -1;
			else {
				/* There might be digits, and maybe a hyphen, at the end */
				char *e = w+5 + strlen(w+5);
				int num = 4;
				int len;
				while (e > w+5 && isdigit(e[-1]))
					e--;
				if (*e) {
					num = atoi(e);
					if (num <= 0) num = 1;
				}
				if (e > w+5 && e[-1] == '-')
					e--;
				len = e - (w+5);
				if ((len == 3 && strncasecmp(w+5,"mdp",3)==0) ||
				    (len == 1 && strncasecmp(w+5,"p",1)==0) ||
				    (len >= 4 && strncasecmp(w+5,"part",4)==0))
					mis.autof = num;
				else 
					fprintf(stderr, Name ": auto type of \"%s\" ignored for %s\n",
						w+5, mis.devname?mis.devname:"unlabeled-array");
			}
		} else {
			fprintf(stderr, Name ": unrecognised word on ARRAY line: %s\n",
				w);
		}
	}
	if (mis.devname == NULL)
		fprintf(stderr, Name ": ARRAY line with no device\n");
	else if (mis.uuid_set == 0 && mis.devices == NULL && mis.super_minor == UnSet && mis.name[0] == 0)
		fprintf(stderr, Name ": ARRAY line %s has no identity information.\n", mis.devname);
	else {
		mi = malloc(sizeof(*mi));
		*mi = mis;
		mi->devname = strdup(mis.devname);
		mi->next = NULL;
		*mddevlp = mi;
		mddevlp = &mi->next;
	}
}

static char *alert_email = NULL;
void mailline(char *line)
{
	char *w;

	for (w=dl_next(line); w != line ; w=dl_next(w)) {
		if (alert_email == NULL)
			alert_email = strdup(w);
		else
			fprintf(stderr, Name ": excess address on MAIL line: %s - ignored\n",
				w);
	}
}


static char *alert_program = NULL;
void programline(char *line)
{
	char *w;

	for (w=dl_next(line); w != line ; w=dl_next(w)) {
		if (alert_program == NULL)
			alert_program = strdup(w);
		else
			fprintf(stderr, Name ": excess program on PROGRAM line: %s - ignored\n",
				w);
	}
}


int loaded = 0;

void load_conffile(char *conffile)
{
	FILE *f;
	char *line;

	if (loaded) return;
	if (conffile == NULL)
		conffile = DefaultConfFile;

	if (strcmp(conffile, "none") == 0) {
		loaded = 1;
		return;
	}
	if (strcmp(conffile, "partitions")==0) {
		char *list = dl_strdup("DEV");
		dl_init(list);
		dl_add(list, dl_strdup("partitions"));
		devline(list);
		free_line(list);
		loaded = 1;
		return;
	}
	f = fopen(conffile, "r");
	/* Debian chose to relocate mdadm.conf into /etc/mdadm/.
	 * To allow Debian users to compile from clean source and still
	 * have a working mdadm, we read /etc/mdadm/mdadm.conf
	 * if /etc/mdadm.conf doesn't exist
	 */
	if (f == NULL &&
	    conffile == DefaultConfFile) {
		f = fopen(DefaultAltConfFile, "r");
		if (f)
			conffile = DefaultAltConfFile;
	}
	if (f == NULL)
		return;

	loaded = 1;
	while ((line=conf_line(f))) {
		switch(match_keyword(line)) {
		case 0: /* DEVICE */
			devline(line);
			break;
		case 1: /* ARRAY */
			arrayline(line);
			break;
		case 2: /* MAIL */
			mailline(line);
			break;
		case 3: /* PROGRAM */
			programline(line);
			break;
		default:
			fprintf(stderr, Name ": Unknown keyword %s\n", line);
		}
		free_line(line);
	}
    
	fclose(f);

/*    printf("got file\n"); */
}

char *conf_get_mailaddr(char *conffile)
{
	load_conffile(conffile);
	return alert_email;
}

char *conf_get_program(char *conffile)
{
	load_conffile(conffile);
	return alert_program;
}


mddev_ident_t conf_get_ident(char *conffile, char *dev)
{
	mddev_ident_t rv;
	load_conffile(conffile);
	rv = mddevlist;
	while (dev && rv && strcmp(dev, rv->devname)!=0)
		rv = rv->next;
	return rv;
}

mddev_dev_t conf_get_devs(char *conffile)
{
	glob_t globbuf;
	struct conf_dev *cd;
	int flags = 0;
	static mddev_dev_t dlist = NULL;
	unsigned int i;

	while (dlist) {
		mddev_dev_t t = dlist;
		dlist = dlist->next;
		free(t->devname);
		free(t);
	}
    
	load_conffile(conffile);
    
	for (cd=cdevlist; cd; cd=cd->next) {
		if (strcasecmp(cd->name, "partitions")==0 && dlist == NULL)
			dlist = load_partitions();
		else {
			glob(cd->name, flags, NULL, &globbuf);
			flags |= GLOB_APPEND;
		}
	}
	if (flags & GLOB_APPEND) {
		for (i=0; i<globbuf.gl_pathc; i++) {
			mddev_dev_t t = malloc(sizeof(*t));
			t->devname = strdup(globbuf.gl_pathv[i]);
			t->next = dlist;
			dlist = t;
/*	printf("one dev is %s\n", t->devname);*/
		}
		globfree(&globbuf);
	}

	return dlist;
}

int match_oneof(char *devices, char *devname)
{
    /* check if one of the comma separated patterns in devices
     * matches devname
     */


    while (devices && *devices) {
	char patn[1024];
	char *p = devices;
	devices = strchr(devices, ',');
	if (!devices)
	    devices = p + strlen(p);
	if (devices-p < 1024) {
		strncpy(patn, p, devices-p);
		patn[devices-p] = 0;
		if (fnmatch(patn, devname, FNM_PATHNAME)==0)
			return 1;
	}
	if (*devices == ',')
		devices++;
    }
    return 0;
}
