/*
 * mdadm - manage Linux "md" devices aka RAID arrays.
 *
 * Copyright (C) 2001-2009 Neil Brown <neilb@suse.de>
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
 *    Email: <neilb@suse.de>
 */

#include	"mdadm.h"
#include	"dlink.h"
#include	<dirent.h>
#include	<glob.h>
#include	<fnmatch.h>
#include	<ctype.h>
#include	<pwd.h>
#include	<grp.h>

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
 * Keywords are DEVICE and ARRAY ... and several others.
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

enum linetype { Devices, Array, Mailaddr, Mailfrom, Program, CreateDev,
		Homehost, AutoMode, Policy, PartPolicy, LTEnd };
char *keywords[] = {
	[Devices]  = "devices",
	[Array]    = "array",
	[Mailaddr] = "mailaddr",
	[Mailfrom] = "mailfrom",
	[Program]  = "program",
	[CreateDev]= "create",
	[Homehost] = "homehost",
	[AutoMode] = "auto",
	[Policy]   = "policy",
	[PartPolicy]="part-policy",
	[LTEnd]    = NULL
};

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

struct mddev_dev *load_partitions(void)
{
	FILE *f = fopen("/proc/partitions", "r");
	char buf[1024];
	struct mddev_dev *rv = NULL;
	if (f == NULL) {
		fprintf(stderr, Name ": cannot open /proc/partitions\n");
		return NULL;
	}
	while (fgets(buf, 1024, f)) {
		int major, minor;
		char *name, *mp;
		struct mddev_dev *d;

		buf[1023] = '\0';
		if (buf[0] != ' ')
			continue;
		major = strtoul(buf, &mp, 10);
		if (mp == buf || *mp != ' ')
			continue;
		minor = strtoul(mp, NULL, 10);

		name = map_dev(major, minor, 1);
		if (!name)
			continue;
		d = malloc(sizeof(*d));
		d->devname = strdup(name);
		d->next = rv;
		d->used = 0;
		rv = d;
	}
	fclose(f);
	return rv;
}

struct mddev_dev *load_containers(void)
{
	struct mdstat_ent *mdstat = mdstat_read(1, 0);
	struct mdstat_ent *ent;
	struct mddev_dev *d;
	struct mddev_dev *rv = NULL;

	if (!mdstat)
		return NULL;

	for (ent = mdstat; ent; ent = ent->next)
		if (ent->metadata_version &&
		    strncmp(ent->metadata_version, "external:", 9) == 0 &&
		    !is_subarray(&ent->metadata_version[9])) {
			d = malloc(sizeof(*d));
			if (!d)
				continue;
			if (asprintf(&d->devname, "/dev/%s", ent->dev) < 0) {
				free(d);
				continue;
			}
			d->next = rv;
			d->used = 0;
			rv = d;
		}
	free_mdstat(mdstat);

	return rv;
}

struct createinfo createinfo = {
	.autof = 2, /* by default, create devices with standard names */
	.symlinks = 1,
#ifdef DEBIAN
	.gid = 6, /* disk */
	.mode = 0660,
#else
	.mode = 0600,
#endif
};

int parse_auto(char *str, char *msg, int config)
{
	int autof;
	if (str == NULL || *str == 0)
		autof = 2;
	else if (strcasecmp(str,"no")==0)
		autof = 1;
	else if (strcasecmp(str,"yes")==0)
		autof = 2;
	else if (strcasecmp(str,"md")==0)
		autof = config?5:3;
	else {
		/* There might be digits, and maybe a hypen, at the end */
		char *e = str + strlen(str);
		int num = 4;
		int len;
		while (e > str && isdigit(e[-1]))
			e--;
		if (*e) {
			num = atoi(e);
			if (num <= 0) num = 1;
		}
		if (e > str && e[-1] == '-')
			e--;
		len = e - str;
		if ((len == 2 && strncasecmp(str,"md",2)==0)) {
			autof = config ? 5 : 3;
		} else if ((len == 3 && strncasecmp(str,"yes",3)==0)) {
			autof = 2;
		} else if ((len == 3 && strncasecmp(str,"mdp",3)==0)) {
			autof = config ? 6 : 4;
		} else if ((len == 1 && strncasecmp(str,"p",1)==0) ||
			   (len >= 4 && strncasecmp(str,"part",4)==0)) {
			autof = 6;
		} else {
			fprintf(stderr, Name ": %s arg of \"%s\" unrecognised: use no,yes,md,mdp,part\n"
				"        optionally followed by a number.\n",
				msg, str);
			exit(2);
		}
		autof |= num << 3;
	}
	return autof;
}

static void createline(char *line)
{
	char *w;
	char *ep;

	for (w=dl_next(line); w!=line; w=dl_next(w)) {
		if (strncasecmp(w, "auto=", 5) == 0)
			createinfo.autof = parse_auto(w+5, "auto=", 1);
		else if (strncasecmp(w, "owner=", 6) == 0) {
			if (w[6] == 0) {
				fprintf(stderr, Name ": missing owner name\n");
				continue;
			}
			createinfo.uid = strtoul(w+6, &ep, 10);
			if (*ep != 0) {
				struct passwd *pw;
				/* must be a name */
				pw = getpwnam(w+6);
				if (pw)
					createinfo.uid = pw->pw_uid;
				else
					fprintf(stderr, Name ": CREATE user %s not found\n", w+6);
			}
		} else if (strncasecmp(w, "group=", 6) == 0) {
			if (w[6] == 0) {
				fprintf(stderr, Name ": missing group name\n");
				continue;
			}
			createinfo.gid = strtoul(w+6, &ep, 10);
			if (*ep != 0) {
				struct group *gr;
				/* must be a name */
				gr = getgrnam(w+6);
				if (gr)
					createinfo.gid = gr->gr_gid;
				else
					fprintf(stderr, Name ": CREATE group %s not found\n", w+6);
			}
		} else if (strncasecmp(w, "mode=", 5) == 0) {
			if (w[5] == 0) {
				fprintf(stderr, Name ": missing CREATE mode\n");
				continue;
			}
			createinfo.mode = strtoul(w+5, &ep, 8);
			if (*ep != 0) {
				createinfo.mode = 0600;
				fprintf(stderr, Name ": unrecognised CREATE mode %s\n",
					w+5);
			}
		} else if (strncasecmp(w, "metadata=", 9) == 0) {
			/* style of metadata to use by default */
			int i;
			for (i=0; superlist[i] && !createinfo.supertype; i++)
				createinfo.supertype =
					superlist[i]->match_metadata_desc(w+9);
			if (!createinfo.supertype)
				fprintf(stderr, Name ": metadata format %s unknown, ignoring\n",
					w+9);
		} else if (strncasecmp(w, "symlinks=yes", 12) == 0)
			createinfo.symlinks = 1;
		else if  (strncasecmp(w, "symlinks=no", 11) == 0)
			createinfo.symlinks = 0;
		else {
			fprintf(stderr, Name ": unrecognised word on CREATE line: %s\n",
				w);
		}
	}
}

void devline(char *line)
{
	char *w;
	struct conf_dev *cd;

	for (w=dl_next(line); w != line; w=dl_next(w)) {
		if (w[0] == '/' || strcasecmp(w, "partitions") == 0 ||
		    strcasecmp(w, "containers") == 0) {
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

struct mddev_ident *mddevlist = NULL;
struct mddev_ident **mddevlp = &mddevlist;

static int is_number(char *w)
{
	/* check if there are 1 or more digits and nothing else */
	int digits = 0;
	while (*w && isdigit(*w)) {
		digits++;
		w++;
	}
	return (digits && ! *w);
}

void arrayline(char *line)
{
	char *w;

	struct mddev_ident mis;
	struct mddev_ident *mi;

	mis.uuid_set = 0;
	mis.super_minor = UnSet;
	mis.level = UnSet;
	mis.raid_disks = UnSet;
	mis.spare_disks = 0;
	mis.devices = NULL;
	mis.devname = NULL;
	mis.spare_group = NULL;
	mis.autof = 0;
	mis.next = NULL;
	mis.st = NULL;
	mis.bitmap_fd = -1;
	mis.bitmap_file = NULL;
	mis.name[0] = 0;
	mis.container = NULL;
	mis.member = NULL;

	for (w=dl_next(line); w!=line; w=dl_next(w)) {
		if (w[0] == '/' || strchr(w, '=') == NULL) {
			/* This names the device, or is '<ignore>'.
			 * The rules match those in create_mddev.
			 * 'w' must be:
			 *  /dev/md/{anything}
			 *  /dev/mdNN
			 *  /dev/md_dNN
			 *  <ignore>
			 *  or anything that doesn't start '/' or '<'
			 */
			if (strcasecmp(w, "<ignore>") == 0 ||
			    strncmp(w, "/dev/md/", 8) == 0 ||
			    (w[0] != '/' && w[0] != '<') ||
			    (strncmp(w, "/dev/md", 7) == 0 && 
			     is_number(w+7)) ||
			    (strncmp(w, "/dev/md_d", 9) == 0 &&
			     is_number(w+9))
				) {
				/* This is acceptable */;
				if (mis.devname)
					fprintf(stderr, Name ": only give one "
						"device per ARRAY line: %s and %s\n",
						mis.devname, w);
				else
					mis.devname = w;
			}else {
				fprintf(stderr, Name ": %s is an invalid name for "
					"an md device - ignored.\n", w);
			}
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
				int minor = strtol(w+12, &endptr, 10);

				if (w[12]==0 || endptr[0]!=0 || minor < 0)
					fprintf(stderr, Name ": invalid super-minor number: %s\n",
						w);
				else
					mis.super_minor = minor;
			}
		} else if (strncasecmp(w, "name=", 5)==0) {
			if (mis.name[0])
				fprintf(stderr, Name ": only specify name once, %s ignored.\n",
					w);
			else if (strlen(w+5) > 32)
				fprintf(stderr, Name ": name too long, ignoring %s\n", w);
			else
				strcpy(mis.name, w+5);

		} else if (strncasecmp(w, "bitmap=", 7) == 0) {
			if (mis.bitmap_file)
				fprintf(stderr, Name ": only specify bitmap file once. %s ignored\n",
					w);
			else
				mis.bitmap_file = strdup(w+7);

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
			mis.autof = parse_auto(w+5, "auto type", 0);
		} else if (strncasecmp(w, "member=", 7) == 0) {
			/* subarray within a container */
			mis.member = strdup(w+7);
		} else if (strncasecmp(w, "container=", 10) == 0) {
			/* the container holding this subarray.  Either a device name
			 * or a uuid */
			mis.container = strdup(w+10);
		} else {
			fprintf(stderr, Name ": unrecognised word on ARRAY line: %s\n",
				w);
		}
	}
	if (mis.uuid_set == 0 && mis.devices == NULL &&
	    mis.super_minor == UnSet && mis.name[0] == 0 &&
	    (mis.container == NULL || mis.member == NULL))
		fprintf(stderr, Name ": ARRAY line %s has no identity information.\n", mis.devname);
	else {
		mi = malloc(sizeof(*mi));
		*mi = mis;
		mi->devname = mis.devname ? strdup(mis.devname) : NULL;
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

static char *alert_mail_from = NULL;
void mailfromline(char *line)
{
	char *w;

	for (w=dl_next(line); w != line ; w=dl_next(w)) {
		if (alert_mail_from == NULL)
			alert_mail_from = strdup(w);
		else {
			char *t = NULL;

			if (xasprintf(&t, "%s %s", alert_mail_from, w) > 0) {
				free(alert_mail_from);
				alert_mail_from = t;
			}
		}
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

static char *home_host = NULL;
static int require_homehost = 1;
void homehostline(char *line)
{
	char *w;

	for (w=dl_next(line); w != line ; w=dl_next(w)) {
		if (strcasecmp(w, "<ignore>")==0)
			require_homehost = 0;
		else if (home_host == NULL) {
			if (strcasecmp(w, "<none>")==0)
				home_host = strdup("");
			else
				home_host = strdup(w);
		}else
			fprintf(stderr, Name ": excess host name on HOMEHOST line: %s - ignored\n",
				w);
	}
}

char auto_yes[] = "yes";
char auto_no[] = "no";
char auto_homehost[] = "homehost";

static int auto_seen = 0;
void autoline(char *line)
{
	char *w;
	char *seen;
	int super_cnt;
	char *dflt = auto_yes;
	int homehost = 0;
	int i;

	if (auto_seen) {
		fprintf(stderr, Name ": AUTO line may only be give once."
			"  Subsequent lines ignored\n");
		return;
	}
	/* Parse the 'auto' line creating policy statements for the 'auto' policy.
	 *
	 * The default is 'yes' but the 'auto' line might over-ride that.
	 * Words in the line are processed in order with the first
	 * match winning.
	 * word can be:
	 *   +version   - that version can be assembled
	 *   -version   - that version cannot be auto-assembled
	 *   yes or +all - any other version can be assembled
	 *   no or -all  - no other version can be assembled.
	 *   homehost   - any array associated by 'homehost' to this
	 *                host can be assembled.
	 *
	 * Thus:
	 *   +ddf -0.90 homehost -all
	 * will auto-assemble any ddf array, no 0.90 array, and
	 * any other array (imsm, 1.x) if and only if it is identified
	 * as belonging to this host.
	 *
	 * We translate that to policy by creating 'auto=yes' when we see
	 * a '+version' line, 'auto=no' if we see '-version' before 'homehost',
	 * or 'auto=homehost' if we see '-version' after 'homehost'.
	 * When we see yes, no, +all or -all we stop and any version that hasn't
	 * been seen gets an appropriate auto= entry.
	 */

	for (super_cnt = 0; superlist[super_cnt]; super_cnt++)
		;
	seen = calloc(super_cnt, 1);

	for (w = dl_next(line); w != line ; w = dl_next(w)) {
		char *val;

		if (strcasecmp(w, "yes") == 0) {
			dflt = auto_yes;
			break;
		}
		if (strcasecmp(w, "no") == 0) {
			if (homehost)
				dflt = auto_homehost;
			else
				dflt = auto_no;
			break;
		}
		if (strcasecmp(w, "homehost") == 0) {
			homehost = 1;
			continue;
		}
		if (w[0] == '+')
			val = auto_yes;
		else if (w[0] == '-') {
			if (homehost)
				val = auto_homehost;
			else
				val = auto_no;
		} else
			continue;

		if (strcasecmp(w+1, "all") == 0) {
			dflt = val;
			break;
		}
		for (i = 0; superlist[i]; i++) {
			const char *version = superlist[i]->name;
			if (strcasecmp(w+1, version) == 0)
				break;
			/* 1 matches 1.x, 0 matches 0.90 */
			if (version[1] == '.' &&
			    strlen(w+1) == 1 &&
			    w[1] == version[0])
				break;
			/* 1.anything matches 1.x */
			if (strcmp(version, "1.x") == 0 &&
			    strncmp(w+1, "1.", 2) == 0)
				break;
		}
		if (superlist[i] == NULL)
			/* ignore this word */
			continue;
		if (seen[i])
			/* already know about this metadata */
			continue;
		policy_add(rule_policy, pol_auto, val, pol_metadata, superlist[i]->name, NULL);
		seen[i] = 1;
	}
	for (i = 0; i < super_cnt; i++)
		if (!seen[i])
			policy_add(rule_policy, pol_auto, dflt, pol_metadata, superlist[i]->name, NULL);

	free(seen);
}

int loaded = 0;

static char *conffile = NULL;
void set_conffile(char *file)
{
	conffile = file;
}

void load_conffile(void)
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
		case Devices:
			devline(line);
			break;
		case Array:
			arrayline(line);
			break;
		case Mailaddr:
			mailline(line);
			break;
		case Mailfrom:
			mailfromline(line);
			break;
		case Program:
			programline(line);
			break;
		case CreateDev:
			createline(line);
			break;
		case Homehost:
			homehostline(line);
			break;
		case AutoMode:
			autoline(line);
			break;
		case Policy:
			policyline(line, rule_policy);
			break;
		case PartPolicy:
			policyline(line, rule_part);
			break;
		default:
			fprintf(stderr, Name ": Unknown keyword %s\n", line);
		}
		free_line(line);
	}

	fclose(f);

/*    printf("got file\n"); */
}

char *conf_get_mailaddr(void)
{
	load_conffile();
	return alert_email;
}

char *conf_get_mailfrom(void)
{
	load_conffile();
	return alert_mail_from;
}

char *conf_get_program(void)
{
	load_conffile();
	return alert_program;
}

char *conf_get_homehost(int *require_homehostp)
{
	load_conffile();
	if (require_homehostp)
		*require_homehostp = require_homehost;
	return home_host;
}

struct createinfo *conf_get_create_info(void)
{
	load_conffile();
	return &createinfo;
}

struct mddev_ident *conf_get_ident(char *dev)
{
	struct mddev_ident *rv;
	load_conffile();
	rv = mddevlist;
	while (dev && rv && (rv->devname == NULL
			     || !devname_matches(dev, rv->devname)))
		rv = rv->next;
	return rv;
}

static void append_dlist(struct mddev_dev **dlp, struct mddev_dev *list)
{
	while (*dlp)
		dlp = &(*dlp)->next;
	*dlp = list;
}

struct mddev_dev *conf_get_devs()
{
	glob_t globbuf;
	struct conf_dev *cd;
	int flags = 0;
	static struct mddev_dev *dlist = NULL;
	unsigned int i;

	while (dlist) {
		struct mddev_dev *t = dlist;
		dlist = dlist->next;
		free(t->devname);
		free(t);
	}

	load_conffile();

	if (cdevlist == NULL) {
		/* default to 'partitions' and 'containers' */
		dlist = load_partitions();
		append_dlist(&dlist, load_containers());
	}

	for (cd=cdevlist; cd; cd=cd->next) {
		if (strcasecmp(cd->name, "partitions")==0)
			append_dlist(&dlist, load_partitions());
		else if (strcasecmp(cd->name, "containers")==0)
			append_dlist(&dlist, load_containers());
		else {
			glob(cd->name, flags, NULL, &globbuf);
			flags |= GLOB_APPEND;
		}
	}
	if (flags & GLOB_APPEND) {
		for (i=0; i<globbuf.gl_pathc; i++) {
			struct mddev_dev *t = malloc(sizeof(*t));
			t->devname = strdup(globbuf.gl_pathv[i]);
			t->next = dlist;
			t->used = 0;
			dlist = t;
/*	printf("one dev is %s\n", t->devname);*/
		}
		globfree(&globbuf);
	}

	return dlist;
}

int conf_test_dev(char *devname)
{
	struct conf_dev *cd;
	if (cdevlist == NULL)
		/* allow anything by default */
		return 1;
	for (cd = cdevlist ; cd ; cd = cd->next) {
		if (strcasecmp(cd->name, "partitions") == 0)
			return 1;
		if (fnmatch(cd->name, devname, FNM_PATHNAME) == 0)
			return 1;
	}
	return 0;
}

int conf_test_metadata(const char *version, struct dev_policy *pol, int is_homehost)
{
	/* If anyone said 'yes', that sticks.
	 * else if homehost applies, use that
	 * else if there is a 'no', say 'no'.
	 * else 'yes'.
	 */
	struct dev_policy *p;
	int no=0, found_homehost=0;
	load_conffile();

	pol = pol_find(pol, pol_auto);
	pol_for_each(p, pol, version) {
		if (strcmp(p->value, "yes") == 0)
			return 1;
		if (strcmp(p->value, "homehost") == 0)
			found_homehost = 1;
		if (strcmp(p->value, "no") == 0)
			no = 1;
	}
	if (is_homehost && found_homehost)
		return 1;
	if (no)
		return 0;
	return 1;
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

int devname_matches(char *name, char *match)
{
	/* See if the given array name matches the
	 * given match from config file.
	 *
	 * First strip and /dev/md/ or /dev/, then
	 * see if there might be a numeric match of
	 *  mdNN with NN
	 * then just strcmp
	 */
	if (strncmp(name, "/dev/md/", 8) == 0)
		name += 8;
	else if (strncmp(name, "/dev/", 5) == 0)
		name += 5;

	if (strncmp(match, "/dev/md/", 8) == 0)
		match += 8;
	else if (strncmp(match, "/dev/", 5) == 0)
		match += 5;


	if (strncmp(name, "md", 2) == 0 &&
	    isdigit(name[2]))
		name += 2;
	if (strncmp(match, "md", 2) == 0 &&
	    isdigit(match[2]))
		match += 2;

	return (strcmp(name, match) == 0);
}

int conf_name_is_free(char *name)
{
	/* Check if this name is already take by an ARRAY entry in
	 * the config file.
	 * It can be taken either by a match on devname, name, or
	 * even super-minor.
	 */
	struct mddev_ident *dev;

	load_conffile();
	for (dev = mddevlist; dev; dev = dev->next) {
		char nbuf[100];
		if (dev->devname && devname_matches(name, dev->devname))
			return 0;
		if (dev->name[0] && devname_matches(name, dev->name))
			return 0;
		sprintf(nbuf, "%d", dev->super_minor);
		if (dev->super_minor != UnSet &&
		    devname_matches(name, nbuf))
			return 0;
	}
	return 1;
}

struct mddev_ident *conf_match(struct supertype *st,
			       struct mdinfo *info,
			       char *devname,
			       int verbose, int *rvp)
{
	struct mddev_ident *array_list, *match;
	array_list = conf_get_ident(NULL);
	match = NULL;
	for (; array_list; array_list = array_list->next) {
		if (array_list->uuid_set &&
		    same_uuid(array_list->uuid, info->uuid, st->ss->swapuuid)
		    == 0) {
			if (verbose >= 2 && array_list->devname)
				fprintf(stderr, Name
					": UUID differs from %s.\n",
					array_list->devname);
			continue;
		}
		if (array_list->name[0] &&
		    strcasecmp(array_list->name, info->name) != 0) {
			if (verbose >= 2 && array_list->devname)
				fprintf(stderr, Name
					": Name differs from %s.\n",
					array_list->devname);
			continue;
		}
		if (array_list->devices && devname &&
		    !match_oneof(array_list->devices, devname)) {
			if (verbose >= 2 && array_list->devname)
				fprintf(stderr, Name
					": Not a listed device for %s.\n",
					array_list->devname);
			continue;
		}
		if (array_list->super_minor != UnSet &&
		    array_list->super_minor != info->array.md_minor) {
			if (verbose >= 2 && array_list->devname)
				fprintf(stderr, Name
					": Different super-minor to %s.\n",
					array_list->devname);
			continue;
		}
		if (!array_list->uuid_set &&
		    !array_list->name[0] &&
		    !array_list->devices &&
		    array_list->super_minor == UnSet) {
			if (verbose >= 2 && array_list->devname)
				fprintf(stderr, Name
					": %s doesn't have any identifying"
					" information.\n",
					array_list->devname);
			continue;
		}
		/* FIXME, should I check raid_disks and level too?? */

		if (match) {
			if (verbose >= 0) {
				if (match->devname && array_list->devname)
					fprintf(stderr, Name
						": we match both %s and %s - "
						"cannot decide which to use.\n",
						match->devname,
						array_list->devname);
				else
					fprintf(stderr, Name
						": multiple lines in mdadm.conf"
						" match\n");
			}
			if (rvp)
				*rvp = 2;
			match = NULL;
			break;
		}
		match = array_list;
	}
	return match;
}

int conf_verify_devnames(struct mddev_ident *array_list)
{
	struct mddev_ident *a1, *a2;

	for (a1 = array_list; a1; a1 = a1->next) {
		if (!a1->devname)
			continue;
		for (a2 = a1->next; a2; a2 = a2->next) {
			if (!a2->devname)
				continue;
			if (strcmp(a1->devname, a2->devname) != 0)
				continue;

			if (a1->uuid_set && a2->uuid_set) {
				char nbuf[64];
				__fname_from_uuid(a1->uuid, 0, nbuf, ':');
				fprintf(stderr,
					Name ": Devices %s and ",
					nbuf);
				__fname_from_uuid(a2->uuid, 0, nbuf, ':');
				fprintf(stderr,
					"%s have the same name: %s\n",
					nbuf, a1->devname);
			} else
				fprintf(stderr, Name ": Device %s given twice"
					" in config file\n", a1->devname);
			return 1;
		}
	}

	return 0;
}
