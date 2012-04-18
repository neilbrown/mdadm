/*
 * mdadm - manage Linux "md" devices aka RAID arrays.
 *
 * Copyright (C) 2011  Neil Brown <neilb@suse.de>
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
#include	<ctype.h>

/* This fill contains various 'library' style function.  They
 * have no dependency on anything outside this file.
 */

int get_mdp_major(void)
{
static int mdp_major = -1;
	FILE *fl;
	char *w;
	int have_block = 0;
	int have_devices = 0;
	int last_num = -1;

	if (mdp_major != -1)
		return mdp_major;
	fl = fopen("/proc/devices", "r");
	if (!fl)
		return -1;
	while ((w = conf_word(fl, 1))) {
		if (have_block && strcmp(w, "devices:")==0)
			have_devices = 1;
		have_block =  (strcmp(w, "Block")==0);
		if (isdigit(w[0]))
			last_num = atoi(w);
		if (have_devices && strcmp(w, "mdp")==0)
			mdp_major = last_num;
		free(w);
	}
	fclose(fl);
	return mdp_major;
}


void fmt_devname(char *name, int num)
{
	if (num >= 0)
		sprintf(name, "md%d", num);
	else
		sprintf(name, "md_d%d", -1-num);
}

char *devnum2devname(int num)
{
	char name[100];
	fmt_devname(name,num);
	return strdup(name);
}

int devname2devnum(char *name)
{
	char *ep;
	int num;
	if (strncmp(name, "md_d", 4)==0)
		num = -1-strtoul(name+4, &ep, 10);
	else
		num = strtoul(name+2, &ep, 10);
	return num;
}

int stat2devnum(struct stat *st)
{
	char path[30];
	char link[200];
	char *cp;
	int n;

	if ((S_IFMT & st->st_mode) == S_IFBLK) {
		if (major(st->st_rdev) == MD_MAJOR)
			return minor(st->st_rdev);
		else if (major(st->st_rdev) == (unsigned)get_mdp_major())
			return -1- (minor(st->st_rdev)>>MdpMinorShift);

		/* must be an extended-minor partition. Look at the
		 * /sys/dev/block/%d:%d link which must look like
		 * ../../block/mdXXX/mdXXXpYY
		 */
		sprintf(path, "/sys/dev/block/%d:%d", major(st->st_rdev),
			minor(st->st_rdev));
		n = readlink(path, link, sizeof(link)-1);
		if (n <= 0)
			return NoMdDev;
		link[n] = 0;
		cp = strrchr(link, '/');
		if (cp) *cp = 0;
		cp = strrchr(link, '/');
		if (cp && strncmp(cp, "/md", 3) == 0)
			return devname2devnum(cp+1);
	}
	return NoMdDev;

}

int fd2devnum(int fd)
{
	struct stat stb;
	if (fstat(fd, &stb) == 0)
		return stat2devnum(&stb);
	return NoMdDev;
}



/*
 * convert a major/minor pair for a block device into a name in /dev, if possible.
 * On the first call, walk /dev collecting name.
 * Put them in a simple linked listfor now.
 */
struct devmap {
    int major, minor;
    char *name;
    struct devmap *next;
} *devlist = NULL;
int devlist_ready = 0;

int add_dev(const char *name, const struct stat *stb, int flag, struct FTW *s)
{
	struct stat st;

	if (S_ISLNK(stb->st_mode)) {
		if (stat(name, &st) != 0)
			return 0;
		stb = &st;
	}

	if ((stb->st_mode&S_IFMT)== S_IFBLK) {
		char *n = strdup(name);
		struct devmap *dm = malloc(sizeof(*dm));
		if (strncmp(n, "/dev/./", 7)==0)
			strcpy(n+4, name+6);
		if (dm) {
			dm->major = major(stb->st_rdev);
			dm->minor = minor(stb->st_rdev);
			dm->name = n;
			dm->next = devlist;
			devlist = dm;
		}
	}
	return 0;
}

#ifndef HAVE_NFTW
#ifdef HAVE_FTW
int add_dev_1(const char *name, const struct stat *stb, int flag)
{
	return add_dev(name, stb, flag, NULL);
}
int nftw(const char *path, int (*han)(const char *name, const struct stat *stb, int flag, struct FTW *s), int nopenfd, int flags)
{
	return ftw(path, add_dev_1, nopenfd);
}
#else
int nftw(const char *path, int (*han)(const char *name, const struct stat *stb, int flag, struct FTW *s), int nopenfd, int flags)
{
	return 0;
}
#endif /* HAVE_FTW */
#endif /* HAVE_NFTW */

/*
 * Find a block device with the right major/minor number.
 * If we find multiple names, choose the shortest.
 * If we find a name in /dev/md/, we prefer that.
 * This applies only to names for MD devices.
 * If 'prefer' is set (normally to e.g. /by-path/)
 * then we prefer a name which contains that string.
 */
char *map_dev_preferred(int major, int minor, int create,
			char *prefer)
{
	struct devmap *p;
	char *regular = NULL, *preferred=NULL;
	int did_check = 0;

	if (major == 0 && minor == 0)
			return NULL;

 retry:
	if (!devlist_ready) {
		char *dev = "/dev";
		struct stat stb;
		while(devlist) {
			struct devmap *d = devlist;
			devlist = d->next;
			free(d->name);
			free(d);
		}
		if (lstat(dev, &stb)==0 &&
		    S_ISLNK(stb.st_mode))
			dev = "/dev/.";
		nftw(dev, add_dev, 10, FTW_PHYS);
		devlist_ready=1;
		did_check = 1;
	}

	for (p=devlist; p; p=p->next)
		if (p->major == major &&
		    p->minor == minor) {
			if (strncmp(p->name, "/dev/md/",8) == 0
			    || (prefer && strstr(p->name, prefer))) {
				if (preferred == NULL ||
				    strlen(p->name) < strlen(preferred))
					preferred = p->name;
			} else {
				if (regular == NULL ||
				    strlen(p->name) < strlen(regular))
					regular = p->name;
			}
		}
	if (!regular && !preferred && !did_check) {
		devlist_ready = 0;
		goto retry;
	}
	if (create && !regular && !preferred) {
		static char buf[30];
		snprintf(buf, sizeof(buf), "%d:%d", major, minor);
		regular = buf;
	}

	return preferred ? preferred : regular;
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
				/* Hack for broken kernels (2.6.14-.24) that put
				 *        "active(auto-read-only)"
				 * in /proc/mdstat instead of
				 *        "active (auto-read-only)"
				 */
				if (c == '(' && len >= 6
				    && strncmp(word+len-6, "active", 6) == 0)
					c = ' ';
			}
		}
		if (c != EOF) ungetc(c, file);
	}
	word[len] = 0;

	/* Further HACK for broken kernels.. 2.6.14-2.6.24 */
	if (strcmp(word, "auto-read-only)") == 0)
		strcpy(word, "(auto-read-only)");

/*    printf("word is <%s>\n", word); */
	if (!wordfound) {
		free(word);
		word = NULL;
	}
	return word;
}
