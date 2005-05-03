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

#if ! defined(__BIG_ENDIAN) && ! defined(__LITTLE_ENDIAN)
#error no endian defined
#endif
#include	"md_u.h"
#include	"md_p.h"
int Examine(mddev_dev_t devlist, int brief, int scan, int SparcAdjust)
{

	/* Read the raid superblock from a device and
	 * display important content.
	 *
	 * If cannot be found, print reason: too small, bad magic
	 *
	 * Print:
	 *   version, ctime, level, size, raid+spare+
	 *   prefered minor
	 *   uuid
	 *
	 *   utime, state etc
	 *
	 * If (brief) gather devices for same array and just print a mdadm.conf line including devices=
	 * if devlist==NULL, use conf_get_devs(
	 */
	int fd; 
	void *super = NULL;
	int rv = 0;
	int err;

	struct array {
		void *super;
		struct mdinfo info;
		void *devs;
		struct array *next;
		int spares;
	} *arrays = NULL;

	for (; devlist ; devlist=devlist->next) {
		fd = open(devlist->devname, O_RDONLY);
		if (fd < 0) {
			if (!scan)
				fprintf(stderr,Name ": cannot open %s: %s\n",
					devlist->devname, strerror(errno));
			err = 1;
		}
		else {
			err = load_super0(fd, &super, (brief||scan)?NULL:devlist->devname);
			close(fd);
		}
		if (err)
			continue;
		if (err) rv =1;

		if (SparcAdjust)
			update_super0(NULL, super, "sparc2.2", devlist->devname, 0);
		/* Ok, its good enough to try, though the checksum could be wrong */
		if (brief) {
			struct array *ap;
			char *d;
			for (ap=arrays; ap; ap=ap->next) {
				if (compare_super0(&ap->super, super)==0)
					break;
			}
			if (!ap) {
				ap = malloc(sizeof(*ap));
				ap->super = super;
				ap->devs = dl_head();
				ap->next = arrays;
				ap->spares = 0;
				arrays = ap;
				getinfo_super0(&ap->info, super);
			} else {
				getinfo_super0(&ap->info, super);
				free(super);
			}
			if (!(ap->info.disk.state & MD_DISK_SYNC))
				ap->spares++;
			d = dl_strdup(devlist->devname);
			dl_add(ap->devs, d);
		} else {
			printf("%s:\n",devlist->devname);
			examine_super0(super);
			free(super);
		}
	}
	if (brief) {
		struct array *ap;
		for (ap=arrays; ap; ap=ap->next) {
			char sep='=';
			char *d;
			brief_examine_super0(ap->super);
			if (ap->spares) printf("   spares=%d", ap->spares);
			printf("   devices");
			for (d=dl_next(ap->devs); d!= ap->devs; d=dl_next(d)) {
				printf("%c%s", sep, d);
				sep=',';
			}
			free(ap->super);
			/* FIXME free ap */
			printf("\n");
		}
	}
	return rv;
}
