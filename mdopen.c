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

#include "mdadm.h"
#include "md_p.h"
#include <ctype.h>

void make_parts(char *dev, int cnt)
{
	/* make 'cnt' partition devices for 'dev'
	 * We use the major/minor from dev and add 1..cnt
	 * If dev ends with a digit, we add "_p%d" else "%d"
	 * If the name exists, we use it's owner/mode,
	 * else that of dev
	 */
	struct stat stb;
	int major, minor;
	int i;
	char *name = malloc(strlen(dev) + 20);
	int dig = isdigit(dev[strlen(dev)-1]);

	if (stat(dev, &stb)!= 0)
		return;
	if (!S_ISBLK(stb.st_mode))
		return;
	major = MAJOR(stb.st_rdev);
	minor = MINOR(stb.st_rdev);
	for (i=1; i <= cnt ; i++) {
		struct stat stb2;
		sprintf(name, "%s%s%d", dev, dig?"_p":"", i);
		if (stat(name, &stb2)==0) {
			if (!S_ISBLK(stb2.st_mode))
				continue;
			if (stb2.st_rdev == MKDEV(major, minor+i))
				continue;
			unlink(name);
		} else {
			stb2 = stb;
		}
		mknod(name, S_IFBLK | 0600, MKDEV(major, minor+i));
		chown(name, stb2.st_uid, stb2.st_gid);
		chmod(name, stb2.st_mode & 07777);
	}
}

/*
 * Open a given md device, and check that it really is one.
 * If 'autof' is given, then we need to create, or recreate, the md device.
 * If the name already exists, and is not a block device, we fail.
 * If it exists and is not an md device, is not the right type (partitioned or not),
 * or is currently in-use, we remove the device, but remember the owner and mode.
 * If it now doesn't exist, we find a few md array and create the device.
 * Default ownership is user=0, group=0 perm=0600
 */
int open_mddev(char *dev, int autof)
{
	int mdfd;
	struct stat stb;
	int major = MD_MAJOR;
	int minor;
	int must_remove = 0;
	struct mdstat_ent *mdlist;
	int num;

	if (autof) {
		/* autof is set, so we need to check that the name is ok,
		 * and possibly create one if not
		 */
		stb.st_mode = 0;
		if (lstat(dev, &stb)==0 && ! S_ISBLK(stb.st_mode)) {
			fprintf(stderr, Name ": %s is not a block device.\n",
				dev);
			return -1;
		}
		/* check major number is correct */
		if (autof>0)
			major = get_mdp_major();
		if (stb.st_mode && MAJOR(stb.st_rdev) != major)
			must_remove = 1;
		if (stb.st_mode && !must_remove) {
			mdu_array_info_t array;
			/* looks ok, see if it is available */
			mdfd = open(dev, O_RDWR, 0);
			if (mdfd < 0) {
				fprintf(stderr, Name ": error opening %s: %s\n",
					dev, strerror(errno));
				return -1;
			} else if (md_get_version(mdfd) <= 0) {
				fprintf(stderr, Name ": %s does not appear to be an md device\n",
					dev);
				close(mdfd);
				return -1;
			}
			if (ioctl(mdfd, GET_ARRAY_INFO, &array)==0) {
				/* already active */
				must_remove = 1;
				close(mdfd);
			} else {
				if (autof > 0)
					make_parts(dev, autof);
				return mdfd;
			}
		}
		/* Ok, need to find a minor that is not in use.
		 * Easiest to read /proc/mdstat, and hunt through for
		 * an unused number 
		 */
		mdlist = mdstat_read(0);
		for (num= (autof>0)?-1:0 ; ; num+= (autof>2)?-1:1) {
			struct mdstat_ent *me;
			for (me=mdlist; me; me=me->next)
				if (me->devnum == num)
					break;
			if (!me) {
				/* doesn't exist if mdstat.
				 * make sure it is new to /dev too
				 */
				char *dn;
				if (autof > 0) 
					minor = (-1-num) << MdpMinorShift;
				else
					minor = num;
				dn = map_dev(major,minor);
				if (dn==NULL || is_standard(dn)) {
					/* this number only used by a 'standard' name,
					 * so it is safe to use
					 */
					break;
				}
			}
		}
		/* 'num' is the number to use, >=0 for md, <0 for mdp */
		if (must_remove) {
			/* never remove a device name that ends /mdNN or /dNN,
			 * that would be confusing 
			 */
			if (is_standard(dev)) {
				fprintf(stderr, Name ": --auto refusing to remove %s as it looks like a standard name.\n",
					dev);
				return -1;
			}
			unlink(dev);
		}

		if (mknod(dev, S_IFBLK|0600, MKDEV(major, minor))!= 0) {
			fprintf(stderr, Name ": failed to create %s\n", dev);
			return -1;
		}
		if (must_remove) {
			chown(dev, stb.st_uid, stb.st_gid);
			chmod(dev, stb.st_mode & 07777);
		}
		make_parts(dev,autof);
	}
	mdfd = open(dev, O_RDWR, 0);
	if (mdfd < 0)
		fprintf(stderr, Name ": error opening %s: %s\n",
			dev, strerror(errno));
	else if (md_get_version(mdfd) <= 0) {
		fprintf(stderr, Name ": %s does not appear to be an md device\n",
			dev);
		close(mdfd);
		mdfd = -1;
	}
	return mdfd;
}

