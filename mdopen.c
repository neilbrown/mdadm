/*
 * mdadm - manage Linux "md" devices aka RAID arrays.
 *
 * Copyright (C) 2001-2006 Neil Brown <neilb@suse.de>
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


void make_dev_symlink(char *dev)
{
	char *new = strdup(dev);

	if (!new) return;
	/* /dev/md/0 -> /dev/md0
	 * /dev/md/d0 -> /dev/md_d0
	 */
	if (isdigit(new[8]))
		strcpy(new+7, new+8);
	else
		new[7] = '_';
	if (symlink(dev+5, new))
		perror(new);
}


void make_parts(char *dev, int cnt, int symlinks)
{
	/* make 'cnt' partition devices for 'dev'
	 * We use the major/minor from dev and add 1..cnt
	 * If dev ends with a digit, we add "p%d" else "%d"
	 * If the name exists, we use it's owner/mode,
	 * else that of dev
	 */
	struct stat stb;
	int major_num, minor_num;
	int i;
	int nlen = strlen(dev) + 20;
	char *name = malloc(nlen);
	int dig = isdigit(dev[strlen(dev)-1]);

	if (cnt==0) cnt=4;
	if (stat(dev, &stb)!= 0)
		return;
	if (!S_ISBLK(stb.st_mode))
		return;
	major_num = major(stb.st_rdev);
	minor_num = minor(stb.st_rdev);
	for (i=1; i <= cnt ; i++) {
		struct stat stb2;
		snprintf(name, nlen, "%s%s%d", dev, dig?"p":"", i);
		if (stat(name, &stb2)==0) {
			if (!S_ISBLK(stb2.st_mode))
				continue;
			if (stb2.st_rdev == makedev(major_num, minor_num+i))
				continue;
			unlink(name);
		} else {
			stb2 = stb;
		}
		if (mknod(name, S_IFBLK | 0600, makedev(major_num, minor_num+i)))
			perror("mknod");
		if (chown(name, stb2.st_uid, stb2.st_gid))
			perror("chown");
		if (chmod(name, stb2.st_mode & 07777))
			perror("chmod");
		if (symlinks && strncmp(name, "/dev/md/", 8) == 0)
			make_dev_symlink(name);
		stat(name, &stb2);
		add_dev(name, &stb2, 0, NULL);
	}
}


/*
 * Open a given md device, and check that it really is one.
 * If 'autof' is given, then we need to create, or recreate, the md device.
 * If the name already exists, and is not a block device, we fail.
 * If it exists and is not an md device, is not the right type (partitioned or not),
 * or is currently in-use, we remove the device, but remember the owner and mode.
 * If it now doesn't exist, we find a new md array and create the device.
 * Default ownership/mode comes from config file.
 */
int open_mddev(char *dev, int autof)
{
	int mdfd;
	struct stat stb;
	int major_num = MD_MAJOR;
	int minor_num = 0;
	int must_remove = 0;
	int num;
	struct createinfo *ci = conf_get_create_info();
	int parts;

	if (autof == 0)
		autof = ci->autof;

	parts = autof >> 3;
	autof &= 7;

	if (autof && autof != 1) {
		/* autof is set, so we need to check that the name is ok,
		 * and possibly create one if not
		 */
		int std;
		stb.st_mode = 0;
		if (stat(dev, &stb)==0 && ! S_ISBLK(stb.st_mode)) {
			fprintf(stderr, Name ": %s is not a block device.\n",
				dev);
			return -1;
		}
		/* check major number is correct */
		num = -1;
		std = is_standard(dev, &num);
		if (std>0) major_num = get_mdp_major();
		switch(autof) {
		case 2: /* only create is_standard names */
			if (!std && !stb.st_mode) {
				fprintf(stderr, Name
			": %s does not exist and is not a 'standard' name "
			"so it cannot be created\n", dev);
				return -1;
			}
			break;
		case 3: /* create md, reject std>0 */
			if (std > 0) {
				fprintf(stderr, Name ": that --auto option "
				"not compatable with device named %s\n", dev);
				return -1;
			}
			break;
		case 4: /* create mdp, reject std<0 */
			if (std < 0) {
				fprintf(stderr, Name ": that --auto option "
				"not compatable with device named %s\n", dev);
				return -1;
			}
			major_num = get_mdp_major();
			break;
		case 5: /* default to md if not standard */
			break;
		case 6: /* default to mdp if not standard */
			if (std == 0) major_num = get_mdp_major();
			break;
		}
		/* major is final. num is -1 if not standard */
		if (stb.st_mode && major(stb.st_rdev) != major_num)
			must_remove = 1;
		if (stb.st_mode && !must_remove) {
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
			if (major_num != MD_MAJOR && parts > 0)
				make_parts(dev, parts, ci->symlinks);
			return mdfd;
		}
		/* Ok, need to find a minor that is not in use.
		 * If the device name is in a 'standard' format,
		 * intuit the minor from that, else
		 * easiest to read /proc/mdstat, and hunt through for
		 * an unused number
		 */
		if (num < 0) {
			/* need to pick an unused number */
			int num = find_free_devnum(major_num != MD_MAJOR);

			if (major_num == MD_MAJOR)
				minor_num = num;
			else
				minor_num = (-1-num) << MdpMinorShift;
		} else if (major_num == MD_MAJOR)
			minor_num = num;
		else
			minor_num = num << MdpMinorShift;
		/* major and minor have been chosen */

		/* If it was a 'standard' name and it is in-use, then
		 * the device could already be correct
		 */
		if (stb.st_mode && major(stb.st_rdev) == major_num &&
		    minor(stb.st_rdev) == minor_num)
			;
		else {
			if (major(makedev(major_num,minor_num)) != major_num ||
			    minor(makedev(major_num,minor_num)) != minor_num) {
				fprintf(stderr, Name ": Need newer C library to use more than 4 partitionable md devices, sorry\n");
				return -1;
			}
			if (must_remove)
				unlink(dev);

			if (strncmp(dev, "/dev/md/", 8) == 0) {
				if (mkdir("/dev/md",0700)==0) {
					if (chown("/dev/md", ci->uid, ci->gid))
						perror("chown /dev/md");
					if (chmod("/dev/md", ci->mode| ((ci->mode>>2) & 0111)))
						perror("chmod /dev/md");
				}
			}
			if (mknod(dev, S_IFBLK|0600, makedev(major_num, minor_num))!= 0) {
				fprintf(stderr, Name ": failed to create %s\n", dev);
				return -1;
			}
			if (must_remove) {
				if (chown(dev, stb.st_uid, stb.st_gid))
					perror("chown");
				if (chmod(dev, stb.st_mode & 07777))
					perror("chmod");
			} else {
				if (chown(dev, ci->uid, ci->gid))
					perror("chown");
				if (chmod(dev, ci->mode))
					perror("chmod");
			}
			stat(dev, &stb);
			add_dev(dev, &stb, 0, NULL);
			if (ci->symlinks && strncmp(dev, "/dev/md/", 8) == 0)
				make_dev_symlink(dev);
			if (major_num != MD_MAJOR)
				make_parts(dev,parts, ci->symlinks);
		}
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


int open_mddev_devnum(char *devname, int devnum, char *name,
		      char *chosen_name, int parts)
{
	/* Open the md device with number 'devnum', possibly using 'devname',
	 * possibly constructing a name with 'name', but in any case, copying
	 * the name into 'chosen_name'
	 */
	int major_num, minor_num;
	struct stat stb;
	int i;
	struct createinfo *ci = conf_get_create_info();

	if (devname)
		strcpy(chosen_name, devname);
	else if (name && strchr(name,'/') == NULL) {
		char *n = strchr(name, ':');
		if (n) n++; else n = name;
		if (isdigit(*n) && devnum < 0)
			sprintf(chosen_name, "/dev/md/d%s", n);
		else
			sprintf(chosen_name, "/dev/md/%s", n);
	} else {
		if (devnum >= 0)
			sprintf(chosen_name, "/dev/md%d", devnum);
		else
			sprintf(chosen_name, "/dev/md/d%d", -1-devnum);
	}
	if (devnum >= 0) {
		major_num = MD_MAJOR;
		minor_num = devnum;
	} else {
		major_num = get_mdp_major();
		minor_num = (-1-devnum) << 6;
	}
	if (stat(chosen_name, &stb) == 0) {
		/* It already exists.  Check it is right. */
		if ( ! S_ISBLK(stb.st_mode) ||
		     stb.st_rdev != makedev(major_num, minor_num)) {
			errno = EEXIST;
			return -1;
		}
	} else {
		/* special case: if --incremental is suggesting a name
		 * in /dev/md/, we make sure the directory exists.
		 */
		if (strncmp(chosen_name, "/dev/md/", 8) == 0) {
			if (mkdir("/dev/md",0700)==0) {
				if (chown("/dev/md", ci->uid, ci->gid))
					perror("chown /dev/md");
				if (chmod("/dev/md", ci->mode|
					          ((ci->mode>>2) & 0111)))
					perror("chmod /dev/md");
			}
		}

		if (mknod(chosen_name, S_IFBLK | 0600,
			  makedev(major_num, minor_num)) != 0) {
			return -1;
		}
		/* FIXME chown/chmod ?? */
	}

	/* Simple locking to avoid --incr being called for the same
	 * array multiple times in parallel.
	 */
	for (i = 0; i < 25 ; i++) {
		int fd;

		fd = open(chosen_name, O_RDWR|O_EXCL);
		if (fd >= 0 || errno != EBUSY) {
			if (devnum < 0)
				make_parts(chosen_name, parts, ci->symlinks);
			return fd;
		}
		usleep(200000);
	}
	return -1;
}
