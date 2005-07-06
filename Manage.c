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
#include "md_u.h"
#include "md_p.h"

#define REGISTER_DEV 		_IO (MD_MAJOR, 1)
#define START_MD     		_IO (MD_MAJOR, 2)
#define STOP_MD      		_IO (MD_MAJOR, 3)

int Manage_ro(char *devname, int fd, int readonly)
{
	/* switch to readonly or rw
	 *
	 * requires >= 0.90.0
	 * first check that array is runing
	 * use RESTART_ARRAY_RW or STOP_ARRAY_RO
	 *
	 */
	mdu_array_info_t array;
	
	if (md_get_version(fd) < 9000) {
		fprintf(stderr, Name ": need md driver version 0.90.0 or later\n");
		return 1;
	}
	if (ioctl(fd, GET_ARRAY_INFO, &array)) {
		fprintf(stderr, Name ": %s does not appear to be active.\n",
			devname);
		return 1;
	}
	
	if (readonly>0) {
		if (ioctl(fd, STOP_ARRAY_RO, NULL)) {
			fprintf(stderr, Name ": failed to set readonly for %s: %s\n",
				devname, strerror(errno));
			return 1;
		}
	} else if (readonly < 0) {
		if (ioctl(fd, RESTART_ARRAY_RW, NULL)) {
			fprintf(stderr, Name ": failed to set writable for %s: %s\n",
				devname, strerror(errno));
			return 1;
		}
	}
	return 0;			
}

int Manage_runstop(char *devname, int fd, int runstop, int quiet)
{
	/* Run or stop the array. array must already be configured
	 * required >= 0.90.0
	 */
	mdu_param_t param; /* unused */

	if (runstop == -1 && md_get_version(fd) < 9000) {
		if (ioctl(fd, STOP_MD, 0)) {
			if (!quiet) fprintf(stderr, Name ": stopping device %s failed: %s\n",
					    devname, strerror(errno));
			return 1;
		}
	}
	
	if (md_get_version(fd) < 9000) {
		fprintf(stderr, Name ": need md driver version 0.90.0 or later\n");
		return 1;
	}
	/*
	if (ioctl(fd, GET_ARRAY_INFO, &array)) {
		fprintf(stderr, Name ": %s does not appear to be active.\n",
			devname);
		return 1;
	}
	*/
	if (runstop>0) {
		if (ioctl(fd, RUN_ARRAY, &param)) {
			fprintf(stderr, Name ": failed to run array %s: %s\n",
				devname, strerror(errno));
			return 1;
		}
	} else if (runstop < 0){
		if (ioctl(fd, STOP_ARRAY, NULL)) {
			if (!quiet)
				fprintf(stderr, Name ": fail to stop array %s: %s\n",
					devname, strerror(errno));
			return 1;
		}
	}
	return 0;
}

int Manage_resize(char *devname, int fd, long long size, int raid_disks)
{
	mdu_array_info_t info;
	if (ioctl(fd, GET_ARRAY_INFO, &info) != 0) {
		fprintf(stderr, Name ": Cannot get array information for %s: %s\n",
			devname, strerror(errno));
		return 1;
	}
	if (size >= 0)
		info.size = size;
	if (raid_disks > 0)
		info.raid_disks = raid_disks;
	if (ioctl(fd, SET_ARRAY_INFO, &info) != 0) {
		fprintf(stderr, Name ": Cannot set device size/shape for %s: %s\n",
			devname, strerror(errno));
		return 1;
	}
	return 0;
}

int Manage_reconfig(char *devname, int fd, int layout)
{
	mdu_array_info_t info;
	if (ioctl(fd, GET_ARRAY_INFO, &info) != 0) {
		fprintf(stderr, Name ": Cannot get array information for %s: %s\n",
			devname, strerror(errno));
		return 1;
	}
	info.layout = layout;
	printf("layout set to %d\n", info.layout);
	if (ioctl(fd, SET_ARRAY_INFO, &info) != 0) {
		fprintf(stderr, Name ": Cannot set layout for %s: %s\n",
			devname, strerror(errno));
		return 1;
	}
	return 0;
}

int Manage_subdevs(char *devname, int fd,
		   mddev_dev_t devlist)
{
	/* do something to each dev.
	 * devmode can be
	 *  'a' - add the device
	 *	   try HOT_ADD_DISK
	 *         If that fails EINVAL, try ADD_NEW_DISK
	 *  'r' - remove the device HOT_REMOVE_DISK
	 *  'f' - set the device faulty SET_DISK_FAULTY
	 */
	mdu_array_info_t array;
	mdu_disk_info_t disc;
	mddev_dev_t dv;
	struct stat stb;
	int j;
	int tfd;
	struct supertype *st;
	void *dsuper = NULL;

	if (ioctl(fd, GET_ARRAY_INFO, &array)) {
		fprintf(stderr, Name ": cannot get array info for %s\n",
			devname);
		return 1;
	}
	for (dv = devlist ; dv; dv=dv->next) {
		if (stat(dv->devname, &stb)) {
			fprintf(stderr, Name ": cannot find %s: %s\n",
				dv->devname, strerror(errno));
			return 1;
		}
		if ((stb.st_mode & S_IFMT) != S_IFBLK) {
			fprintf(stderr, Name ": %s is not a block device.\n",
				dv->devname);
			return 1;
		}
		switch(dv->disposition){
		default:
			fprintf(stderr, Name ": internal error - devmode[%s]=%d\n",
				dv->devname, dv->disposition);
			return 1;
		case 'a':
			/* add the device - hot or cold */
			/* Make sure it isn' in use (in 2.6 or later) */
			tfd = open(dv->devname, O_RDONLY|O_EXCL);
			if (tfd < 0) {
				fprintf(stderr, Name ": Cannot open %s: %s\n",
					dv->devname, strerror(errno));
				return 1;
			}
			close(tfd);
#if 0
			if (array.major_version == 0) {
#else
				if (md_get_version(fd)%100 < 2) {
#endif
				if (ioctl(fd, HOT_ADD_DISK,
					  (unsigned long)stb.st_rdev)==0) {
					fprintf(stderr, Name ": hot added %s\n",
						dv->devname);
					continue;
				}

				fprintf(stderr, Name ": hot add failed for %s: %s\n",
					dv->devname, strerror(errno));
				return 1;
			}

			/* need to find a sample superblock to copy, and
			 * a spare slot to use 
			 */
			st = super_by_version(array.major_version,
					      array.minor_version);
			if (!st) {
				fprintf(stderr, Name ": unsupport array - version %d.%d\n",
					array.major_version, array.minor_version);
				return 1;
			}
			for (j=0; j<array.raid_disks+array.spare_disks+ array.failed_disks; j++) {
				char *dev;
				int dfd;
				disc.number = j;
				if (ioctl(fd, GET_DISK_INFO, &disc))
					continue;
				if (disc.major==0 && disc.minor==0)
					continue;
				if ((disc.state & 4)==0) continue; /* sync */
				/* Looks like a good device to try */
				dev = map_dev(disc.major, disc.minor);
				if (!dev) continue;
				dfd = open(dev, O_RDONLY);
				if (dfd < 0) continue;
				if (st->ss->load_super(st, dfd, &dsuper, NULL)) {
					close(dfd);
					continue;
				}
				close(dfd);
				break;
			}
			if (!dsuper) {
				fprintf(stderr, Name ": cannot find valid superblock in this array - HELP\n");
				return 1;
			}
			for (j=0; j<array.nr_disks; j++) {
				disc.number = j;
				if (ioctl(fd, GET_DISK_INFO, &disc))
					break;
				if (disc.major==0 && disc.minor==0)
					break;
				if (disc.state & 8) /* removed */
					break;
			}
			disc.major = major(stb.st_rdev);
			disc.minor = minor(stb.st_rdev);
			disc.number =j;
			disc.state = 0;
			if (st->ss->write_init_super(st, dsuper, &disc, dv->devname))
				return 1;
			if (ioctl(fd,ADD_NEW_DISK, &disc)) {
				fprintf(stderr, Name ": add new device failed for %s: %s\n",
					dv->devname, strerror(errno));
				return 1;
			}
			fprintf(stderr, Name ": added %s\n", dv->devname);
			break;

		case 'r':
			/* hot remove */
			/* FIXME check that it is a current member */
			if (ioctl(fd, HOT_REMOVE_DISK, (unsigned long)stb.st_rdev)) {
				fprintf(stderr, Name ": hot remove failed for %s: %s\n",
					dv->devname, strerror(errno));
				return 1;
			}
			fprintf(stderr, Name ": hot removed %s\n", dv->devname);
			break;

		case 'f': /* set faulty */
			/* FIXME check current member */
			if (ioctl(fd, SET_DISK_FAULTY, (unsigned long) stb.st_rdev)) {
				fprintf(stderr, Name ": set device faulty failed for %s:  %s\n",
					dv->devname, strerror(errno));
				return 1;
			}
			fprintf(stderr, Name ": set %s faulty in %s\n",
				dv->devname, devname);
			break;
		}
	}
	return 0;
	
}
