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

int Manage_runstop(char *devname, int fd, int runstop)
{
	/* Run or stop the array. array must already be configured
	 * required >= 0.90.0
	 */
	mdu_param_t param; /* unused */

	if (runstop == -1 && md_get_version(fd) < 9000) {
		if (ioctl(fd, STOP_MD, 0)) {
			fprintf(stderr, Name ": stopping device %s failed: %s\n",
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
			fprintf(stderr, Name ": fail to stop array %s: %s\n",
				devname, strerror(errno));
			return 1;
		}
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
	int save_errno;
	static char buf[4096];

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
			if (ioctl(fd, HOT_ADD_DISK, (unsigned long)stb.st_rdev)==0) {
				fprintf(stderr, Name ": hot added %s\n",
					dv->devname);
				continue;
			}
			save_errno = errno;
			if (read(fd, buf, sizeof(buf)) > 0) {
				/* array is active, so don't try to add.
				 * i.e. something is wrong 
				 */
				fprintf(stderr, Name ": hot add failed for %s: %s\n",
					dv->devname, strerror(save_errno));
				return 1;
			}
			/* try ADD_NEW_DISK.
			 * we might be creating, we might be assembling,
			 * it is hard to tell.
			 * set up number/raid_disk/state just
			 * in case
			 */
			for (j=0; j<array.nr_disks; j++) {
				if (ioctl(fd, GET_DISK_INFO, &disc))
					break;
				if (disc.major==0 && disc.minor==0)
					break;
				if (disc.state & 8) /* removed */
					break;
			}
			disc.number =j;
			disc.raid_disk = j;
			disc.state = 0;
			disc.major = MAJOR(stb.st_rdev);
			disc.minor = MINOR(stb.st_rdev);
			if (ioctl(fd,ADD_NEW_DISK, &disc)) {
				fprintf(stderr, Name ": add new disk failed for %s: %s\n",
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
				fprintf(stderr, Name ": set disk faulty failed for %s:  %s\n",
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
