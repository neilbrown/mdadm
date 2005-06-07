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

#define REGISTER_DEV 		_IO (MD_MAJOR, 1)
#define START_MD     		_IO (MD_MAJOR, 2)
#define STOP_MD      		_IO (MD_MAJOR, 3)

int Build(char *mddev, int mdfd, int chunk, int level, int layout,
	  int raiddisks,
	  mddev_dev_t devlist, int assume_clean,
	  char *bitmap_file, int bitmap_chunk, int delay)
{
	/* Build a linear or raid0 arrays without superblocks
	 * We cannot really do any checks, we just do it.
	 * For md_version < 0.90.0, we call REGISTER_DEV
	 * with the device numbers, and then
	 * START_MD giving the "geometry"
	 * geometry is 0xpp00cc
	 * where pp is personality: 1==linear, 2=raid0
	 * cc = chunk size factor: 0==4k, 1==8k etc.
	 *
	 * For md_version >= 0.90.0 we call
	 * SET_ARRAY_INFO,  ADD_NEW_DISK, RUN_ARRAY
	 *
	 */
	int verbose = 0;
	int i;
	int vers;
	struct stat stb;
	int subdevs = 0;
	mddev_dev_t dv;
	int bitmap_fd;

	/* scan all devices, make sure they really are block devices */
	for (dv = devlist; dv; dv=dv->next) {
		if (stat(dv->devname, &stb)) {
			fprintf(stderr, Name ": Cannot find %s: %s\n",
				dv->devname, strerror(errno));
			return 1;
		}
		if ((stb.st_mode & S_IFMT) != S_IFBLK) {
			fprintf(stderr, Name ": %s is not a block device.\n",
				dv->devname);
			return 1;
		}
		subdevs++;
	}

	if (raiddisks != subdevs) {
		fprintf(stderr, Name ": requested %d devices in array but listed %d\n",
			raiddisks, subdevs);
		return 1;
	}

	if (layout == UnSet)
		switch(level) {
		default: /* no layout */
			layout = 0;
			break;
		case 10:
			layout = 0x102; /* near=2, far=1 */
			if (verbose)
				fprintf(stderr,
					Name ": layout defaults to n1\n");
			break;
		case 5:
		case 6:
			layout = map_name(r5layout, "default");
			if (verbose)
				fprintf(stderr,
					Name ": layout defaults to %s\n", map_num(r5layout, layout));
			break;
		case LEVEL_FAULTY:
			layout = map_name(faultylayout, "default");

			if (verbose)
				fprintf(stderr,
					Name ": layout defaults to %s\n", map_num(faultylayout, layout));
			break;
		}


	vers = md_get_version(mdfd);
	
	/* looks Ok, go for it */
	if (vers >= 9000) {
		mdu_array_info_t array;
		array.level = level;
		array.size = 0;
		array.nr_disks = raiddisks;
		array.raid_disks = raiddisks;
		array.md_minor = 0;
		if (fstat(mdfd, &stb)==0)
			array.md_minor = minor(stb.st_rdev);
		array.not_persistent = 1;
		array.state = 0; /* not clean, but no errors */
		if (assume_clean)
			array.state |= 1;
		array.active_disks = raiddisks;
		array.working_disks = raiddisks;
		array.spare_disks = 0;
		array.failed_disks = 0;
		if (chunk == 0)  
			chunk = 64;
		array.chunk_size = chunk*1024;
		array.layout = layout;
		if (ioctl(mdfd, SET_ARRAY_INFO, &array)) {
			fprintf(stderr, Name ": SET_ARRAY_INFO failed for %s: %s\n",
				mddev, strerror(errno));
			return 1;
		}
	} else if (bitmap_file) {
		fprintf(stderr, Name ": bitmaps not supported with this kernel\n");
		return 1;
	}
	/* now add the devices */
	for ((i=0), (dv = devlist) ; dv ; i++, dv=dv->next) {
		if (stat(dv->devname, &stb)) {
			fprintf(stderr, Name ": Wierd: %s has disappeared.\n",
				dv->devname);
			goto abort;
		}
		if ((stb.st_mode & S_IFMT)!= S_IFBLK) {
			fprintf(stderr, Name ": Wierd: %s is no longer a block device.\n",
				dv->devname);
			goto abort;
		}
		if (vers>= 9000) {
			mdu_disk_info_t disk;
			disk.number = i;
			disk.raid_disk = i;
			disk.state = 6;
			disk.major = major(stb.st_rdev);
			disk.minor = minor(stb.st_rdev);
			if (ioctl(mdfd, ADD_NEW_DISK, &disk)) {
				fprintf(stderr, Name ": ADD_NEW_DISK failed for %s: %s\n",
					dv->devname, strerror(errno));
				goto abort;
			}
		} else {
			if (ioctl(mdfd, REGISTER_DEV, &stb.st_rdev)) {
				fprintf(stderr, Name ": REGISTER_DEV failed for %s: %s.\n",
					dv->devname, strerror(errno));
				goto abort;
			}
		}
	}
	/* now to start it */
	if (vers >= 9000) {
		mdu_param_t param; /* not used by syscall */
		if (bitmap_file) {
			bitmap_fd = open(bitmap_file, O_RDWR);
			if (bitmap_fd < 0) {
				if (bitmap_chunk == UnSet) {
					fprintf(stderr, Name ": %s cannot be openned.",
						bitmap_file);
					return 1;
				}
				if (CreateBitmap(bitmap_file, 1, NULL, bitmap_chunk,
						 delay, 0/* FIXME size */)) {
					return 1;
				}
				bitmap_fd = open(bitmap_file, O_RDWR);
				if (bitmap_fd < 0) {
					fprintf(stderr, Name ": %s cannot be openned.",
						bitmap_file);
					return 1;
				}
			}				
			if (bitmap_fd >= 0) {
				if (ioctl(mdfd, SET_BITMAP_FILE, bitmap_fd) < 0) {
					fprintf(stderr, Name ": Cannot set bitmap file for %s: %s\n",
						mddev, strerror(errno));
					return 1;
				}
			}
		}
		if (ioctl(mdfd, RUN_ARRAY, &param)) {
			fprintf(stderr, Name ": RUN_ARRAY failed: %s\n",
				strerror(errno));
			goto abort;
		}
	} else {
		unsigned long arg;
		arg=0;
		while (chunk > 4096) {
			arg++;
			chunk >>= 1;
		}
		if (level == 0)
			chunk |= 0x20000;
		else 	chunk |= 0x10000;
		if (ioctl(mdfd, START_MD, arg)) {
			fprintf(stderr, Name ": START_MD failed: %s\n",
				strerror(errno));
			goto abort;
		}
	}
	fprintf(stderr, Name ": array %s built and started.\n",
		mddev);
	return 0;

 abort:
	if (vers >= 9000)
	    ioctl(mdfd, STOP_ARRAY, 0);
	else
	    ioctl(mdfd, STOP_MD, 0);
	return 1;
		
}

