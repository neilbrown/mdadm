/*
 * mdadm - manage Linux "md" devices aka RAID arrays.
 *
 * Copyright (C) 2001-2004 Neil Brown <neilb@cse.unsw.edu.au>
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

int Grow_Add_device(char *devname, int fd, char *newdev)
{
	/* Add a device to an active array.
	 * Currently, just extend a linear array.
	 * This requires writing a new superblock on the
	 * new device, calling the kernel to add the device,
	 * and if that succeeds, update the superblock on
	 * all other devices.
	 * This means that we need to *find* all other devices.
	 */
	mdu_array_info_t array;
	mdu_disk_info_t disk;
	mdp_super_t super;
	struct stat stb;
	int nfd, fd2;
	int d, nd;
	

	if (ioctl(fd, GET_ARRAY_INFO, &array) < 0) {
		fprintf(stderr, Name ": cannot get array info for %s\n", devname);
		return 1;
	}

	if (array.level != -1) {
		fprintf(stderr, Name ": can only add devices to linear arrays\n");
		return 1;
	}

	nfd = open(newdev, O_RDWR|O_EXCL);
	if (nfd < 0) {
		fprintf(stderr, Name ": cannot open %s\n", newdev);
		return 1;
	}
	fstat(nfd, &stb);
	if ((stb.st_mode & S_IFMT) != S_IFBLK) {
		fprintf(stderr, Name ": %s is not a block device!\n", newdev);
		close(nfd);
		return 1;
	}
	/* now check out all the devices and make sure we can read the superblock */
	for (d=0 ; d < array.raid_disks ; d++) {
		mdu_disk_info_t disk;
		char *dv;

		disk.number = d;
		if (ioctl(fd, GET_DISK_INFO, &disk) < 0) {
			fprintf(stderr, Name ": cannot get device detail for device %d\n",
				d);
			return 1;
		}
		dv = map_dev(disk.major, disk.minor);
		if (!dv) {
			fprintf(stderr, Name ": cannot find device file for device %d\n",
				d);
			return 1;
		}
		fd2 = open(dv, O_RDWR);
		if (!fd2) {
			fprintf(stderr, Name ": cannot open device file %s\n", dv);
			return 1;
		}
		if (load_super(fd2, &super)) {
			fprintf(stderr, Name ": cannot find super block on %s\n", dv);
			close(fd2);
			return 1;
		}
		close(fd2);
	}
	/* Ok, looks good. Lets update the superblock and write it out to
	 * newdev.
	 */
	
	memset(&super.disks[d], 0, sizeof(super.disks[d]));
	super.disks[d].number = d;
	super.disks[d].major = MAJOR(stb.st_rdev);
	super.disks[d].minor = MINOR(stb.st_rdev);
	super.disks[d].raid_disk = d;
	super.disks[d].state = (1 << MD_DISK_SYNC) | (1 << MD_DISK_ACTIVE);

	super.this_disk = super.disks[d];
	super.sb_csum = calc_sb_csum(&super);
	if (store_super(nfd, &super)) {
		fprintf(stderr, Name ": Cannot store new superblock on %s\n", newdev);
		close(nfd);
		return 1;
	}
	disk.number = d;
	disk.major = MAJOR(stb.st_rdev);
	disk.minor = MINOR(stb.st_rdev);
	disk.raid_disk = d;
	disk.state = (1 << MD_DISK_SYNC) | (1 << MD_DISK_ACTIVE);
	close(nfd);
	if (ioctl(fd, ADD_NEW_DISK, &disk) != 0) {
		fprintf(stderr, Name ": Cannot add new disk to this array\n");
		return 1;
	}
	/* Well, that seems to have worked.
	 * Now go through and update all superblocks
	 */

	if (ioctl(fd, GET_ARRAY_INFO, &array) < 0) {
		fprintf(stderr, Name ": cannot get array info for %s\n", devname);
		return 1;
	}

	nd = d;
	for (d=0 ; d < array.raid_disks ; d++) {
		mdu_disk_info_t disk;
		char *dv;

		disk.number = d;
		if (ioctl(fd, GET_DISK_INFO, &disk) < 0) {
			fprintf(stderr, Name ": cannot get device detail for device %d\n",
				d);
			return 1;
		}
		dv = map_dev(disk.major, disk.minor);
		if (!dv) {
			fprintf(stderr, Name ": cannot find device file for device %d\n",
				d);
			return 1;
		}
		fd2 = open(dv, O_RDWR);
		if (fd2 < 0) {
			fprintf(stderr, Name ": cannot open device file %s\n", dv);
			return 1;
		}
		if (load_super(fd2, &super)) {
			fprintf(stderr, Name ": cannot find super block on %s\n", dv);
			close(fd);
			return 1;
		}
		super.raid_disks = nd+1;
		super.nr_disks = nd+1;
		super.active_disks = nd+1;
		super.working_disks = nd+1;
		memset(&super.disks[nd], 0, sizeof(super.disks[nd]));
		super.disks[nd].number = nd;
		super.disks[nd].major = MAJOR(stb.st_rdev);
		super.disks[nd].minor = MINOR(stb.st_rdev);
		super.disks[nd].raid_disk = nd;
		super.disks[nd].state = (1 << MD_DISK_SYNC) | (1 << MD_DISK_ACTIVE);

		super.this_disk = super.disks[d];
		super.sb_csum = calc_sb_csum(&super);
		if (store_super(fd2, &super)) {
			fprintf(stderr, Name ": Cannot store new superblock on %s\n", dv);
			close(fd2);
			return 1;
		}
		close(fd2);
	}

	return 0;
}
