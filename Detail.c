/*
 * mdctl - manage Linux "md" devices aka RAID arrays.
 *
 * Copyright (C) 2001 Neil Brown <neilb@cse.unsw.edu.au>
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

#include	"mdctl.h"
#include	"md_p.h"
#include	"md_u.h"

int Detail(char *dev)
{
	/*
	 * Print out details for an md array by using
	 * GET_ARRAY_INFO and GET_DISK_INFO ioctl calls
	 */

	int fd = open(dev, O_RDONLY, 0);
	int vers;
	mdu_array_info_t array;
	int d;
	time_t atime;
	char *c;

	if (fd < 0) {
		fprintf(stderr, Name ": cannot open %s: %s\n",
			dev, strerror(errno));
		return 1;
	}
	vers = md_get_version(fd);
	if (vers < 0) {
		fprintf(stderr, Name ": %s does not appear to be an md device\n",
			dev);
		close(fd);
		return 1;
	}
	if (vers < 9000) {
		fprintf(stderr, Name ": cannot get detail for md device %s: driver version too old.\n",
			dev);
		close(fd);
		return 1;
	}
	if (ioctl(fd, GET_ARRAY_INFO, &array)<0) {
		if (errno == ENODEV)
			fprintf(stderr, Name ": md device %s does not appear to be active.\n",
				dev);
		else
			fprintf(stderr, Name ": cannot get array detail for %s: %s\n",
				dev, strerror(errno));
		close(fd);
		return 1;
	}
	/* Ok, we have some info to print... */
	printf("%s:\n", dev);
	printf("        Version : %02d.%02d.%02d\n",
	       array.major_version, array.minor_version, array.patch_version);
	atime = array.ctime;
	printf("  Creation Time : %.24s\n", ctime(&atime));
	c = map_num(pers, array.level);
	printf("     Raid Level : %s\n", c?c:"-unknown-");
	printf("           Size : %d\n", array.size);
	printf("     Raid Disks : %d\n", array.raid_disks);
	printf("    Total Disks : %d\n", array.nr_disks);
	printf("Preferred Minor : %d\n", array.md_minor);
	printf("    Persistance : Superblock is %spersistant\n",
	       array.not_persistent?"not ":"");
	printf("\n");
	atime = array.utime;
	printf("    Update Time : %.24s\n", ctime(&atime));
	printf("          State : %s, %serrors\n",
	       (array.state&(1<<MD_SB_CLEAN))?"clean":"dirty",
	       (array.state&(1<<MD_SB_ERRORS))?"":"no-");
	printf("  Active Drives : %d\n", array.active_disks);
	printf(" Working Drives : %d\n", array.working_disks);
	printf("  Failed Drives : %d\n", array.failed_disks);
	printf("   Spare Drives : %d\n", array.spare_disks);
	printf("\n");
	if (array.level == 5) {
		c = map_num(r5layout, array.layout);
		printf("         Layout : %s\n", c?c:"-unknown-");
	}
	printf("     Chunk Size : %dK\n", array.chunk_size/1024);
	printf("\n");
	printf("    Number   Major   Minor   RaidDisk   State\n");
	for (d= 0; d<array.nr_disks; d++) {
		mdu_disk_info_t disk;
		disk.number = d;
		if (ioctl(fd, GET_DISK_INFO, &disk) < 0) {
			fprintf(stderr, Name ": cannot get disk detail for disk %d: %s\n",
				d, strerror(errno));
			continue;
		}
		printf("   %5d   %5d    %5d    %5d     ", 
		       disk.number, disk.major, disk.minor, disk.raid_disk);
		if (disk.state & (1<<MD_DISK_FAULTY)) printf(" faulty");
		if (disk.state & (1<<MD_DISK_ACTIVE)) printf(" active");
		if (disk.state & (1<<MD_DISK_SYNC)) printf(" sync");
		if (disk.state & (1<<MD_DISK_REMOVED)) printf(" removed");
		printf("\n");
	}
	return 0;
}
