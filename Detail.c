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
#include	"md_p.h"
#include	"md_u.h"

int Detail(char *dev, int brief)
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
	char *devices = NULL;

	mdp_super_t super;
	int have_super = 0;

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
	c = map_num(pers, array.level);
	if (brief) 
		printf("ARRAY %s level=%s num-devices=%d", dev, c?c:"-unknown-",array.raid_disks );
	else {
		unsigned long array_size;
		unsigned long long larray_size;
#ifdef BLKGETSIZE64
		if (ioctl(fd, BLKGETSIZE64, &larray_size)==0)
			;
		else
#endif
			if (ioctl(fd, BLKGETSIZE, &array_size)==0) {
				larray_size = array_size;
				larray_size <<= 9;
			}
		
		else larray_size = 0;

		printf("%s:\n", dev);
		printf("        Version : %02d.%02d.%02d\n",
		       array.major_version, array.minor_version, array.patch_version);
		atime = array.ctime;
		printf("  Creation Time : %.24s\n", ctime(&atime));
		printf("     Raid Level : %s\n", c?c:"-unknown-");
		if (larray_size)
		printf("     Array Size : %llu%s\n", (larray_size>>10), human_size(larray_size));
		if (array.level >= 1)
			printf("    Device Size : %d%s\n", array.size, human_size((long long)array.size<<10));
		printf("   Raid Devices : %d\n", array.raid_disks);
		printf("  Total Devices : %d\n", array.nr_disks);
		printf("Preferred Minor : %d\n", array.md_minor);
		printf("    Persistence : Superblock is %spersistent\n",
		       array.not_persistent?"not ":"");
		printf("\n");
		atime = array.utime;
		printf("    Update Time : %.24s\n", ctime(&atime));
		printf("          State : %s, %serrors\n",
		       (array.state&(1<<MD_SB_CLEAN))?"clean":"dirty",
		       (array.state&(1<<MD_SB_ERRORS))?"":"no-");
		printf(" Active Devices : %d\n", array.active_disks);
		printf("Working Devices : %d\n", array.working_disks);
		printf(" Failed Devices : %d\n", array.failed_disks);
		printf("  Spare Devices : %d\n", array.spare_disks);
		printf("\n");
		if (array.level == 5) {
			c = map_num(r5layout, array.layout);
			printf("         Layout : %s\n", c?c:"-unknown-");
		}
		switch (array.level) {
		case 0:
		case 4:
		case 5:
			printf("     Chunk Size : %dK\n", array.chunk_size/1024);
			break;
		case -1:
			printf("       Rounding : %dK\n", array.chunk_size/1024);
			break;
		default: break;
		}
	
		printf("\n");
		printf("    Number   Major   Minor   RaidDevice State\n");
	}
	for (d= 0; d<MD_SB_DISKS; d++) {
		mdu_disk_info_t disk;
		char *dv;
		disk.number = d;
		if (ioctl(fd, GET_DISK_INFO, &disk) < 0) {
			if (d < array.raid_disks)
				fprintf(stderr, Name ": cannot get device detail for device %d: %s\n",
					d, strerror(errno));
			continue;
		}
		if (d >= array.raid_disks &&
		    disk.major == 0 &&
		    disk.minor == 0)
			continue;
		if (!brief) {
			printf("   %5d   %5d    %5d    %5d     ", 
			       disk.number, disk.major, disk.minor, disk.raid_disk);
			if (disk.state & (1<<MD_DISK_FAULTY)) printf(" faulty");
			if (disk.state & (1<<MD_DISK_ACTIVE)) printf(" active");
			if (disk.state & (1<<MD_DISK_SYNC)) printf(" sync");
			if (disk.state & (1<<MD_DISK_REMOVED)) printf(" removed");
		}
		if ((dv=map_dev(disk.major, disk.minor))) {
			if (brief) {
				if (devices) {
					devices = realloc(devices,
							  strlen(devices)+1+strlen(dv)+1);
					strcat(strcat(devices,","),dv);
				} else
					devices = strdup(dv);
			} else
				printf("   %s", dv);
			if (!have_super && (disk.state & (1<<MD_DISK_ACTIVE))) {
				/* try to read the superblock from this device
				 * to get more info
				 */
				int fd = open(dv, O_RDONLY);
				if (fd >=0 &&
				    load_super(fd, &super) ==0 &&
				    super.ctime == array.ctime &&
				    super.level == array.level)
					have_super = 1;
			}
		}
		if (!brief) printf("\n");
	}
	if (have_super) {
		if (brief) printf(" UUID=");
		else printf("           UUID : ");
	    	if (super.minor_version >= 90)
			printf("%08x:%08x:%08x:%08x", super.set_uuid0, super.set_uuid1,
			       super.set_uuid2, super.set_uuid3);
		else
			printf("%08x", super.set_uuid0);
		if (!brief) 
			printf("\n         Events : %d.%d\n", super.events_hi, super.events_lo);
	}
	if (brief && devices) printf("\n   devices=%s", devices);
	if (brief) printf("\n");
	return 0;
}
