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

#include	"md_u.h"
#include	"md_p.h"
int Examine(char *dev)
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
	 */
	int fd = open(dev, O_RDONLY, 0);
	time_t atime;
	mdp_super_t super;
	int d;
	int rv;

	if (fd < 0) {
		fprintf(stderr,"mdctl: cannot open %s: %s\n",
			dev, strerror(errno));
		return 1;
	}

	rv = load_super(fd, &super);
	close(fd);
	switch(rv) {
	case 1:
		fprintf(stderr, "mdctl: cannot find device size for %s: %s\n",
			dev, strerror(errno));
		return 1;
	case 2:
/*		fprintf(stderr, "mdctl: %s is too small for md: size is %ld sectors\n",
			dev, size);
*/
		fprintf(stderr, "mdctl: %s is too small for md\n",
			dev);
		return 1;
	case 3:
		fprintf(stderr, "mdctl: Cannot seek to superblock on %s: %s\n",
			dev, strerror(errno));
		return 1;
	case 4:
		fprintf(stderr, "mdctl: Cannot read superblock on %s\n",
			dev);
		return 1;
	case 5:
		fprintf(stderr, "mdctl: No super block found on %s (Expected magic %08x, got %08x)\n",
			dev, MD_SB_MAGIC, super.md_magic);
		return 1;
	case 6:
		fprintf(stderr, "mdctl: Cannot interpret superblock on %s - version is %d\n",
			dev, super.major_version);
		return 1;
	}
    
	/* Ok, its good enough to try, though the checksum could be wrong */
	printf("%s:\n",dev);
	printf("          Magic : %08x\n", super.md_magic);
	printf("        Version : %02d.%02d.%02d\n", super.major_version, super.minor_version,
	       super.patch_version);
	if (super.minor_version >= 90)
		printf("           UUID : %08x:%08x:%08x:%08x\n", super.set_uuid0, super.set_uuid1,
		       super.set_uuid2, super.set_uuid3);
	else
		printf("           UUID : %08x\n", super.set_uuid0);

	atime = super.ctime;
	printf("  Creation Time : %.24s\n", ctime(&atime));
	printf("     Raid Level : %d\n", super.level);
	printf("           Size : %d\n", super.size);
	printf("     Raid Disks : %d\n", super.raid_disks);
	printf("    Total Disks : %d\n", super.nr_disks);
	printf("Preferred Minor : %d\n", super.md_minor);
	printf("\n");
	atime = super.utime;
	printf("    Update Time : %.24s\n", ctime(&atime));
	printf("          State : %s, %serrors\n",
	       (super.state&(1<<MD_SB_CLEAN))?"clean":"dirty",
	       (super.state&(1<<MD_SB_ERRORS))?"":"no-");
	printf("  Active Drives : %d\n", super.active_disks);
	printf(" Working Drives : %d\n", super.working_disks);
	printf("  Failed Drives : %d\n", super.failed_disks);
	printf("   Spare Drives : %d\n", super.spare_disks);
	printf("   - checksum not checked yet - \n");
	printf("         Events : %d.%d\n", super.events_hi, super.events_lo);
	printf("\n");
	printf("         Layout : %d\n", super.layout);
	printf("     Chunk Size : %dK\n", super.chunk_size/1024);
	printf("\n");
	printf("      Number   Major   Minor   RaidDisk   State\n");
	for (d= -1; d<(signed int)super.nr_disks; d++) {
		mdp_disk_t *dp;
		char nb[5];
		if (d>=0) dp = &super.disks[d];
		else dp = &super.this_disk;
		sprintf(nb, "%4d", d);
		printf("%4s %5d   %5d    %5d    %5d     ", d < 0 ? "this" :  nb,
		       dp->number, dp->major, dp->minor, dp->raid_disk);
		if (dp->state & (1<<MD_DISK_FAULTY)) printf(" faulty");
		if (dp->state & (1<<MD_DISK_ACTIVE)) printf(" active");
		if (dp->state & (1<<MD_DISK_SYNC)) printf(" sync");
		if (dp->state & (1<<MD_DISK_REMOVED)) printf(" removed");
		printf("\n");
	}
	return 0;
}
