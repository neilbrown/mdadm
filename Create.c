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
#include	"md_u.h"
#include	"md_p.h"

int Create(char *mddev, int mdfd,
	   int chunk, int level, int layout, unsigned long size, int raiddisks, int sparedisks,
	   int subdevs, mddev_dev_t devlist,
	   int runstop, int verbose, int force)
{
	/*
	 * Create a new raid array.
	 *
	 * First check that necessary details are available
	 * (i.e. level, raid-disks)
	 *
	 * Then check each disk to see what might be on it
	 * and report anything interesting.
	 *
	 * If anything looks odd, and runstop not set,
	 * abort.
	 *
	 * SET_ARRAY_INFO and ADD_NEW_DISK, and
	 * if runstop==run, or raiddisks diskswere used,
	 * RUN_ARRAY
	 */
	unsigned long minsize=0, maxsize=0;
	char *mindisc = NULL;
	char *maxdisc = NULL;
	int dnum;
	mddev_dev_t dv;
	int fail=0, warn=0;
	struct stat stb;
	int first_missing = MD_SB_DISKS*2;
	int missing_disks = 0;
	int insert_point = MD_SB_DISKS*2; /* where to insert a missing drive */
	mddev_dev_t moved_disk = NULL; /* the disk that was moved out of the insert point */

	mdu_array_info_t array;
	

	if (md_get_version(mdfd) < 9000) {
		fprintf(stderr, Name ": Create requires md driver version 0.90.0 or later\n");
		return 1;
	}
	if (level == UnSet) {
		fprintf(stderr,
			Name ": a RAID level is needed to create an array.\n");
		return 1;
	}
	if (raiddisks < 1) {
		fprintf(stderr,
			Name ": a number of --raid-devices must be given to create an array\n");
		return 1;
	}
	if (raiddisks < 4 && level == 6) {
		fprintf(stderr,
			Name ": at least 4 raid-devices needed for level 6\n");
		return 1;
	}
	if (raiddisks > 256 && level == 6) {
		fprintf(stderr,
			Name ": no more than 256 raid-devices supported for level 6\n");
		return 1;
	}
	if (raiddisks < 2 && level >= 4) {
		fprintf(stderr,
			Name ": at least 2 raid-devices needed for level 4 or 5\n");
		return 1;
	}
	if (raiddisks+sparedisks > MD_SB_DISKS) {
		fprintf(stderr,
			Name ": too many devices requested: %d+%d > %d\n",
			raiddisks, sparedisks, MD_SB_DISKS);
		return 1;
	}
	if (subdevs > raiddisks+sparedisks) {
		fprintf(stderr, Name ": You have listed more devices (%d) than are in the array(%d)!\n", subdevs, raiddisks+sparedisks);
		return 1;
	}
	if (subdevs < raiddisks+sparedisks) {
		fprintf(stderr, Name ": You haven't given enough devices (real or missing) to create this array\n");
		return 1;
	}

	/* now set some defaults */
	if (layout == UnSet)
		switch(level) {
		default: /* no layout */
			layout = 0;
			break;
		case 5:
		case 6:
			layout = map_name(r5layout, "default");
			if (verbose)
				fprintf(stderr,
					Name ": layout defaults to %s\n", map_num(r5layout, layout));
			break;
		}

	switch(level) {
	case 4:
	case 5:
	case 6:
	case 0:
	case -1: /* linear */
		if (chunk == 0) {
			chunk = 64;
			if (verbose)
				fprintf(stderr, Name ": chunk size defaults to 64K\n");
		}
		break;
	default: /* raid1, multipath */
		if (chunk) {
			chunk = 0;
			if (verbose)
				fprintf(stderr, Name ": chunk size ignored for this level\n");
		}
		break;
	}

	/* now look at the subdevs */
	array.active_disks = 0;
	array.working_disks = 0;
	dnum = 0;
	for (dv=devlist; dv; dv=dv->next, dnum++) {
		char *dname = dv->devname;
		unsigned long dsize, freesize;
		int fd;
		if (strcasecmp(dname, "missing")==0) {
			if (first_missing > dnum)
				first_missing = dnum;
			missing_disks ++;
			continue;
		}
		array.working_disks++;
		if (dnum < raiddisks)
			array.active_disks++;
		fd = open(dname, O_RDONLY, 0);
		if (fd <0 ) {
			fprintf(stderr, Name ": Cannot open %s: %s\n",
				dname, strerror(errno));
			fail=1;
			continue;
		}
		if (ioctl(fd, BLKGETSIZE, &dsize)) {
			fprintf(stderr, Name ": Cannot get size of %s: %s\n",
				dname, strerror(errno));
			fail = 1;
			close(fd);
			continue;
		}
		if (dsize < MD_RESERVED_SECTORS*2) {
			fprintf(stderr, Name ": %s is too small: %luK\n",
				dname, dsize/2);
			fail = 1;
			close(fd);
			continue;
		}
		freesize = MD_NEW_SIZE_SECTORS(dsize);
		freesize /= 2;

		if (size && freesize < size) {
			fprintf(stderr, Name ": %s is smaller that given size."
				" %luK < %luK + superblock\n", dname, freesize, size);
			fail = 1;
			close(fd);
			continue;
		}
		if (maxdisc == NULL || (maxdisc && freesize > maxsize)) {
			maxdisc = dname;
			maxsize = freesize;
		}
		if (mindisc ==NULL || (mindisc && freesize < minsize)) {
			mindisc = dname;
			minsize = freesize;
		}
		warn |= check_ext2(fd, dname);
		warn |= check_reiser(fd, dname);
		warn |= check_raid(fd, dname);
		close(fd);
	}
	if (fail) {
		fprintf(stderr, Name ": create aborted\n");
		return 1;
	}
	if (size == 0) {
		if (mindisc == NULL) {
			fprintf(stderr, Name ": no size and no drives given - aborting create.\n");
			return 1;
		}
		size = minsize;
		if (verbose && level>0)
			fprintf(stderr, Name ": size set to %luK\n", size);
	}
	if (level >= 1 && ((maxsize-size)*100 > maxsize)) {
		fprintf(stderr, Name ": largest drive (%s) exceed size (%luK) by more than 1%%\n",
			maxdisc, size);
		warn = 1;
	}

	if (warn) {
		if (runstop!= 1) {
			if (!ask("Continue creating array? ")) {
				fprintf(stderr, Name ": create aborted.\n");
				return 1;
			}
		} else {
			if (verbose)
				fprintf(stderr, Name ": creation continuing despite oddities due to --run\n");
		}
	}

	/* If this is  raid5, we want to configure the last active slot
	 * as missing, so that a reconstruct happens (faster than re-parity)
	 * FIX: Can we do this for raid6 as well?
	 */
	if (force == 0 && first_missing >= raiddisks) {
		switch ( level ) {
		case 5:
			insert_point = raiddisks-1;
			sparedisks++;
			array.active_disks--;
			missing_disks++;
			break;
		default:
			break;
		}
	}
	
	/* Ok, lets try some ioctls */

	array.level = level;
	array.size = size;
	array.raid_disks = raiddisks;
	/* The kernel should *know* what md_minor we are dealing
	 * with, but it chooses to trust me instead. Sigh
	 */
	array.md_minor = 0;
	if (fstat(mdfd, &stb)==0)
		array.md_minor = MINOR(stb.st_rdev);
	array.not_persistent = 0;
	/*** FIX: Need to do something about RAID-6 here ***/
	if ( (level == 5 || level == 6) &&
	     (insert_point < raiddisks || first_missing < raiddisks) )
		array.state = 1; /* clean, but one+ drive will be missing */
	else
		array.state = 0; /* not clean, but no errors */

	/* There is lots of redundancy in these disk counts,
	 * raid_disks is the most meaningful value
	 *          it describes the geometry of the array
	 *          it is constant
	 * nr_disks is total number of used slots.
	 *          it should be raid_disks+spare_disks
	 * spare_disks is the number of extra disks present
	 *          see above
	 * active_disks is the number of working disks in
	 *          active slots. (With raid_disks)
	 * working_disks is the total number of working disks,
	 *          including spares
	 * failed_disks is the number of disks marked failed
	 *
         * Ideally, the kernel would keep these (except raid_disks)
	 * up-to-date as we ADD_NEW_DISK, but it doesn't (yet).
	 * So for now, we assume that all raid and spare
	 * devices will be given.
	 */
	array.spare_disks=sparedisks;
	array.failed_disks=missing_disks;
	array.nr_disks = array.working_disks + array.failed_disks;
	array.layout = layout;
	array.chunk_size = chunk*1024;

	if (ioctl(mdfd, SET_ARRAY_INFO, &array)) {
		fprintf(stderr, Name ": SET_ARRAY_INFO failed for %s: %s\n",
			mddev, strerror(errno));
		return 1;
	}
	
	for (dnum=0, dv = devlist ; dv ; dv=(dv->next)?(dv->next):moved_disk, dnum++) {
		int fd;
		struct stat stb;
		mdu_disk_info_t disk;

		disk.number = dnum;
		if (dnum == insert_point) {
			moved_disk = dv;
		}
		disk.raid_disk = disk.number;
		if (disk.raid_disk < raiddisks)
			disk.state = 6; /* active and in sync */
		else
			disk.state = 0;
		if (dnum == insert_point ||
		    strcasecmp(dv->devname, "missing")==0) {
			disk.major = 0;
			disk.minor = 0;
			disk.state = 1; /* faulty */
		} else {
			fd = open(dv->devname, O_RDONLY, 0);
			if (fd < 0) {
				fprintf(stderr, Name ": failed to open %s after earlier success - aborting\n",
					dv->devname);
				return 1;
			}
			fstat(fd, &stb);
			disk.major = MAJOR(stb.st_rdev);
			disk.minor = MINOR(stb.st_rdev);
			close(fd);
		}
		if (ioctl(mdfd, ADD_NEW_DISK, &disk)) {
			fprintf(stderr, Name ": ADD_NEW_DISK for %s failed: %s\n",
				dv->devname, strerror(errno));
			return 1;
		}
		if (dv == moved_disk && dnum != insert_point) break;
	}

	/* param is not actually used */
	if (runstop == 1 || subdevs >= raiddisks) {
		mdu_param_t param;
		if (ioctl(mdfd, RUN_ARRAY, &param)) {
			fprintf(stderr, Name ": RUN_ARRAY failed: %s\n",
				strerror(errno));
			Manage_runstop(mddev, mdfd, -1);
			return 1;
		}
		fprintf(stderr, Name ": array %s started.\n", mddev);
	} else {
		fprintf(stderr, Name ": not starting array - not enough devices.\n");
	}
	return 0;
}
