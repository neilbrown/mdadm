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

int Assemble(char *mddev, int mdfd,
	     mddev_ident_t ident, char *conffile,
	     int subdevs, char **subdev,
	     int readonly, int runstop,
	     int verbose, int force)
{
	/*
	 * The task of Assemble is to find a collection of
	 * devices that should (according to their superblocks)
	 * form an array, and to give this collection to the MD driver.
	 * In Linux-2.4 and later, this involves submitting a
	 * SET_ARRAY_INFO ioctl with no arg - to prepare
	 * the array - and then submit a number of
	 * ADD_NEW_DISK ioctls to add disks into
	 * the array.  Finally RUN_ARRAY might
	 * be submitted to start the array.
	 *
	 * Much of the work of Assemble is in finding and/or
	 * checking the disks to make sure they look right.
	 *
	 * If mddev is not set, then scan must be and we
	 *  read through the config file for dev+uuid mapping
	 *  We recurse, setting mddev, for each device that
	 *    - isn't running
	 *    - has a valid uuid (or any uuid if !uuidset
	 *
	 * If mddev is set, we try to determine state of md.
	 *   check version - must be at least 0.90.0
	 *   check kernel version.  must be at least 2.4.
	 *    If not, we can possibly fall back on START_ARRAY
	 *   Try to GET_ARRAY_INFO.
	 *     If possible, give up
	 *     If not, try to STOP_ARRAY just to make sure
	 *
	 * If !uuidset and scan, look in conf-file for uuid
	 *       If not found, give up
	 * If !subdevs and scan and uuidset, get list of devs from conf-file 
	 *
	 * For each device:
	 *   Check superblock - discard if bad
	 *   Check uuid (set if we don't have one) - discard if no match
	 *   Check superblock similarity if we have a superbloc - discard if different
	 *   Record events, devicenum, utime
	 * This should give us a list of devices for the array
	 * We should collect the most recent event and utime numbers
	 *
	 * Count disks with recent enough event count
	 * While force && !enough disks
	 *    Choose newest rejected disks, update event count
	 *     mark clean and rewrite superblock
	 * If recent kernel:
	 *    SET_ARRAY_INFO
	 *    foreach device with recent events : ADD_NEW_DISK
	 *    if runstop == 1 || "enough" disks and runstop==0 -> RUN_ARRAY
	 * If old kernel:
	 *    Check the device numbers in superblock are right
	 *    update superblock if any changes
	 *    START_ARRAY
	 *
	 */
	int old_linux = 0;
	int vers;
	mdu_array_info_t array;
	mddev_dev_t devlist = NULL;
	mdp_super_t first_super, super;
	struct {
		char *devname;
		int major, minor;
		long long events;
		time_t utime;
		int uptodate;
		int raid_disk;
	} devices[MD_SB_DISKS];
	int best[MD_SB_DISKS]; /* indexed by raid_disk */
	int devcnt = 0, okcnt, sparecnt;
	int i;
	int most_recent = 0;
	int chosen_drive = -1;
	int change = 0;
	
	vers = md_get_version(mdfd);
	if (vers <= 0) {
		fprintf(stderr, Name ": %s appears not to be an md device.\n");
		return 1;
	}
	if (vers < 9000) {
		fprintf(stderr, Name ": Assemble requires driver version 0.90.0 or later.\n"
			"    Upgrade your kernel or try --build\n");
		return 1;
	}
	if (get_linux_version() < 2004000)
		old_linux = 1;

	if (ioctl(mdfd, GET_ARRAY_INFO, &array)>=0) {
		fprintf(stderr, Name ": device %s already active - cannot assemble it\n",
			mddev);
		return 1;
	}
	ioctl(mdfd, STOP_ARRAY, NULL); /* just incase it was started but has no content */

	/*
	 * If any subdevs are listed, then any that don't
	 * match ident are discarded.  Remainder must all match and
	 * become the array.
	 * If no subdevs, then we scan all devices in the config file, but
	 * there must be something in the identity
	 */

	if (subdevs == 0 &&
	    ident->uuid_set == 0 &&
	    ident->super_minor < 0 &&
	    ident->devices == NULL) {
		fprintf(stderr, Name ": No identity information available for %s - cannot assemble.\n",
			mddev);
		return 1;
	}
	if (subdevs==0)
		devlist = conf_get_devs(conffile);

	first_super.md_magic = 0;
	for (i=0; i<MD_SB_DISKS; i++)
		best[i] = -1;

	if (verbose)
	    fprintf(stderr, Name ": looking for devices for %s\n",
		    mddev);

	while (subdevs || devlist) {
		char *devname;
		int this_uuid[4];
		int dfd;
		struct stat stb;
		int inargv;
		int havesuper=0;

		if (subdevs) {
			devname = *subdev++;
			subdevs--;
			inargv=1;
		} else {
			devname = devlist->devname;
			devlist = devlist->next;
			inargv=0;
		}

		if (ident->devices &&
		    !match_oneof(ident->devices, devname))
			continue;
		
		dfd = open(devname, O_RDONLY, 0);
		if (dfd < 0) {
			if (inargv || verbose)
				fprintf(stderr, Name ": cannot open device %s: %s\n",
					devname, strerror(errno));
		} else if (fstat(dfd, &stb)< 0) {
			/* Impossible! */
			fprintf(stderr, Name ": fstat failed for %s: %s\n",
				devname, strerror(errno));
			close(dfd);
		} if ((stb.st_mode & S_IFMT) != S_IFBLK) {
			fprintf(stderr, Name ": %d is not a block device.\n",
				devname);
			close(dfd);
		} if (load_super(dfd, &super)) {
			if (inargv || verbose)
				fprintf( stderr, Name ": no RAID superblock on %s\n",
					 devname);
			close(dfd);
		} else {
			havesuper =1;
			uuid_from_super(this_uuid, &super);
			close(dfd);
		}

		if (ident->uuid_set &&
		    (!havesuper || same_uuid(this_uuid, ident->uuid)==0)) {
			if (inargv || verbose)
				fprintf(stderr, Name ": %s has wrong uuid.\n",
					devname);
			continue;
		}
		if (ident->super_minor >= 0 &&
		    (!havesuper || ident->super_minor != super.md_minor)) {
			if (inargv || verbose)
				fprintf(stderr, Name ": %s has wrong super-minor.\n",
					devname);
			continue;
		}

		/* If we are this far, then we are commited to this device.
		 * If the super_block doesn't exist, or doesn't match others,
		 * then we cannot continue
		 */
		if (verbose)
			fprintf(stderr, Name ": %s is identified as a member of %s.\n",
				devname, mddev);

		if (!havesuper) {
			fprintf(stderr, Name ": %s has no superblock - assembly aborted\n",
				devname);
			return 1;
		}
		if (compare_super(&first_super, &super)) {
			fprintf(stderr, Name ": superblock on %s doesn't match others - assembly aborted\n",
				devname);
			return 1;
		}

		if (devcnt >= MD_SB_DISKS) {
		    fprintf(stderr, Name ": ouch - too many devices appear to be in this array. Ignoring %s\n",
			    devname);
		    continue;
		}
		devices[devcnt].devname = devname;
		devices[devcnt].major = MAJOR(stb.st_rdev);
		devices[devcnt].minor = MINOR(stb.st_rdev);
		devices[devcnt].events = md_event(&super);
		devices[devcnt].utime = super.utime;
		devices[devcnt].raid_disk = super.this_disk.raid_disk;
		devices[devcnt].uptodate = 0;
		if (most_recent < devcnt) {
			if (devices[devcnt].events
			    > devices[most_recent].events)
				most_recent = devcnt;
		}
		i = devices[devcnt].raid_disk;
		if (i>=0 && i < MD_SB_DISKS)
			if (best[i] == -1
			    || devices[best[i]].events < devices[devcnt].events)
				best[i] = devcnt;

		devcnt++;
	}

	if (devcnt == 0) {
		fprintf(stderr, Name ": no devices found for %s\n",
			mddev);
		return 1;
	}
	/* now we have some devices that might be suitable.
	 * I wonder how many
	 */
	okcnt = 0;
	sparecnt=0;
	for (i=0; i< MD_SB_DISKS;i++) {
		int j = best[i];
		if (j < 0) continue;
		if (devices[j].events+1 >=
		    devices[most_recent].events) {
			devices[j].uptodate = 1;
			if (i < first_super.raid_disks)
				okcnt++;
			else
				sparecnt++;
		}
	}
	while (force && !enough(first_super.level, first_super.raid_disks, okcnt)) {
		/* Choose the newest best drive which is
		 * not up-to-date, update the superblock
		 * and add it.
		 */
		int fd;
		for (i=0; i<first_super.raid_disks; i++) {
			int j = best[i];
			if (j>=0 &&
			    !devices[j].uptodate &&
			    devices[j].events > 0 &&
			    (chosen_drive < 0 ||
			     devices[j].events > devices[chosen_drive].events))
				chosen_drive = j;
		}
		if (chosen_drive < 0)
			break;
		fprintf(stderr, Name ": forcing event count in %s(%d) from %d upto %d\n",
			devices[chosen_drive].devname, devices[chosen_drive].raid_disk,
			(int)(devices[chosen_drive].events),
			(int)(devices[most_recent].events));
		fd = open(devices[chosen_drive].devname, O_RDWR);
		if (fd < 0) {
			fprintf(stderr, Name ": Couldn't open %s for write - not updating\n",
				devices[chosen_drive].devname);
			devices[chosen_drive].events = 0;
			continue;
		}
		if (load_super(fd, &super)) {
			close(fd);
			fprintf(stderr, Name ": RAID superblock disappeared from %s - not updating.\n",
				devices[chosen_drive].devname);
			devices[chosen_drive].events = 0;
			continue;
		}
		super.events_hi = (devices[most_recent].events>>32)&0xFFFFFFFF;
		super.events_lo = (devices[most_recent].events)&0xFFFFFFFF;
		super.sb_csum = calc_sb_csum(&super);
/*DRYRUN*/	if (store_super(fd, &super)) {
			close(fd);
			fprintf(stderr, Name ": Could not re-write superblock on %s\n",
				devices[chosen_drive].devname);
			devices[chosen_drive].events = 0;
			continue;
		}
		close(fd);
		devices[chosen_drive].events = devices[most_recent].events;
		devices[chosen_drive].uptodate = 1;
		okcnt++;
	}

	/* Now we want to look at the superblock which the kernel will base things on
	 * and compare the devices that we think are working with the devices that the
	 * superblock thinks are working.
	 * If there are differences and --force is given, then update this chosen
	 * superblock.
	 */
	for (i=0; chosen_drive < 0 && i<MD_SB_DISKS; i++) {
		int j = best[i];
		int fd;
		if (j<0)
			continue;
		if (!devices[j].uptodate)
			continue;
		chosen_drive = j;
		if ((fd=open(devices[j].devname, O_RDONLY))< 0) {
			fprintf(stderr, Name ": Cannot open %s: %s\n",
				devices[j].devname, strerror(errno));
			return 1;
		}
		if (load_super(fd, &super)) {
			close(fd);
			fprintf(stderr, Name ": RAID superblock has disappeared from %s\n",
				devices[j].devname);
			return 1;
		}
		close(fd);
	}

	for (i=0; i<MD_SB_DISKS; i++) {
		int j = best[i];
		if (j<0)
			continue;
		if (!devices[j].uptodate)
			continue;
		if (devices[j].major != super.disks[j].major ||
		    devices[j].minor != super.disks[j].minor) {
			change |= 1;
			super.disks[j].major = devices[j].major;
			super.disks[j].minor = devices[j].minor;
		}
		if (devices[j].uptodate &&
		    (super.disks[i].state & (1 << MD_DISK_FAULTY))) {
			if (force) {
				fprintf(stderr, Name ": "
					"clearing FAULT flag for device %d in %s for %s\n",
					j, mddev, devices[j].devname);
				super.disks[i].state &= ~(1<<MD_DISK_FAULTY);
				change |= 2;
			} else {
				fprintf(stderr, Name ": "
					"device %d in %s is marked faulty in superblock, but %s seems ok\n",
					i, mddev, devices[j].devname);
			}
		}
		if (!devices[j].uptodate &&
		    !(super.disks[i].state & (1 << MD_DISK_FAULTY))) {
			fprintf(stderr, Name ": devices %d of %s is not marked FAULTY in superblock, but cannot be found\n",
				i, mddev);
		}
	}

	if ((force && (change & 2))
	    || (old_linux && (change & 1))) {
		int fd;
		super.sb_csum = calc_sb_csum(&super);
		fd = open(devices[chosen_drive].devname, O_RDWR);
		if (fd < 0) {
			fprintf(stderr, Name ": Could open %s for write - cannot Assemble array.\n",
				devices[chosen_drive].devname);
			return 1;
		}
		if (store_super(fd, &super)) {
			close(fd);
			fprintf(stderr, Name ": Could not re-write superblock on %s\n",
				devices[chosen_drive].devname);
			return 1;
		}
		close(fd);
		change = 0;
	}

	/* Almost ready to actually *do* something */
	if (!old_linux) {
		if (ioctl(mdfd, SET_ARRAY_INFO, NULL) != 0) {
			fprintf(stderr, Name ": SET_ARRAY_INFO failed for %s: %s\n",
				mddev, strerror(errno));
			return 1;
		}
		/* First, add the raid disks, but add the chosen one last */
		for (i=0; i<=MD_SB_DISKS; i++) {
			int j;
			if (i < MD_SB_DISKS) {
				j = best[i];
				if (j == chosen_drive)
					continue;
			} else
				j = chosen_drive;

			if (j >= 0 && devices[j].uptodate) {
				mdu_disk_info_t disk;
				memset(&disk, 0, sizeof(disk));
				disk.major = devices[j].major;
				disk.minor = devices[j].minor;
				if (ioctl(mdfd, ADD_NEW_DISK, &disk)!=0) {
					fprintf(stderr, Name ": failed to add %s to %s: %s\n",
						devices[j].devname,
						mddev,
						strerror(errno));
					if (i < first_super.raid_disks)
						okcnt--;
					else
						sparecnt--;
				} else if (verbose)
					fprintf(stderr, Name ": added %s to %s as %d\n",
						devices[j].devname, mddev, devices[j].raid_disk);
			} else if (verbose && i < first_super.raid_disks)
				fprintf(stderr, Name ": no uptodate device for slot %d of %s\n",
					i, mddev);
		}
		
		if (runstop == 1 ||
		    (runstop == 0 && 
		     enough(first_super.level, first_super.raid_disks, okcnt))) {
			if (ioctl(mdfd, RUN_ARRAY, NULL)==0) {
				fprintf(stderr, Name ": %s has been started with %d drive%s",
					mddev, okcnt, okcnt==1?"":"s");
				if (sparecnt)
					fprintf(stderr, " and %d spare%s", sparecnt, sparecnt==1?"":"s");
				fprintf(stderr, ".\n");
				return 0;
			}
			fprintf(stderr, Name ": failed to RUN_ARRAY %s: %s\n",
				mddev, strerror(errno));
			return 1;
		}
		if (runstop == -1) {
			fprintf(stderr, Name ": %s assembled from %d drive%s, but not started.\n",
				mddev, okcnt, okcnt==1?"":"s");
			return 0;
		}
		fprintf(stderr, Name ": %s assembled from %d drive%s - not enough to start it.\n",
			mddev, okcnt, okcnt==1?"":"s");
		return 1;
	} else {
		/* The "chosen_drive" is a good choice, and if necessary, the superblock has
		 * been updated to point to the current locations of devices.
		 * so we can just start the array
		 */
		int dev;
		dev = MKDEV(devices[chosen_drive].major,
			    devices[chosen_drive].minor);
		if (ioctl(mdfd, START_ARRAY, dev)) {
		    fprintf(stderr, Name ": Cannot start array: %s\n",
			    strerror(errno));
		}
		
	}
}
