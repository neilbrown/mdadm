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

int Assemble(char *mddev, int mdfd,
	     int uuid[4], int uuidset,
	     char *conffile, int scan,
	     int subdevs, char **subdev,
	     int readonly, int runstop,
	     int verbose, int force)
{
	/*
	 * The task of Assemble is to submit a
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
	} devices[MD_SB_DISKS];
	int best[MD_SB_DISKS]; /* indexed by raid_disk */
	int devcnt = 0, okcnt;
	int i;
	int most_recent = 0;
	
	if (!mddev && !scan) {
		fputs(Name ": internal error - Assemble called with no devie or scan\n", stderr);
		return 1;
	}
	if (!mddev) {
		mddev_uuid_t device_list;
		int found = 0;
		device_list = conf_get_uuids(conffile);
		if (!device_list) {
			fprintf(stderr, Name ": No devices found in config file\n");
			return 1;
		}
		while (device_list) {
			if (!uuidset || same_uuid(device_list->uuid,uuid)) {
				mdfd = open(device_list->devname, O_RDONLY, 0);
				if (mdfd < 0) {
					fprintf(stderr,
						Name ": error opening %s: %s\n",
						device_list->devname,
						strerror(errno));
					continue;
				}
				if (Assemble(device_list->devname, mdfd,
					 device_list->uuid, 1,
					 conffile, 1,
					 subdevs, subdev,
					     readonly, runstop, verbose, force)==0)
					found++;
				close(mdfd);
			}
			device_list = device_list->next;
		}
		if (found)
			return 0;
		fprintf(stderr,Name ": Did not successful Assemble any devices\n");
		return 1;
	}

	/*
	 * Ok, we have an mddev, check it out
	 */
	vers = md_get_version(mdfd);
	if (vers <= 0) {
		fprintf(stderr, Name ": %s appears not to be an md device.\n");
		return 1;
	}
	if (vers < 9000) {
		fprintf(stderr, Name ": Assemble requires driver version 0.90.0 or later.\n"
			"    Upgrade your kernel or try --Build\n");
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
	 * We have a valid mddev, check out uuid
	 */
	if (!uuidset && scan) {
		/* device must be listed with uuid in conf file */
		mddev_uuid_t device_list;
		device_list = conf_get_uuids(conffile);
		while (device_list &&
		       strcmp(device_list->devname, mddev) != 0)
			device_list = device_list->next;

		if (!device_list) {
			fprintf(stderr, Name ": --scan set and no uuid found for %s in config file.\n",
				mddev);
			return 1;
		}
		/* the uuid is safe until next call to conf_get_uuids */
		uuid = device_list->uuid;
		uuidset = 1;
	}

	/* Now to start looking at devices.
	 * If no devices were given, but a uuid is available and
	 * --scan was set, then we should scan all devices listed in the
	 * config file
	 *
	 */
	if (subdevs==0 && scan && uuidset)
		devlist = conf_get_devs(conffile);

	if (subdevs == 0 && devlist == NULL) {
		fprintf(stderr, Name ": no devices given for %s\n", mddev);
		return 1;
	}
	/* now for each device */
	first_super.md_magic = 0;
	for (i=0; i<MD_SB_DISKS; i++)
		best[i] = -1;

	while (subdevs || devlist) {
		char *devname;
		int this_uuid[4];
		int dfd;
		struct stat stb;
		int inargv;
		if (subdevs) {
			devname = *subdev++;
			subdevs--;
			inargv=1;
		} else {
			devname = devlist->devname;
			devlist = devlist->next;
			inargv=0;
		}

		dfd = open(devname, O_RDONLY, 0);
		if (dfd < 0) {
			if (inargv || verbose)
				fprintf(stderr, Name ": cannot open device %s: %s\n",
					devname, strerror(errno));
			continue;
		}
		if (fstat(dfd, &stb)< 0) {
		    /* Impossible! */
		    fprintf(stderr, Name ": fstat failed for %s: %s\n",
			    devname, strerror(errno));
		    close(dfd);
		    continue;
		}
		if ((stb.st_mode & S_IFMT) != S_IFBLK) {
		    fprintf(stderr, Name ": %d is not a block device.\n",
			    devname);
		    close(dfd);
		    continue;
		}
		if (load_super(dfd, &super)) {
			if (inargv || verbose)
				fprintf( stderr, Name ": no RAID superblock on %s\n",
					 devname);
			close(dfd);
			continue;
		}
		close(dfd);
		if (compare_super(&first_super, &super)) {
			if (inargv || verbose)
				fprintf(stderr, Name ": superblock on %s doesn't match\n",
					devname);
			continue;
		}
		if (uuidset) {
			uuid_from_super(this_uuid, &first_super);
			if (!same_uuid(this_uuid, uuid)) {
				if (inargv || verbose)
					fprintf(stderr, Name ": %s has wrong uuid.\n",
						devname);
				continue;
			}
		} else {
			uuid_from_super(uuid, &first_super);
			uuidset = 1;
		}

		/* Ok, this one is at least worth considering */
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
		devices[devcnt].uptodate = 0;
		if (most_recent < devcnt) {
			if (devices[devcnt].events
			    > devices[most_recent].events)
				most_recent = devcnt;
		}
		i = super.this_disk.raid_disk;
		if (best[i] == -1
		    || devices[best[i]].events < devices[devcnt].events) {
			best[i] = devcnt;
		}
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
	for (i=0; i< first_super.raid_disks;i++) {
		int j = best[i];
		if (j < 0) continue;
		if (devices[j].events+1 >=
		    devices[most_recent].events) {
			devices[j].uptodate = 1;
			okcnt++;
		}
	}
	while (force && !enough(first_super.level, first_super.raid_disks, okcnt)) {
		/* Choose the newest best drive which is
		 * not up-to-date, update the superblock
		 * and add it.
		 */
		fprintf(stderr,"NotImplementedYet\n");
		/* FIXME */
		exit(2);
	}
	/* Almost ready to actually *do* something */
	if (!old_linux) {
		if (ioctl(mdfd, SET_ARRAY_INFO, NULL) != 0) {
			fprintf(stderr, Name ": SET_ARRAY_INFO failed for %s: %s\n",
				mddev, strerror(errno));
			return 1;
		}
		/* First, add the raid disks */
		for (i=0; i<first_super.raid_disks; i++) {
			int j = best[i];
			if (devices[j].uptodate) {
				mdu_disk_info_t disk;
				memset(&disk, 0, sizeof(disk));
				disk.major = devices[j].major;
				disk.minor = devices[j].minor;
				if (ioctl(mdfd, ADD_NEW_DISK, &disk)!=0) {
					fprintf(stderr, Name ": failed to add %s to %s: %s\n",
						devices[j].devname,
						mddev,
						strerror(errno));
				} else
					okcnt--;
			} else if (verbose)
				fprintf(stderr, Name ": no uptodate device for slot %d of %s\n",
					i, mddev);
		}
		if (runstop == 1 ||
		    (runstop == 0 && 
		     enough(first_super.level, first_super.raid_disks, okcnt))) {
			if (ioctl(mdfd, RUN_ARRAY, NULL)==0)
				return 0;
			fprintf(stderr, Name ": failed to RUN_ARRAY %s: %s\n",
				mddev, strerror(errno));
			return 1;
		}
		if (runstop == -1)
			return 0;
		else return 1;
	} else {
		/* FIXME */
		return 1;
	}
}
