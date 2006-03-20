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

int Assemble(struct supertype *st, char *mddev, int mdfd,
	     mddev_ident_t ident, char *conffile,
	     mddev_dev_t devlist,
	     int readonly, int runstop,
	     char *update,
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
	 * If mddev is not set, then scan must be set and we
	 *  read through the config file for dev+uuid mapping
	 *  We recurse, setting mddev, for each device that
	 *    - isn't running
	 *    - has a valid uuid (or any uuid if !uuidset)
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
	 * If !devlist and scan and uuidset, get list of devs from conf-file 
	 *
	 * For each device:
	 *   Check superblock - discard if bad
	 *   Check uuid (set if we don't have one) - discard if no match
	 *   Check superblock similarity if we have a superblock - discard if different
	 *   Record events, devicenum
	 * This should give us a list of devices for the array
	 * We should collect the most recent event number
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
	void *first_super = NULL, *super = NULL;
	struct {
		char *devname;
		unsigned int major, minor;
		unsigned int oldmajor, oldminor;
		long long events;
		int uptodate;
		int state;
		int raid_disk;
		int disk_nr;
	} *devices;
	int *best = NULL; /* indexed by raid_disk */
	unsigned int bestcnt = 0;
	int devcnt = 0;
	unsigned int okcnt, sparecnt;
	unsigned int req_cnt;
	unsigned int i;
	int most_recent = 0;
	int chosen_drive;
	int change = 0;
	int inargv = 0;
	int start_partial_ok = force || devlist==NULL;
	unsigned int num_devs;
	mddev_dev_t tmpdev;
	struct mdinfo info;
	struct mddev_ident_s ident2;
	char *avail;
	int nextspare = 0;
	
	vers = md_get_version(mdfd);
	if (vers <= 0) {
		fprintf(stderr, Name ": %s appears not to be an md device.\n", mddev);
		return 1;
	}
	if (vers < 9000) {
		fprintf(stderr, Name ": Assemble requires driver version 0.90.0 or later.\n"
			"    Upgrade your kernel or try --build\n");
		return 1;
	}
	if (get_linux_version() < 2004000)
		old_linux = 1;

	if (ioctl(mdfd, GET_ARRAY_INFO, &info.array)>=0) {
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

	if (!devlist &&
	    ident->uuid_set == 0 &&
	    ident->super_minor < 0 &&
	    ident->devices == NULL) {
		fprintf(stderr, Name ": No identity information available for %s - cannot assemble.\n",
			mddev);
		return 1;
	}
	if (devlist == NULL)
		devlist = conf_get_devs(conffile);
	else inargv = 1;

	tmpdev = devlist; num_devs = 0;
	while (tmpdev) {
		num_devs++;
		tmpdev = tmpdev->next;
	}
	devices = malloc(num_devs * sizeof(*devices));

	if (!st && ident->st) st = ident->st;

	if (verbose>0)
	    fprintf(stderr, Name ": looking for devices for %s\n",
		    mddev);

	while ( devlist) {
		char *devname;
		int dfd;
		struct stat stb;
		struct supertype *tst = st;

		devname = devlist->devname;
		devlist = devlist->next;

		if (ident->devices &&
		    !match_oneof(ident->devices, devname)) {
			if ((inargv && verbose>=0) || verbose > 0)
				fprintf(stderr, Name ": %s is not one of %s\n", devname, ident->devices);
			continue;
		}

		if (super) {
			free(super);
			super = NULL;
		}
		
		dfd = dev_open(devname, O_RDONLY|O_EXCL);
		if (dfd < 0) {
			if ((inargv && verbose >= 0) || verbose > 0)
				fprintf(stderr, Name ": cannot open device %s: %s\n",
					devname, strerror(errno));
		} else if (fstat(dfd, &stb)< 0) {
			/* Impossible! */
			fprintf(stderr, Name ": fstat failed for %s: %s\n",
				devname, strerror(errno));
		} else if ((stb.st_mode & S_IFMT) != S_IFBLK) {
			fprintf(stderr, Name ": %s is not a block device.\n",
				devname);
		} else if (!tst && (tst = guess_super(dfd)) == NULL) {
			if ((inargv && verbose >= 0) || verbose > 0)
				fprintf(stderr, Name ": no recogniseable superblock\n");
		} else if (tst->ss->load_super(tst,dfd, &super, NULL)) {
			if ((inargv && verbose >= 0) || verbose > 0)
				fprintf( stderr, Name ": no RAID superblock on %s\n",
					 devname);
		} else {
			tst->ss->getinfo_super(&info, &ident2, super);
		}
		if (dfd >= 0) close(dfd);

		if (ident->uuid_set && (!update || strcmp(update, "uuid")!= 0) &&
		    (!super || same_uuid(info.uuid, ident->uuid, tst->ss->swapuuid)==0)) {
			if ((inargv && verbose >= 0) || verbose > 0)
				fprintf(stderr, Name ": %s has wrong uuid.\n",
					devname);
			continue;
		}
		if (ident->name[0] &&
		    (!super || strncmp(ident2.name, ident->name, 32)!=0)) {
			if ((inargv && verbose >= 0) || verbose > 0)
				fprintf(stderr, Name ": %s has wrong name.\n",
					devname);
			continue;
		}
		if (ident->super_minor != UnSet &&
		    (!super || ident->super_minor != info.array.md_minor)) {
			if ((inargv && verbose >= 0) || verbose > 0)
				fprintf(stderr, Name ": %s has wrong super-minor.\n",
					devname);
			continue;
		}
		if (ident->level != UnSet &&
		    (!super|| ident->level != info.array.level)) {
			if ((inargv && verbose >= 0) || verbose > 0)
				fprintf(stderr, Name ": %s has wrong raid level.\n",
					devname);
			continue;
		}
		if (ident->raid_disks != UnSet &&
		    (!super || ident->raid_disks!= info.array.raid_disks)) {
			if ((inargv && verbose >= 0) || verbose > 0)
				fprintf(stderr, Name ": %s requires wrong number of drives.\n",
					devname);
			continue;
		}

		/* If we are this far, then we are commited to this device.
		 * If the super_block doesn't exist, or doesn't match others,
		 * then we cannot continue
		 */

		if (!super) {
			fprintf(stderr, Name ": %s has no superblock - assembly aborted\n",
				devname);
			free(first_super);
			return 1;
		}


		st = tst; /* commit to this format, if haven't already */
		if (st->ss->compare_super(&first_super, super)) {
			fprintf(stderr, Name ": superblock on %s doesn't match others - assembly aborted\n",
				devname);
			free(super);
			free(first_super);
			return 1;
		}

		/* looks like a good enough match to update the super block if needed */
		if (update) {
			/* prepare useful information in info structures */
			struct stat stb2;
			fstat(mdfd, &stb2);
			info.array.md_minor = minor(stb2.st_rdev);

			if (strcmp(update, "uuid")==0 &&
			    !ident->uuid_set) {
				int rfd;
				if ((rfd = open("/dev/urandom", O_RDONLY)) < 0 ||
				    read(rfd, ident->uuid, 16) != 16) {
					*(__u32*)(ident->uuid) = random();
					*(__u32*)(ident->uuid+1) = random();
					*(__u32*)(ident->uuid+2) = random();
					*(__u32*)(ident->uuid+3) = random();
				}
				if (rfd >= 0) close(rfd);
				ident->uuid_set = 1;
			}
			memcpy(info.uuid, ident->uuid, 16);
			st->ss->update_super(&info, super, update, devname, verbose);
			
			dfd = dev_open(devname, O_RDWR|O_EXCL);
			if (dfd < 0) 
				fprintf(stderr, Name ": Cannot open %s for superblock update\n",
					devname);
			else if (st->ss->store_super(st, dfd, super))
				fprintf(stderr, Name ": Could not re-write superblock on %s.\n",
					devname);
			if (dfd >= 0)
				close(dfd);
		}

		if (verbose > 0)
			fprintf(stderr, Name ": %s is identified as a member of %s, slot %d.\n",
				devname, mddev, info.disk.raid_disk);
		devices[devcnt].devname = devname;
		devices[devcnt].major = major(stb.st_rdev);
		devices[devcnt].minor = minor(stb.st_rdev);
		devices[devcnt].oldmajor = info.disk.major;
		devices[devcnt].oldminor = info.disk.minor;
		devices[devcnt].events = info.events;
		devices[devcnt].raid_disk = info.disk.raid_disk;
		devices[devcnt].disk_nr = info.disk.number;
		devices[devcnt].uptodate = 0;
		devices[devcnt].state = info.disk.state;
		if (most_recent < devcnt) {
			if (devices[devcnt].events
			    > devices[most_recent].events)
				most_recent = devcnt;
		}
		if (info.array.level == -4) 
			/* with multipath, the raid_disk from the superblock is meaningless */
			i = devcnt;
		else
			i = devices[devcnt].raid_disk;
		if (i+1 == 0) {
			if (nextspare < info.array.raid_disks)
				nextspare = info.array.raid_disks;
			i = nextspare++;
		}
		if (i < 10000) {
			if (i >= bestcnt) {
				unsigned int newbestcnt = i+10;
				int *newbest = malloc(sizeof(int)*newbestcnt);
				unsigned int c;
				for (c=0; c < newbestcnt; c++)
					if (c < bestcnt)
						newbest[c] = best[c];
					else
						newbest[c] = -1;
				if (best)free(best);
				best = newbest;
				bestcnt = newbestcnt;
			}
			if (best[i] == -1
			    || devices[best[i]].events < devices[devcnt].events)
				best[i] = devcnt;
		}
		devcnt++;
	}

	if (super)
		free(super);
	super = NULL;

	if (update && strcmp(update, "byteorder")==0)
		st->minor_version = 90;

	if (devcnt == 0) {
		fprintf(stderr, Name ": no devices found for %s\n",
			mddev);
		free(first_super);
		return 1;
	}

	st->ss->getinfo_super(&info, &ident2, first_super);

	/* now we have some devices that might be suitable.
	 * I wonder how many
	 */
	avail = malloc(info.array.raid_disks);
	memset(avail, 0, info.array.raid_disks);
	okcnt = 0;
	sparecnt=0;
	for (i=0; i< bestcnt ;i++) {
		int j = best[i];
		int event_margin = 1; /* always allow a difference of '1'
				       * like the kernel does
				       */
		if (j < 0) continue;
		/* note: we ignore error flags in multipath arrays
		 * as they don't make sense
		 */
		if (info.array.level != -4)
			if (!(devices[j].state & (1<<MD_DISK_SYNC))) {
				if (!(devices[j].state & (1<<MD_DISK_FAULTY)))
					sparecnt++;
				continue;
			}
		if (devices[j].events+event_margin >=
		    devices[most_recent].events) {
			devices[j].uptodate = 1;
			if (i < info.array.raid_disks) {
				okcnt++;
				avail[i]=1;
			} else
				sparecnt++;
		}
	}
	while (force && !enough(info.array.level, info.array.raid_disks,
				info.array.layout,
				avail, okcnt)) {
		/* Choose the newest best drive which is
		 * not up-to-date, update the superblock
		 * and add it.
		 */
		int fd;
		chosen_drive = -1;
		for (i=0; i<info.array.raid_disks && i < bestcnt; i++) {
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
		if (verbose >= 0)
			fprintf(stderr, Name ": forcing event count in %s(%d) from %d upto %d\n",
				devices[chosen_drive].devname, devices[chosen_drive].raid_disk,
				(int)(devices[chosen_drive].events),
				(int)(devices[most_recent].events));
		fd = dev_open(devices[chosen_drive].devname, O_RDWR|O_EXCL);
		if (fd < 0) {
			fprintf(stderr, Name ": Couldn't open %s for write - not updating\n",
				devices[chosen_drive].devname);
			devices[chosen_drive].events = 0;
			continue;
		}
		if (st->ss->load_super(st,fd, &super, NULL)) {
			close(fd);
			fprintf(stderr, Name ": RAID superblock disappeared from %s - not updating.\n",
				devices[chosen_drive].devname);
			devices[chosen_drive].events = 0;
			continue;
		}
		info.events = devices[most_recent].events;
		st->ss->update_super(&info, super, "force", devices[chosen_drive].devname, verbose);

		if (st->ss->store_super(st, fd, super)) {
			close(fd);
			fprintf(stderr, Name ": Could not re-write superblock on %s\n",
				devices[chosen_drive].devname);
			devices[chosen_drive].events = 0;
			free(super);
			continue;
		}
		close(fd);
		devices[chosen_drive].events = devices[most_recent].events;
		devices[chosen_drive].uptodate = 1;
		avail[chosen_drive] = 1;
		okcnt++;
		free(super);
	}

	/* Now we want to look at the superblock which the kernel will base things on
	 * and compare the devices that we think are working with the devices that the
	 * superblock thinks are working.
	 * If there are differences and --force is given, then update this chosen
	 * superblock.
	 */
	chosen_drive = -1;
	super = NULL;
	for (i=0; chosen_drive < 0 && i<bestcnt; i++) {
		int j = best[i];
		int fd;

		if (j<0)
			continue;
		if (!devices[j].uptodate)
			continue;
		chosen_drive = j;
		if ((fd=dev_open(devices[j].devname, O_RDONLY|O_EXCL))< 0) {
			fprintf(stderr, Name ": Cannot open %s: %s\n",
				devices[j].devname, strerror(errno));
			return 1;
		}
		if (st->ss->load_super(st,fd, &super, NULL)) {
			close(fd);
			fprintf(stderr, Name ": RAID superblock has disappeared from %s\n",
				devices[j].devname);
			return 1;
		}
		close(fd);
	}
	if (super == NULL) {
		fprintf(stderr, Name ": No suitable drives found for %s\n", mddev);
		return 1;
	}
	st->ss->getinfo_super(&info, &ident2, super);
	for (i=0; i<bestcnt; i++) {
		int j = best[i];
		unsigned int desired_state;

		if (i < info.array.raid_disks)
			desired_state = (1<<MD_DISK_ACTIVE) | (1<<MD_DISK_SYNC);
		else
			desired_state = 0;

		if (j<0)
			continue;
		if (!devices[j].uptodate)
			continue;
		info.disk.number = devices[j].disk_nr;
		info.disk.raid_disk = i;
		info.disk.state = desired_state;

		if (devices[j].uptodate &&
		    st->ss->update_super(&info, super, "assemble", NULL, verbose)) {
			if (force) {
				if (verbose >= 0)
					fprintf(stderr, Name ": "
						"clearing FAULTY flag for device %d in %s for %s\n",
						j, mddev, devices[j].devname);
				change = 1;
			} else {
				if (verbose >= -1)
					fprintf(stderr, Name ": "
						"device %d in %s has wrong state in superblock, but %s seems ok\n",
						i, mddev, devices[j].devname);
			}
		}
#if 0
		if (!devices[j].uptodate &&
		    !(super.disks[i].state & (1 << MD_DISK_FAULTY))) {
			fprintf(stderr, Name ": devices %d of %s is not marked FAULTY in superblock, but cannot be found\n",
				i, mddev);
		}
#endif
	}
	if (force && okcnt == info.array.raid_disks-1) {
		/* FIXME check event count */
		change += st->ss->update_super(&info, super, "force", 
					devices[chosen_drive].devname, verbose);
	}

	if (change) {
		int fd;
		fd = dev_open(devices[chosen_drive].devname, O_RDWR|O_EXCL);
		if (fd < 0) {
			fprintf(stderr, Name ": Could not open %s for write - cannot Assemble array.\n",
				devices[chosen_drive].devname);
			return 1;
		}
		if (st->ss->store_super(st, fd, super)) {
			close(fd);
			fprintf(stderr, Name ": Could not re-write superblock on %s\n",
				devices[chosen_drive].devname);
			return 1;
		}
		close(fd);
	}

	/* If we are in the middle of a reshape we may need to restore saved data
	 * that was moved aside due to the reshape overwriting live data
	 * The code of doing this lives in Grow.c
	 */
#ifndef MDASSEMBLE
	if (info.reshape_active) {
		int err = 0;
		int *fdlist = malloc(sizeof(int)* bestcnt);
		for (i=0; i<bestcnt; i++) {
			int j = best[i];
			if (j >= 0) {
				fdlist[i] = dev_open(devices[j].devname, O_RDWR|O_EXCL);
				if (fdlist[i] < 0) {
					fprintf(stderr, Name ": Could not open %s for write - cannot Assemble array.\n",
						devices[j].devname);
					err = 1;
					break;
				}
			} else
				fdlist[i] = -1;
		}
		if (!err)
			err = Grow_restart(st, &info, fdlist, bestcnt);
		while (i>0) {
			i--;
			if (fdlist[i]>=0) close(fdlist[i]);
		}
		if (err) {
			fprintf(stderr, Name ": Failed to restore critical section for reshape, sorry.\n");
			return err;
		}
	}
#endif
	/* count number of in-sync devices according to the superblock.
	 * We must have this number to start the array without -s or -R
	 */
	req_cnt = info.array.working_disks;

	/* Almost ready to actually *do* something */
	if (!old_linux) {
		int rv;
		if ((vers % 100) >= 1) { /* can use different versions */
			mdu_array_info_t inf;
			memset(&inf, 0, sizeof(inf));
			inf.major_version = st->ss->major;
			inf.minor_version = st->minor_version;
			rv = ioctl(mdfd, SET_ARRAY_INFO, &inf);
		} else 
			rv = ioctl(mdfd, SET_ARRAY_INFO, NULL);

		if (rv) {
			fprintf(stderr, Name ": SET_ARRAY_INFO failed for %s: %s\n",
				mddev, strerror(errno));
			return 1;
		}
		if (ident->bitmap_fd >= 0) {
			if (ioctl(mdfd, SET_BITMAP_FILE, ident->bitmap_fd) != 0) {
				fprintf(stderr, Name ": SET_BITMAP_FILE failed.\n");
				return 1;
			}
		}
					
		/* First, add the raid disks, but add the chosen one last */
		for (i=0; i<= bestcnt; i++) {
			int j;
			if (i < bestcnt) {
				j = best[i];
				if (j == chosen_drive)
					continue;
			} else
				j = chosen_drive;

			if (j >= 0 /* && devices[j].uptodate */) {
				mdu_disk_info_t disk;
				memset(&disk, 0, sizeof(disk));
				disk.major = devices[j].major;
				disk.minor = devices[j].minor;
				if (ioctl(mdfd, ADD_NEW_DISK, &disk)!=0) {
					fprintf(stderr, Name ": failed to add %s to %s: %s\n",
						devices[j].devname,
						mddev,
						strerror(errno));
					if (i < info.array.raid_disks || i == bestcnt)
						okcnt--;
					else
						sparecnt--;
				} else if (verbose > 0)
					fprintf(stderr, Name ": added %s to %s as %d\n",
						devices[j].devname, mddev, devices[j].raid_disk);
			} else if (verbose > 0 && i < info.array.raid_disks)
				fprintf(stderr, Name ": no uptodate device for slot %d of %s\n",
					i, mddev);
		}
		
		if (runstop == 1 ||
		    (runstop == 0 && 
		     ( enough(info.array.level, info.array.raid_disks, info.array.layout, avail, okcnt) &&
		       (okcnt >= req_cnt || start_partial_ok)
			     ))) {
			if (ioctl(mdfd, RUN_ARRAY, NULL)==0) {
				if (verbose >= 0) {
					fprintf(stderr, Name ": %s has been started with %d drive%s",
						mddev, okcnt, okcnt==1?"":"s");
					if (okcnt < info.array.raid_disks) 
						fprintf(stderr, " (out of %d)", info.array.raid_disks);
					if (sparecnt)
						fprintf(stderr, " and %d spare%s", sparecnt, sparecnt==1?"":"s");
					fprintf(stderr, ".\n");
				}
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
		if (verbose >= 0) {
			fprintf(stderr, Name ": %s assembled from %d drive%s", mddev, okcnt, okcnt==1?"":"s");
			if (sparecnt)
				fprintf(stderr, " and %d spare%s", sparecnt, sparecnt==1?"":"s");
			if (!enough(info.array.level, info.array.raid_disks, info.array.layout, avail, okcnt))
				fprintf(stderr, " - not enough to start the array.\n");
			else {
				if (req_cnt == info.array.raid_disks)
					fprintf(stderr, " - need all %d to start it", req_cnt);
				else
					fprintf(stderr, " - need %d of %d to start", req_cnt, info.array.raid_disks);
				fprintf(stderr, " (use --run to insist).\n");
			}
		}
		return 1;
	} else {
		/* The "chosen_drive" is a good choice, and if necessary, the superblock has
		 * been updated to point to the current locations of devices.
		 * so we can just start the array
		 */
		unsigned long dev;
		dev = makedev(devices[chosen_drive].major,
			    devices[chosen_drive].minor);
		if (ioctl(mdfd, START_ARRAY, dev)) {
		    fprintf(stderr, Name ": Cannot start array: %s\n",
			    strerror(errno));
		}
		
	}
	return 0;
}
