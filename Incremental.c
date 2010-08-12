/*
 * Incremental.c - support --incremental.  Part of:
 * mdadm - manage Linux "md" devices aka RAID arrays.
 *
 * Copyright (C) 2006-2009 Neil Brown <neilb@suse.de>
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
 *    Email: <neilb@suse.de>
 *    Paper: Neil Brown
 *           Novell Inc
 *           GPO Box Q1283
 *           QVB Post Office, NSW 1230
 *           Australia
 */

#include	"mdadm.h"

static int count_active(struct supertype *st, int mdfd, char **availp,
			struct mdinfo *info);
static void find_reject(int mdfd, struct supertype *st, struct mdinfo *sra,
			int number, __u64 events, int verbose,
			char *array_name);

int Incremental(char *devname, int verbose, int runstop,
		struct supertype *st, char *homehost, int require_homehost,
		int autof)
{
	/* Add this device to an array, creating the array if necessary
	 * and starting the array if sensible or - if runstop>0 - if possible.
	 *
	 * This has several steps:
	 *
	 * 1/ Check if device is permitted by mdadm.conf, reject if not.
	 * 2/ Find metadata, reject if none appropriate (check
	 *       version/name from args)
	 * 3/ Check if there is a match in mdadm.conf
	 * 3a/ if not, check for homehost match.  If no match, assemble as
	 *    a 'foreign' array.
	 * 4/ Determine device number.
	 * - If in mdadm.conf with std name, use that
	 * - UUID in /var/run/mdadm.map  use that
	 * - If name is suggestive, use that. unless in use with different uuid.
	 * - Choose a free, high number.
	 * - Use a partitioned device unless strong suggestion not to.
	 *         e.g. auto=md
	 *   Don't choose partitioned for containers.
	 * 5/ Find out if array already exists
	 * 5a/ if it does not
	 * - choose a name, from mdadm.conf or 'name' field in array.
	 * - create the array
	 * - add the device
	 * 5b/ if it does
	 * - check one drive in array to make sure metadata is a reasonably
	 *       close match.  Reject if not (e.g. different type)
	 * - add the device
	 * 6/ Make sure /var/run/mdadm.map contains this array.
	 * 7/ Is there enough devices to possibly start the array?
	 *     For a container, this means running Incremental_container.
	 * 7a/ if not, finish with success.
	 * 7b/ if yes,
	 * - read all metadata and arrange devices like -A does
	 * - if number of OK devices match expected, or -R and there are enough,
	 *   start the array (auto-readonly).
	 */
	struct stat stb;
	struct mdinfo info;
	struct mddev_ident_s *array_list, *match;
	char chosen_name[1024];
	int rv;
	struct map_ent *mp, *map = NULL;
	int dfd, mdfd;
	char *avail;
	int active_disks;
	int trustworthy = FOREIGN;
	char *name_to_use;
	mdu_array_info_t ainf;

	struct createinfo *ci = conf_get_create_info();


	/* 1/ Check if device is permitted by mdadm.conf */

	if (!conf_test_dev(devname)) {
		if (verbose >= 0)
			fprintf(stderr, Name
				": %s not permitted by mdadm.conf.\n",
				devname);
		return 1;
	}

	/* 2/ Find metadata, reject if none appropriate (check
	 *            version/name from args) */

	dfd = dev_open(devname, O_RDONLY|O_EXCL);
	if (dfd < 0) {
		if (verbose >= 0)
			fprintf(stderr, Name ": cannot open %s: %s.\n",
				devname, strerror(errno));
		return 1;
	}
	if (fstat(dfd, &stb) < 0) {
		if (verbose >= 0)
			fprintf(stderr, Name ": fstat failed for %s: %s.\n",
				devname, strerror(errno));
		close(dfd);
		return 1;
	}
	if ((stb.st_mode & S_IFMT) != S_IFBLK) {
		if (verbose >= 0)
			fprintf(stderr, Name ": %s is not a block device.\n",
				devname);
		close(dfd);
		return 1;
	}

	if (st == NULL && (st = guess_super(dfd)) == NULL) {
		if (verbose >= 0)
			fprintf(stderr, Name
				": no recognisable superblock on %s.\n",
				devname);
		close(dfd);
		return 1;
	}
	if (st->ss->load_super(st, dfd, NULL)) {
		if (verbose >= 0)
			fprintf(stderr, Name ": no RAID superblock on %s.\n",
				devname);
		close(dfd);
		return 1;
	}
	close (dfd);

	memset(&info, 0, sizeof(info));
	st->ss->getinfo_super(st, &info);
	/* 3/ Check if there is a match in mdadm.conf */

	array_list = conf_get_ident(NULL);
	match = NULL;
	for (; array_list; array_list = array_list->next) {
		if (array_list->uuid_set &&
		    same_uuid(array_list->uuid, info.uuid, st->ss->swapuuid)
		    == 0) {
			if (verbose >= 2 && array_list->devname)
				fprintf(stderr, Name
					": UUID differs from %s.\n",
					array_list->devname);
			continue;
		}
		if (array_list->name[0] &&
		    strcasecmp(array_list->name, info.name) != 0) {
			if (verbose >= 2 && array_list->devname)
				fprintf(stderr, Name
					": Name differs from %s.\n",
					array_list->devname);
			continue;
		}
		if (array_list->devices &&
		    !match_oneof(array_list->devices, devname)) {
			if (verbose >= 2 && array_list->devname)
				fprintf(stderr, Name
					": Not a listed device for %s.\n",
					array_list->devname);
			continue;
		}
		if (array_list->super_minor != UnSet &&
		    array_list->super_minor != info.array.md_minor) {
			if (verbose >= 2 && array_list->devname)
				fprintf(stderr, Name
					": Different super-minor to %s.\n",
					array_list->devname);
			continue;
		}
		if (!array_list->uuid_set &&
		    !array_list->name[0] &&
		    !array_list->devices &&
		    array_list->super_minor == UnSet) {
			if (verbose >= 2 && array_list->devname)
				fprintf(stderr, Name
			     ": %s doesn't have any identifying information.\n",
					array_list->devname);
			continue;
		}
		/* FIXME, should I check raid_disks and level too?? */

		if (match) {
			if (verbose >= 0) {
				if (match->devname && array_list->devname)
					fprintf(stderr, Name
		   ": we match both %s and %s - cannot decide which to use.\n",
						match->devname, array_list->devname);
				else
					fprintf(stderr, Name
						": multiple lines in mdadm.conf match\n");
			}
			return 2;
		}
		match = array_list;
	}

	if (match && match->devname
	    && strcasecmp(match->devname, "<ignore>") == 0) {
		if (verbose >= 0)
			fprintf(stderr, Name ": array containing %s is explicitly"
				" ignored by mdadm.conf\n",
				devname);
		return 1;
	}

	/* 3a/ if not, check for homehost match.  If no match, continue
	 * but don't trust the 'name' in the array. Thus a 'random' minor
	 * number will be assigned, and the device name will be based
	 * on that. */
	if (match)
		trustworthy = LOCAL;
	else if (st->ss->match_home(st, homehost) == 1)
		trustworthy = LOCAL;
	else if (st->ss->match_home(st, "any") == 1)
		trustworthy = LOCAL_ANY;
	else
		trustworthy = FOREIGN;


	if (!match && !conf_test_metadata(st->ss->name,
					  (trustworthy == LOCAL))) {
		if (verbose >= 1)
			fprintf(stderr, Name
				": %s has metadata type %s for which "
				"auto-assembly is disabled\n",
				devname, st->ss->name);
		return 1;
	}
	if (trustworthy == LOCAL_ANY)
		trustworthy = LOCAL;

	/* There are three possible sources for 'autof':  command line,
	 * ARRAY line in mdadm.conf, or CREATE line in mdadm.conf.
	 * ARRAY takes precedence, then command line, then
	 * CREATE.
	 */
	if (match && match->autof)
		autof = match->autof;
	if (autof == 0)
		autof = ci->autof;

	if (st->ss->container_content && st->loaded_container) {
		if ((runstop > 0 && info.container_enough >= 0) ||
		    info.container_enough > 0)
			/* pass */;
		else {
			if (verbose)
				fprintf(stderr, Name ": not enough devices to start the container\n");
			return 0;
		}

		/* This is a pre-built container array, so we do something
		 * rather different.
		 */
		return Incremental_container(st, devname, verbose, runstop,
					     autof, trustworthy);
	}

	name_to_use = info.name;
	if (name_to_use[0] == 0 &&
	    info.array.level == LEVEL_CONTAINER &&
	    trustworthy == LOCAL) {
		name_to_use = info.text_version;
		trustworthy = METADATA;
	}
	if (name_to_use[0] && trustworthy != LOCAL &&
	    ! require_homehost &&
	    conf_name_is_free(name_to_use))
		trustworthy = LOCAL;

	/* strip "hostname:" prefix from name if we have decided
	 * to treat it as LOCAL
	 */
	if (trustworthy == LOCAL && strchr(name_to_use, ':') != NULL)
		name_to_use = strchr(name_to_use, ':')+1;

	/* 4/ Check if array exists.
	 */
	if (map_lock(&map))
		fprintf(stderr, Name ": failed to get exclusive lock on "
			"mapfile\n");
	mp = map_by_uuid(&map, info.uuid);
	if (mp)
		mdfd = open_dev(mp->devnum);
	else
		mdfd = -1;

	if (mdfd < 0) {
		struct mdinfo *sra;
		struct mdinfo dinfo;

		/* Couldn't find an existing array, maybe make a new one */
		mdfd = create_mddev(match ? match->devname : NULL,
				    name_to_use, autof, trustworthy, chosen_name);

		if (mdfd < 0)
			return 1;

		sysfs_init(&info, mdfd, 0);

		if (set_array_info(mdfd, st, &info) != 0) {
			fprintf(stderr, Name ": failed to set array info for %s: %s\n",
				chosen_name, strerror(errno));
			close(mdfd);
			return 2;
		}

		dinfo = info;
		dinfo.disk.major = major(stb.st_rdev);
		dinfo.disk.minor = minor(stb.st_rdev);
		if (add_disk(mdfd, st, &info, &dinfo) != 0) {
			fprintf(stderr, Name ": failed to add %s to %s: %s.\n",
				devname, chosen_name, strerror(errno));
			ioctl(mdfd, STOP_ARRAY, 0);
			close(mdfd);
			return 2;
		}
		sra = sysfs_read(mdfd, fd2devnum(mdfd), GET_DEVS);
		if (!sra || !sra->devs || sra->devs->disk.raid_disk >= 0) {
			/* It really should be 'none' - must be old buggy
			 * kernel, and mdadm -I may not be able to complete.
			 * So reject it.
			 */
			ioctl(mdfd, STOP_ARRAY, NULL);
			fprintf(stderr, Name
		      ": You have an old buggy kernel which cannot support\n"
				"      --incremental reliably.  Aborting.\n");
			close(mdfd);
			sysfs_free(sra);
			return 2;
		}
		info.array.working_disks = 1;
		sysfs_free(sra);
		/* 6/ Make sure /var/run/mdadm.map contains this array. */
		map_update(&map, fd2devnum(mdfd),
			   info.text_version,
			   info.uuid, chosen_name);
	} else {
	/* 5b/ if it does */
	/* - check one drive in array to make sure metadata is a reasonably */
	/*        close match.  Reject if not (e.g. different type) */
	/* - add the device */
		char dn[20];
		int dfd2;
		int err;
		struct mdinfo *sra;
		struct supertype *st2;
		struct mdinfo info2, *d;

		if (mp->path)
			strcpy(chosen_name, mp->path);
		else
			strcpy(chosen_name, devnum2devname(mp->devnum));

		/* It is generally not OK to add non-spare drives to a
		 * running array as they are probably missing because
		 * they failed.  However if runstop is 1, then the
		 * array was possibly started early and our best be is
		 * to add this anyway.  It would probably be good to
		 * allow explicit policy statement about this.
		 */
		if ((info.disk.state & (1<<MD_DISK_SYNC)) != 0
		    && runstop < 1) {
			int active = 0;
			
			if (st->ss->external) {
				char *devname = devnum2devname(fd2devnum(mdfd));

				active = devname && is_container_active(devname);
				free(devname);
			} else if (ioctl(mdfd, GET_ARRAY_INFO, &ainf) == 0)
				active = 1;
			if (active) {
				fprintf(stderr, Name
					": not adding %s to active array (without --run) %s\n",
					devname, chosen_name);
				close(mdfd);
				return 2;
			}
		}
		sra = sysfs_read(mdfd, fd2devnum(mdfd), (GET_DEVS | GET_STATE));
		if (!sra)
			return 2;

		if (sra->devs) {
			sprintf(dn, "%d:%d", sra->devs->disk.major,
				sra->devs->disk.minor);
			dfd2 = dev_open(dn, O_RDONLY);
			st2 = dup_super(st);
			if (st2->ss->load_super(st2, dfd2, NULL) ||
			    st->ss->compare_super(st, st2) != 0) {
				fprintf(stderr, Name
					": metadata mismatch between %s and "
					"chosen array %s\n",
					devname, chosen_name);
				close(mdfd);
				close(dfd2);
				return 2;
			}
			close(dfd2);
			memset(&info2, 0, sizeof(info2));
			st2->ss->getinfo_super(st2, &info2);
			st2->ss->free_super(st2);
			if (info.array.level != info2.array.level ||
			    memcmp(info.uuid, info2.uuid, 16) != 0 ||
			    info.array.raid_disks != info2.array.raid_disks) {
				fprintf(stderr, Name
					": unexpected difference between %s and %s.\n",
					chosen_name, devname);
				close(mdfd);
				return 2;
			}
		}
		info2.disk.major = major(stb.st_rdev);
		info2.disk.minor = minor(stb.st_rdev);
		/* add disk needs to know about containers */
		if (st->ss->external)
			sra->array.level = LEVEL_CONTAINER;
		err = add_disk(mdfd, st, sra, &info2);
		if (err < 0 && errno == EBUSY) {
			/* could be another device present with the same
			 * disk.number. Find and reject any such
			 */
			find_reject(mdfd, st, sra, info.disk.number,
				    info.events, verbose, chosen_name);
			err = add_disk(mdfd, st, sra, &info2);
		}
		if (err < 0) {
			fprintf(stderr, Name ": failed to add %s to %s: %s.\n",
				devname, chosen_name, strerror(errno));
			close(mdfd);
			return 2;
		}
		info.array.working_disks = 0;
		for (d = sra->devs; d; d=d->next)
			info.array.working_disks ++;
			
	}

	/* 7/ Is there enough devices to possibly start the array? */
	/* 7a/ if not, finish with success. */
	if (info.array.level == LEVEL_CONTAINER) {
		/* Try to assemble within the container */
		map_unlock(&map);
		sysfs_uevent(&info, "change");
		if (verbose >= 0)
			fprintf(stderr, Name
				": container %s now has %d devices\n",
				chosen_name, info.array.working_disks);
		wait_for(chosen_name, mdfd);
		close(mdfd);
		rv = Incremental(chosen_name, verbose, runstop,
				 NULL, homehost, require_homehost, autof);
		if (rv == 1)
			/* Don't fail the whole -I if a subarray didn't
			 * have enough devices to start yet
			 */
			rv = 0;
		return rv;
	}
	avail = NULL;
	active_disks = count_active(st, mdfd, &avail, &info);
	if (enough(info.array.level, info.array.raid_disks,
		   info.array.layout, info.array.state & 1,
		   avail, active_disks) == 0) {
		free(avail);
		if (verbose >= 0)
			fprintf(stderr, Name
			     ": %s attached to %s, not enough to start (%d).\n",
				devname, chosen_name, active_disks);
		map_unlock(&map);
		close(mdfd);
		return 0;
	}
	free(avail);

	/* 7b/ if yes, */
	/* - if number of OK devices match expected, or -R and there */
	/*             are enough, */
	/*   + add any bitmap file  */
	/*   + start the array (auto-readonly). */

	if (ioctl(mdfd, GET_ARRAY_INFO, &ainf) == 0) {
		if (verbose >= 0)
			fprintf(stderr, Name
			   ": %s attached to %s which is already active.\n",
				devname, chosen_name);
		close(mdfd);
		map_unlock(&map);
		return 0;
	}

	map_unlock(&map);
	if (runstop > 0 || active_disks >= info.array.working_disks) {
		struct mdinfo *sra;
		/* Let's try to start it */
		if (match && match->bitmap_file) {
			int bmfd = open(match->bitmap_file, O_RDWR);
			if (bmfd < 0) {
				fprintf(stderr, Name
					": Could not open bitmap file %s.\n",
					match->bitmap_file);
				close(mdfd);
				return 1;
			}
			if (ioctl(mdfd, SET_BITMAP_FILE, bmfd) != 0) {
				close(bmfd);
				fprintf(stderr, Name
					": Failed to set bitmapfile for %s.\n",
					chosen_name);
				close(mdfd);
				return 1;
			}
			close(bmfd);
		}
		sra = sysfs_read(mdfd, fd2devnum(mdfd), 0);
		if ((sra == NULL || active_disks >= info.array.working_disks)
		    && trustworthy != FOREIGN)
			rv = ioctl(mdfd, RUN_ARRAY, NULL);
		else
			rv = sysfs_set_str(sra, NULL,
					   "array_state", "read-auto");
		if (rv == 0) {
			if (verbose >= 0)
				fprintf(stderr, Name
			   ": %s attached to %s, which has been started.\n",
					devname, chosen_name);
			rv = 0;
			wait_for(chosen_name, mdfd);
		} else {
			fprintf(stderr, Name
                             ": %s attached to %s, but failed to start: %s.\n",
				devname, chosen_name, strerror(errno));
			rv = 1;
		}
	} else {
		if (verbose >= 0)
			fprintf(stderr, Name
                          ": %s attached to %s, not enough to start safely.\n",
				devname, chosen_name);
		rv = 0;
	}
	close(mdfd);
	return rv;
}

static void find_reject(int mdfd, struct supertype *st, struct mdinfo *sra,
			int number, __u64 events, int verbose,
			char *array_name)
{
	/* Find a device attached to this array with a disk.number of number
	 * and events less than the passed events, and remove the device.
	 */
	struct mdinfo *d;
	mdu_array_info_t ra;

	if (ioctl(mdfd, GET_ARRAY_INFO, &ra) == 0)
		return; /* not safe to remove from active arrays
			 * without thinking more */

	for (d = sra->devs; d ; d = d->next) {
		char dn[10];
		int dfd;
		struct mdinfo info;
		sprintf(dn, "%d:%d", d->disk.major, d->disk.minor);
		dfd = dev_open(dn, O_RDONLY);
		if (dfd < 0)
			continue;
		if (st->ss->load_super(st, dfd, NULL)) {
			close(dfd);
			continue;
		}
		st->ss->getinfo_super(st, &info);
		st->ss->free_super(st);
		close(dfd);

		if (info.disk.number != number ||
		    info.events >= events)
			continue;

		if (d->disk.raid_disk > -1)
			sysfs_set_str(sra, d, "slot", "none");
		if (sysfs_set_str(sra, d, "state", "remove") == 0)
			if (verbose >= 0)
				fprintf(stderr, Name
					": removing old device %s from %s\n",
					d->sys_name+4, array_name);
	}
}

static int count_active(struct supertype *st, int mdfd, char **availp,
			struct mdinfo *bestinfo)
{
	/* count how many devices in sra think they are active */
	struct mdinfo *d;
	int cnt = 0, cnt1 = 0;
	__u64 max_events = 0;
	struct mdinfo *sra = sysfs_read(mdfd, -1, GET_DEVS | GET_STATE);
	char *avail = NULL;

	if (!sra)
		return 0;

	for (d = sra->devs ; d ; d = d->next) {
		char dn[30];
		int dfd;
		int ok;
		struct mdinfo info;

		sprintf(dn, "%d:%d", d->disk.major, d->disk.minor);
		dfd = dev_open(dn, O_RDONLY);
		if (dfd < 0)
			continue;
		ok =  st->ss->load_super(st, dfd, NULL);
		close(dfd);
		if (ok != 0)
			continue;
		st->ss->getinfo_super(st, &info);
		if (!avail) {
			avail = malloc(info.array.raid_disks);
			if (!avail) {
				fprintf(stderr, Name ": out of memory.\n");
				exit(1);
			}
			memset(avail, 0, info.array.raid_disks);
			*availp = avail;
		}

		if (info.disk.state & (1<<MD_DISK_SYNC))
		{
			if (cnt == 0) {
				cnt++;
				max_events = info.events;
				avail[info.disk.raid_disk] = 2;
				st->ss->getinfo_super(st, bestinfo);
			} else if (info.events == max_events) {
				cnt++;
				avail[info.disk.raid_disk] = 2;
			} else if (info.events == max_events-1) {
				cnt1++;
				avail[info.disk.raid_disk] = 1;
			} else if (info.events < max_events - 1)
				;
			else if (info.events == max_events+1) {
				int i;
				cnt1 = cnt;
				cnt = 1;
				max_events = info.events;
				for (i=0; i<info.array.raid_disks; i++)
					if (avail[i])
						avail[i]--;
				avail[info.disk.raid_disk] = 2;
				st->ss->getinfo_super(st, bestinfo);
			} else { /* info.events much bigger */
				cnt = 1; cnt1 = 0;
				memset(avail, 0, info.disk.raid_disk);
				max_events = info.events;
				st->ss->getinfo_super(st, bestinfo);
			}
		}
		st->ss->free_super(st);
	}
	return cnt + cnt1;
}

int IncrementalScan(int verbose)
{
	/* look at every device listed in the 'map' file.
	 * If one is found that is not running then:
	 *  look in mdadm.conf for bitmap file.
	 *   if one exists, but array has none, add it.
	 *  try to start array in auto-readonly mode
	 */
	struct map_ent *mapl = NULL;
	struct map_ent *me;
	mddev_ident_t devs, mddev;
	int rv = 0;

	map_read(&mapl);
	devs = conf_get_ident(NULL);

	for (me = mapl ; me ; me = me->next) {
		mdu_array_info_t array;
		mdu_bitmap_file_t bmf;
		struct mdinfo *sra;
		int mdfd = open_dev(me->devnum);

		if (mdfd < 0)
			continue;
		if (ioctl(mdfd, GET_ARRAY_INFO, &array) == 0 ||
		    errno != ENODEV) {
			close(mdfd);
			continue;
		}
		/* Ok, we can try this one.   Maybe it needs a bitmap */
		for (mddev = devs ; mddev ; mddev = mddev->next)
			if (mddev->devname && me->path
			    && devname_matches(mddev->devname, me->path))
				break;
		if (mddev && mddev->bitmap_file) {
			/*
			 * Note: early kernels will wrongly fail this, so it
			 * is a hint only
			 */
			int added = -1;
			if (ioctl(mdfd, GET_ARRAY_INFO, &bmf) < 0) {
				int bmfd = open(mddev->bitmap_file, O_RDWR);
				if (bmfd >= 0) {
					added = ioctl(mdfd, SET_BITMAP_FILE,
						      bmfd);
					close(bmfd);
				}
			}
			if (verbose >= 0) {
				if (added == 0)
					fprintf(stderr, Name
						": Added bitmap %s to %s\n",
						mddev->bitmap_file, me->path);
				else if (errno != EEXIST)
					fprintf(stderr, Name
					   ": Failed to add bitmap to %s: %s\n",
						me->path, strerror(errno));
			}
		}
		sra = sysfs_read(mdfd, 0, 0);
		if (sra) {
			if (sysfs_set_str(sra, NULL,
					  "array_state", "read-auto") == 0) {
				if (verbose >= 0)
					fprintf(stderr, Name
						": started array %s\n",
						me->path ?: devnum2devname(me->devnum));
			} else {
				fprintf(stderr, Name
					": failed to start array %s: %s\n",
					me->path ?: devnum2devname(me->devnum),
					strerror(errno));
				rv = 1;
			}
		}
	}
	return rv;
}

static char *container2devname(char *devname)
{
	char *mdname = NULL;

	if (devname[0] == '/') {
		int fd = open(devname, O_RDONLY);
		if (fd >= 0) {
			mdname = devnum2devname(fd2devnum(fd));
			close(fd);
		}
	} else {
		int uuid[4];
		struct map_ent *mp, *map = NULL;
					
		if (!parse_uuid(devname, uuid))
			return mdname;
		mp = map_by_uuid(&map, uuid);
		if (mp)
			mdname = devnum2devname(mp->devnum);
		map_free(map);
	}

	return mdname;
}

int Incremental_container(struct supertype *st, char *devname, int verbose,
			  int runstop, int autof, int trustworthy)
{
	/* Collect the contents of this container and for each
	 * array, choose a device name and assemble the array.
	 */

	struct mdinfo *list = st->ss->container_content(st);
	struct mdinfo *ra;
	struct map_ent *map = NULL;

	if (map_lock(&map))
		fprintf(stderr, Name ": failed to get exclusive lock on "
			"mapfile\n");

	for (ra = list ; ra ; ra = ra->next) {
		int mdfd;
		char chosen_name[1024];
		struct map_ent *mp;
		struct mddev_ident_s *match = NULL;

		mp = map_by_uuid(&map, ra->uuid);

		if (mp) {
			mdfd = open_dev(mp->devnum);
			if (mp->path)
				strcpy(chosen_name, mp->path);
			else
				strcpy(chosen_name, devnum2devname(mp->devnum));
		} else {

			/* Check in mdadm.conf for container == devname and
			 * member == ra->text_version after second slash.
			 */
			char *sub = strchr(ra->text_version+1, '/');
			struct mddev_ident_s *array_list;
			if (sub) {
				sub++;
				array_list = conf_get_ident(NULL);
			} else
				array_list = NULL;
			for(; array_list ; array_list = array_list->next) {
				char *dn;
				if (array_list->member == NULL ||
				    array_list->container == NULL)
					continue;
				if (strcmp(array_list->member, sub) != 0)
					continue;
				if (array_list->uuid_set &&
				    !same_uuid(ra->uuid, array_list->uuid, st->ss->swapuuid))
					continue;
				dn = container2devname(array_list->container);
				if (dn == NULL)
					continue;
				if (strncmp(dn, ra->text_version+1,
					    strlen(dn)) != 0 ||
				    ra->text_version[strlen(dn)+1] != '/') {
					free(dn);
					continue;
				}
				free(dn);
				/* we have a match */
				match = array_list;
				if (verbose>0)
					fprintf(stderr, Name ": match found for member %s\n",
						array_list->member);
				break;
			}

			if (match && match->devname &&
			    strcasecmp(match->devname, "<ignore>") == 0) {
				if (verbose > 0)
					fprintf(stderr, Name ": array %s/%s is "
						"explicitly ignored by mdadm.conf\n",
						match->container, match->member);
				return 2;
			}
			if (match)
				trustworthy = LOCAL;

			mdfd = create_mddev(match ? match->devname : NULL,
					    ra->name,
					    autof,
					    trustworthy,
					    chosen_name);
		}

		if (mdfd < 0) {
			fprintf(stderr, Name ": failed to open %s: %s.\n",
				chosen_name, strerror(errno));
			return 2;
		}

		assemble_container_content(st, mdfd, ra, runstop,
					   chosen_name, verbose);
	}
	map_unlock(&map);
	return 0;
}

/*
 * IncrementalRemove - Attempt to see if the passed in device belongs to any
 * raid arrays, and if so first fail (if needed) and then remove the device.
 *
 * @devname - The device we want to remove
 *
 * Note: the device name must be a kernel name like "sda", so
 * that we can find it in /proc/mdstat
 */
int IncrementalRemove(char *devname, int verbose)
{
	int mdfd;
	int rv;
	struct mdstat_ent *ent;
	struct mddev_dev_s devlist;

	if (strchr(devname, '/')) {
		fprintf(stderr, Name ": incremental removal requires a "
			"kernel device name, not a file: %s\n", devname);
		return 1;
	}
	ent = mdstat_by_component(devname);
	if (!ent) {
		fprintf(stderr, Name ": %s does not appear to be a component "
			"of any array\n", devname);
		return 1;
	}
	mdfd = open_dev(ent->devnum);
	if (mdfd < 0) {
		fprintf(stderr, Name ": Cannot open array %s!!\n", ent->dev);
		return 1;
	}
	memset(&devlist, 0, sizeof(devlist));
	devlist.devname = devname;
	devlist.disposition = 'f';
	Manage_subdevs(ent->dev, mdfd, &devlist, verbose, 0);
	devlist.disposition = 'r';
	rv = Manage_subdevs(ent->dev, mdfd, &devlist, verbose, 0);
	close(mdfd);
	return rv;
}
