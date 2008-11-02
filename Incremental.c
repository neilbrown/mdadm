/*
 * Incremental.c - support --incremental.  Part of:
 * mdadm - manage Linux "md" devices aka RAID arrays.
 *
 * Copyright (C) 2006 Neil Brown <neilb@suse.de>
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
		struct supertype *st, char *homehost, int autof)
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
	 * 3a/ if not, check for homehost match.  If no match, reject.
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
	int devnum;
	struct map_ent *mp, *map = NULL;
	int dfd, mdfd;
	char *avail;
	int active_disks;
	int uuid_for_name = 0;
	char *name_to_use;
	char nbuf[64];

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

	if (st->ss->container_content && st->loaded_container) {
		/* This is a pre-built container array, so we do something
		 * rather different.
		 */
		return Incremental_container(st, devname, verbose, runstop,
					     autof);
	}

	memset(&info, 0, sizeof(info));
	st->ss->getinfo_super(st, &info);
	/* 3/ Check if there is a match in mdadm.conf */

	array_list = conf_get_ident(NULL);
	match = NULL;
	for (; array_list; array_list = array_list->next) {
		if (array_list->uuid_set &&
		    same_uuid(array_list->uuid, info.uuid, st->ss->swapuuid)
		    == 0) {
			if (verbose >= 2)
				fprintf(stderr, Name
					": UUID differs from %s.\n",
					array_list->devname);
			continue;
		}
		if (array_list->name[0] &&
		    strcasecmp(array_list->name, info.name) != 0) {
			if (verbose >= 2)
				fprintf(stderr, Name
					": Name differs from %s.\n",
					array_list->devname);
			continue;
		}
		if (array_list->devices &&
		    !match_oneof(array_list->devices, devname)) {
			if (verbose >= 2)
				fprintf(stderr, Name
					": Not a listed device for %s.\n",
					array_list->devname);
			continue;
		}
		if (array_list->super_minor != UnSet &&
		    array_list->super_minor != info.array.md_minor) {
			if (verbose >= 2)
				fprintf(stderr, Name
					": Different super-minor to %s.\n",
					array_list->devname);
			continue;
		}
		if (!array_list->uuid_set &&
		    !array_list->name[0] &&
		    !array_list->devices &&
		    array_list->super_minor == UnSet) {
			if (verbose  >= 2)
				fprintf(stderr, Name
			     ": %s doesn't have any identifying information.\n",
					array_list->devname);
			continue;
		}
		/* FIXME, should I check raid_disks and level too?? */

		if (match) {
			if (verbose >= 0)
				fprintf(stderr, Name
		   ": we match both %s and %s - cannot decide which to use.\n",
					match->devname, array_list->devname);
			return 2;
		}
		match = array_list;
	}

	/* 3a/ if not, check for homehost match.  If no match, continue
	 * but don't trust the 'name' in the array. Thus a 'random' minor
	 * number will be assigned, and the device name will be based
	 * on that. */
	if (!match) {
		if (homehost == NULL ||
		       st->ss->match_home(st, homehost) != 1)
			uuid_for_name = 1;
	}
	/* 4/ Determine device number. */
	/* - If in mdadm.conf with std name, get number from name. */
	/* - UUID in /var/run/mdadm.map  get number from mapping */
	/* - If name is suggestive, use that. unless in use with */
	/*           different uuid. */
	/* - Choose a free, high number. */
	/* - Use a partitioned device unless strong suggestion not to. */
	/*         e.g. auto=md */
	mp = map_by_uuid(&map, info.uuid);

	if (uuid_for_name && ! mp) {
		name_to_use = fname_from_uuid(st, &info, nbuf, '-');
		if (verbose >= 0)
			fprintf(stderr, Name
		": not found in mdadm.conf and not identified by homehost"
				" - using uuid based name\n");
	} else
		name_to_use = info.name;

	/* There are three possible sources for 'autof':  command line,
	 * ARRAY line in mdadm.conf, or CREATE line in mdadm.conf.
	 * ARRAY takes precedence, then command line, then
	 * CREATE.
	 */
	if (match && match->autof)
		autof = match->autof;
	if (autof == 0)
		autof = ci->autof;

	if (match && (rv = is_standard(match->devname, &devnum))) {
		devnum = (rv > 0) ? (-1-devnum) : devnum;
	} else if (mp != NULL)
		devnum = mp->devnum;
	else {
		/* Have to guess a bit. */
		int use_partitions = 1;
		char *np, *ep;
		char *nm, nbuf[1024];
		struct stat stb2;

		if ((autof&7) == 3 || (autof&7) == 5)
			use_partitions = 0;
		if (st->ss->external)
			use_partitions = 0;
		np = strchr(name_to_use, ':');
		if (np)
			np++;
		else
			np = name_to_use;
		devnum = strtoul(np, &ep, 10);
		if (ep > np && *ep == 0) {
			/* This is a number.  Let check that it is unused. */
			if (mddev_busy(use_partitions ? (-1-devnum) : devnum))
				devnum = -1;
		} else
			devnum = -1;

		if (match)
			nm = match->devname;
		else {
			sprintf(nbuf, "/dev/md/%s", np);
			nm = nbuf;
		}
		if (stat(nm, &stb2) == 0 &&
		    S_ISBLK(stb2.st_mode) &&
		    major(stb2.st_rdev) == (use_partitions ?
					    get_mdp_major() : MD_MAJOR)) {
			if (use_partitions)
				devnum = minor(stb2.st_rdev) >> MdpMinorShift;
			else
				devnum = minor(stb2.st_rdev);
			if (mddev_busy(use_partitions ? (-1-devnum) : devnum))
				devnum = -1;
		}

		if (devnum < 0) {
			/* Haven't found anything yet, choose something free */
			devnum = find_free_devnum(use_partitions);

			if (devnum == NoMdDev) {
				fprintf(stderr, Name
					": No spare md devices!!\n");
				return 2;
			}
		} else
			devnum = use_partitions ? (-1-devnum) : devnum;
	}

	mdfd = open_mddev_devnum(match ? match->devname : mp ? mp->path : NULL,
				 devnum,
				 name_to_use,
				 chosen_name, autof >> 3);
	if (mdfd < 0) {
		fprintf(stderr, Name ": failed to open %s: %s.\n",
			chosen_name, strerror(errno));
		return 2;
	}
	sysfs_init(&info, mdfd, 0);

	/* 5/ Find out if array already exists */
	if (! mddev_busy(devnum)) {
	/* 5a/ if it does not */
	/* - choose a name, from mdadm.conf or 'name' field in array. */
	/* - create the array */
	/* - add the device */
		struct mdinfo *sra;
		struct mdinfo dinfo;

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
		sra = sysfs_read(mdfd, devnum, GET_DEVS);
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
		sra = sysfs_read(mdfd, devnum, (GET_DEVS | GET_STATE));

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
		info2.disk.major = major(stb.st_rdev);
		info2.disk.minor = minor(stb.st_rdev);
		/* add disk needs to know about containers */
		if (st->ss->external)
			sra->array.level = LEVEL_CONTAINER;
		err = add_disk(mdfd, st2, sra, &info2);
		if (err < 0 && errno == EBUSY) {
			/* could be another device present with the same
			 * disk.number. Find and reject any such
			 */
			find_reject(mdfd, st, sra, info.disk.number,
				    info.events, verbose, chosen_name);
			err = add_disk(mdfd, st2, sra, &info2);
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
	/* 6/ Make sure /var/run/mdadm.map contains this array. */
	map_update(&map, devnum,
		   info.text_version,
		   info.uuid, chosen_name);

	/* 7/ Is there enough devices to possibly start the array? */
	/* 7a/ if not, finish with success. */
	if (info.array.level == LEVEL_CONTAINER) {
		/* Try to assemble within the container */
		close(mdfd);
		if (verbose >= 0)
			fprintf(stderr, Name
				": container %s now has %d devices\n",
				chosen_name, info.array.working_disks);
		return Incremental(chosen_name, verbose, runstop,
				   NULL, homehost, autof);
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
		close(mdfd);
		return 0;
	}
	free(avail);

	/* 7b/ if yes, */
	/* - if number of OK devices match expected, or -R and there */
	/*             are enough, */
	/*   + add any bitmap file  */
	/*   + start the array (auto-readonly). */
{
	mdu_array_info_t ainf;

	if (ioctl(mdfd, GET_ARRAY_INFO, &ainf) == 0) {
		if (verbose >= 0)
			fprintf(stderr, Name
			   ": %s attached to %s which is already active.\n",
				devname, chosen_name);
		close (mdfd);
		return 0;
	}
}
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
		sra = sysfs_read(mdfd, devnum, 0);
		if ((sra == NULL || active_disks >= info.array.working_disks)
		    && uuid_for_name == 0)
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

void RebuildMap(void)
{
	struct mdstat_ent *mdstat = mdstat_read(0, 0);
	struct mdstat_ent *md;
	struct map_ent *map = NULL;
	int mdp = get_mdp_major();

	for (md = mdstat ; md ; md = md->next) {
		struct mdinfo *sra = sysfs_read(-1, md->devnum, GET_DEVS);
		struct mdinfo *sd;

		for (sd = sra->devs ; sd ; sd = sd->next) {
			char dn[30];
			int dfd;
			int ok;
			struct supertype *st;
			char *path;
			struct mdinfo info;

			sprintf(dn, "%d:%d", sd->disk.major, sd->disk.minor);
			dfd = dev_open(dn, O_RDONLY);
			if (dfd < 0)
				continue;
			st = guess_super(dfd);
			if ( st == NULL)
				ok = -1;
			else
				ok = st->ss->load_super(st, dfd, NULL);
			close(dfd);
			if (ok != 0)
				continue;
			st->ss->getinfo_super(st, &info);
			if (md->devnum > 0)
				path = map_dev(MD_MAJOR, md->devnum, 0);
			else
				path = map_dev(mdp, (-1-md->devnum)<< 6, 0);
			map_add(&map, md->devnum,
				info.text_version,
				info.uuid, path ? : "/unknown");
			st->ss->free_super(st);
			break;
		}
	}
	map_write(map);
	map_free(map);
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
		char path[1024];
		mdu_array_info_t array;
		mdu_bitmap_file_t bmf;
		struct mdinfo *sra;
		int mdfd = open_mddev_devnum(me->path, me->devnum,
					     NULL, path, 0);
		if (mdfd < 0)
			continue;
		if (ioctl(mdfd, GET_ARRAY_INFO, &array) == 0 ||
		    errno != ENODEV) {
			close(mdfd);
			continue;
		}
		/* Ok, we can try this one.   Maybe it needs a bitmap */
		for (mddev = devs ; mddev ; mddev = mddev->next)
			if (strcmp(mddev->devname, me->path) == 0)
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
						me->path);
			} else {
				fprintf(stderr, Name
					": failed to start array %s: %s\n",
					me->path, strerror(errno));
				rv = 1;
			}
		}
	}
	return rv;
}

static char *container2devname(char *devname)
{
	int fd = open(devname, O_RDONLY);
	char *mdname = NULL;

	if (fd >= 0) {
		mdname = devnum2devname(fd2devnum(fd));
		close(fd);
	}

	return mdname;
}

int Incremental_container(struct supertype *st, char *devname, int verbose,
			  int runstop, int autof)
{
	/* Collect the contents of this container and for each
	 * array, choose a device name and assemble the array.
	 */

	struct mdinfo *list = st->ss->container_content(st);
	struct mdinfo *ra;
	char *mdname = container2devname(devname);

	if (!mdname) {
		fprintf(stderr, Name": failed to determine device name\n");
		return 2;
	}

	for (ra = list ; ra ; ra = ra->next) {
		struct mdinfo *dev, *sra;
		int devnum = -1;
		int mdfd;
		char chosen_name[1024];
		int usepart = 1;
		char *n;
		int working = 0, preexist = 0;
		struct map_ent *mp, *map = NULL;
		char nbuf[64];
		char *name_to_use;
		struct mddev_ident_s *match = NULL;

		if ((autof&7) == 3 || (autof&7) == 5)
			usepart = 0;

		mp = map_by_uuid(&map, ra->uuid);

		name_to_use = ra->name;
		if (! name_to_use ||
		    ! *name_to_use ||
		    (*devname != '/' || strncmp("UUID-", strrchr(devname,'/')+1,5) == 0)
			)
			name_to_use = fname_from_uuid(st, ra, nbuf, '-');
		    
		if (!mp) {

			/* Check in mdadm.conf for devices == devname and
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
				int fd;
				char *dn;
				if (array_list->member == NULL ||
				    array_list->container == NULL)
					continue;
				if (strcmp(array_list->member, sub) != 0)
					continue;
				if (array_list->uuid_set &&
				    !same_uuid(ra->uuid, array_list->uuid, st->ss->swapuuid))
					continue;
				fd = open(array_list->container, O_RDONLY);
				if (fd < 0)
					continue;
				dn = devnum2devname(fd2devnum(fd));
				close(fd);
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
		}

		if (match && is_standard(match->devname, &devnum))
			/* we have devnum now */;
		else if (mp)
			devnum = mp->devnum;
		else if (is_standard(name_to_use, &devnum))
			/* have devnum */;
		else {
			n = name_to_use;
			if (*n == 'd')
				n++;
			if (*n && devnum < 0) {
				devnum = strtoul(n, &n, 10);
				if (devnum >= 0 && (*n == 0 || *n == ' ')) {
					/* Use this devnum */
					usepart = (name_to_use[0] == 'd');
					if (mddev_busy(usepart ? (-1-devnum) : devnum))
						devnum = -1;
				} else
					devnum = -1;
			}

			if (devnum < 0) {
				char *nm = name_to_use;
				char nbuf[1024];
				struct stat stb;
				if (strchr(nm, ':'))
					nm = strchr(nm, ':')+1;
				sprintf(nbuf, "/dev/md/%s", nm);

				if (stat(nbuf, &stb) == 0 &&
				    S_ISBLK(stb.st_mode) &&
				    major(stb.st_rdev) == (usepart ?
							   get_mdp_major() : MD_MAJOR)){
					if (usepart)
						devnum = minor(stb.st_rdev)
							>> MdpMinorShift;
					else
						devnum = minor(stb.st_rdev);
					if (mddev_busy(usepart ? (-1-devnum) : devnum))
						devnum = -1;
				}
			}

			if (devnum >= 0)
				devnum = usepart ? (-1-devnum) : devnum;
			else
				devnum = find_free_devnum(usepart);
		}
		mdfd = open_mddev_devnum(mp ? mp->path : match ? match->devname : NULL,
					 devnum, name_to_use,
					 chosen_name, autof>>3);

		if (mdfd < 0) {
			fprintf(stderr, Name ": failed to open %s: %s.\n",
				chosen_name, strerror(errno));
			return 2;
		}


		sysfs_init(ra, mdfd, 0);

		sra = sysfs_read(mdfd, 0, GET_VERSION);
		if (sra == NULL || strcmp(sra->text_version, ra->text_version) != 0)
			if (sysfs_set_array(ra, md_get_version(mdfd)) != 0)
				return 1;
		if (sra)
			sysfs_free(sra);

		for (dev = ra->devs; dev; dev = dev->next)
			if (sysfs_add_disk(ra, dev) == 0)
				working++;
			else if (errno == EEXIST)
				preexist++;
		if (working == 0)
			/* Nothing new, don't try to start */ ;
		else if (runstop > 0 ||
			 (working + preexist) >= ra->array.working_disks) {
			switch(ra->array.level) {
			case LEVEL_LINEAR:
			case LEVEL_MULTIPATH:
			case 0:
				sysfs_set_str(ra, NULL, "array_state",
					      "active");
				break;
			default:
				sysfs_set_str(ra, NULL, "array_state",
					      "readonly");
				/* start mdmon if needed. */
				if (!mdmon_running(st->container_dev))
					start_mdmon(st->container_dev);
				ping_monitor(devnum2devname(st->container_dev));
				break;
			}
			sysfs_set_safemode(ra, ra->safe_mode_delay);
			if (verbose >= 0) {
				fprintf(stderr, Name
					": Started %s with %d devices",
					chosen_name, working + preexist);
				if (preexist)
					fprintf(stderr, " (%d new)", working);
				fprintf(stderr, "\n");
			}
			/* FIXME should have an O_EXCL and wait for read-auto */
		} else
			if (verbose >= 0)
				fprintf(stderr, Name
					": %s assembled with %d devices but "
					"not started\n",
					chosen_name, working);
		close(mdfd);
		map_update(&map, devnum,
			   ra->text_version,
			   ra->uuid, chosen_name);
	}
	return 0;
}
