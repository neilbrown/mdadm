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
#include	<dirent.h>
#include	<ctype.h>

static int count_active(struct supertype *st, struct mdinfo *sra,
			int mdfd, char **availp,
			struct mdinfo *info);
static void find_reject(int mdfd, struct supertype *st, struct mdinfo *sra,
			int number, __u64 events, int verbose,
			char *array_name);
static int try_spare(char *devname, int *dfdp, struct dev_policy *pol,
		     struct map_ent *target,
		     struct supertype *st, int verbose);

static int Incremental_container(struct supertype *st, char *devname,
				 char *homehost,
				 int verbose, int runstop, int autof,
				 int freeze_reshape);

int Incremental(char *devname, int verbose, int runstop,
		struct supertype *st, char *homehost, int require_homehost,
		int autof, int freeze_reshape)
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
	struct mdinfo info, dinfo;
	struct mdinfo *sra = NULL, *d;
	struct mddev_ident *match;
	char chosen_name[1024];
	int rv = 1;
	struct map_ent *mp, *map = NULL;
	int dfd = -1, mdfd = -1;
	char *avail = NULL;
	int active_disks;
	int trustworthy;
	char *name_to_use;
	mdu_array_info_t ainf;
	struct dev_policy *policy = NULL;
	struct map_ent target_array;
	int have_target;

	struct createinfo *ci = conf_get_create_info();

	if (stat(devname, &stb) < 0) {
		if (verbose >= 0)
			fprintf(stderr, Name ": stat failed for %s: %s.\n",
				devname, strerror(errno));
		return rv;
	}
	if ((stb.st_mode & S_IFMT) != S_IFBLK) {
		if (verbose >= 0)
			fprintf(stderr, Name ": %s is not a block device.\n",
				devname);
		return rv;
	}
	dfd = dev_open(devname, O_RDONLY|O_EXCL);
	if (dfd < 0) {
		if (verbose >= 0)
			fprintf(stderr, Name ": cannot open %s: %s.\n",
				devname, strerror(errno));
		return rv;
	}
	/* If the device is a container, we do something very different */
	if (must_be_container(dfd)) {
		if (!st)
			st = super_by_fd(dfd, NULL);
		if (st && st->ss->load_container)
			rv = st->ss->load_container(st, dfd, NULL);

		close(dfd);
		if (!rv && st->ss->container_content) {
			if (map_lock(&map))
				fprintf(stderr, Name ": failed to get "
					"exclusive lock on mapfile\n");
			rv = Incremental_container(st, devname, homehost,
						   verbose, runstop, autof,
						   freeze_reshape);
			map_unlock(&map);
			return rv;
		}

		fprintf(stderr, Name ": %s is not part of an md array.\n",
			devname);
		return rv;
	}

	/* 1/ Check if device is permitted by mdadm.conf */

	if (!conf_test_dev(devname)) {
		if (verbose >= 0)
			fprintf(stderr, Name
				": %s not permitted by mdadm.conf.\n",
				devname);
		goto out;
	}

	/* 2/ Find metadata, reject if none appropriate (check
	 *            version/name from args) */

	if (fstat(dfd, &stb) < 0) {
		if (verbose >= 0)
			fprintf(stderr, Name ": fstat failed for %s: %s.\n",
				devname, strerror(errno));
		goto out;
	}
	if ((stb.st_mode & S_IFMT) != S_IFBLK) {
		if (verbose >= 0)
			fprintf(stderr, Name ": %s is not a block device.\n",
				devname);
		goto out;
	}

	dinfo.disk.major = major(stb.st_rdev);
	dinfo.disk.minor = minor(stb.st_rdev);

	policy = disk_policy(&dinfo);
	have_target = policy_check_path(&dinfo, &target_array);

	if (st == NULL && (st = guess_super(dfd)) == NULL) {
		if (verbose >= 0)
			fprintf(stderr, Name
				": no recognisable superblock on %s.\n",
				devname);
		rv = try_spare(devname, &dfd, policy,
			       have_target ? &target_array : NULL,
			       st, verbose);
		goto out;
	}
	if (st->ss->compare_super == NULL ||
	    st->ss->load_super(st, dfd, NULL)) {
		if (verbose >= 0)
			fprintf(stderr, Name ": no RAID superblock on %s.\n",
				devname);
		rv = try_spare(devname, &dfd, policy,
			       have_target ? &target_array : NULL,
			       st, verbose);
		free(st);
		goto out;
	}
	close (dfd); dfd = -1;

	st->ss->getinfo_super(st, &info, NULL);

	/* 3/ Check if there is a match in mdadm.conf */
	match = conf_match(st, &info, devname, verbose, &rv);
	if (!match && rv == 2)
		goto out;

	if (match && match->devname
	    && strcasecmp(match->devname, "<ignore>") == 0) {
		if (verbose >= 0)
			fprintf(stderr, Name ": array containing %s is explicitly"
				" ignored by mdadm.conf\n",
				devname);
		goto out;
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


	if (!match && !conf_test_metadata(st->ss->name, policy,
					  (trustworthy == LOCAL))) {
		if (verbose >= 1)
			fprintf(stderr, Name
				": %s has metadata type %s for which "
				"auto-assembly is disabled\n",
				devname, st->ss->name);
		goto out;
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

	name_to_use = info.name;
	if (name_to_use[0] == 0 &&
	    info.array.level == LEVEL_CONTAINER) {
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

		/* Couldn't find an existing array, maybe make a new one */
		mdfd = create_mddev(match ? match->devname : NULL,
				    name_to_use, autof, trustworthy, chosen_name);

		if (mdfd < 0)
			goto out_unlock;

		sysfs_init(&info, mdfd, 0);

		if (set_array_info(mdfd, st, &info) != 0) {
			fprintf(stderr, Name ": failed to set array info for %s: %s\n",
				chosen_name, strerror(errno));
			rv = 2;
			goto out_unlock;
		}

		dinfo = info;
		dinfo.disk.major = major(stb.st_rdev);
		dinfo.disk.minor = minor(stb.st_rdev);
		if (add_disk(mdfd, st, &info, &dinfo) != 0) {
			fprintf(stderr, Name ": failed to add %s to %s: %s.\n",
				devname, chosen_name, strerror(errno));
			ioctl(mdfd, STOP_ARRAY, 0);
			rv = 2;
			goto out_unlock;
		}
		sra = sysfs_read(mdfd, -1, (GET_DEVS | GET_STATE |
					    GET_OFFSET | GET_SIZE));
	
		if (!sra || !sra->devs || sra->devs->disk.raid_disk >= 0) {
			/* It really should be 'none' - must be old buggy
			 * kernel, and mdadm -I may not be able to complete.
			 * So reject it.
			 */
			ioctl(mdfd, STOP_ARRAY, NULL);
			fprintf(stderr, Name
		      ": You have an old buggy kernel which cannot support\n"
				"      --incremental reliably.  Aborting.\n");
			rv = 2;
			goto out_unlock;
		}
		info.array.working_disks = 1;
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
		struct supertype *st2;
		struct mdinfo info2, *d;

		sra = sysfs_read(mdfd, -1, (GET_DEVS | GET_STATE |
					    GET_OFFSET | GET_SIZE));
	
		if (mp->path)
			strcpy(chosen_name, mp->path);
		else
			strcpy(chosen_name, devnum2devname(mp->devnum));

		/* It is generally not OK to add non-spare drives to a
		 * running array as they are probably missing because
		 * they failed.  However if runstop is 1, then the
		 * array was possibly started early and our best bet is
		 * to add this anyway.
		 * Also if action policy is re-add or better we allow
		 * re-add.
		 * This doesn't apply to containers as the 'non-spare'
		 * flag has a different meaning.  The test has to happen
		 * at the device level there
		 */
		if (!st->ss->external
		    && (info.disk.state & (1<<MD_DISK_SYNC)) != 0
		    && ! policy_action_allows(policy, st->ss->name,
					      act_re_add)
		    && runstop < 1) {
			if (ioctl(mdfd, GET_ARRAY_INFO, &ainf) == 0) {
				fprintf(stderr, Name
					": not adding %s to active array (without --run) %s\n",
					devname, chosen_name);
				rv = 2;
				goto out_unlock;
			}
		}
		if (!sra) {
			rv = 2;
			goto out_unlock;
		}
		if (sra->devs) {
			sprintf(dn, "%d:%d", sra->devs->disk.major,
				sra->devs->disk.minor);
			dfd2 = dev_open(dn, O_RDONLY);
			if (dfd2 < 0) {
				fprintf(stderr, Name
					": unable to open %s\n", devname);
				rv = 2;
				goto out_unlock;
			}
			st2 = dup_super(st);
			if (st2->ss->load_super(st2, dfd2, NULL) ||
			    st->ss->compare_super(st, st2) != 0) {
				fprintf(stderr, Name
					": metadata mismatch between %s and "
					"chosen array %s\n",
					devname, chosen_name);
				close(dfd2);
				rv = 2;
				goto out_unlock;
			}
			close(dfd2);
			st2->ss->getinfo_super(st2, &info2, NULL);
			st2->ss->free_super(st2);
			if (info.array.level != info2.array.level ||
			    memcmp(info.uuid, info2.uuid, 16) != 0 ||
			    info.array.raid_disks != info2.array.raid_disks) {
				fprintf(stderr, Name
					": unexpected difference between %s and %s.\n",
					chosen_name, devname);
				rv = 2;
				goto out_unlock;
			}
		}
		info.disk.major = major(stb.st_rdev);
		info.disk.minor = minor(stb.st_rdev);
		/* add disk needs to know about containers */
		if (st->ss->external)
			sra->array.level = LEVEL_CONTAINER;
		err = add_disk(mdfd, st, sra, &info);
		if (err < 0 && errno == EBUSY) {
			/* could be another device present with the same
			 * disk.number. Find and reject any such
			 */
			find_reject(mdfd, st, sra, info.disk.number,
				    info.events, verbose, chosen_name);
			err = add_disk(mdfd, st, sra, &info);
		}
		if (err < 0) {
			fprintf(stderr, Name ": failed to add %s to %s: %s.\n",
				devname, chosen_name, strerror(errno));
			rv = 2;
			goto out_unlock;
		}
		info.array.working_disks = 0;
		for (d = sra->devs; d; d=d->next)
			info.array.working_disks ++;
			
	}

	/* 7/ Is there enough devices to possibly start the array? */
	/* 7a/ if not, finish with success. */
	if (info.array.level == LEVEL_CONTAINER) {
		int devnum = devnum; /* defined and used iff ->external */
		/* Try to assemble within the container */
		sysfs_uevent(sra, "change");
		if (verbose >= 0)
			fprintf(stderr, Name
				": container %s now has %d device%s\n",
				chosen_name, info.array.working_disks,
				info.array.working_disks==1?"":"s");
		wait_for(chosen_name, mdfd);
		if (st->ss->external)
			devnum = fd2devnum(mdfd);
		if (st->ss->load_container)
			rv = st->ss->load_container(st, mdfd, NULL);
		close(mdfd);
		sysfs_free(sra);
		if (!rv)
			rv = Incremental_container(st, chosen_name, homehost,
						   verbose, runstop, autof,
						   freeze_reshape);
		map_unlock(&map);
		if (rv == 1)
			/* Don't fail the whole -I if a subarray didn't
			 * have enough devices to start yet
			 */
			rv = 0;
		/* after spare is added, ping monitor for external metadata
		 * so that it can eg. try to rebuild degraded array */
		if (st->ss->external)
			ping_monitor_by_id(devnum);
		return rv;
	}

	/* We have added something to the array, so need to re-read the
	 * state.  Eventually this state should be kept up-to-date as
	 * things change.
	 */
	sysfs_free(sra);
	sra = sysfs_read(mdfd, -1, (GET_DEVS | GET_STATE |
				    GET_OFFSET | GET_SIZE));
	active_disks = count_active(st, sra, mdfd, &avail, &info);
	if (enough(info.array.level, info.array.raid_disks,
		   info.array.layout, info.array.state & 1,
		   avail) == 0) {
		if (verbose >= 0)
			fprintf(stderr, Name
			     ": %s attached to %s, not enough to start (%d).\n",
				devname, chosen_name, active_disks);
		rv = 0;
		goto out_unlock;
	}

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
		rv = 0;
		goto out_unlock;
	}

	map_unlock(&map);
	if (runstop > 0 || active_disks >= info.array.working_disks) {
		struct mdinfo *dsk;
		/* Let's try to start it */
		if (match && match->bitmap_file) {
			int bmfd = open(match->bitmap_file, O_RDWR);
			if (bmfd < 0) {
				fprintf(stderr, Name
					": Could not open bitmap file %s.\n",
					match->bitmap_file);
				goto out;
			}
			if (ioctl(mdfd, SET_BITMAP_FILE, bmfd) != 0) {
				close(bmfd);
				fprintf(stderr, Name
					": Failed to set bitmapfile for %s.\n",
					chosen_name);
				goto out;
			}
			close(bmfd);
		}
		/* Need to remove from the array any devices which
		 * 'count_active' discerned were too old or inappropriate
		 */
		for (d = sra ? sra->devs : NULL ; d ; d = d->next)
			if (d->disk.state & (1<<MD_DISK_REMOVED))
				remove_disk(mdfd, st, sra, d);

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
			/* We just started the array, so some devices
			 * might have been evicted from the array
			 * because their event counts were too old.
			 * If the action=re-add policy is in-force for
			 * those devices we should re-add them now.
			 */
			for (dsk = sra->devs; dsk ; dsk = dsk->next) {
				if (disk_action_allows(dsk, st->ss->name, act_re_add) &&
				    add_disk(mdfd, st, sra, dsk) == 0)
					fprintf(stderr, Name
						": %s re-added to %s\n",
						dsk->sys_name, chosen_name);
			}
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
out:
	free(avail);
	if (dfd >= 0)
		close(dfd);
	if (mdfd >= 0)
		close(mdfd);
	if (policy)
		dev_policy_free(policy);
	if (sra)
		sysfs_free(sra);
	return rv;
out_unlock:
	map_unlock(&map);
	goto out;
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
		st->ss->getinfo_super(st, &info, NULL);
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

static int count_active(struct supertype *st, struct mdinfo *sra,
			int mdfd, char **availp,
			struct mdinfo *bestinfo)
{
	/* count how many devices in sra think they are active */
	struct mdinfo *d;
	int cnt = 0;
	__u64 max_events = 0;
	char *avail = NULL;
	int *best = NULL;
	char *devmap = NULL;
	int numdevs = 0;
	int devnum;
	int b, i;
	int raid_disks = 0;

	if (!sra)
		return 0;

	for (d = sra->devs ; d ; d = d->next)
		numdevs++;
	for (d = sra->devs, devnum=0 ; d ; d = d->next, devnum++) {
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
		info.array.raid_disks = raid_disks;
		st->ss->getinfo_super(st, &info, devmap + raid_disks * devnum);
		if (!avail) {
			raid_disks = info.array.raid_disks;
			avail = calloc(raid_disks, 1);
			if (!avail) {
				fprintf(stderr, Name ": out of memory.\n");
				exit(1);
			}
			*availp = avail;

			best = calloc(raid_disks, sizeof(int));
			devmap = calloc(raid_disks * numdevs, 1);

			st->ss->getinfo_super(st, &info, devmap);
		}

		if (info.disk.state & (1<<MD_DISK_SYNC))
		{
			if (cnt == 0) {
				cnt++;
				max_events = info.events;
				avail[info.disk.raid_disk] = 2;
				best[info.disk.raid_disk] = devnum;
				st->ss->getinfo_super(st, bestinfo, NULL);
			} else if (info.events == max_events) {
				avail[info.disk.raid_disk] = 2;
				best[info.disk.raid_disk] = devnum;
			} else if (info.events == max_events-1) {
				if (avail[info.disk.raid_disk] == 0) {
					avail[info.disk.raid_disk] = 1;
					best[info.disk.raid_disk] = devnum;
				}
			} else if (info.events < max_events - 1)
				;
			else if (info.events == max_events+1) {
				int i;
				max_events = info.events;
				for (i=0; i < raid_disks; i++)
					if (avail[i])
						avail[i]--;
				avail[info.disk.raid_disk] = 2;
				best[info.disk.raid_disk] = devnum;
				st->ss->getinfo_super(st, bestinfo, NULL);
			} else { /* info.events much bigger */
				memset(avail, 0, raid_disks);
				max_events = info.events;
				avail[info.disk.raid_disk] = 2;
				best[info.disk.raid_disk] = devnum;
				st->ss->getinfo_super(st, bestinfo, NULL);
			}
		}
		st->ss->free_super(st);
	}
	if (!avail)
		return 0;
	/* We need to reject any device that thinks the best device is
	 * failed or missing */
	for (b = 0; b < raid_disks; b++)
		if (avail[b] == 2)
			break;
	cnt = 0;
	for (i = 0 ; i < raid_disks ; i++) {
		if (i != b && avail[i])
			if (devmap[raid_disks * best[i] + b] == 0) {
				/* This device thinks 'b' is failed -
				 * don't use it */
				devnum = best[i];
				for (d=sra->devs ; devnum; d = d->next)
					devnum--;
				d->disk.state |= (1 << MD_DISK_REMOVED);
				avail[i] = 0;
			}
		if (avail[i])
			cnt++;
	}
	free(best);
	free(devmap);
	return cnt;
}

/* test if container has degraded member(s) */
static int container_members_max_degradation(struct map_ent *map, struct map_ent *me)
{
	mdu_array_info_t array;
	int afd;
	int max_degraded = 0;

	for(; map; map = map->next) {
		if (!is_subarray(map->metadata) ||
		    devname2devnum(map->metadata+1) != me->devnum)
			continue;
		afd = open_dev(map->devnum);
		if (afd < 0)
			continue;
		/* most accurate information regarding array degradation */
		if (ioctl(afd, GET_ARRAY_INFO, &array) >= 0) {
			int degraded = array.raid_disks - array.active_disks -
				       array.spare_disks;
			if (degraded > max_degraded)
				max_degraded = degraded;
		}
		close(afd);
	}
	return (max_degraded);
}

static int array_try_spare(char *devname, int *dfdp, struct dev_policy *pol,
			   struct map_ent *target, int bare,
			   struct supertype *st, int verbose)
{
	/* This device doesn't have any md metadata
	 * The device policy allows 'spare' and if !bare, it allows spare-same-slot.
	 * If 'st' is not set, then we only know that some metadata allows this,
	 * others possibly don't.
	 * So look for a container or array to attach the device to.
	 * Prefer 'target' if that is set and the array is found.
	 *
	 * If st is set, then only arrays of that type are considered
	 * Return 0 on success, or some exit code on failure, probably 1.
	 */
	int rv = 1;
	struct stat stb;
	struct map_ent *mp, *map = NULL;
	struct mdinfo *chosen = NULL;
	int dfd = *dfdp;

	if (fstat(dfd, &stb) != 0)
		return 1;

	/*
	 * Now we need to find a suitable array to add this to.
	 * We only accept arrays that:
	 *  - match 'st'
	 *  - are in the same domains as the device
	 *  - are of an size for which the device will be useful
	 * and we choose the one that is the most degraded
	 */

	if (map_lock(&map)) {
		fprintf(stderr, Name ": failed to get exclusive lock on "
			"mapfile\n");
		return 1;
	}
	for (mp = map ; mp ; mp = mp->next) {
		struct supertype *st2;
		struct domainlist *dl = NULL;
		struct mdinfo *sra;
		unsigned long long devsize;
		unsigned long long component_size = 0;

		if (is_subarray(mp->metadata))
			continue;
		if (st) {
			st2 = st->ss->match_metadata_desc(mp->metadata);
			if (!st2 ||
			    (st->minor_version >= 0 &&
			     st->minor_version != st2->minor_version)) {
				if (verbose > 1)
					fprintf(stderr, Name ": not adding %s to %s as metadata type doesn't match\n",
						devname, mp->path);
				free(st2);
				continue;
			}
			free(st2);
		}
		sra = sysfs_read(-1, mp->devnum,
				 GET_DEVS|GET_OFFSET|GET_SIZE|GET_STATE|
				 GET_DEGRADED|GET_COMPONENT|GET_VERSION);
		if (!sra) {
			/* Probably a container - no degraded info */
			sra = sysfs_read(-1, mp->devnum,
					 GET_DEVS|GET_OFFSET|GET_SIZE|GET_STATE|
					 GET_COMPONENT|GET_VERSION);
			if (sra)
				sra->array.failed_disks = -1;
		}
		if (!sra)
			continue;
		if (st == NULL) {
			int i;
			st2 = NULL;
			for(i=0; !st2 && superlist[i]; i++)
				st2 = superlist[i]->match_metadata_desc(
					sra->text_version);
			if (!st2) {
				if (verbose > 1)
					fprintf(stderr, Name ": not adding %s to %s"
						" as metadata not recognised.\n",
						devname, mp->path);
				goto next;
			}
			/* Need to double check the 'act_spare' permissions applies
			 * to this metadata.
			 */
			if (!policy_action_allows(pol, st2->ss->name, act_spare))
				goto next;
			if (!bare && !policy_action_allows(pol, st2->ss->name,
							   act_spare_same_slot))
				goto next;
		} else
			st2 = st;
		/* update number of failed disks for mostly degraded
		 * container member */
		if (sra->array.failed_disks == -1)
			sra->array.failed_disks = container_members_max_degradation(map, mp);

		get_dev_size(dfd, NULL, &devsize);
		if (sra->component_size == 0) {
			/* true for containers, here we must read superblock
			 * to obtain minimum spare size */
			struct supertype *st3 = dup_super(st2);
			int mdfd = open_dev(mp->devnum);
			if (mdfd < 0) {
				free(st3);
				goto next;
			}
			if (st3->ss->load_container &&
			    !st3->ss->load_container(st3, mdfd, mp->path)) {
				component_size = st3->ss->min_acceptable_spare_size(st3);
				st3->ss->free_super(st3);
			}
			free(st3);
			close(mdfd);
		}
		if ((sra->component_size > 0 &&
		     st2->ss->avail_size(st2, devsize) < sra->component_size)
		    ||
		    (sra->component_size == 0 && devsize < component_size)) {
			if (verbose > 1)
				fprintf(stderr, Name ": not adding %s to %s as it is too small\n",
					devname, mp->path);
			goto next;
		}
		/* test against target.
		 * If 'target' is set and 'bare' is false, we only accept
		 * arrays/containers that match 'target'.
		 * If 'target' is set and 'bare' is true, we prefer the
		 * array which matches 'target'.
		 * target is considered only if we deal with degraded array
		 */
		if (target && policy_action_allows(pol, st2->ss->name,
						   act_spare_same_slot)) {
			if (strcmp(target->metadata, mp->metadata) == 0 &&
			    memcmp(target->uuid, mp->uuid,
				   sizeof(target->uuid)) == 0 &&
			    sra->array.failed_disks > 0) {
				/* This is our target!! */
				if (chosen)
					sysfs_free(chosen);
				chosen = sra;
				sra = NULL;
				/* skip to end so we don't check any more */
				while (mp->next)
					mp = mp->next;
				goto next;
			}
			/* not our target */
			if (!bare)
				goto next;
		}

		dl = domain_from_array(sra, st2->ss->name);
		if (domain_test(dl, pol, st2->ss->name) != 1) {
			/* domain test fails */
			if (verbose > 1)
				fprintf(stderr, Name ": not adding %s to %s as"
					" it is not in a compatible domain\n",
					devname, mp->path);

			goto next;
		}
		/* all tests passed, OK to add to this array */
		if (!chosen) {
			chosen = sra;
			sra = NULL;
		} else if (chosen->array.failed_disks < sra->array.failed_disks) {
			sysfs_free(chosen);
			chosen = sra;
			sra = NULL;
		}
	next:
		if (sra)
			sysfs_free(sra);
		if (st != st2)
			free(st2);
		if (dl)
			domain_free(dl);
	}
	if (chosen) {
		/* add current device to chosen array as a spare */
		int mdfd = open_dev(devname2devnum(chosen->sys_name));
		if (mdfd >= 0) {
			struct mddev_dev devlist;
			char devname[20];
			devlist.next = NULL;
			devlist.used = 0;
			devlist.re_add = 0;
			devlist.writemostly = 0;
			devlist.devname = devname;
			sprintf(devname, "%d:%d", major(stb.st_rdev),
				minor(stb.st_rdev));
			devlist.disposition = 'a';
			close(dfd);
			*dfdp = -1;
			rv =  Manage_subdevs(chosen->sys_name, mdfd, &devlist,
					     -1, 0, NULL, 0);
			close(mdfd);
		}
		if (verbose > 0) {
			if (rv == 0)
				fprintf(stderr, Name ": added %s as spare for %s\n",
					devname, chosen->sys_name);
			else
				fprintf(stderr, Name ": failed to add %s as spare for %s\n",
					devname, chosen->sys_name);
		}
		sysfs_free(chosen);
	}
	map_unlock(&map);
	return rv;
}

static int partition_try_spare(char *devname, int *dfdp, struct dev_policy *pol,
			       struct supertype *st, int verbose)
{
	/* we know that at least one partition virtual-metadata is
	 * allowed to incorporate spares like this device.  We need to
	 * find a suitable device to copy partition information from.
	 *
	 * Getting a list of all disk (not partition) devices is
	 * slightly non-trivial.  We could look at /sys/block, but
	 * that is theoretically due to be removed.  Maybe best to use
	 * /dev/disk/by-path/?* and ignore names ending '-partNN' as
	 * we depend on this directory of 'path' info.  But that fails
	 * to find loop devices and probably others.  Maybe don't
	 * worry about that, they aren't the real target.
	 *
	 * So: check things in /dev/disk/by-path to see if they are in
	 * a compatible domain, then load the partition table and see
	 * if it is OK for the new device, and choose the largest
	 * partition table that fits.
	 */
	DIR *dir;
	struct dirent *de;
	char *chosen = NULL;
	unsigned long long chosen_size = 0;
	struct supertype *chosen_st = NULL;
	int fd;

	dir = opendir("/dev/disk/by-path");
	if (!dir)
		return 1;
	while ((de = readdir(dir)) != NULL) {
		char *ep;
		struct dev_policy *pol2 = NULL;
		struct domainlist *domlist = NULL;
		int fd = -1;
		struct mdinfo info;
		struct supertype *st2 = NULL;
		char *devname = NULL;
		unsigned long long devsectors;

		if (de->d_ino == 0 ||
		    de->d_name[0] == '.' ||
		    (de->d_type != DT_LNK && de->d_type != DT_UNKNOWN))
			goto next;

		ep = de->d_name + strlen(de->d_name);
		while (ep > de->d_name &&
		       isdigit(ep[-1]))
			ep--;
		if (ep > de->d_name + 5 &&
		    strncmp(ep-5, "-part", 5) == 0)
			/* This is a partition - skip it */
			goto next;

		pol2 = path_policy(de->d_name, type_disk);

		domain_merge(&domlist, pol2, st ? st->ss->name : NULL);
		if (domain_test(domlist, pol, st ? st->ss->name : NULL) != 1)
			/* new device is incompatible with this device. */
			goto next;

		domain_free(domlist);
		domlist = NULL;

		if (asprintf(&devname, "/dev/disk/by-path/%s", de->d_name) != 1) {
			devname = NULL;
			goto next;
		}
		fd = open(devname, O_RDONLY);
		if (fd < 0)
			goto next;
		if (get_dev_size(fd, devname, &devsectors) == 0)
			goto next;
		devsectors >>= 9;

		if (st)
			st2 = dup_super(st);
		else
			st2 = guess_super_type(fd, guess_partitions);
		if (st2 == NULL ||
		    st2->ss->load_super(st2, fd, NULL) < 0)
			goto next;

		if (!st) {
			/* Check domain policy again, this time referring to metadata */
			domain_merge(&domlist, pol2, st2->ss->name);
			if (domain_test(domlist, pol, st2->ss->name) != 1)
				/* Incompatible devices for this metadata type */
				goto next;
			if (!policy_action_allows(pol, st2->ss->name, act_spare))
				/* Some partition types allow sparing, but not
				 * this one.
				 */
				goto next;
		}

		st2->ss->getinfo_super(st2, &info, NULL);
		if (info.component_size > devsectors)
			/* This partitioning doesn't fit in the device */
			goto next;

		/* This is an acceptable device to copy partition
		 * metadata from.  We could just stop here, but I
		 * think I want to keep looking incase a larger
		 * metadata which makes better use of the device can
		 * be found.
		 */
		if (chosen == NULL ||
		    chosen_size < info.component_size) {
			chosen_size = info.component_size;
			free(chosen);
			chosen = devname;
			devname = NULL;
			if (chosen_st) {
				chosen_st->ss->free_super(chosen_st);
				free(chosen_st);
			}
			chosen_st = st2;
			st2 = NULL;
		}

	next:
		free(devname);
		domain_free(domlist);
		dev_policy_free(pol2);
		if (st2)
			st2->ss->free_super(st2);
		free(st2);

		if (fd >= 0)
			close(fd);
	}

	closedir(dir);

	if (!chosen)
		return 1;

	/* 'chosen' is the best device we can find.  Let's write its
	 * metadata to devname dfd is read-only so don't use that
	 */
	fd = open(devname, O_RDWR);
	if (fd >= 0) {
		chosen_st->ss->store_super(chosen_st, fd);
		close(fd);
	}
	free(chosen);
	chosen_st->ss->free_super(chosen_st);
	free(chosen_st);
	return 0;
}

static int is_bare(int dfd)
{
	unsigned long long size = 0;
	char bufpad[4096 + 4096];
	char *buf = (char*)(((long)bufpad + 4096) & ~4095);

	if (lseek(dfd, 0, SEEK_SET) != 0 ||
	    read(dfd, buf, 4096) != 4096)
		return 0;

	if (buf[0] != '\0' && buf[0] != '\x5a' && buf[0] != '\xff')
		return 0;
	if (memcmp(buf, buf+1, 4095) != 0)
		return 0;

	/* OK, first 4K appear blank, try the end. */
	get_dev_size(dfd, NULL, &size);
	if (lseek(dfd, size-4096, SEEK_SET) < 0 ||
	    read(dfd, buf, 4096) != 4096)
		return 0;

	if (buf[0] != '\0' && buf[0] != '\x5a' && buf[0] != '\xff')
		return 0;
	if (memcmp(buf, buf+1, 4095) != 0)
		return 0;

	return 1;
}

/* adding a spare to a regular array is quite different from adding one to
 * a set-of-partitions virtual array.
 * This function determines which is worth trying and tries as appropriate.
 * Arrays are given priority over partitions.
 */
static int try_spare(char *devname, int *dfdp, struct dev_policy *pol,
		     struct map_ent *target,
		     struct supertype *st, int verbose)
{
	int i;
	int rv;
	int arrays_ok = 0;
	int partitions_ok = 0;
	int dfd = *dfdp;
	int bare;

	/* Can only add a spare if device has at least one domain */
	if (pol_find(pol, pol_domain) == NULL)
		return 1;
	/* And only if some action allows spares */
	if (!policy_action_allows(pol, st?st->ss->name:NULL, act_spare))
		return 1;

	/* Now check if the device is bare.
	 * bare devices can always be added as a spare
	 * non-bare devices can only be added if spare-same-slot is permitted,
	 * and this device is replacing a previous device - in which case 'target'
	 * will be set.
	 */
	if (!is_bare(dfd)) {
		/* Must have a target and allow same_slot */
		/* Later - may allow force_spare without target */
		if (!target ||
		    !policy_action_allows(pol, st?st->ss->name:NULL,
					  act_spare_same_slot)) {
			if (verbose > 1)
				fprintf(stderr, Name ": %s is not bare, so not "
					"considering as a spare\n",
					devname);
			return 1;
		}
		bare = 0;
	} else
		bare = 1;

	/* It might be OK to add this device to an array - need to see
	 * what arrays might be candidates.
	 */
	if (st) {
		/* just try try 'array' or 'partition' based on this metadata */
		if (st->ss->add_to_super)
			return array_try_spare(devname, dfdp, pol, target, bare,
					       st, verbose);
		else
			return partition_try_spare(devname, dfdp, pol,
						   st, verbose);
	}
	/* No metadata was specified or found so options are open.
	 * Check for whether any array metadata, or any partition metadata
	 * might allow adding the spare.  This check is just help to avoid
	 * a more costly scan of all arrays when we can be sure that will
	 * fail.
	 */
	for (i = 0; (!arrays_ok || !partitions_ok) && superlist[i] ; i++) {
		if (superlist[i]->add_to_super && !arrays_ok &&
		    policy_action_allows(pol, superlist[i]->name, act_spare))
			arrays_ok = 1;
		if (superlist[i]->add_to_super == NULL && !partitions_ok &&
		    policy_action_allows(pol, superlist[i]->name, act_spare))
			partitions_ok = 1;
	}
	rv = 1;
	if (arrays_ok)
		rv = array_try_spare(devname, dfdp, pol, target, bare,
				     st, verbose);
	if (rv != 0 && partitions_ok)
		rv = partition_try_spare(devname, dfdp, pol, st, verbose);
	return rv;
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
	struct mddev_ident *devs, *mddev;
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
			sysfs_free(sra);
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

static int Incremental_container(struct supertype *st, char *devname,
				 char *homehost, int verbose,
				 int runstop, int autof, int freeze_reshape)
{
	/* Collect the contents of this container and for each
	 * array, choose a device name and assemble the array.
	 */

	struct mdinfo *list;
	struct mdinfo *ra;
	struct map_ent *map = NULL;
	struct mdinfo info;
	int trustworthy;
	struct mddev_ident *match;
	int rv = 0;
	struct domainlist *domains;
	struct map_ent *smp;
	int suuid[4];
	int sfd;
	int ra_blocked = 0;
	int ra_all = 0;

	st->ss->getinfo_super(st, &info, NULL);

	if ((runstop > 0 && info.container_enough >= 0) ||
	    info.container_enough > 0)
		/* pass */;
	else {
		if (verbose)
			fprintf(stderr, Name ": not enough devices to start the container\n");
		return 0;
	}

	match = conf_match(st, &info, devname, verbose, &rv);
	if (match == NULL && rv == 2)
		return rv;

	/* Need to compute 'trustworthy' */
	if (match)
		trustworthy = LOCAL;
	else if (st->ss->match_home(st, homehost) == 1)
		trustworthy = LOCAL;
	else if (st->ss->match_home(st, "any") == 1)
		trustworthy = LOCAL;
	else
		trustworthy = FOREIGN;

	list = st->ss->container_content(st, NULL);
	/* when nothing to activate - quit */
	if (list == NULL)
		return 0;
	for (ra = list ; ra ; ra = ra->next) {
		int mdfd;
		char chosen_name[1024];
		struct map_ent *mp;
		struct mddev_ident *match = NULL;

		ra_all++;
		/* do not activate arrays blocked by metadata handler */
		if (ra->array.state & (1 << MD_SB_BLOCK_VOLUME)) {
			fprintf(stderr, Name ": Cannot activate array %s in %s.\n",
				ra->text_version, devname);
			ra_blocked++;
			continue;
		}
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
			struct mddev_ident *array_list;
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
					   chosen_name, verbose, NULL,
					   freeze_reshape);
		close(mdfd);
	}

	/* don't move spares to container with volume being activated
	   when all volumes are blocked */
	if (ra_all == ra_blocked)
		return 0;

	/* Now move all suitable spares from spare container */
	domains = domain_from_array(list, st->ss->name);
	memcpy(suuid, uuid_zero, sizeof(int[4]));
	if (domains &&
	    (smp = map_by_uuid(&map, suuid)) != NULL &&
	    (sfd = open(smp->path, O_RDONLY)) >= 0) {
		/* spare container found */
		struct supertype *sst =
			super_imsm.match_metadata_desc("imsm");
		struct mdinfo *sinfo;
		unsigned long long min_size = 0;
		if (st->ss->min_acceptable_spare_size)
			min_size = st->ss->min_acceptable_spare_size(st);
		if (!sst->ss->load_container(sst, sfd, NULL)) {
			close(sfd);
			sinfo = container_choose_spares(sst, min_size,
							domains, NULL,
							st->ss->name, 0);
			sst->ss->free_super(sst);
			if (sinfo){
				int count = 0;
				struct mdinfo *disks = sinfo->devs;
				while (disks) {
					/* move spare from spare
					 * container to currently
					 * assembled one
					 */
					if (move_spare(
						    smp->path,
						    devname,
						    makedev(disks->disk.major,
							    disks->disk.minor)))
						count++;
					disks = disks->next;
				}
				if (count)
					fprintf(stderr, Name
						": Added %d spare%s to %s\n",
						count, count>1?"s":"", devname);
			}
			sysfs_free(sinfo);
		} else
			close(sfd);
	}
	domain_free(domains);
	return 0;
}

/*
 * IncrementalRemove - Attempt to see if the passed in device belongs to any
 * raid arrays, and if so first fail (if needed) and then remove the device.
 *
 * @devname - The device we want to remove
 * @id_path - name as found in /dev/disk/by-path for this device
 *
 * Note: the device name must be a kernel name like "sda", so
 * that we can find it in /proc/mdstat
 */
int IncrementalRemove(char *devname, char *id_path, int verbose)
{
	int mdfd;
	int rv;
	struct mdstat_ent *ent;
	struct mddev_dev devlist;

	if (!id_path)
		dprintf(Name ": incremental removal without --path <id_path> "
			"lacks the possibility to re-add new device in this "
			"port\n");

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
		free_mdstat(ent);
		return 1;
	}

	if (id_path) {
		struct map_ent *map = NULL, *me;
		me = map_by_devnum(&map, ent->devnum);
		if (me)
			policy_save_path(id_path, me);
		map_free(map);
	}

	memset(&devlist, 0, sizeof(devlist));
	devlist.devname = devname;
	devlist.disposition = 'f';
	/* for a container, we must fail each member array */
	if (ent->metadata_version &&
	    strncmp(ent->metadata_version, "external:", 9) == 0) {
		struct mdstat_ent *mdstat = mdstat_read(0, 0);
		struct mdstat_ent *memb;
		for (memb = mdstat ; memb ; memb = memb->next)
			if (is_container_member(memb, ent->dev)) {
				int subfd = open_dev(memb->devnum);
				if (subfd >= 0) {
					Manage_subdevs(memb->dev, subfd,
						       &devlist, verbose, 0,
						       NULL, 0);
					close(subfd);
				}
			}
		free_mdstat(mdstat);
	} else
		Manage_subdevs(ent->dev, mdfd, &devlist, verbose, 0, NULL, 0);
	devlist.disposition = 'r';
	rv = Manage_subdevs(ent->dev, mdfd, &devlist, verbose, 0, NULL, 0);
	close(mdfd);
	free_mdstat(ent);
	return rv;
}
