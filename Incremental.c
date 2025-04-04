/*
 * Incremental.c - support --incremental.  Part of:
 * mdadm - manage Linux "md" devices aka RAID arrays.
 *
 * Copyright (C) 2006-2013 Neil Brown <neilb@suse.de>
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
#include	"xmalloc.h"

#include	<sys/wait.h>
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
				 struct context *c, char *only);

int Incremental(struct mddev_dev *devlist, struct context *c,
		struct supertype *st)
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
	dev_t rdev, rdev2;
	struct mdinfo info, dinfo;
	struct mdinfo *sra = NULL, *d;
	struct mddev_ident *match;
	char chosen_name[1024];
	char *md_devname;
	int rv = 1;
	struct map_ent *mp, *map = NULL;
	int dfd = -1, mdfd = -1;
	char *avail = NULL;
	int active_disks;
	int trustworthy;
	char *name_to_use;
	struct dev_policy *policy = NULL;
	struct map_ent target_array;
	int have_target;
	char *devname = devlist->devname;
	int journal_device_missing = 0;

	if (!stat_is_blkdev(devname, &rdev))
		return rv;
	dfd = dev_open(devname, O_RDONLY);
	if (dfd < 0) {
		if (c->verbose >= 0)
			pr_err("cannot open %s: %s.\n",
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
				pr_err("failed to get exclusive lock on mapfile\n");
			if (c->export)
				printf("MD_DEVNAME=%s\n", devname);
			rv = Incremental_container(st, devname, c, NULL);
			map_unlock(&map);
			return rv;
		}

		pr_err("%s is not part of an md array.\n",
			devname);
		return rv;
	}

	/* 1/ Check if device is permitted by mdadm.conf */

	for (;devlist; devlist = devlist->next)
		if (conf_test_dev(devlist->devname))
			break;
	if (!devlist) {
		devlist = conf_get_devs();
		for (;devlist; devlist = devlist->next) {
			if (stat_is_blkdev(devlist->devname, &rdev2) &&
			    rdev2 == rdev)
				break;
		}
	}
	if (!devlist) {
		if (c->verbose >= 0)
			pr_err("%s not permitted by mdadm.conf.\n",
			       devname);
		goto out;
	}

	/* 2/ Find metadata, reject if none appropriate (check
	 *            version/name from args) */

	if (!fstat_is_blkdev(dfd, devname, &rdev))
		goto out;

	dinfo.disk.major = major(rdev);
	dinfo.disk.minor = minor(rdev);

	policy = disk_policy(&dinfo);
	have_target = policy_check_path(&dinfo, &target_array);

	if (st == NULL && (st = guess_super_type(dfd, guess_array)) == NULL) {
		if (c->verbose >= 0)
			pr_err("no recognisable superblock on %s.\n",
			       devname);
		rv = try_spare(devname, &dfd, policy,
			       have_target ? &target_array : NULL,
			       NULL, c->verbose);
		goto out;
	}
	st->ignore_hw_compat = 0;

	if (st->ss->compare_super == NULL ||
	    st->ss->load_super(st, dfd, c->verbose >= 0 ? devname : NULL)) {
		if (c->verbose >= 0)
			pr_err("no RAID superblock on %s.\n",
				devname);
		rv = try_spare(devname, &dfd, policy,
			       have_target ? &target_array : NULL,
			       st, c->verbose);
		free(st);
		goto out;
	}
	close (dfd); dfd = -1;

	st->ss->getinfo_super(st, &info, NULL);

	/* 3/ Check if there is a match in mdadm.conf */
	match = conf_match(st, &info, devname, c->verbose, &rv);
	if (!match && rv == 2)
		goto out;

	if (match && match->devname && is_devname_ignore(match->devname) == true) {
		if (c->verbose >= 0)
			pr_err("array containing %s is explicitly ignored by mdadm.conf\n",
				devname);
		goto out;
	}

	/* 3a/ if not, check for homehost match.  If no match, continue
	 * but don't trust the 'name' in the array. Thus a 'random' minor
	 * number will be assigned, and the device name will be based
	 * on that. */
	if (match)
		trustworthy = LOCAL;
	else if (st->ss->match_home(st, c->homehost) == 1)
		trustworthy = LOCAL;
	else if (st->ss->match_home(st, "any") == 1)
		trustworthy = LOCAL_ANY;
	else
		trustworthy = FOREIGN;

	if (!match && !conf_test_metadata(st->ss->name, policy,
					  (trustworthy == LOCAL))) {
		if (c->verbose >= 1)
			pr_err("%s has metadata type %s for which auto-assembly is disabled\n",
			       devname, st->ss->name);
		goto out;
	}
	if (trustworthy == LOCAL_ANY)
		trustworthy = LOCAL;

	name_to_use = info.name;
	if (name_to_use[0] == 0 && is_container(info.array.level)) {
		name_to_use = info.text_version;
		trustworthy = METADATA;
	}
	if (name_to_use[0] && trustworthy != LOCAL &&
	    ! c->require_homehost &&
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
		pr_err("failed to get exclusive lock on mapfile\n");
	/* Now check we can get O_EXCL.  If not, probably "mdadm -A" has
	 * taken over
	 */
	dfd = dev_open(devname, O_RDONLY|O_EXCL);
	if (dfd < 0) {
		if (c->verbose >= 0)
			pr_err("cannot reopen %s: %s.\n",
				devname, strerror(errno));
		goto out_unlock;
	}
	/* Cannot hold it open while we add the device to the array,
	 * so we must release the O_EXCL and depend on the map_lock()
	 * So now is the best time to remove any partitions.
	 */
	remove_partitions(dfd);
	close(dfd);
	dfd = -1;

	mp = map_by_uuid(&map, info.uuid);
	if (mp)
		mdfd = open_dev(mp->devnm);
	else
		mdfd = -1;

	if (mdfd < 0) {

		/* Skip the clustered ones. This should be started by
		 * clustering resource agents
		 */
		if (info.array.state & (1 << MD_SB_CLUSTERED))
			goto out_unlock;

		/* Couldn't find an existing array, maybe make a new one */
		mdfd = create_mddev(match ? match->devname : NULL, name_to_use, trustworthy,
				    chosen_name, 0);

		if (mdfd < 0)
			goto out_unlock;

		if (sysfs_init(&info, mdfd, NULL)) {
			pr_err("unable to initialize sysfs for %s\n",
			       chosen_name);
			rv = 2;
			goto out_unlock;
		}

		if (set_array_info(mdfd, st, &info) != 0) {
			pr_err("failed to set array info for %s: %s\n",
				chosen_name, strerror(errno));
			rv = 2;
			goto out_unlock;
		}

		dinfo = info;
		dinfo.disk.major = major(rdev);
		dinfo.disk.minor = minor(rdev);
		if (add_disk(mdfd, st, &info, &dinfo) != 0) {
			pr_err("failed to add %s to new array %s: %s.\n",
				devname, chosen_name, strerror(errno));
			ioctl(mdfd, STOP_ARRAY, 0);
			rv = 2;
			goto out_unlock;
		}
		sra = sysfs_read(mdfd, NULL, (GET_DEVS | GET_STATE |
					      GET_OFFSET | GET_SIZE));

		if (!sra || !sra->devs || sra->devs->disk.raid_disk >= 0) {
			/* It really should be 'none' - must be old buggy
			 * kernel, and mdadm -I may not be able to complete.
			 * So reject it.
			 */
			ioctl(mdfd, STOP_ARRAY, NULL);
			pr_err("You have an old buggy kernel which cannot support\n      --incremental reliably.  Aborting.\n");
			rv = 2;
			goto out_unlock;
		}
		info.array.working_disks = 1;
		/* 6/ Make sure /var/run/mdadm.map contains this array. */
		map_update(&map, fd2devnm(mdfd),
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

		sra = sysfs_read(mdfd, NULL, (GET_DEVS | GET_STATE |
					    GET_OFFSET | GET_SIZE));

		if (mp->path)
			strcpy(chosen_name, mp->path);
		else
			strcpy(chosen_name, mp->devnm);

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
		if (!st->ss->external &&
		    (info.disk.state & (1 << MD_DISK_SYNC)) != 0 &&
		    !policy_action_allows(policy, st->ss->name, act_re_add) &&
		    c->runstop < 1) {
			if (md_array_active(mdfd)) {
				pr_err("not adding %s to active array (without --run) %s\n",
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
				pr_err("unable to open %s\n", devname);
				rv = 2;
				goto out_unlock;
			}
			st2 = dup_super(st);
			if (st2->ss->load_super(st2, dfd2, NULL) ||
			    st->ss->compare_super(st, st2, 1) != 0) {
				pr_err("metadata mismatch between %s and chosen array %s\n",
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
				pr_err("unexpected difference between %s and %s.\n",
				       chosen_name, devname);
				rv = 2;
				goto out_unlock;
			}
		}
		info.disk.major = major(rdev);
		info.disk.minor = minor(rdev);
		/* add disk needs to know about containers */
		if (st->ss->external)
			sra->array.level = LEVEL_CONTAINER;

		if (info.array.state & (1 << MD_SB_CLUSTERED))
			info.disk.state |= (1 << MD_DISK_CLUSTER_ADD);

		err = add_disk(mdfd, st, sra, &info);
		if (err < 0 && errno == EBUSY) {
			/* could be another device present with the same
			 * disk.number. Find and reject any such
			 */
			find_reject(mdfd, st, sra, info.disk.number,
				    info.events, c->verbose, chosen_name);
			err = add_disk(mdfd, st, sra, &info);
		}
		if (err < 0 && errno == EINVAL &&
		    info.disk.state & (1<<MD_DISK_SYNC)) {
			/* Maybe it needs to be added as a spare */
			if (policy_action_allows(policy, st->ss->name,
						 act_force_spare)) {
				info.disk.state &= ~(1<<MD_DISK_SYNC);
				err = add_disk(mdfd, st, sra, &info);
			} else
				if (c->verbose >= 0)
					pr_err("can only add %s to %s as a spare, and force-spare is not set.\n",
					       devname, chosen_name);
		}
		if (err < 0) {
			pr_err("failed to add %s to existing array %s: %s.\n",
				devname, chosen_name, strerror(errno));
			rv = 2;
			goto out_unlock;
		}
		info.array.working_disks = 0;
		for (d = sra->devs; d; d=d->next)
			info.array.working_disks ++;

	}
	if (strncmp(chosen_name, DEV_MD_DIR, DEV_MD_DIR_LEN) == 0)
		md_devname = chosen_name + DEV_MD_DIR_LEN;
	else
		md_devname = chosen_name;
	if (c->export) {
		printf("MD_DEVICE=%s\n", fd2devnm(mdfd));
		printf("MD_DEVNAME=%s\n", md_devname);
		printf("MD_FOREIGN=%s\n", trustworthy == FOREIGN ? "yes" : "no");
	}

	/* 7/ Is there enough devices to possibly start the array? */
	/* 7a/ if not, finish with success. */
	if (is_container(info.array.level)) {
		char devnm[32];
		/* Try to assemble within the container */
		sysfs_uevent(sra, "change");
		if (!c->export && c->verbose >= 0)
			pr_err("container %s now has %d device%s\n",
			       chosen_name, info.array.working_disks,
			       info.array.working_disks == 1?"":"s");
		sysfs_rules_apply(chosen_name, &info);
		wait_for(chosen_name, mdfd);
		if (st->ss->external)
			strcpy(devnm, fd2devnm(mdfd));
		if (st->ss->load_container)
			rv = st->ss->load_container(st, mdfd, NULL);
		close(mdfd);
		sysfs_free(sra);
		if (!rv)
			rv = Incremental_container(st, chosen_name, c, NULL);
		map_unlock(&map);
		/* after spare is added, ping monitor for external metadata
		 * so that it can eg. try to rebuild degraded array */
		if (st->ss->external)
			ping_monitor(devnm);
		return rv;
	}

	/* We have added something to the array, so need to re-read the
	 * state.  Eventually this state should be kept up-to-date as
	 * things change.
	 */
	sysfs_free(sra);
	sra = sysfs_read(mdfd, NULL, (GET_DEVS | GET_STATE |
				    GET_OFFSET | GET_SIZE));
	active_disks = count_active(st, sra, mdfd, &avail, &info);

	if (!avail)
		goto out_unlock;

	journal_device_missing = (info.journal_device_required) && (info.journal_clean == 0);

	if (info.consistency_policy == CONSISTENCY_POLICY_PPL)
		info.array.state |= 1;

	if (enough(info.array.level, info.array.raid_disks,
		   info.array.layout, info.array.state & 1, avail) == 0) {
		if (c->export) {
			printf("MD_STARTED=no\n");
		} else if (c->verbose >= 0)
			pr_err("%s attached to %s, not enough to start (%d).\n",
			       devname, chosen_name, active_disks);
		rv = 0;
		goto out_unlock;
	}

	/* 7b/ if yes, */
	/* - if number of OK devices match expected, or -R and there */
	/*             are enough, */
	/*   + add any bitmap file  */
	/*   + start the array (auto-readonly). */

	if (md_array_active(mdfd)) {
		if (c->export) {
			printf("MD_STARTED=already\n");
		} else if (c->verbose >= 0)
			pr_err("%s attached to %s which is already active.\n",
			       devname, chosen_name);
		rv = 0;
		goto out_unlock;
	}

	map_unlock(&map);
	if (c->runstop > 0 || (!journal_device_missing && active_disks >= info.array.working_disks)) {
		struct mdinfo *dsk;
		/* Let's try to start it */

		if (journal_device_missing)
			pr_err("Trying to run with missing journal device\n");
		if (info.reshape_active && !(info.reshape_active & RESHAPE_NO_BACKUP)) {
			pr_err("%s: This array is being reshaped and cannot be started\n",
			       chosen_name);
			cont_err("by --incremental.  Please use --assemble\n");
			goto out;
		}

		/* Need to remove from the array any devices which
		 * 'count_active' discerned were too old or inappropriate
		 */
		for (d = sra ? sra->devs : NULL ; d ; d = d->next)
			if (d->disk.state & (1<<MD_DISK_REMOVED))
				remove_disk(mdfd, st, sra, d);

		if ((sra == NULL || active_disks >= info.array.working_disks) &&
		    trustworthy != FOREIGN)
			rv = ioctl(mdfd, RUN_ARRAY, NULL);
		else
			rv = sysfs_set_str(sra, NULL,
					   "array_state", "read-auto");
		/* Array might be O_EXCL which  will interfere with
		 * fsck and mount.  So re-open without O_EXCL.
		 */
		reopen_mddev(mdfd);
		if (rv == 0) {
			if (c->export) {
				printf("MD_STARTED=yes\n");
			} else if (c->verbose >= 0)
				pr_err("%s attached to %s, which has been started.\n",
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
				if (disk_action_allows(dsk, st->ss->name,
						       act_re_add) &&
				    add_disk(mdfd, st, sra, dsk) == 0)
					pr_err("%s re-added to %s\n",
					       dsk->sys_name, chosen_name);
			}
		} else {
			pr_err("%s attached to %s, but failed to start: %s.\n",
			       devname, chosen_name, strerror(errno));
			rv = 1;
		}
	} else {
		if (c->export) {
			printf("MD_STARTED=unsafe\n");
		} else if (journal_device_missing) {
			pr_err("Journal device is missing, not safe to start yet.\n");
		} else if (c->verbose >= 0)
			pr_err("%s attached to %s, not enough to start safely.\n",
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

	if (md_array_active(mdfd))
		return; /* not safe to remove from active arrays
			 * without thinking more */

	for (d = sra->devs; d ; d = d->next) {
		char dn[24]; // 2*11 bytes for ints (including sign) + colon + null byte
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

		if (info.disk.number != number || info.events >= events)
			continue;

		if (d->disk.raid_disk > -1)
			sysfs_set_str(sra, d, "slot", STR_COMMON_NONE);
		if (sysfs_set_str(sra, d, "state", "remove") == 0)
			if (verbose >= 0)
				pr_err("removing old device %s from %s\n",
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
	int replcnt = 0;
	__u64 max_events = 0;
	__u64 max_journal_events = 0;
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
	for (d = sra->devs, devnum = 0 ; d ; d = d->next, devnum++) {
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
		if (info.disk.raid_disk == MD_DISK_ROLE_JOURNAL &&
		    info.events > max_journal_events)
			max_journal_events = info.events;
		if (!avail) {
			raid_disks = info.array.raid_disks;
			avail = xcalloc(raid_disks, 1);
			*availp = avail;

			best = xcalloc(raid_disks, sizeof(int));
			devmap = xcalloc(raid_disks, numdevs);

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
				for (i = 0; i < raid_disks; i++)
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
		} else if (info.disk.state & (1<<MD_DISK_REPLACEMENT))
			replcnt++;
		st->ss->free_super(st);
	}
	if (max_events > 0 && max_journal_events >= max_events - 1)
		bestinfo->journal_clean = 1;

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
	/* Also need to reject any spare device with an event count that
	 * is too high
	 */
	for (d = sra->devs; d; d = d->next) {
		if (!(d->disk.state & (1<<MD_DISK_SYNC)) &&
		    d->events > max_events)
			d->disk.state |= (1 << MD_DISK_REMOVED);
	}
	free(best);
	free(devmap);
	return cnt + replcnt;
}

/* test if container has degraded member(s) */
static int
container_members_max_degradation(struct map_ent *map, struct map_ent *me)
{
	struct mdinfo *sra;
	int degraded, max_degraded = 0;

	for(; map; map = map->next) {
		if (!metadata_container_matches(map->metadata, me->devnm))
			continue;
		/* most accurate information regarding array degradation */
		sra = sysfs_read(-1, map->devnm,
				 GET_DISKS | GET_DEVS | GET_STATE);
		if (!sra)
			continue;
		degraded = sra->array.raid_disks - sra->array.active_disks -
			sra->array.spare_disks;
		if (degraded > max_degraded)
			max_degraded = degraded;
		sysfs_free(sra);
	}

	return max_degraded;
}

/**
 * incremental_external_test_spare_criteria() - helper to test spare criteria.
 * @st: supertype, must be not NULL, it is duplicated here.
 * @container_devnm: devnm of the container.
 * @disk_fd: file descriptor of device to tested.
 * @verbose: verbose flag.
 *
 * The function is used on new drive verification path to check if it can be added to external
 * container. To test spare criteria, metadata must be loaded. It duplicates super to not mess in
 * original one.
 * Function is executed if superblock supports get_spare_criteria(), otherwise success is returned.
 */
mdadm_status_t incremental_external_test_spare_criteria(struct supertype *st, char *container_devnm,
							int disk_fd, int verbose)
{
	mdadm_status_t rv = MDADM_STATUS_ERROR;
	char container_devname[PATH_MAX];
	struct spare_criteria sc = {0};
	struct supertype *dup;

	if (!st->ss->get_spare_criteria)
		return MDADM_STATUS_SUCCESS;

	dup = dup_super(st);
	snprintf(container_devname, PATH_MAX, "/dev/%s", container_devnm);

	if (dup->ss->get_spare_criteria(dup, container_devname, &sc) != 0) {
		if (verbose > 1)
			pr_err("Failed to get spare criteria for %s\n", container_devname);
		goto out;
	}

	if (!disk_fd_matches_criteria(dup, disk_fd, &sc)) {
		if (verbose > 1)
			pr_err("Disk does not match spare criteria for %s\n", container_devname);
		goto out;
	}

	rv = MDADM_STATUS_SUCCESS;

out:
	dev_policy_free(sc.pols);
	dup->ss->free_super(dup);
	free(dup);

	return rv;
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
	dev_t rdev;
	struct map_ent *mp, *map = NULL;
	struct mdinfo *chosen = NULL;
	int dfd = *dfdp;

	if (!fstat_is_blkdev(dfd, devname, &rdev))
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
		pr_err("failed to get exclusive lock on mapfile\n");
		return 1;
	}
	for (mp = map ; mp ; mp = mp->next) {
		struct supertype *st2;
		struct domainlist *dl = NULL;
		struct mdinfo *sra;
		unsigned long long freesize = 0;

		if (is_subarray(mp->metadata))
			continue;
		if (st) {
			st2 = st->ss->match_metadata_desc(mp->metadata);
			if (!st2 ||
			    (st->minor_version >= 0 &&
			     st->minor_version != st2->minor_version)) {
				if (verbose > 1)
					pr_err("not adding %s to %s as metadata type doesn't match\n",
						devname, mp->path);
				free(st2);
				continue;
			}
			free(st2);
		}
		sra = sysfs_read(-1, mp->devnm,
				 GET_DEVS|GET_OFFSET|GET_SIZE|GET_STATE|
				 GET_COMPONENT|GET_VERSION);
		if (sra)
			sra->array.failed_disks = -1;
		else
			continue;
		if (st == NULL) {
			int i;
			st2 = NULL;
			for(i = 0; !st2 && superlist[i]; i++)
				st2 = superlist[i]->match_metadata_desc(
					sra->text_version);
			if (!st2) {
				if (verbose > 1)
					pr_err("not adding %s to %s as metadata not recognised.\n",
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

		if (sra->component_size == 0) {
			/* true for containers */
			if (incremental_external_test_spare_criteria(st2, mp->devnm, dfd, verbose))
				goto next;
		}

		if (sra->component_size > 0 &&
		    st2->ss->validate_geometry(st2, sra->array.level, sra->array.layout,
						sra->array.raid_disks, &sra->array.chunk_size,
						sra->component_size,
						sra->devs ? sra->devs->data_offset : INVALID_SECTORS,
						devname, &freesize, sra->consistency_policy,
						0) && freesize < sra->component_size) {
			if (verbose > 1)
				pr_err("not adding %s to %s as it is too small\n",
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
				pr_err("not adding %s to %s as it is not in a compatible domain\n",
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
		sysfs_free(sra);
		if (st != st2)
			free(st2);
		if (dl)
			domain_free(dl);
	}
	if (chosen) {
		/* add current device to chosen array as a spare */
		int mdfd = open_dev(chosen->sys_name);
		if (mdfd >= 0) {
			struct mddev_dev devlist;
			char chosen_devname[24]; // 2*11 for int (including signs) + colon + null
			devlist.next = NULL;
			devlist.used = 0;
			devlist.writemostly = FlagDefault;
			devlist.failfast = FlagDefault;
			devlist.devname = chosen_devname;
			sprintf(chosen_devname, "%d:%d", major(rdev),
				minor(rdev));
			devlist.disposition = 'a';
			close(dfd);
			*dfdp = -1;
			rv =  Manage_subdevs(chosen->sys_name, mdfd, &devlist,
					     -1, 0, UOPT_UNDEFINED, 0);
			close(mdfd);
		}
		if (verbose > 0) {
			if (rv == 0)
				pr_err("added %s as spare for %s\n",
					devname, chosen->sys_name);
			else
				pr_err("failed to add %s as spare for %s\n",
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
		char *dev_path_name = NULL;
		unsigned long long devsectors;
		char *pathlist[2];

		if (de->d_ino == 0 || de->d_name[0] == '.' ||
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

		pathlist[0] = de->d_name;
		pathlist[1] = NULL;
		pol2 = path_policy(pathlist, type_disk);

		domain_merge(&domlist, pol2, st ? st->ss->name : NULL);
		if (domain_test(domlist, pol, st ? st->ss->name : NULL) != 1)
			/* new device is incompatible with this device. */
			goto next;

		domain_free(domlist);
		domlist = NULL;

		if (asprintf(&dev_path_name, "/dev/disk/by-path/%s", de->d_name) != 1) {
			dev_path_name = NULL;
			goto next;
		}
		fd = open(dev_path_name, O_RDONLY);
		if (fd < 0)
			goto next;
		if (get_dev_size(fd, dev_path_name, &devsectors) == 0)
			goto next;
		devsectors >>= 9;

		if (st)
			st2 = dup_super(st);
		else
			st2 = guess_super_type(fd, guess_partitions);
		if (st2 == NULL || st2->ss->load_super(st2, fd, NULL) < 0)
			goto next;
		st2->ignore_hw_compat = 0;

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
		if (chosen == NULL || chosen_size < info.component_size) {
			chosen_size = info.component_size;
			free(chosen);
			chosen = dev_path_name;
			dev_path_name = NULL;
			if (chosen_st) {
				chosen_st->ss->free_super(chosen_st);
				free(chosen_st);
			}
			chosen_st = st2;
			st2 = NULL;
		}

	next:
		free(dev_path_name);
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
	if ((size >= 4096 && lseek(dfd, size-4096, SEEK_SET) < 0) ||
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
				pr_err("%s is not bare, so not considering as a spare\n",
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
		/* just try to add 'array' or 'partition' based on this metadata */
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

int IncrementalScan(struct context *c, char *devnm)
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
	char container[32];
	char *only = NULL;

	map_read(&mapl);
	devs = conf_get_ident(NULL);

restart:
	for (me = mapl ; me ; me = me->next) {
		struct mdinfo *sra;
		int mdfd;

		if (devnm && strcmp(devnm, me->devnm) != 0)
			continue;
		if (me->metadata[0] == '/') {
			char *sl;

			if (!devnm)
				continue;

			/* member array, need to work on container */
			strncpy(container, me->metadata+1, 32);
			container[31] = 0;
			sl = strchr(container, '/');
			if (sl)
				*sl = 0;
			only = devnm;
			devnm = container;
			goto restart;
		}
		mdfd = open_dev(me->devnm);

		if (!is_fd_valid(mdfd))
			continue;
		if (!isdigit(me->metadata[0])) {
			/* must be a container */
			struct supertype *st = super_by_fd(mdfd, NULL);
			int ret = 0;
			struct map_ent *map = NULL;

			if (st && st->ss->load_container)
				ret = st->ss->load_container(st, mdfd, NULL);
			close_fd(&mdfd);
			if (!ret && st && st->ss->container_content) {
				if (map_lock(&map))
					pr_err("failed to get exclusive lock on mapfile\n");
				ret = Incremental_container(st, me->path, c, only);
				map_unlock(&map);
			}
			if (ret)
				rv = 1;
			continue;
		}
		if (md_array_active(mdfd)) {
			close_fd(&mdfd);
			continue;
		}
		/* Ok, we can try this one.   Maybe it needs a bitmap */
		for (mddev = devs ; mddev ; mddev = mddev->next)
			if (mddev->devname && me->path &&
			    devname_matches(mddev->devname, me->path))
				break;

		/* FIXME check for reshape_active and consider not
		 * starting array.
		 */
		sra = sysfs_read(mdfd, NULL, 0);
		if (sra) {
			if (sysfs_set_str(sra, NULL,
					  "array_state", "read-auto") == 0) {
				if (c->verbose >= 0)
					pr_err("started array %s\n",
					       me->path ?: me->devnm);
			} else {
				pr_err("failed to start array %s: %s\n",
				       me->path ?: me->devnm,
				       strerror(errno));
				rv = 1;
			}
			sysfs_free(sra);
		}
		close_fd(&mdfd);
	}
	map_free(mapl);
	return rv;
}

static char *container2devname(char *devname)
{
	char *mdname = NULL;

	if (devname[0] == '/') {
		int fd = open(devname, O_RDONLY);
		if (fd >= 0) {
			mdname = xstrdup(fd2devnm(fd));
			close(fd);
		}
	} else {
		int uuid[4];
		struct map_ent *mp, *map = NULL;

		if (!parse_uuid(devname, uuid))
			return mdname;
		mp = map_by_uuid(&map, uuid);
		if (mp)
			mdname = xstrdup(mp->devnm);
		map_free(map);
	}

	return mdname;
}

static int Incremental_container(struct supertype *st, char *devname,
				 struct context *c, char *only)
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
	int result = 0;

	st->ss->getinfo_super(st, &info, NULL);

	if (info.container_enough < 0 || (info.container_enough == 0 && c->runstop < 1)) {
		if (c->export)
			printf("MD_STARTED=no\n");
		else if (c->verbose)
			pr_err("Not enough devices to start the container.\n");

		return 0;
	}

	match = conf_match(st, &info, devname, c->verbose, &rv);
	if (match == NULL && rv == 2)
		return rv;

	/* Need to compute 'trustworthy' */
	if (match)
		trustworthy = LOCAL;
	else if (st->ss->match_home(st, c->homehost) == 1)
		trustworthy = LOCAL;
	else if (st->ss->match_home(st, "any") == 1)
		trustworthy = LOCAL;
	else
		trustworthy = FOREIGN;

	list = st->ss->container_content(st, NULL);
	/* when nothing to activate - quit */
	if (list == NULL) {
		if (c->export) {
			printf("MD_STARTED=nothing\n");
		}
		return 0;
	}
	for (ra = list ; ra ; ra = ra->next) {
		int mdfd = -1;
		char chosen_name[1024];
		struct map_ent *mp;
		struct mddev_ident *match = NULL;

		/* do not activate arrays blocked by metadata handler */
		if (ra->array.state & (1 << MD_SB_BLOCK_VOLUME)) {
			pr_err("Cannot activate array %s in %s.\n",
				ra->text_version, devname);
			continue;
		}
		mp = map_by_uuid(&map, ra->uuid);

		if (mp) {
			mdfd = open_dev(mp->devnm);
			if (!is_fd_valid(mdfd)) {
				pr_err("failed to open %s: %s.\n",
				       mp->devnm, strerror(errno));
				rv = 2;
				goto release;
			}
			if (mp->path)
				strcpy(chosen_name, mp->path);
			else
				strcpy(chosen_name, mp->devnm);
		} else if (!only) {

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
				if (c->verbose>0)
					pr_err("match found for member %s\n",
						array_list->member);
				break;
			}

			if (match && match->devname && is_devname_ignore(match->devname) == true) {
				if (c->verbose > 0)
					pr_err("array %s/%s is explicitly ignored by mdadm.conf\n",
					       match->container, match->member);
				continue;
			}
			if (match)
				trustworthy = LOCAL;

			mdfd = create_mddev(match ? match->devname : NULL, ra->name, trustworthy,
					    chosen_name, 0);

			if (!is_fd_valid(mdfd)) {
				pr_err("create_mddev failed with chosen name %s: %s.\n",
				       chosen_name, strerror(errno));
				rv = 2;
				goto release;
			}
		}

		if (only && (!mp || strcmp(mp->devnm, only) != 0)) {
			close_fd(&mdfd);
			continue;
		}

		assemble_container_content(st, mdfd, ra, c,
					   chosen_name, &result);
		map_free(map);
		map = NULL;
		close_fd(&mdfd);
	}
	if (c->export && result) {
		char sep = '=';
		printf("MD_STARTED");
		if (result & INCR_NO) {
			printf("%cno", sep);
			sep = ',';
		}
		if (result & INCR_UNSAFE) {
			printf("%cunsafe", sep);
			sep = ',';
		}
		if (result & INCR_ALREADY) {
			printf("%calready", sep);
			sep = ',';
		}
		if (result & INCR_YES) {
			printf("%cyes", sep);
			sep = ',';
		}
		printf("\n");
	}

release:
	map_free(map);
	sysfs_free(list);
	return rv;
}

/**
 * is_devnode_path() - check if the devname passed might be devnode path.
 * @devnode: the path to check.
 *
 * Devnode must be located directly in /dev directory. It is not checking existence of the file
 * because the device might no longer exist during removal from raid array.
 */
static bool is_devnode_path(char *devnode)
{
	char *devnm = strrchr(devnode, '/');

	if (!devnm || *(devnm + 1) == 0)
		return false;

	if (strncmp(devnode, DEV_DIR, DEV_DIR_LEN) == 0 && devnode + DEV_DIR_LEN - 1 == devnm)
		return true;

	return false;
}

/**
 * Incremental_remove_external() - Remove the device from external container.
 * @device_devnm: block device to remove.
 * @container_devnm: the parent container
 * @mdstat: mdstat file content.
 * @verbose: verbose flag.
 *
 * Fail member device in each subarray and remove member device from external container.
 * The resposibility of removing member disks from external subararys belongs to mdmon.
 */
static mdadm_status_t Incremental_remove_external(char *device_devnm, char *container_devnm,
						  struct mdstat_ent *mdstat, int verbose)
{
	mdadm_status_t rv = MDADM_STATUS_SUCCESS;
	struct mdstat_ent *memb;

	for (memb = mdstat ; memb ; memb = memb->next) {
		mdadm_status_t ret = MDADM_STATUS_SUCCESS;
		int state_fd;

		if (!is_container_member(memb, container_devnm))
			continue;

		/*
		 * Checking mdstat is pointles because it might be outdated, try open descriptor
		 * instead. If it fails, we are fine with that, device is already gone.
		 */
		state_fd = sysfs_open_memb_attr(memb->devnm, device_devnm, "state", O_RDWR);
		if (!is_fd_valid(state_fd))
			continue;

		ret = sysfs_set_memb_state_fd(state_fd, MEMB_STATE_FAULTY, NULL);
		if (ret && verbose >= 0)
			pr_err("Cannot fail member device %s in external subarray %s.\n",
				device_devnm, memb->devnm);

		close_fd(&state_fd);

		/*
		 * Don't remove member device from container if it failed to remove it
		 * from any member array.
		 */
		rv |= ret;
	}

	if (rv == MDADM_STATUS_SUCCESS)
		rv = sysfs_set_memb_state(container_devnm, device_devnm, MEMB_STATE_REMOVE);

	if (rv && verbose >= 0)
		pr_err("Cannot remove member device %s from container %s.\n", device_devnm,
		       container_devnm);

	return rv;
}

/**
 * Incremental_remove() - Remove the device from all raid arrays.
 * @devname: the device we want to remove, it could be kernel device name or devnode.
 * @id_path: optional, /dev/disk/by-path path to save for bare scenarios support.
 * @verbose: verbose flag.
 *
 * First, fail the device (if needed) and then remove the device. This code is critical for system
 * funtionality and that is why it is keept as simple as possible. We do not load devices using
 * sysfs_read() because any unerelated failure may lead us to abort. We also do not call
 * Manage_Subdevs().
 */
int Incremental_remove(char *devname, char *id_path, int verbose)
{
	mdadm_status_t rv = MDADM_STATUS_SUCCESS;
	char *devnm = basename(devname);
	char buf[SYSFS_MAX_BUF_SIZE];
	struct mdstat_ent *mdstat;
	struct mdstat_ent *ent;
	struct mdinfo mdi;
	int mdfd = -1;

	if (strcmp(devnm, devname) != 0)
		if (!is_devnode_path(devname)) {
			pr_err("Cannot remove \"%s\", devnode path or kernel device name is allowed.\n",
			       devname);
			return 1;
		}

	mdstat = mdstat_read(0, 0);
	if (!mdstat) {
		pr_err("Cannot read /proc/mdstat file, aborting\n");
		return 1;
	}

	ent = mdstat_find_by_member_name(mdstat, devnm);
	if (!ent) {
		if (verbose >= 0)
			pr_vrb("%s does not appear to be a component of any array\n", devnm);
		goto out;
	}

	if (sysfs_init(&mdi, -1, ent->devnm)) {
		pr_err("unable to initialize sysfs for: %s\n", devnm);
		goto out;
	}

	mdfd = open_dev_excl(ent->devnm);
	if (is_fd_valid(mdfd)) {
		char *array_state_file = "array_state";

		/**
		 * This is a workaround for the old issue.
		 * Incremental_remove() triggered from udev rule when disk is removed from OS
		 * tries to set array in auto-read-only mode. This can interrupt rebuild
		 * process which is started automatically, e.g. if array is mounted and
		 * spare disk is available (I/O errors limit might be achieved faster than disk is
		 * removed by mdadm). Prevent Incremental_remove() from setting array
		 * into "auto-read-only", by requiring  exclusive open to succeed.
		 */
		close_fd(&mdfd);

		if (sysfs_get_str(&mdi, NULL, array_state_file, buf, sizeof(buf)) > 0) {
			char *str_read_auto = map_num_s(sysfs_array_states, ARRAY_READ_AUTO);
			char *str_active = map_num_s(sysfs_array_states, ARRAY_ACTIVE);
			char *str_clean = map_num_s(sysfs_array_states, ARRAY_CLEAN);

			if (strncmp(buf, str_active, strlen(str_active)) == 0 ||
			    strncmp(buf, str_clean, strlen(str_clean)) == 0)
				sysfs_set_str(&mdi, NULL, array_state_file, str_read_auto);
		}
	}

	mdfd = open_dev(ent->devnm);
	if (mdfd < 0) {
		if (verbose >= 0)
			pr_err("Cannot open array %s!!\n", ent->devnm);
		goto out;
	}

	if (id_path) {
		struct map_ent *map = NULL, *me;

		me = map_by_devnm(&map, ent->devnm);
		if (me)
			policy_save_path(id_path, me);
		map_free(map);
	}

	if (is_mdstat_ent_external(ent)) {
		rv = Incremental_remove_external(devnm, ent->devnm, mdstat, verbose);
		goto out;
	}

	/* Native arrays are handled separatelly to provide more detailed error handling */
	rv = sysfs_set_memb_state(ent->devnm, devnm, MEMB_STATE_FAULTY);
	if (rv && verbose >= 0)
		pr_err("Cannot fail member device %s in array %s.\n", devnm, ent->devnm);

	if (rv == MDADM_STATUS_SUCCESS)
		rv = sysfs_set_memb_state(ent->devnm, devnm, MEMB_STATE_REMOVE);

	if (rv && verbose >= 0)
		pr_err("Cannot remove member device %s from %s.\n", devnm, ent->devnm);

out:
	close_fd(&mdfd);
	free_mdstat(mdstat);
	return rv;
}
