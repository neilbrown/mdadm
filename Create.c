/*
 * mdadm - manage Linux "md" devices aka RAID arrays.
 *
 * Copyright (C) 2001-2013 Neil Brown <neilb@suse.de>
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
 */

#include	"mdadm.h"
#include	"md_u.h"
#include	"md_p.h"
#include	"udev.h"
#include	"xmalloc.h"

#include	<ctype.h>
#include	<fcntl.h>
#include	<signal.h>
#include	<sys/signalfd.h>
#include	<sys/wait.h>

#ifndef FALLOC_FL_ZERO_RANGE
#define FALLOC_FL_ZERO_RANGE 16
#endif

static int round_size_and_verify(unsigned long long *size, int chunk)
{
	if (*size == 0)
		return 0;
	*size &= ~(unsigned long long)(chunk - 1);
	if (*size == 0) {
		pr_err("Size cannot be smaller than chunk.\n");
		return 1;
	}
	return 0;
}

/**
 * default_layout() - Get default layout for level.
 * @st: metadata requested, could be NULL.
 * @level: raid level requested.
 * @verbose: verbose level.
 *
 * Try to ask metadata handler first, otherwise use global defaults.
 *
 * Return: Layout or &UnSet, return value meaning depends of level used.
 */
int default_layout(struct supertype *st, int level, int verbose)
{
	int layout = UnSet;
	mapping_t *layout_map = NULL;
	char *layout_name = NULL;

	if (st && st->ss->default_geometry)
		st->ss->default_geometry(st, &level, &layout, NULL);

	if (layout != UnSet)
		return layout;

	switch (level) {
	default: /* no layout */
		layout = 0;
		break;
	case 0:
		/* Leave unset - metadata handler choose default */
		break;
	case 10:
		layout = 0x102; /* near=2, far=1 */
		layout_name = "n2";
		break;
	case 5:
	case 6:
		layout_map = r5layout;
		break;
	case LEVEL_FAULTY:
		layout_map = faultylayout;
		break;
	}

	if (layout_map) {
		layout = map_name(layout_map, "default");
		layout_name = map_num_s(layout_map, layout);
	}
	if (layout_name && verbose > 0)
		pr_err("layout defaults to %s\n", layout_name);

	return layout;
}

static pid_t write_zeroes_fork(int fd, struct shape *s, struct supertype *st,
			       struct mddev_dev *dv)

{
	const unsigned long long req_size = 1 << 30;
	unsigned long long offset_bytes, size_bytes, sz;
	sigset_t sigset;
	int ret = 0;
	pid_t pid;

	size_bytes = KIB_TO_BYTES(s->size);

	/*
	 * If size_bytes is zero, this is a zoned raid array where
	 * each disk is of a different size and uses its full
	 * disk. Thus zero the entire disk.
	 */
	if (!size_bytes && !get_dev_size(fd, dv->devname, &size_bytes))
		return -1;

	if (dv->data_offset != INVALID_SECTORS)
		offset_bytes = SEC_TO_BYTES(dv->data_offset);
	else
		offset_bytes = SEC_TO_BYTES(st->data_offset);

	pr_info("zeroing data from %lld to %lld on: %s\n",
		offset_bytes, size_bytes, dv->devname);

	pid = fork();
	if (pid < 0) {
		pr_err("Could not fork to zero disks: %s\n", strerror(errno));
		return pid;
	} else if (pid != 0) {
		return pid;
	}

	sigemptyset(&sigset);
	sigaddset(&sigset, SIGINT);
	sigprocmask(SIG_UNBLOCK, &sigset, NULL);

	while (size_bytes) {
		/*
		 * Split requests to the kernel into 1GB chunks seeing the
		 * fallocate() call is not interruptible and blocking a
		 * ctrl-c for several minutes is not desirable.
		 *
		 * 1GB is chosen as a compromise: the user may still have
		 * to wait several seconds if they ctrl-c on devices that
		 * zero slowly, but will reduce the number of requests
		 * required and thus the overhead on devices that perform
		 * better.
		 */
		sz = size_bytes;
		if (sz >= req_size)
			sz = req_size;

		if (fallocate(fd, FALLOC_FL_ZERO_RANGE | FALLOC_FL_KEEP_SIZE,
			      offset_bytes, sz)) {
			pr_err("zeroing %s failed: %s\n", dv->devname,
			       strerror(errno));
			ret = 1;
			break;
		}

		offset_bytes += sz;
		size_bytes -= sz;
	}

	exit(ret);
}

static int wait_for_zero_forks(int *zero_pids, int count)
{
	int wstatus, ret = 0, i, sfd, wait_count = 0;
	struct signalfd_siginfo fdsi;
	bool interrupted = false;
	sigset_t sigset;
	ssize_t s;
	pid_t pid;

	for (i = 0; i < count; i++)
		if (zero_pids[i])
			wait_count++;
	if (!wait_count)
		return 0;

	sigemptyset(&sigset);
	sigaddset(&sigset, SIGINT);
	sigaddset(&sigset, SIGCHLD);
	sigprocmask(SIG_BLOCK, &sigset, NULL);

	sfd = signalfd(-1, &sigset, 0);
	if (sfd < 0) {
		pr_err("Unable to create signalfd: %s\n", strerror(errno));
		return 1;
	}

	while (wait_count) {
		s = read(sfd, &fdsi, sizeof(fdsi));
		if (s != sizeof(fdsi)) {
			pr_err("Invalid signalfd read: %s\n", strerror(errno));
			close(sfd);
			return 1;
		}

		if (fdsi.ssi_signo == SIGINT) {
			printf("\n");
			pr_info("Interrupting zeroing processes, please wait...\n");
			interrupted = true;
		} else if (fdsi.ssi_signo == SIGCHLD) {
			for (i = 0; i < count; i++) {
				if (!zero_pids[i])
					continue;

				pid = waitpid(zero_pids[i], &wstatus, WNOHANG);
				if (pid <= 0)
					continue;

				zero_pids[i] = 0;
				if (!WIFEXITED(wstatus) || WEXITSTATUS(wstatus))
					ret = 1;
				wait_count--;
			}
		}
	}

	close(sfd);

	if (interrupted) {
		pr_err("zeroing interrupted!\n");
		return 1;
	}

	if (ret)
		pr_err("zeroing failed!\n");
	else
		pr_info("zeroing finished\n");

	return ret;
}

static int add_disk_to_super(int mdfd, struct shape *s, struct context *c,
		struct supertype *st, struct mddev_dev *dv,
		struct mdinfo *info, int have_container, int major_num,
		int *zero_pid)
{
	dev_t rdev;
	int fd;

	if (dv->disposition == 'j') {
		info->disk.raid_disk = MD_DISK_ROLE_JOURNAL;
		info->disk.state = (1<<MD_DISK_JOURNAL);
	} else if (info->disk.raid_disk < s->raiddisks) {
		info->disk.state = (1<<MD_DISK_ACTIVE) |
			(1<<MD_DISK_SYNC);
	} else {
		info->disk.state = 0;
	}

	if (dv->writemostly == FlagSet) {
		if (major_num == BITMAP_MAJOR_CLUSTERED) {
			pr_err("Can not set %s --write-mostly with a clustered bitmap\n",dv->devname);
			return 1;
		} else {
			info->disk.state |= (1<<MD_DISK_WRITEMOSTLY);
		}

	}

	if (dv->failfast == FlagSet)
		info->disk.state |= (1<<MD_DISK_FAILFAST);

	if (have_container) {
		fd = -1;
	} else {
		if (st->ss->external && st->container_devnm[0])
			fd = open(dv->devname, O_RDWR);
		else
			fd = open(dv->devname, O_RDWR|O_EXCL);

		if (fd < 0) {
			pr_err("failed to open %s after earlier success - aborting\n",
			       dv->devname);
			return 1;
		}
		if (!fstat_is_blkdev(fd, dv->devname, &rdev)) {
			close(fd);
			return 1;
		}
		info->disk.major = major(rdev);
		info->disk.minor = minor(rdev);
	}
	if (fd >= 0)
		remove_partitions(fd);
	if (st->ss->add_to_super(st, &info->disk, fd, dv->devname,
				 dv->data_offset)) {
		ioctl(mdfd, STOP_ARRAY, NULL);
		close_fd(&fd);
		return 1;
	}
	st->ss->getinfo_super(st, info, NULL);

	if (fd >= 0 && s->write_zeroes) {
		*zero_pid = write_zeroes_fork(fd, s, st, dv);
		if (*zero_pid <= 0) {
			ioctl(mdfd, STOP_ARRAY, NULL);
			close(fd);
			return 1;
		}
	}

	if (have_container && c->verbose > 0)
		pr_err("Using %s for device %d\n",
		       map_dev(info->disk.major, info->disk.minor, 0),
		       info->disk.number);

	if (!have_container) {
		/* getinfo_super might have lost these ... */
		info->disk.major = major(rdev);
		info->disk.minor = minor(rdev);
	}

	return 0;
}

static int update_metadata(int mdfd, struct shape *s, struct supertype *st,
			   struct map_ent **map, struct mdinfo *info,
			   char *chosen_name)
{
	struct mdinfo info_new;
	struct map_ent *me = NULL;

	/* check to see if the uuid has changed due to these
	 * metadata changes, and if so update the member array
	 * and container uuid.  Note ->write_init_super clears
	 * the subarray cursor such that ->getinfo_super once
	 * again returns container info.
	 */
	st->ss->getinfo_super(st, &info_new, NULL);
	if (st->ss->external && !is_container(s->level) &&
	    !same_uuid(info_new.uuid, info->uuid, 0)) {
		map_update(map, fd2devnm(mdfd),
			   info_new.text_version,
			   info_new.uuid, chosen_name);
		me = map_by_devnm(map, st->container_devnm);
	}

	if (st->ss->write_init_super(st)) {
		st->ss->free_super(st);
		return 1;
	}

	/*
	 * Before activating the array, perform extra steps
	 * required to configure the internal write-intent
	 * bitmap.
	 */
	if (info_new.consistency_policy == CONSISTENCY_POLICY_BITMAP &&
	    st->ss->set_bitmap && st->ss->set_bitmap(st, info)) {
		st->ss->free_super(st);
		return 1;
	}

	/* update parent container uuid */
	if (me) {
		char *path = xstrdup(me->path);

		st->ss->getinfo_super(st, &info_new, NULL);
		map_update(map, st->container_devnm, info_new.text_version,
			   info_new.uuid, path);
		free(path);
	}

	flush_metadata_updates(st);
	st->ss->free_super(st);

	return 0;
}

static int add_disks(int mdfd, struct mdinfo *info, struct shape *s,
		     struct context *c, struct supertype *st,
		     struct map_ent **map, struct mddev_dev *devlist,
		     int total_slots, int have_container, int insert_point,
		     int major_num, char *chosen_name)
{
	struct mddev_dev *moved_disk = NULL;
	int pass, raid_disk_num, dnum;
	int zero_pids[total_slots];
	struct mddev_dev *dv;
	struct mdinfo *infos;
	sigset_t sigset, orig_sigset;
	int ret = 0;

	/*
	 * Block SIGINT so the main thread will always wait for the
	 * zeroing processes when being interrupted. Otherwise the
	 * zeroing processes will finish their work in the background
	 * keeping the disk busy.
	 */
	sigemptyset(&sigset);
	sigaddset(&sigset, SIGINT);
	sigaddset(&sigset, SIGCHLD);
	sigprocmask(SIG_BLOCK, &sigset, &orig_sigset);
	memset(zero_pids, 0, sizeof(zero_pids));

	infos = xmalloc(sizeof(*infos) * total_slots);
	enable_fds(total_slots);
	for (pass = 1; pass <= 2; pass++) {
		for (dnum = 0, raid_disk_num = 0, dv = devlist; dv;
		     dv = (dv->next) ? (dv->next) : moved_disk, dnum++) {
			if (dnum >= total_slots)
				abort();
			if (dnum == insert_point) {
				raid_disk_num += 1;
				moved_disk = dv;
				continue;
			}
			if (strcasecmp(dv->devname, "missing") == 0) {
				raid_disk_num += 1;
				continue;
			}
			if (have_container)
				moved_disk = NULL;
			if (have_container && dnum < total_slots - 1)
				/* repeatedly use the container */
				moved_disk = dv;

			switch(pass) {
			case 1:
				infos[dnum] = *info;
				infos[dnum].disk.number = dnum;
				infos[dnum].disk.raid_disk = raid_disk_num++;

				if (dv->disposition == 'j')
					raid_disk_num--;

				ret = add_disk_to_super(mdfd, s, c, st, dv,
						&infos[dnum], have_container,
						major_num, &zero_pids[dnum]);
				if (ret)
					goto out;

				break;
			case 2:
				infos[dnum].errors = 0;

				ret = add_disk(mdfd, st, info, &infos[dnum]);
				if (ret) {
					pr_err("ADD_NEW_DISK for %s failed: %s\n",
					       dv->devname, strerror(errno));
					if (errno == EINVAL &&
					    info->array.level == 0) {
						pr_err("Possibly your kernel doesn't support RAID0 layouts.\n");
						pr_err("Either upgrade, or use --layout=dangerous\n");
					}
					goto out;
				}
				break;
			}
			if (!have_container &&
			    dv == moved_disk && dnum != insert_point) break;
		}

		if (pass == 1) {
			ret = wait_for_zero_forks(zero_pids, total_slots);
			if (ret)
				goto out;

			ret = update_metadata(mdfd, s, st, map, info,
					      chosen_name);
			if (ret)
				goto out;
		}
	}

out:
	if (ret)
		wait_for_zero_forks(zero_pids, total_slots);
	free(infos);
	sigprocmask(SIG_SETMASK, &orig_sigset, NULL);
	return ret;
}

int Create(struct supertype *st, struct mddev_ident *ident, int subdevs,
	   struct mddev_dev *devlist, struct shape *s, struct context *c)
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
	 * if runstop==run, or raiddisks disks were used,
	 * RUN_ARRAY
	 */
	int mdfd;
	unsigned long long minsize = 0, maxsize = 0;
	dev_policy_t *custom_pols = NULL;
	char *mindisc = NULL;
	char *maxdisc = NULL;
	char *name = ident->name;
	int *uuid = ident->uuid_set == 1 ? ident->uuid : NULL;
	int dnum;
	struct mddev_dev *dv;
	dev_t rdev;
	int fail = 0, warn = 0;
	int first_missing = subdevs * 2;
	int second_missing = subdevs * 2;
	int missing_disks = 0;
	int insert_point = subdevs * 2; /* where to insert a missing drive */
	int total_slots;
	int rv;
	int have_container = 0;
	int container_fd = -1;
	int need_mdmon = 0;
	unsigned long long bitmapsize;
	struct mdinfo info;
	int did_default = 0;
	int do_default_layout = 0;
	int do_default_chunk = 0;
	char chosen_name[1024];
	struct map_ent *map = NULL;
	unsigned long long newsize;
	mdu_array_info_t inf;
	int major_num = BITMAP_MAJOR_HI;

	if (s->btype == BitmapCluster) {
		major_num = BITMAP_MAJOR_CLUSTERED;
		if (c->nodes <= 1) {
			pr_err("At least 2 nodes are needed for cluster-md\n");
			return 1;
		}
	}

	memset(&info, 0, sizeof(info));
	if (s->level == UnSet && st && st->ss->default_geometry)
		st->ss->default_geometry(st, &s->level, NULL, NULL);
	if (s->level == UnSet) {
		pr_err("a RAID level is needed to create an array.\n");
		return 1;
	}
	if (s->raiddisks < 4 && s->level == 6) {
		pr_err("at least 4 raid-devices needed for level 6\n");
		return 1;
	}
	if (s->raiddisks > 256 && s->level == 6) {
		pr_err("no more than 256 raid-devices supported for level 6\n");
		return 1;
	}
	if (s->raiddisks < 2 && s->level >= 4) {
		pr_err("at least 2 raid-devices needed for level %d\n", s->level);
		return 1;
	}
	if (s->level <= 0 && s->sparedisks) {
		pr_err("This level does not support spare devices\n");
		return 1;
	}

	if (subdevs == 1 && strcmp(devlist->devname, "missing") != 0) {
		/* If given a single device, it might be a container, and we can
		 * extract a device list from there
		 */
		int fd;

		memset(&inf, 0, sizeof(inf));
		fd = open(devlist->devname, O_RDONLY);
		if (fd >= 0 &&
		    md_get_array_info(fd, &inf) == 0 && inf.raid_disks == 0) {
			/* yep, looks like a container */
			if (st) {
				rv = st->ss->load_container(st, fd,
							    devlist->devname);
				if (rv == 0)
					have_container = 1;
			} else {
				st = super_by_fd(fd, NULL);
				if (st && !(rv = st->ss->
					    load_container(st, fd,
							   devlist->devname)))
					have_container = 1;
				else
					st = NULL;
			}
			if (have_container) {
				subdevs = s->raiddisks;
				first_missing = subdevs * 2;
				second_missing = subdevs * 2;
				insert_point = subdevs * 2;

				if (mddev_test_and_add_drive_policies(st, &custom_pols, fd, 1))
					exit(1);
			}
		}
		if (fd >= 0)
			close(fd);
	}
	if (st && st->ss->external && s->sparedisks) {
		pr_err("This metadata type does not support spare disks at create time\n");
		return 1;
	}
	if (subdevs > s->raiddisks+s->sparedisks+s->journaldisks) {
		pr_err("You have listed more devices (%d) than are in the array(%d)!\n", subdevs, s->raiddisks+s->sparedisks);
		return 1;
	}
	if (!have_container && subdevs < s->raiddisks+s->sparedisks+s->journaldisks) {
		pr_err("You haven't given enough devices (real or missing) to create this array\n");
		return 1;
	}
	if (s->btype != BitmapNone && s->level <= 0) {
		pr_err("bitmaps not meaningful with level %s\n",
			map_num(pers, s->level)?:"given");
		return 1;
	}

	/* now set some defaults */

	if (s->layout == UnSet) {
		do_default_layout = 1;
		s->layout = default_layout(st, s->level, c->verbose);
	}

	if (s->level == 10)
		/* check layout fits in array*/
		if ((s->layout&255) * ((s->layout>>8)&255) > s->raiddisks) {
			pr_err("that layout requires at least %d devices\n",
				(s->layout&255) * ((s->layout>>8)&255));
			return 1;
		}

	switch(s->level) {
	case 4:
	case 5:
	case 10:
	case 6:
	case 0:
		if (s->chunk == 0 || s->chunk == UnSet) {
			s->chunk = UnSet;
			do_default_chunk = 1;
			/* chunk will be set later */
		}
		break;
	case LEVEL_LINEAR:
		/* a chunksize of zero 0s perfectly valid (and preferred) since 2.6.16 */
		break;
	case 1:
	case LEVEL_FAULTY:
	case LEVEL_MULTIPATH:
	case LEVEL_CONTAINER:
		if (s->chunk) {
			pr_err("specifying chunk size is forbidden for this level\n");
			return 1;
		}
		break;
	default:
		pr_err("unknown level %d\n", s->level);
		return 1;
	}

	if (s->size == MAX_SIZE)
		/* use '0' to mean 'max' now... */
		s->size = 0;
	if (s->size && s->chunk && s->chunk != UnSet)
		if (round_size_and_verify(&s->size, s->chunk))
			return 1;

	newsize = s->size * 2;
	if (st && ! st->ss->validate_geometry(st, s->level, s->layout, s->raiddisks,
					      &s->chunk, s->size*2,
					      s->data_offset, NULL,
					      &newsize, s->consistency_policy,
					      c->verbose >= 0))
		return 1;

	if (s->chunk && s->chunk != UnSet) {
		newsize &= ~(unsigned long long)(s->chunk*2 - 1);
		if (do_default_chunk) {
			/* default chunk was just set */
			if (c->verbose > 0)
				pr_err("chunk size defaults to %dK\n", s->chunk);
			if (round_size_and_verify(&s->size, s->chunk))
				return 1;
			do_default_chunk = 0;
		}
	}

	if (s->size == 0) {
		s->size = newsize / 2;
		if (s->level == 1)
			/* If this is ever reshaped to RAID5, we will
			 * need a chunksize.  So round it off a bit
			 * now just to be safe
			 */
			s->size &= ~(64ULL-1);

		if (s->size && c->verbose > 0)
			pr_err("setting size to %lluK\n", s->size);
	}

	/* now look at the subdevs */
	info.array.active_disks = 0;
	info.array.working_disks = 0;
	dnum = 0;
	for (dv = devlist; dv; dv = dv->next)
		if (s->data_offset == VARIABLE_OFFSET)
			dv->data_offset = INVALID_SECTORS;
		else
			dv->data_offset = s->data_offset;

	for (dv=devlist; dv && !have_container; dv=dv->next, dnum++) {
		char *dname = dv->devname;
		unsigned long long freesize;
		int dfd;
		char *doff;

		if (strcasecmp(dname, "missing") == 0) {
			if (first_missing > dnum)
				first_missing = dnum;
			if (second_missing > dnum && dnum > first_missing)
				second_missing = dnum;
			missing_disks ++;
			continue;
		}
		if (s->data_offset == VARIABLE_OFFSET) {
			doff = strchr(dname, ':');
			if (doff) {
				*doff++ = 0;
				dv->data_offset = parse_size(doff);
			} else
				dv->data_offset = INVALID_SECTORS;
		} else
			dv->data_offset = s->data_offset;

		dfd = open(dname, O_RDONLY);
		if (dfd < 0) {
			pr_err("cannot open %s: %s\n",
				dname, strerror(errno));
			exit(2);
		}
		if (!fstat_is_blkdev(dfd, dname, NULL)) {
			close(dfd);
			exit(2);
		}

		info.array.working_disks++;
		if (dnum < s->raiddisks && dv->disposition != 'j')
			info.array.active_disks++;
		if (st == NULL) {
			struct createinfo *ci = conf_get_create_info();
			if (ci)
				st = ci->supertype;
		}
		if (st == NULL) {
			/* Need to choose a default metadata, which is different
			 * depending on geometry of array.
			 */
			int i;
			char *name = "default";
			for(i = 0; !st && superlist[i]; i++) {
				st = superlist[i]->match_metadata_desc(name);
				if (!st)
					continue;
				if (do_default_layout)
					s->layout = default_layout(st, s->level, c->verbose);
				switch (st->ss->validate_geometry(
						st, s->level, s->layout, s->raiddisks,
						&s->chunk, s->size*2,
						dv->data_offset, dname,
						&freesize, s->consistency_policy,
						c->verbose > 0)) {
				case -1: /* Not valid, message printed, and not
					  * worth checking any further */
					exit(2);
					break;
				case 0: /* Geometry not valid */
					free(st);
					st = NULL;
					s->chunk = do_default_chunk ? UnSet : s->chunk;
					break;
				case 1:	/* All happy */
					break;
				}
			}

			if (!st) {
				int dfd = open(dname, O_RDONLY|O_EXCL);
				if (dfd < 0) {
					pr_err("cannot open %s: %s\n",
						dname, strerror(errno));
					exit(2);
				}
				pr_err("device %s not suitable for any style of array\n",
					dname);
				exit(2);
			}
			if (st->ss != &super0 ||
			    st->minor_version != 90)
				did_default = 1;
		} else {
			if (do_default_layout)
				s->layout = default_layout(st, s->level, 0);
			if (!st->ss->validate_geometry(st, s->level, s->layout,
						       s->raiddisks,
						       &s->chunk, s->size*2,
						       dv->data_offset,
						       dname, &freesize,
						       s->consistency_policy,
						       c->verbose >= 0)) {

				pr_err("%s is not suitable for this array.\n",
				       dname);
				fail = 1;
				continue;
			}
		}

		if (drive_test_and_add_policies(st, &custom_pols, dfd, 1))
			exit(1);

		close(dfd);

		if (dv->disposition == 'j')
			goto skip_size_check;  /* skip write journal for size check */

		freesize /= 2; /* convert to K */
		if (s->chunk && s->chunk != UnSet) {
			/* round to chunk size */
			freesize = freesize & ~(s->chunk-1);
			if (do_default_chunk) {
				/* default chunk was just set */
				if (c->verbose > 0)
					pr_err("chunk size defaults to %dK\n", s->chunk);
				if (round_size_and_verify(&s->size, s->chunk))
					return 1;
				do_default_chunk = 0;
			}
		}
		if (!freesize) {
			pr_err("no free space left on %s\n", dname);
			fail = 1;
			continue;
		}

		if (s->size && freesize < s->size) {
			pr_err("%s is smaller than given size. %lluK < %lluK + metadata\n",
				dname, freesize, s->size);
			fail = 1;
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
	skip_size_check:
		if (c->runstop != 1 || c->verbose >= 0) {
			int fd = open(dname, O_RDONLY);
			if (fd < 0) {
				pr_err("Cannot open %s: %s\n",
					dname, strerror(errno));
				fail = 1;
				continue;
			}
			warn |= check_ext2(fd, dname);
			warn |= check_reiser(fd, dname);
			warn |= check_raid(fd, dname);
			if (strcmp(st->ss->name, "1.x") == 0 &&
			    st->minor_version >= 1)
				/* metadata at front */
				warn |= check_partitions(fd, dname, 0, 0);
			else if (s->level == 1 || is_container(s->level) ||
				 (s->level == 0 && s->raiddisks == 1))
				/* partitions could be meaningful */
				warn |= check_partitions(fd, dname, freesize*2, s->size*2);
			else
				/* partitions cannot be meaningful */
				warn |= check_partitions(fd, dname, 0, 0);
			if (strcmp(st->ss->name, "1.x") == 0 &&
			    st->minor_version >= 1 &&
			    did_default &&
			    s->level == 1 &&
			    (warn & 1024) == 0) {
				warn |= 1024;
				pr_err("Note: this array has metadata at the start and\n"
					"    may not be suitable as a boot device.  If you plan to\n"
					"    store '/boot' on this device please ensure that\n"
					"    your boot-loader understands md/v1.x metadata, or use\n"
					"    --metadata=0.90\n");
			}
			close(fd);
		}
	}

	if (missing_disks == dnum && !have_container) {
		pr_err("Subdevs can't be all missing\n");
		return 1;
	}
	if (s->raiddisks + s->sparedisks > st->max_devs) {
		pr_err("Too many devices: %s metadata only supports %d\n",
			st->ss->name, st->max_devs);
		return 1;
	}
	if (have_container)
		info.array.working_disks = s->raiddisks;
	if (fail) {
		pr_err("create aborted\n");
		return 1;
	}
	if (s->size == 0) {
		if (mindisc == NULL && !have_container) {
			pr_err("no size and no drives given - aborting create.\n");
			return 1;
		}
		if (s->level > 0 || s->level == LEVEL_MULTIPATH ||
		    s->level == LEVEL_FAULTY || st->ss->external) {
			/* size is meaningful */
			if (!st->ss->validate_geometry(st, s->level, s->layout,
						       s->raiddisks,
						       &s->chunk, minsize*2,
						       s->data_offset,
						       NULL, NULL,
						       s->consistency_policy, 0)) {
				pr_err("devices too large for RAID level %d\n", s->level);
				return 1;
			}
			s->size = minsize;
			if (s->level == 1)
				/* If this is ever reshaped to RAID5, we will
				 * need a chunksize.  So round it off a bit
				 * now just to be safe
				 */
				s->size &= ~(64ULL-1);
			if (c->verbose > 0)
				pr_err("size set to %lluK\n", s->size);
		}
	}

	if (s->consistency_policy == CONSISTENCY_POLICY_PPL &&
	    !st->ss->write_init_ppl) {
		pr_err("%s metadata does not support PPL\n", st->ss->name);
		return 1;
	}

	if (st->ss == &super_imsm && s->level == 10 && s->raiddisks > 4) {
		/* Print no matter runstop was specifed */
		pr_err("Warning! VROC UEFI driver does not support RAID10 in requested layout.\n");
		pr_err("Array won't be suitable as boot device.\n");
		warn = 1;
	}

	if (!have_container && s->level > 0 && ((maxsize-s->size)*100 > maxsize)) {
		if (c->runstop != 1 || c->verbose >= 0)
			pr_err("largest drive (%s) exceeds size (%lluK) by more than 1%%\n",
				maxdisc, s->size);
		warn = 1;
	}

	if (st->ss->detail_platform && st->ss->detail_platform(0, 1, NULL) != 0) {
		if (c->runstop != 1 || c->verbose >= 0)
			pr_err("%s unable to enumerate platform support\n"
				"    array may not be compatible with hardware/firmware\n",
				st->ss->name);
		warn = 1;
	}
	st->nodes = c->nodes;
	st->cluster_name = c->homecluster;

	if (warn) {
		if (c->runstop!= 1) {
			if (!ask("Continue creating array")) {
				pr_err("create aborted.\n");
				return 1;
			}
		} else {
			if (c->verbose > 0)
				pr_err("creation continuing despite oddities due to --run\n");
		}
	}

	/* If this is raid4/5, we want to configure the last active slot
	 * as missing, so that a reconstruct happens (faster than re-parity)
	 * FIX: Can we do this for raid6 as well?
	 */
	if (st->ss->external == 0 && s->assume_clean == 0 &&
	    c->force == 0 && first_missing >= s->raiddisks) {
		switch (s->level) {
		case 4:
		case 5:
			insert_point = s->raiddisks-1;
			s->sparedisks++;
			info.array.active_disks--;
			missing_disks++;
			break;
		default:
			break;
		}
	}
	/* For raid6, if creating with 1 missing drive, make a good drive
	 * into a spare, else the create will fail
	 */
	if (s->assume_clean == 0 && c->force == 0 && first_missing < s->raiddisks &&
	    st->ss->external == 0 &&
	    second_missing >= s->raiddisks && s->level == 6) {
		insert_point = s->raiddisks - 1;
		if (insert_point == first_missing)
			insert_point--;
		s->sparedisks ++;
		info.array.active_disks--;
		missing_disks++;
	}

	if (s->level <= 0 && first_missing < subdevs * 2) {
		pr_err("This level does not support missing devices\n");
		return 1;
	}

	/* We need to create the device */
	map_lock(&map);
	mdfd = create_mddev(ident->devname, ident->name, LOCAL, chosen_name, 1);
	if (mdfd < 0) {
		map_unlock(&map);
		return 1;
	}
	/* verify if chosen_name is not in use,
	 * it could be in conflict with already existing device
	 * e.g. container, array
	 */
	if (strncmp(chosen_name, DEV_MD_DIR, DEV_MD_DIR_LEN) == 0 &&
	    map_by_name(&map, chosen_name + DEV_MD_DIR_LEN)) {
		pr_err("Array name %s is in use already.\n", chosen_name);
		close(mdfd);
		map_unlock(&map);
		udev_unblock();
		return 1;
	}

	memset(&inf, 0, sizeof(inf));
	md_get_array_info(mdfd, &inf);
	if (inf.working_disks != 0) {
		pr_err("another array by this name is already running.\n");
		goto abort_locked;
	}

	/* Ok, lets try some ioctls */

	info.array.level = s->level;
	info.array.size = s->size;
	info.array.raid_disks = s->raiddisks;
	/* The kernel should *know* what md_minor we are dealing
	 * with, but it chooses to trust me instead. Sigh
	 */
	info.array.md_minor = 0;
	if (fstat_is_blkdev(mdfd, chosen_name, &rdev))
		info.array.md_minor = minor(rdev);
	info.array.not_persistent = 0;

	if (((s->level == 4 || s->level == 5) &&
	     (insert_point < s->raiddisks || first_missing < s->raiddisks)) ||
	    (s->level == 6 && (insert_point < s->raiddisks ||
			       second_missing < s->raiddisks)) ||
	    (s->level <= 0) || s->assume_clean) {
		info.array.state = 1; /* clean, but one+ drive will be missing*/
		info.resync_start = MaxSector;
	} else {
		info.array.state = 0; /* not clean, but no errors */
		info.resync_start = 0;
	}
	if (s->level == 10) {
		/* for raid10, the bitmap size is the capacity of the array,
		 * which is array.size * raid_disks / ncopies;
		 * .. but convert to sectors.
		 */
		int ncopies = ((s->layout>>8) & 255) * (s->layout & 255);
		bitmapsize = s->size * s->raiddisks / ncopies * 2;
/*		printf("bms=%llu as=%d rd=%d nc=%d\n", bitmapsize, s->size, s->raiddisks, ncopies);*/
	} else
		bitmapsize = s->size * 2;

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
	info.array.spare_disks=s->sparedisks;
	info.array.failed_disks=missing_disks;
	info.array.nr_disks = info.array.working_disks
		+ info.array.failed_disks;
	info.array.layout = s->layout;
	info.array.chunk_size = s->chunk*1024;

	if (*name == 0) {
		/* base name on devname */
		/*  /dev/md0 -> 0
		 *  /dev/md_d0 -> d0
		 *  /dev/md_foo -> foo
		 *  /dev/md/1 -> 1
		 *  /dev/md/d1 -> d1
		 *  /dev/md/home -> home
		 *  /dev/mdhome -> home
		 */
		/* FIXME compare this with rules in create_mddev */
		name = strrchr(chosen_name, '/');

		if (name) {
			name++;
			if (strncmp(name, "md_", 3) == 0 &&
			    strlen(name) > 3 && (name - chosen_name) == 5 /* /dev/ */)
				name += 3;
			else if (strncmp(name, "md", 2) == 0 &&
				 strlen(name) > 2 && isdigit(name[2]) &&
				 (name - chosen_name) == 5 /* /dev/ */)
				name += 2;
		}
	}
	if (!st->ss->init_super(st, &info.array, s, name, c->homehost, uuid,
				s->data_offset))
		goto abort_locked;

	total_slots = info.array.nr_disks;
	st->ss->getinfo_super(st, &info, NULL);
	if (sysfs_init(&info, mdfd, NULL)) {
		pr_err("unable to initialize sysfs\n");
		goto abort_locked;
	}

	if (did_default) {
		if (is_subarray(info.text_version)) {
			char devnm[MD_NAME_MAX];
			struct mdinfo *mdi;

			sysfs_get_container_devnm(&info, devnm);

			mdi = sysfs_read(-1, devnm, GET_VERSION | GET_DEVS);
			if (!mdi) {
				pr_err("Cannot open sysfs for container %s\n", devnm);
				goto abort_locked;
			}

			if (sysfs_test_and_add_drive_policies(st, &custom_pols, mdi, 1))
				goto abort_locked;

			if (c->verbose >= 0)
				pr_info("Creating array inside %s container /dev/%s\n",
					mdi->text_version, devnm);

			sysfs_free(mdi);
		} else if (c->verbose >= 0) {
			pr_info("Defaulting to version %s metadata\n", info.text_version);
		}
	}

	map_update(&map, fd2devnm(mdfd), info.text_version,
		   info.uuid, chosen_name);
	/* Keep map locked until devices have been added to array
	 * to stop another mdadm from finding and using those devices.
	 */

	if (s->btype == BitmapInternal || s->btype == BitmapCluster) {
		if (!st->ss->add_internal_bitmap) {
			pr_err("internal bitmaps not supported with %s metadata\n",
				st->ss->name);
			goto abort_locked;
		}
		if (st->ss->add_internal_bitmap(st, &s->bitmap_chunk,
						c->delay, s->write_behind,
						bitmapsize, 1, major_num)) {
			pr_err("Given bitmap chunk size not supported.\n");
			goto abort_locked;
		}
	}

	if (sysfs_init(&info, mdfd, NULL)) {
		pr_err("unable to initialize sysfs\n");
		goto abort_locked;
	}

	if (st->ss->external && st->container_devnm[0]) {
		/* member */

		/* When creating a member, we need to be careful
		 * to negotiate with mdmon properly.
		 * If it is already running, we cannot write to
		 * the devices and must ask it to do that part.
		 * If it isn't running, we write to the devices,
		 * and then start it.
		 * We hold an exclusive open on the container
		 * device to make sure mdmon doesn't exit after
		 * we checked that it is running.
		 *
		 * For now, fail if it is already running.
		 */
		container_fd = open_dev_excl(st->container_devnm);
		if (container_fd < 0) {
			pr_err("Cannot get exclusive open on container - weird.\n");
			goto abort_locked;
		}
		if (mdmon_running(st->container_devnm)) {
			if (c->verbose)
				pr_err("reusing mdmon for %s.\n",
					st->container_devnm);
			st->update_tail = &st->updates;
		} else
			need_mdmon = 1;
	}
	rv = set_array_info(mdfd, st, &info);
	if (rv) {
		pr_err("failed to set array info for %s: %s\n", chosen_name, strerror(errno));
		goto abort_locked;
	}

	if (add_disks(mdfd, &info, s, c, st, &map, devlist, total_slots,
		      have_container, insert_point, major_num, chosen_name))
		goto abort_locked;

	map_unlock(&map);

	if (is_container(s->level)) {
		/* No need to start.  But we should signal udev to
		 * create links */
		sysfs_uevent(&info, "change");
		if (c->verbose >= 0)
			pr_err("container %s prepared.\n", chosen_name);
		wait_for(chosen_name, mdfd);
	} else if (c->runstop == 1 || subdevs >= s->raiddisks) {
		if (st->ss->external) {
			int err;
			switch(s->level) {
			case LEVEL_LINEAR:
			case LEVEL_MULTIPATH:
			case 0:
				err = sysfs_set_str(&info, NULL, "array_state",
						    c->readonly
						    ? "readonly"
						    : "active");
				need_mdmon = 0;
				break;
			default:
				err = sysfs_set_str(&info, NULL, "array_state",
						    "readonly");
				break;
			}
			sysfs_set_safemode(&info, info.safe_mode_delay);
			if (err) {
				pr_err("failed to activate array.\n");
				ioctl(mdfd, STOP_ARRAY, NULL);
				goto abort;
			}
		} else if (c->readonly &&
			   sysfs_attribute_available(
				   &info, NULL, "array_state")) {
			if (sysfs_set_str(&info, NULL,
					  "array_state", "readonly") < 0) {
				pr_err("Failed to start array: %s\n",
				       strerror(errno));
				ioctl(mdfd, STOP_ARRAY, NULL);
				goto abort;
			}
		} else {
			/* param is not actually used */
			mdu_param_t param;
			if (ioctl(mdfd, RUN_ARRAY, &param)) {
				pr_err("RUN_ARRAY failed: %s\n",
				       strerror(errno));
				if (errno == 524 /* ENOTSUP */ &&
				    info.array.level == 0)
					cont_err("Please use --layout=original or --layout=alternate\n");
				if (info.array.chunk_size & (info.array.chunk_size-1)) {
					cont_err("Problem may be that chunk size is not a power of 2\n");
				}
				ioctl(mdfd, STOP_ARRAY, NULL);
				goto abort;
			}
			/* if start_ro module parameter is set, array is
			 * auto-read-only, which is bad as the resync won't
			 * start.  So lets make it read-write now.
			 */
			ioctl(mdfd, RESTART_ARRAY_RW, NULL);
		}
		if (c->verbose >= 0)
			pr_info("array %s started.\n", chosen_name);
		if (st->ss->external && st->container_devnm[0]) {
			if (need_mdmon) {
				start_mdmon(st->container_devnm);
				if (wait_for_mdmon_control_socket(st->container_devnm) != MDADM_STATUS_SUCCESS)
					goto abort;
			}
			ping_monitor(st->container_devnm);
			close(container_fd);
		}
		wait_for(chosen_name, mdfd);
	} else {
		pr_err("not starting array - not enough devices.\n");
	}
	udev_unblock();
	close(mdfd);
	sysfs_uevent(&info, "change");
	dev_policy_free(custom_pols);

	return 0;

 abort:
	udev_unblock();
	map_lock(&map);
 abort_locked:
	map_remove(&map, fd2devnm(mdfd));
	map_unlock(&map);

	close_fd(&mdfd);
	close_fd(&container_fd);

	dev_policy_free(custom_pols);
	return 1;
}
