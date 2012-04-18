/*
 * mdadm - manage Linux "md" devices aka RAID arrays.
 *
 * Copyright (C) 2001-2009 Neil Brown <neilb@suse.de>
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

#include "mdadm.h"
#include "md_u.h"
#include "md_p.h"
#include <ctype.h>

#define REGISTER_DEV 		_IO (MD_MAJOR, 1)
#define START_MD     		_IO (MD_MAJOR, 2)
#define STOP_MD      		_IO (MD_MAJOR, 3)

int Manage_ro(char *devname, int fd, int readonly)
{
	/* switch to readonly or rw
	 *
	 * requires >= 0.90.0
	 * first check that array is runing
	 * use RESTART_ARRAY_RW or STOP_ARRAY_RO
	 *
	 */
	mdu_array_info_t array;
#ifndef MDASSEMBLE
	struct mdinfo *mdi;
#endif
	int rv = 0;

	if (md_get_version(fd) < 9000) {
		fprintf(stderr, Name ": need md driver version 0.90.0 or later\n");
		return 1;
	}
#ifndef MDASSEMBLE
	/* If this is an externally-manage array, we need to modify the
	 * metadata_version so that mdmon doesn't undo our change.
	 */
	mdi = sysfs_read(fd, -1, GET_LEVEL|GET_VERSION);
	if (mdi &&
	    mdi->array.major_version == -1 &&
	    is_subarray(mdi->text_version)) {
		char vers[64];
		strcpy(vers, "external:");
		strcat(vers, mdi->text_version);
		if (readonly > 0) {
			int rv;
			/* We set readonly ourselves. */
			vers[9] = '-';
			sysfs_set_str(mdi, NULL, "metadata_version", vers);

			close(fd);
			rv = sysfs_set_str(mdi, NULL, "array_state", "readonly");

			if (rv < 0) {
				fprintf(stderr, Name ": failed to set readonly for %s: %s\n",
					devname, strerror(errno));

				vers[9] = mdi->text_version[0];
				sysfs_set_str(mdi, NULL, "metadata_version", vers);
				rv = 1;
				goto out;
			}
		} else {
			char *cp;
			/* We cannot set read/write - must signal mdmon */
			vers[9] = '/';
			sysfs_set_str(mdi, NULL, "metadata_version", vers);

			cp = strchr(vers+10, '/');
			if (cp)
				*cp = 0;
			ping_monitor(vers+10);
			if (mdi->array.level <= 0)
				sysfs_set_str(mdi, NULL, "array_state", "active");
		}
		goto out;
	}
#endif
	if (ioctl(fd, GET_ARRAY_INFO, &array)) {
		fprintf(stderr, Name ": %s does not appear to be active.\n",
			devname);
		rv = 1;
		goto out;
	}

	if (readonly>0) {
		if (ioctl(fd, STOP_ARRAY_RO, NULL)) {
			fprintf(stderr, Name ": failed to set readonly for %s: %s\n",
				devname, strerror(errno));
			rv = 1;
			goto out;
		}
	} else if (readonly < 0) {
		if (ioctl(fd, RESTART_ARRAY_RW, NULL)) {
			fprintf(stderr, Name ": failed to set writable for %s: %s\n",
				devname, strerror(errno));
			rv = 1;
			goto out;
		}
	}
out:
#ifndef MDASSEMBLE
	if (mdi)
		sysfs_free(mdi);
#endif
	return rv;
}

#ifndef MDASSEMBLE

static void remove_devices(int devnum, char *path)
{
	/* 
	 * Remove names at 'path' - possibly with
	 * partition suffixes - which link to the 'standard'
	 * name for devnum.  These were probably created
	 * by mdadm when the array was assembled.
	 */
	char base[40];
	char *path2;
	char link[1024];
	int n;
	int part;
	char *be;
	char *pe;

	if (!path)
		return;

	if (devnum >= 0)
		sprintf(base, "/dev/md%d", devnum);
	else
		sprintf(base, "/dev/md_d%d", -1-devnum);
	be = base + strlen(base);

	path2 = malloc(strlen(path)+20);
	strcpy(path2, path);
	pe = path2 + strlen(path2);
	
	for (part = 0; part < 16; part++) {
		if (part) {
			sprintf(be, "p%d", part);

			if (isdigit(pe[-1]))
				sprintf(pe, "p%d", part);
			else
				sprintf(pe, "%d", part);
		}
		n = readlink(path2, link, sizeof(link));
		if (n > 0 && (int)strlen(base) == n &&
		    strncmp(link, base, n) == 0)
			unlink(path2);
	}
	free(path2);
}
	

int Manage_runstop(char *devname, int fd, int runstop, int quiet)
{
	/* Run or stop the array. array must already be configured
	 * required >= 0.90.0
	 * Only print failure messages if quiet == 0;
	 * quiet > 0 means really be quiet
	 * quiet < 0 means we will try again if it fails.
	 */
	mdu_param_t param; /* unused */
	int rv = 0;

	if (runstop == -1 && md_get_version(fd) < 9000) {
		if (ioctl(fd, STOP_MD, 0)) {
			if (quiet == 0) fprintf(stderr,
						Name ": stopping device %s "
						"failed: %s\n",
						devname, strerror(errno));
			return 1;
		}
	}

	if (md_get_version(fd) < 9000) {
		fprintf(stderr, Name ": need md driver version 0.90.0 or later\n");
		return 1;
	}
	/*
	if (ioctl(fd, GET_ARRAY_INFO, &array)) {
		fprintf(stderr, Name ": %s does not appear to be active.\n",
			devname);
		return 1;
	}
	*/
	if (runstop>0) {
		if (ioctl(fd, RUN_ARRAY, &param)) {
			fprintf(stderr, Name ": failed to run array %s: %s\n",
				devname, strerror(errno));
			return 1;
		}
		if (quiet <= 0)
			fprintf(stderr, Name ": started %s\n", devname);
	} else if (runstop < 0){
		struct map_ent *map = NULL;
		struct stat stb;
		struct mdinfo *mdi;
		int devnum;
		int err;
		int count;
		/* If this is an mdmon managed array, just write 'inactive'
		 * to the array state and let mdmon clear up.
		 */
		devnum = fd2devnum(fd);
		/* Get EXCL access first.  If this fails, then attempting
		 * to stop is probably a bad idea.
		 */
		close(fd);
		fd = open(devname, O_RDONLY|O_EXCL);
		if (fd < 0 || fd2devnum(fd) != devnum) {
			if (fd >= 0)
				close(fd);
			fprintf(stderr,
				Name ": Cannot get exclusive access to %s:"
				"Perhaps a running "
				"process, mounted filesystem "
				"or active volume group?\n",
				devname);
			return 1;
		}
		mdi = sysfs_read(fd, -1, GET_LEVEL|GET_VERSION);
		if (mdi &&
		    mdi->array.level > 0 &&
		    is_subarray(mdi->text_version)) {
			int err;
			/* This is mdmon managed. */
			close(fd);

			count = 25;
			while (count &&
			       (err = sysfs_set_str(mdi, NULL,
						    "array_state",
						    "inactive")) < 0
			       && errno == EBUSY) {
				usleep(200000);
				count--;
			}
			if (err && !quiet) {
				fprintf(stderr, Name
					": failed to stop array %s: %s\n",
					devname, strerror(errno));
				rv = 1;
				goto out;
			}

			/* Give monitor a chance to act */
			ping_monitor(mdi->text_version);

			fd = open_dev_excl(devnum);
			if (fd < 0) {
				fprintf(stderr, Name
					": failed to completely stop %s"
					": Device is busy\n",
					devname);
				rv = 1;
				goto out;
			}
		} else if (mdi &&
			   mdi->array.major_version == -1 &&
			   mdi->array.minor_version == -2 &&
			   !is_subarray(mdi->text_version)) {
			struct mdstat_ent *mds, *m;
			/* container, possibly mdmon-managed.
			 * Make sure mdmon isn't opening it, which
			 * would interfere with the 'stop'
			 */
			ping_monitor(mdi->sys_name);

			/* now check that there are no existing arrays
			 * which are members of this array
			 */
			mds = mdstat_read(0, 0);
			for (m=mds; m; m=m->next)
				if (m->metadata_version &&
				    strncmp(m->metadata_version, "external:", 9)==0 &&
				    is_subarray(m->metadata_version+9) &&
				    devname2devnum(m->metadata_version+10) == devnum) {
					if (!quiet)
						fprintf(stderr, Name
							": Cannot stop container %s: "
							"member %s still active\n",
							devname, m->dev);
					free_mdstat(mds);
					rv = 1;
					goto out;
				}
		}

		/* As we have an O_EXCL open, any use of the device
		 * which blocks STOP_ARRAY is probably a transient use,
		 * so it is reasonable to retry for a while - 5 seconds.
		 */
		count = 25; err = 0;
		while (count && fd >= 0
		       && (err = ioctl(fd, STOP_ARRAY, NULL)) < 0
		       && errno == EBUSY) {
			usleep(200000);
			count --;
		}
		if (fd >= 0 && err) {
			if (quiet == 0) {
				fprintf(stderr, Name
					": failed to stop array %s: %s\n",
					devname, strerror(errno));
				if (errno == EBUSY)
					fprintf(stderr, "Perhaps a running "
						"process, mounted filesystem "
						"or active volume group?\n");
			}
			rv = 1;
			goto out;
		}
		/* prior to 2.6.28, KOBJ_CHANGE was not sent when an md array
		 * was stopped, so We'll do it here just to be sure.  Drop any
		 * partitions as well...
		 */
		if (fd >= 0)
			ioctl(fd, BLKRRPART, 0);
		if (mdi)
			sysfs_uevent(mdi, "change");

		
		if (devnum != NoMdDev &&
		    (stat("/dev/.udev", &stb) != 0 ||
		     check_env("MDADM_NO_UDEV"))) {
			struct map_ent *mp = map_by_devnum(&map, devnum);
			remove_devices(devnum, mp ? mp->path : NULL);
		}


		if (quiet <= 0)
			fprintf(stderr, Name ": stopped %s\n", devname);
		map_lock(&map);
		map_remove(&map, devnum);
		map_unlock(&map);
	out:
		if (mdi)
			sysfs_free(mdi);
	}
	return rv;
}

int Manage_resize(char *devname, int fd, long long size, int raid_disks)
{
	mdu_array_info_t info;
	if (ioctl(fd, GET_ARRAY_INFO, &info) != 0) {
		fprintf(stderr, Name ": Cannot get array information for %s: %s\n",
			devname, strerror(errno));
		return 1;
	}
	if (size >= 0)
		info.size = size;
	if (raid_disks > 0)
		info.raid_disks = raid_disks;
	if (ioctl(fd, SET_ARRAY_INFO, &info) != 0) {
		fprintf(stderr, Name ": Cannot set device size/shape for %s: %s\n",
			devname, strerror(errno));
		return 1;
	}
	return 0;
}

int Manage_subdevs(char *devname, int fd,
		   struct mddev_dev *devlist, int verbose, int test,
		   char *update, int force)
{
	/* do something to each dev.
	 * devmode can be
	 *  'a' - add the device
	 *	   try HOT_ADD_DISK
	 *         If that fails EINVAL, try ADD_NEW_DISK
	 *  'r' - remove the device HOT_REMOVE_DISK
	 *        device can be 'faulty' or 'detached' in which case all
	 *	  matching devices are removed.
	 *  'f' - set the device faulty SET_DISK_FAULTY
	 *        device can be 'detached' in which case any device that
	 *	  is inaccessible will be marked faulty.
	 * For 'f' and 'r', the device can also be a kernel-internal
	 * name such as 'sdb'.
	 */
	struct mddev_dev *add_devlist = NULL;
	mdu_array_info_t array;
	mdu_disk_info_t disc;
	unsigned long long array_size;
	struct mddev_dev *dv, *next = NULL;
	struct stat stb;
	int j, jnext = 0;
	int tfd = -1;
	struct supertype *st, *tst;
	char *subarray = NULL;
	int duuid[4];
	int ouuid[4];
	int lfd = -1;
	int sysfd = -1;
	int count = 0; /* number of actions taken */
	struct mdinfo info;
	int frozen = 0;

	if (ioctl(fd, GET_ARRAY_INFO, &array)) {
		fprintf(stderr, Name ": cannot get array info for %s\n",
			devname);
		goto abort;
	}
	sysfs_init(&info, fd, 0);

	/* array.size is only 32 bit and may be truncated.
	 * So read from sysfs if possible, and record number of sectors
	 */

	array_size = get_component_size(fd);
	if (array_size <= 0)
		array_size = array.size * 2;

	tst = super_by_fd(fd, &subarray);
	if (!tst) {
		fprintf(stderr, Name ": unsupport array - version %d.%d\n",
			array.major_version, array.minor_version);
		goto abort;
	}

	stb.st_rdev = 0;
	for (dv = devlist, j=0 ; dv; dv = next, j = jnext) {
		unsigned long long ldsize;
		char dvname[20];
		char *dnprintable = dv->devname;
		char *add_dev = dv->devname;
		int err;
		int array_failed;

		next = dv->next;
		jnext = 0;

		if (strcmp(dv->devname, "failed")==0 ||
		    strcmp(dv->devname, "faulty")==0) {
			int remaining_disks = array.nr_disks;
			if (dv->disposition != 'r') {
				fprintf(stderr, Name ": %s only meaningful "
					"with -r, not -%c\n",
					dv->devname, dv->disposition);
				goto abort;
			}
			for (; j < MAX_DISKS && remaining_disks > 0; j++) {
				unsigned dev;
				disc.number = j;
				if (ioctl(fd, GET_DISK_INFO, &disc))
					continue;
				if (disc.major == 0 && disc.minor == 0)
					continue;
				remaining_disks --;
				if ((disc.state & 1) == 0) /* faulty */
					continue;
				dev = makedev(disc.major, disc.minor);
				if (stb.st_rdev == dev)
					/* already did that one */
					continue;
				stb.st_rdev = dev;
				next = dv;
				/* same slot again next time - things might
				 * have reshuffled */
				jnext = j;
				sprintf(dvname,"%d:%d", disc.major, disc.minor);
				dnprintable = dvname;
				break;
			}
			if (next != dv)
				continue;
		} else if (strcmp(dv->devname, "detached") == 0) {
			int remaining_disks = array.nr_disks;
			if (dv->disposition != 'r' && dv->disposition != 'f') {
				fprintf(stderr, Name ": %s only meaningful "
					"with -r of -f, not -%c\n",
					dv->devname, dv->disposition);
				goto abort;
			}
			for (; j < MAX_DISKS && remaining_disks > 0; j++) {
				int sfd;
				unsigned dev;
				disc.number = j;
				if (ioctl(fd, GET_DISK_INFO, &disc))
					continue;
				if (disc.major == 0 && disc.minor == 0)
					continue;
				remaining_disks --;
				sprintf(dvname,"%d:%d", disc.major, disc.minor);
				sfd = dev_open(dvname, O_RDONLY);
				if (sfd >= 0) {
					close(sfd);
					continue;
				}
				if (dv->disposition == 'f' &&
				    (disc.state & 1) == 1) /* already faulty */
					continue;
				if (errno != ENXIO)
					continue;
				dev = makedev(disc.major, disc.minor);
				if (stb.st_rdev == dev)
					/* already did that one */
					continue;
				stb.st_rdev = dev;
				next = dv;
				/* same slot again next time - things might
				 * have reshuffled */
				jnext = j;
				dnprintable = dvname;
				break;
			}
			if (next != dv)
				continue;
		} else if (strcmp(dv->devname, "missing") == 0) {
			if (dv->disposition != 'a' || dv->re_add == 0) {
				fprintf(stderr, Name ": 'missing' only meaningful "
					"with --re-add\n");
				goto abort;
			}
			if (add_devlist == NULL)
				add_devlist = conf_get_devs();
			if (add_devlist == NULL) {
				fprintf(stderr, Name ": no devices to scan for missing members.");
				continue;
			}
			add_dev = add_devlist->devname;
			add_devlist = add_devlist->next;
			if (add_devlist != NULL)
				next = dv;
			if (stat(add_dev, &stb) < 0)
				continue;
		} else if (strchr(dv->devname, '/') == NULL &&
			   strchr(dv->devname, ':') == NULL &&
			   strlen(dv->devname) < 50) {
			/* Assume this is a kernel-internal name like 'sda1' */
			int found = 0;
			char dname[55];
			if (dv->disposition != 'r' && dv->disposition != 'f') {
				fprintf(stderr, Name ": %s only meaningful "
					"with -r or -f, not -%c\n",
					dv->devname, dv->disposition);
				goto abort;
			}

			sprintf(dname, "dev-%s", dv->devname);
			sysfd = sysfs_open(fd2devnum(fd), dname, "block/dev");
			if (sysfd >= 0) {
				char dn[20];
				int mj,mn;
				if (sysfs_fd_get_str(sysfd, dn, 20) > 0 &&
				    sscanf(dn, "%d:%d", &mj,&mn) == 2) {
					stb.st_rdev = makedev(mj,mn);
					found = 1;
				}
				close(sysfd);
				sysfd = -1;
			}
			if (!found) {
				sysfd = sysfs_open(fd2devnum(fd), dname, "state");
				if (sysfd < 0) {
					fprintf(stderr, Name ": %s does not appear "
						"to be a component of %s\n",
						dv->devname, devname);
					goto abort;
				}
			}
		} else {
			j = 0;

			tfd = dev_open(dv->devname, O_RDONLY);
			if (tfd < 0 && dv->disposition == 'r' &&
			    lstat(dv->devname, &stb) == 0)
				/* Be happy, the lstat worked, that is
				 * enough for --remove
				 */
				;
			else {
				if (tfd < 0 || fstat(tfd, &stb) != 0) {
					fprintf(stderr, Name ": cannot find %s: %s\n",
						dv->devname, strerror(errno));
					if (tfd >= 0)
						close(tfd);
					goto abort;
				}
				close(tfd);
				tfd = -1;
			}
			if ((stb.st_mode & S_IFMT) != S_IFBLK) {
				fprintf(stderr, Name ": %s is not a "
					"block device.\n",
					dv->devname);
				goto abort;
			}
		}
		switch(dv->disposition){
		default:
			fprintf(stderr, Name ": internal error - devmode[%s]=%d\n",
				dv->devname, dv->disposition);
			goto abort;
		case 'a':
			/* add the device */
			if (subarray) {
				fprintf(stderr, Name ": Cannot add disks to a"
					" \'member\' array, perform this"
					" operation on the parent container\n");
				goto abort;
			}
			/* Make sure it isn't in use (in 2.6 or later) */
			tfd = dev_open(add_dev, O_RDONLY|O_EXCL|O_DIRECT);
			if (tfd < 0 && add_dev != dv->devname)
				continue;
			if (tfd < 0) {
				fprintf(stderr, Name ": Cannot open %s: %s\n",
					dv->devname, strerror(errno));
				goto abort;
			}
			if (!frozen) {
				if (sysfs_freeze_array(&info) == 1)
					frozen = 1;
				else
					frozen = -1;
			}

			st = dup_super(tst);

			if (array.not_persistent==0)
				st->ss->load_super(st, tfd, NULL);

			if (add_dev == dv->devname) {
				if (!get_dev_size(tfd, dv->devname, &ldsize)) {
					st->ss->free_super(st);
					close(tfd);
					goto abort;
				}
			} else if (!get_dev_size(tfd, NULL, &ldsize)) {
				st->ss->free_super(st);
				close(tfd);
				tfd = -1;
				continue;
			}

			if (tst->ss->validate_geometry(
				    tst, array.level, array.layout,
				    array.raid_disks, NULL,
				    ldsize >> 9, NULL, NULL, 0) == 0) {
				if (!force) {
					fprintf(stderr, Name
						": %s is larger than %s can "
						"effectively use.\n"
						"       Add --force is you "
						"really want to add this device.\n",
						add_dev, devname);
					st->ss->free_super(st);
					close(tfd);
					goto abort;
				}
				fprintf(stderr, Name
					": %s is larger than %s can "
					"effectively use.\n"
					"       Adding anyway as --force "
					"was given.\n",
					add_dev, devname);
			}
			if (!tst->ss->external &&
			    array.major_version == 0 &&
			    md_get_version(fd)%100 < 2) {
				close(tfd);
				st->ss->free_super(st);
				tfd = -1;
				if (ioctl(fd, HOT_ADD_DISK,
					  (unsigned long)stb.st_rdev)==0) {
					if (verbose >= 0)
						fprintf(stderr, Name ": hot added %s\n",
							add_dev);
					continue;
				}

				fprintf(stderr, Name ": hot add failed for %s: %s\n",
					add_dev, strerror(errno));
				goto abort;
			}

			if (array.not_persistent == 0 || tst->ss->external) {

				/* need to find a sample superblock to copy, and
				 * a spare slot to use.
				 * For 'external' array (well, container based),
				 * We can just load the metadata for the array.
				 */
				if (tst->sb)
					/* already loaded */;
				else if (tst->ss->external) {
					tst->ss->load_container(tst, fd, NULL);
				} else for (j = 0; j < tst->max_devs; j++) {
					char *dev;
					int dfd;
					disc.number = j;
					if (ioctl(fd, GET_DISK_INFO, &disc))
						continue;
					if (disc.major==0 && disc.minor==0)
						continue;
					if ((disc.state & 4)==0) continue; /* sync */
					/* Looks like a good device to try */
					dev = map_dev(disc.major, disc.minor, 1);
					if (!dev) continue;
					dfd = dev_open(dev, O_RDONLY);
					if (dfd < 0) continue;
					if (tst->ss->load_super(tst, dfd,
								NULL)) {
						close(dfd);
						continue;
					}
					close(dfd);
					break;
				}
				/* FIXME this is a bad test to be using */
				if (!tst->sb &&
				    dv->re_add) {
					/* we are re-adding a device to a
					 * completely dead array - have to depend
					 * on kernel to check
					 */
				} else if (!tst->sb) {
					close(tfd);
					st->ss->free_super(st);
					fprintf(stderr, Name ": cannot load array metadata from %s\n", devname);
					goto abort;
				}

				/* Make sure device is large enough */
				if (tst->ss->avail_size(tst, ldsize/512) <
				    array_size) {
					close(tfd);
					tfd = -1;
					st->ss->free_super(st);
					if (add_dev != dv->devname)
						continue;
					fprintf(stderr, Name ": %s not large enough to join array\n",
						dv->devname);
					goto abort;
				}

				/* Possibly this device was recently part of the array
				 * and was temporarily removed, and is now being re-added.
				 * If so, we can simply re-add it.
				 */

				if (st->sb) {
					struct mdinfo mdi;
					st->ss->getinfo_super(st, &mdi, NULL);
					st->ss->uuid_from_super(st, ouuid);
					if (tst->sb)
						tst->ss->uuid_from_super(tst, duuid);
					else
						/* Assume uuid matches: kernel will check */
						memcpy(duuid, ouuid, sizeof(ouuid));
					if ((mdi.disk.state & (1<<MD_DISK_ACTIVE)) &&
					    !(mdi.disk.state & (1<<MD_DISK_FAULTY)) &&
					    memcmp(duuid, ouuid, sizeof(ouuid))==0) {
						/* look like it is worth a try.  Need to
						 * make sure kernel will accept it though.
						 */
						/* re-add doesn't work for version-1 superblocks
						 * before 2.6.18 :-(
						 */
						if (array.major_version == 1 &&
						    get_linux_version() <= 2006018)
							goto skip_re_add;
						disc.number = mdi.disk.number;
						if (ioctl(fd, GET_DISK_INFO, &disc) != 0
						    || disc.major != 0 || disc.minor != 0
							)
							goto skip_re_add;
						disc.major = major(stb.st_rdev);
						disc.minor = minor(stb.st_rdev);
						disc.number = mdi.disk.number;
						disc.raid_disk = mdi.disk.raid_disk;
						disc.state = mdi.disk.state;
						if (dv->writemostly == 1)
							disc.state |= 1 << MD_DISK_WRITEMOSTLY;
						if (dv->writemostly == 2)
							disc.state &= ~(1 << MD_DISK_WRITEMOSTLY);
						remove_partitions(tfd);
						close(tfd);
						tfd = -1;
						if (update || dv->writemostly > 0) {
							int rv = -1;
							tfd = dev_open(dv->devname, O_RDWR);
							if (tfd < 0) {
								fprintf(stderr, Name ": failed to open %s for"
									" superblock update during re-add\n", dv->devname);
								st->ss->free_super(st);
								goto abort;
							}

							if (dv->writemostly == 1)
								rv = st->ss->update_super(
									st, NULL, "writemostly",
									devname, verbose, 0, NULL);
							if (dv->writemostly == 2)
								rv = st->ss->update_super(
									st, NULL, "readwrite",
									devname, verbose, 0, NULL);
							if (update)
								rv = st->ss->update_super(
									st, NULL, update,
									devname, verbose, 0, NULL);
							if (rv == 0)
								rv = st->ss->store_super(st, tfd);
							close(tfd);
							tfd = -1;
							if (rv != 0) {
								fprintf(stderr, Name ": failed to update"
									" superblock during re-add\n");
								st->ss->free_super(st);
								goto abort;
							}
						}
						/* don't even try if disk is marked as faulty */
						errno = 0;
						if (ioctl(fd, ADD_NEW_DISK, &disc) == 0) {
							if (verbose >= 0)
								fprintf(stderr, Name ": re-added %s\n", add_dev);
							count++;
							st->ss->free_super(st);
							continue;
						}
						if (errno == ENOMEM || errno == EROFS) {
							fprintf(stderr, Name ": add new device failed for %s: %s\n",
								add_dev, strerror(errno));
							st->ss->free_super(st);
							if (add_dev != dv->devname)
								continue;
							goto abort;
						}
					}
				skip_re_add:
					st->ss->free_super(st);
				}
				if (add_dev != dv->devname) {
					if (verbose > 0)
						fprintf(stderr, Name
							": --re-add for %s to %s is not possible\n",
							add_dev, devname);
					if (tfd >= 0) {
						close(tfd);
						tfd = -1;
					}
					continue;
				}
				if (dv->re_add) {
					if (tfd >= 0)
						close(tfd);
					fprintf(stderr, Name
						": --re-add for %s to %s is not possible\n",
						dv->devname, devname);
					goto abort;
				}
				if (array.active_disks < array.raid_disks) {
					char *avail = calloc(array.raid_disks, 1);
					int d;
					int found = 0;

					for (d = 0; d < MAX_DISKS && found < array.active_disks; d++) {
						disc.number = d;
						if (ioctl(fd, GET_DISK_INFO, &disc))
							continue;
						if (disc.major == 0 && disc.minor == 0)
							continue;
						if (!(disc.state & (1<<MD_DISK_SYNC)))
							continue;
						avail[disc.raid_disk] = 1;
						found++;
					}
					array_failed = !enough(array.level, array.raid_disks, 
							       array.layout, 1, avail);
				} else
					array_failed = 0;
				if (array_failed) {
					fprintf(stderr, Name ": %s has failed so using --add cannot work and might destroy\n",
						devname);
					fprintf(stderr, Name ": data on %s.  You should stop the array and re-assemble it.\n",
						dv->devname);
					if (tfd >= 0)
						close(tfd);
					goto abort;
				}
			} else {
				/* non-persistent. Must ensure that new drive
				 * is at least array.size big.
				 */
				if (ldsize/512 < array_size) {
					fprintf(stderr, Name ": %s not large enough to join array\n",
						dv->devname);
					if (tfd >= 0)
						close(tfd);
					goto abort;
				}
			}
			/* committed to really trying this device now*/
			if (tfd >= 0) {
				remove_partitions(tfd);
				close(tfd);
				tfd = -1;
			}
			/* in 2.6.17 and earlier, version-1 superblocks won't
			 * use the number we write, but will choose a free number.
			 * we must choose the same free number, which requires
			 * starting at 'raid_disks' and counting up
			 */
			for (j = array.raid_disks; j< tst->max_devs; j++) {
				disc.number = j;
				if (ioctl(fd, GET_DISK_INFO, &disc))
					break;
				if (disc.major==0 && disc.minor==0)
					break;
				if (disc.state & 8) /* removed */
					break;
			}
			disc.major = major(stb.st_rdev);
			disc.minor = minor(stb.st_rdev);
			disc.number =j;
			disc.state = 0;
			if (array.not_persistent==0) {
				int dfd;
				if (dv->writemostly == 1)
					disc.state |= 1 << MD_DISK_WRITEMOSTLY;
				dfd = dev_open(dv->devname, O_RDWR | O_EXCL|O_DIRECT);
				if (tst->ss->add_to_super(tst, &disc, dfd,
							  dv->devname)) {
					close(dfd);
					goto abort;
				}
				if (tst->ss->write_init_super(tst)) {
					close(dfd);
					goto abort;
				}
			} else if (dv->re_add) {
				/*  this had better be raid1.
				 * As we are "--re-add"ing we must find a spare slot
				 * to fill.
				 */
				char *used = malloc(array.raid_disks);
				memset(used, 0, array.raid_disks);
				for (j=0; j< tst->max_devs; j++) {
					mdu_disk_info_t disc2;
					disc2.number = j;
					if (ioctl(fd, GET_DISK_INFO, &disc2))
						continue;
					if (disc2.major==0 && disc2.minor==0)
						continue;
					if (disc2.state & 8) /* removed */
						continue;
					if (disc2.raid_disk < 0)
						continue;
					if (disc2.raid_disk > array.raid_disks)
						continue;
					used[disc2.raid_disk] = 1;
				}
				for (j=0 ; j<array.raid_disks; j++)
					if (!used[j]) {
						disc.raid_disk = j;
						disc.state |= (1<<MD_DISK_SYNC);
						break;
					}
				free(used);
			}
			if (dv->writemostly == 1)
				disc.state |= (1 << MD_DISK_WRITEMOSTLY);
			if (tst->ss->external) {
				/* add a disk
				 * to an external metadata container */
				struct mdinfo new_mdi;
				struct mdinfo *sra;
				int container_fd;
				int devnum = fd2devnum(fd);
				int dfd;

				container_fd = open_dev_excl(devnum);
				if (container_fd < 0) {
					fprintf(stderr, Name ": add failed for %s:"
						" could not get exclusive access to container\n",
						dv->devname);
					tst->ss->free_super(tst);
					goto abort;
				}

				dfd = dev_open(dv->devname, O_RDWR | O_EXCL|O_DIRECT);
				if (mdmon_running(tst->container_dev))
					tst->update_tail = &tst->updates;
				if (tst->ss->add_to_super(tst, &disc, dfd,
							  dv->devname)) {
					close(dfd);
					close(container_fd);
					goto abort;
				}
				if (tst->update_tail)
					flush_metadata_updates(tst);
				else
					tst->ss->sync_metadata(tst);

				sra = sysfs_read(container_fd, -1, 0);
				if (!sra) {
					fprintf(stderr, Name ": add failed for %s: sysfs_read failed\n",
						dv->devname);
					close(container_fd);
					tst->ss->free_super(tst);
					goto abort;
				}
				sra->array.level = LEVEL_CONTAINER;
				/* Need to set data_offset and component_size */
				tst->ss->getinfo_super(tst, &new_mdi, NULL);
				new_mdi.disk.major = disc.major;
				new_mdi.disk.minor = disc.minor;
				new_mdi.recovery_start = 0;
				/* Make sure fds are closed as they are O_EXCL which
				 * would block add_disk */
				tst->ss->free_super(tst);
				if (sysfs_add_disk(sra, &new_mdi, 0) != 0) {
					fprintf(stderr, Name ": add new device to external metadata"
						" failed for %s\n", dv->devname);
					close(container_fd);
					sysfs_free(sra);
					goto abort;
				}
				ping_monitor_by_id(devnum);
				sysfs_free(sra);
				close(container_fd);
			} else {
				tst->ss->free_super(tst);
				if (ioctl(fd, ADD_NEW_DISK, &disc)) {
					fprintf(stderr, Name ": add new device failed for %s as %d: %s\n",
						dv->devname, j, strerror(errno));
					goto abort;
				}
			}
			if (verbose >= 0)
				fprintf(stderr, Name ": added %s\n", dv->devname);
			break;

		case 'r':
			/* hot remove */
			if (subarray) {
				fprintf(stderr, Name ": Cannot remove disks from a"
					" \'member\' array, perform this"
					" operation on the parent container\n");
				if (sysfd >= 0)
					close(sysfd);
				goto abort;
			}
			if (tst->ss->external) {
				/* To remove a device from a container, we must
				 * check that it isn't in use in an array.
				 * This involves looking in the 'holders'
				 * directory - there must be just one entry,
				 * the container.
				 * To ensure that it doesn't get used as a
				 * hold spare while we are checking, we
				 * get an O_EXCL open on the container
				 */
				int dnum = fd2devnum(fd);
				lfd = open_dev_excl(dnum);
				if (lfd < 0) {
					fprintf(stderr, Name
						": Cannot get exclusive access "
						" to container - odd\n");
					if (sysfd >= 0)
						close(sysfd);
					goto abort;
				}
				/* in the detached case it is not possible to
				 * check if we are the unique holder, so just
				 * rely on the 'detached' checks
				 */
				if (strcmp(dv->devname, "detached") == 0 ||
				    sysfd >= 0 ||
				    sysfs_unique_holder(dnum, stb.st_rdev))
					/* pass */;
				else {
					fprintf(stderr, Name
						": %s is %s, cannot remove.\n",
						dnprintable,
						errno == EEXIST ? "still in use":
						"not a member");
					close(lfd);
					goto abort;
				}
			}
			/* FIXME check that it is a current member */
			if (sysfd >= 0) {
				/* device has been removed and we don't know
				 * the major:minor number
				 */
				int n = write(sysfd, "remove", 6);
				if (n != 6)
					err = -1;
				else
					err = 0;
				close(sysfd);
				sysfd = -1;
			} else {
				err = ioctl(fd, HOT_REMOVE_DISK, (unsigned long)stb.st_rdev);
				if (err && errno == ENODEV) {
					/* Old kernels rejected this if no personality
					 * registered */
					struct mdinfo *sra = sysfs_read(fd, 0, GET_DEVS);
					struct mdinfo *dv = NULL;
					if (sra)
						dv = sra->devs;
					for ( ; dv ; dv=dv->next)
						if (dv->disk.major == (int)major(stb.st_rdev) &&
						    dv->disk.minor == (int)minor(stb.st_rdev))
							break;
					if (dv)
						err = sysfs_set_str(sra, dv,
								    "state", "remove");
					else
						err = -1;
					if (sra)
						sysfs_free(sra);
				}
			}
			if (err) {
				fprintf(stderr, Name ": hot remove failed "
					"for %s: %s\n",	dnprintable,
					strerror(errno));
				if (lfd >= 0)
					close(lfd);
				goto abort;
			}
			if (tst->ss->external) {
				/*
				 * Before dropping our exclusive open we make an
				 * attempt at preventing mdmon from seeing an
				 * 'add' event before reconciling this 'remove'
				 * event.
				 */
				char *name = devnum2devname(fd2devnum(fd));

				if (!name) {
					fprintf(stderr, Name ": unable to get container name\n");
					goto abort;
				}

				ping_manager(name);
				free(name);
			}
			if (lfd >= 0)
				close(lfd);
			count++;
			if (verbose >= 0)
				fprintf(stderr, Name ": hot removed %s from %s\n",
					dnprintable, devname);
			break;

		case 'f': /* set faulty */
			/* FIXME check current member */
			if ((sysfd >= 0 && write(sysfd, "faulty", 6) != 6) ||
			    (sysfd < 0 && ioctl(fd, SET_DISK_FAULTY,
						(unsigned long) stb.st_rdev))) {
				fprintf(stderr, Name ": set device faulty failed for %s:  %s\n",
					dnprintable, strerror(errno));
				if (sysfd >= 0)
					close(sysfd);
				goto abort;
			}
			if (sysfd >= 0)
				close(sysfd);
			sysfd = -1;
			count++;
			if (verbose >= 0)
				fprintf(stderr, Name ": set %s faulty in %s\n",
					dnprintable, devname);
			break;
		}
	}
	if (frozen > 0)
		sysfs_set_str(&info, NULL, "sync_action","idle");
	if (test && count == 0)
		return 2;
	return 0;

abort:
	if (frozen > 0)
		sysfs_set_str(&info, NULL, "sync_action","idle");
	return 1;
}

int autodetect(void)
{
	/* Open any md device, and issue the RAID_AUTORUN ioctl */
	int rv = 1;
	int fd = dev_open("9:0", O_RDONLY);
	if (fd >= 0) {
		if (ioctl(fd, RAID_AUTORUN, 0) == 0)
			rv = 0;
		close(fd);
	}
	return rv;
}

int Update_subarray(char *dev, char *subarray, char *update, struct mddev_ident *ident, int quiet)
{
	struct supertype supertype, *st = &supertype;
	int fd, rv = 2;

	memset(st, 0, sizeof(*st));

	fd = open_subarray(dev, subarray, st, quiet);
	if (fd < 0)
		return 2;

	if (!st->ss->update_subarray) {
		if (!quiet)
			fprintf(stderr,
				Name ": Operation not supported for %s metadata\n",
				st->ss->name);
		goto free_super;
	}

	if (mdmon_running(st->devnum))
		st->update_tail = &st->updates;

	rv = st->ss->update_subarray(st, subarray, update, ident);

	if (rv) {
		if (!quiet)
			fprintf(stderr, Name ": Failed to update %s of subarray-%s in %s\n",
				update, subarray, dev);
	} else if (st->update_tail)
		flush_metadata_updates(st);
	else
		st->ss->sync_metadata(st);

	if (rv == 0 && strcmp(update, "name") == 0 && !quiet)
		fprintf(stderr,
			Name ": Updated subarray-%s name from %s, UUIDs may have changed\n",
			subarray, dev);

 free_super:
	st->ss->free_super(st);
	close(fd);

	return rv;
}

/* Move spare from one array to another
 * If adding to destination array fails
 * add back to original array
 * Returns 1 on success, 0 on failure */
int move_spare(char *from_devname, char *to_devname, dev_t devid)
{
	struct mddev_dev devlist;
	char devname[20];

	/* try to remove and add */
	int fd1 = open(to_devname, O_RDONLY);
	int fd2 = open(from_devname, O_RDONLY);

	if (fd1 < 0 || fd2 < 0) {
		if (fd1>=0) close(fd1);
		if (fd2>=0) close(fd2);
		return 0;
	}

	devlist.next = NULL;
	devlist.used = 0;
	devlist.re_add = 0;
	devlist.writemostly = 0;
	devlist.devname = devname;
	sprintf(devname, "%d:%d", major(devid), minor(devid));

	devlist.disposition = 'r';
	if (Manage_subdevs(from_devname, fd2, &devlist, -1, 0, NULL, 0) == 0) {
		devlist.disposition = 'a';
		if (Manage_subdevs(to_devname, fd1, &devlist, -1, 0, NULL, 0) == 0) {
			/* make sure manager is aware of changes */
			ping_manager(to_devname);
			ping_manager(from_devname);
			close(fd1);
			close(fd2);
			return 1;
		}
		else Manage_subdevs(from_devname, fd2, &devlist, -1, 0, NULL, 0);
	}
	close(fd1);
	close(fd2);
	return 0;
}
#endif
