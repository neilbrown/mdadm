/*
 * mdadm - manage Linux "md" devices aka RAID arrays.
 *
 * Copyright (C) 2001-2006 Neil Brown <neilb@suse.de>
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
#include "md_u.h"
#include "md_p.h"

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

	if (md_get_version(fd) < 9000) {
		fprintf(stderr, Name ": need md driver version 0.90.0 or later\n");
		return 1;
	}
	if (ioctl(fd, GET_ARRAY_INFO, &array)) {
		fprintf(stderr, Name ": %s does not appear to be active.\n",
			devname);
		return 1;
	}

	if (readonly>0) {
		if (ioctl(fd, STOP_ARRAY_RO, NULL)) {
			fprintf(stderr, Name ": failed to set readonly for %s: %s\n",
				devname, strerror(errno));
			return 1;
		}
	} else if (readonly < 0) {
		if (ioctl(fd, RESTART_ARRAY_RW, NULL)) {
			fprintf(stderr, Name ": failed to set writable for %s: %s\n",
				devname, strerror(errno));
			return 1;
		}
	}
	return 0;
}

#ifndef MDASSEMBLE

int Manage_runstop(char *devname, int fd, int runstop, int quiet)
{
	/* Run or stop the array. array must already be configured
	 * required >= 0.90.0
	 */
	mdu_param_t param; /* unused */

	if (runstop == -1 && md_get_version(fd) < 9000) {
		if (ioctl(fd, STOP_MD, 0)) {
			if (!quiet) fprintf(stderr, Name ": stopping device %s failed: %s\n",
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
		if (ioctl(fd, STOP_ARRAY, NULL)) {
			if (quiet==0)
				fprintf(stderr, Name ": fail to stop array %s: %s\n",
					devname, strerror(errno));
			return 1;
		}
		if (quiet <= 0)
			fprintf(stderr, Name ": stopped %s\n", devname);
		if (fstat(fd, &stb) == 0) {
			int devnum;
			if (major(stb.st_rdev) == MD_MAJOR)
				devnum = minor(stb.st_rdev);
			else
				devnum = -1-(minor(stb.st_rdev)>>6);
			map_delete(&map, devnum);
			map_write(map);
			map_free(map);
		}
	}
	return 0;
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

int Manage_reconfig(char *devname, int fd, int layout)
{
	mdu_array_info_t info;
	if (ioctl(fd, GET_ARRAY_INFO, &info) != 0) {
		fprintf(stderr, Name ": Cannot get array information for %s: %s\n",
			devname, strerror(errno));
		return 1;
	}
	info.layout = layout;
	printf("layout set to %d\n", info.layout);
	if (ioctl(fd, SET_ARRAY_INFO, &info) != 0) {
		fprintf(stderr, Name ": Cannot set layout for %s: %s\n",
			devname, strerror(errno));
		return 1;
	}
	return 0;
}

int Manage_subdevs(char *devname, int fd,
		   mddev_dev_t devlist, int verbose)
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
	 */
	mdu_array_info_t array;
	mdu_disk_info_t disc;
	unsigned long long array_size;
	mddev_dev_t dv, next = NULL;
	struct stat stb;
	int j, jnext = 0;
	int tfd;
	struct supertype *st, *tst;
	int duuid[4];
	int ouuid[4];

	if (ioctl(fd, GET_ARRAY_INFO, &array)) {
		fprintf(stderr, Name ": cannot get array info for %s\n",
			devname);
		return 1;
	}

	/* array.size is only 32 bit and may be truncated.
	 * So read from sysfs if possible, and record number of sectors
	 */

	array_size = get_component_size(fd);
	if (array_size <= 0)
		array_size = array.size * 2;

	tst = super_by_fd(fd);
	if (!tst) {
		fprintf(stderr, Name ": unsupport array - version %d.%d\n",
			array.major_version, array.minor_version);
		return 1;
	}

	for (dv = devlist, j=0 ; dv; dv = next, j = jnext) {
		unsigned long long ldsize;
		char dvname[20];
		char *dnprintable = dv->devname;

		next = dv->next;
		jnext = 0;

		if (strcmp(dv->devname, "failed")==0 ||
		    strcmp(dv->devname, "faulty")==0) {
			if (dv->disposition != 'r') {
				fprintf(stderr, Name ": %s only meaningful "
					"with -r, not -%c\n",
					dv->devname, dv->disposition);
				return 1;
			}
			for (; j < array.raid_disks + array.nr_disks ; j++) {
				disc.number = j;
				if (ioctl(fd, GET_DISK_INFO, &disc))
					continue;
				if (disc.major == 0 && disc.minor == 0)
					continue;
				if ((disc.state & 1) == 0) /* faulty */
					continue;
				stb.st_rdev = makedev(disc.major, disc.minor);
				next = dv;
				jnext = j+1;
				sprintf(dvname,"%d:%d", disc.major, disc.minor);
				dnprintable = dvname;
				break;
			}
			if (jnext == 0)
				continue;
		} else if (strcmp(dv->devname, "detached") == 0) {
			if (dv->disposition != 'r' && dv->disposition != 'f') {
				fprintf(stderr, Name ": %s only meaningful "
					"with -r of -f, not -%c\n",
					dv->devname, dv->disposition);
				return 1;
			}
			for (; j < array.raid_disks + array.nr_disks; j++) {
				int sfd;
				disc.number = j;
				if (ioctl(fd, GET_DISK_INFO, &disc))
					continue;
				if (disc.major == 0 && disc.minor == 0)
					continue;
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
				stb.st_rdev = makedev(disc.major, disc.minor);
				next = dv;
				jnext = j+1;
				dnprintable = dvname;
				break;
			}
			if (jnext == 0)
				continue;
		} else {
			j = 0;

			if (stat(dv->devname, &stb)) {
				fprintf(stderr, Name ": cannot find %s: %s\n",
					dv->devname, strerror(errno));
				return 1;
			}
			if ((stb.st_mode & S_IFMT) != S_IFBLK) {
				fprintf(stderr, Name ": %s is not a "
					"block device.\n",
					dv->devname);
				return 1;
			}
		}
		switch(dv->disposition){
		default:
			fprintf(stderr, Name ": internal error - devmode[%s]=%d\n",
				dv->devname, dv->disposition);
			return 1;
		case 'a':
			/* add the device */

			/* Make sure it isn't in use (in 2.6 or later) */
			tfd = open(dv->devname, O_RDONLY|O_EXCL);
			if (tfd < 0) {
				fprintf(stderr, Name ": Cannot open %s: %s\n",
					dv->devname, strerror(errno));
				return 1;
			}
			remove_partitions(tfd);

			st = dup_super(tst);

			if (array.not_persistent==0)
				st->ss->load_super(st, tfd, NULL);

			if (!get_dev_size(tfd, dv->devname, &ldsize)) {
				close(tfd);
				return 1;
			}
			close(tfd);

			if (array.major_version == 0 &&
			    md_get_version(fd)%100 < 2) {
				if (ioctl(fd, HOT_ADD_DISK,
					  (unsigned long)stb.st_rdev)==0) {
					if (verbose >= 0)
						fprintf(stderr, Name ": hot added %s\n",
							dv->devname);
					continue;
				}

				fprintf(stderr, Name ": hot add failed for %s: %s\n",
					dv->devname, strerror(errno));
				return 1;
			}

			if (array.not_persistent == 0) {

				/* need to find a sample superblock to copy, and
				 * a spare slot to use
				 */
				for (j = 0; j < tst->max_devs; j++) {
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
				if (!tst->sb) {
					fprintf(stderr, Name ": cannot find valid superblock in this array - HELP\n");
					return 1;
				}

				/* Make sure device is large enough */
				if (tst->ss->avail_size(tst, ldsize/512) <
				    array_size) {
					fprintf(stderr, Name ": %s not large enough to join array\n",
						dv->devname);
					return 1;
				}

				/* Possibly this device was recently part of the array
				 * and was temporarily removed, and is now being re-added.
				 * If so, we can simply re-add it.
				 */
				tst->ss->uuid_from_super(tst, duuid);

				/* re-add doesn't work for version-1 superblocks
				 * before 2.6.18 :-(
				 */
				if (array.major_version == 1 &&
				    get_linux_version() <= 2006018)
					;
				else if (st->sb) {
					st->ss->uuid_from_super(st, ouuid);
					if (memcmp(duuid, ouuid, sizeof(ouuid))==0) {
						/* looks close enough for now.  Kernel
						 * will worry about whether a bitmap
						 * based reconstruction is possible.
						 */
						struct mdinfo mdi;
						st->ss->getinfo_super(st, &mdi);
						disc.major = major(stb.st_rdev);
						disc.minor = minor(stb.st_rdev);
						disc.number = mdi.disk.number;
						disc.raid_disk = mdi.disk.raid_disk;
						disc.state = mdi.disk.state;
						if (dv->writemostly)
							disc.state |= 1 << MD_DISK_WRITEMOSTLY;
						if (ioctl(fd, ADD_NEW_DISK, &disc) == 0) {
							if (verbose >= 0)
								fprintf(stderr, Name ": re-added %s\n", dv->devname);
							continue;
						}
						/* fall back on normal-add */
					}
				}
			} else {
				/* non-persistent. Must ensure that new drive
				 * is at least array.size big.
				 */
				if (ldsize/512 < array_size) {
					fprintf(stderr, Name ": %s not large enough to join array\n",
						dv->devname);
					return 1;
				}
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
				if (dv->writemostly)
					disc.state |= 1 << MD_DISK_WRITEMOSTLY;
				tst->ss->add_to_super(tst, &disc);
				if (tst->ss->write_init_super(tst, &disc,
							      dv->devname))
					return 1;
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
			}
			if (dv->writemostly)
				disc.state |= (1 << MD_DISK_WRITEMOSTLY);
			if (ioctl(fd,ADD_NEW_DISK, &disc)) {
				fprintf(stderr, Name ": add new device failed for %s as %d: %s\n",
					dv->devname, j, strerror(errno));
				return 1;
			}
			if (verbose >= 0)
				fprintf(stderr, Name ": added %s\n", dv->devname);
			break;

		case 'r':
			/* hot remove */
			/* FIXME check that it is a current member */
			if (ioctl(fd, HOT_REMOVE_DISK, (unsigned long)stb.st_rdev)) {
				fprintf(stderr, Name ": hot remove failed "
					"for %s: %s\n",	dnprintable,
					strerror(errno));
				return 1;
			}
			if (verbose >= 0)
				fprintf(stderr, Name ": hot removed %s\n",
					dnprintable);
			break;

		case 'f': /* set faulty */
			/* FIXME check current member */
			if (ioctl(fd, SET_DISK_FAULTY, (unsigned long) stb.st_rdev)) {
				fprintf(stderr, Name ": set device faulty failed for %s:  %s\n",
					dnprintable, strerror(errno));
				return 1;
			}
			if (verbose >= 0)
				fprintf(stderr, Name ": set %s faulty in %s\n",
					dnprintable, devname);
			break;
		}
	}
	return 0;

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
#endif
