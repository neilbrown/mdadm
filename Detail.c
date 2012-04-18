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

#include	"mdadm.h"
#include	"md_p.h"
#include	"md_u.h"
#include	<dirent.h>

int Detail(char *dev, int brief, int export, int test, char *homehost, char *prefer)
{
	/*
	 * Print out details for an md array by using
	 * GET_ARRAY_INFO and GET_DISK_INFO ioctl calls
	 */

	int fd = open(dev, O_RDONLY);
	int vers;
	mdu_array_info_t array;
	mdu_disk_info_t *disks;
	int next;
	int d;
	time_t atime;
	char *c;
	char *devices = NULL;
	int spares = 0;
	struct stat stb;
	int is_26 = get_linux_version() >= 2006000;
	int is_rebuilding = 0;
	int failed = 0;
	struct supertype *st;
	char *subarray = NULL;
	int max_disks = MD_SB_DISKS; /* just a default */
	struct mdinfo *info = NULL;
	struct mdinfo *sra;
	char *member = NULL;
	char *container = NULL;

	int rv = test ? 4 : 1;
	int avail_disks = 0;
	char *avail = NULL;

	if (fd < 0) {
		fprintf(stderr, Name ": cannot open %s: %s\n",
			dev, strerror(errno));
		return rv;
	}
	vers = md_get_version(fd);
	if (vers < 0) {
		fprintf(stderr, Name ": %s does not appear to be an md device\n",
			dev);
		close(fd);
		return rv;
	}
	if (vers < 9000) {
		fprintf(stderr, Name ": cannot get detail for md device %s: driver version too old.\n",
			dev);
		close(fd);
		return rv;
	}
	if (ioctl(fd, GET_ARRAY_INFO, &array)<0) {
		if (errno == ENODEV)
			fprintf(stderr, Name ": md device %s does not appear to be active.\n",
				dev);
		else
			fprintf(stderr, Name ": cannot get array detail for %s: %s\n",
				dev, strerror(errno));
		close(fd);
		return rv;
	}
	sra = sysfs_read(fd, 0, GET_VERSION);
	st = super_by_fd(fd, &subarray);

	if (fstat(fd, &stb) != 0 && !S_ISBLK(stb.st_mode))
		stb.st_rdev = 0;
	rv = 0;

	if (st)
		max_disks = st->max_devs;

	if (subarray) {
		/* This is a subarray of some container.
		 * We want the name of the container, and the member
		 */
		int dn = st->container_dev;

		member = subarray;
		container = map_dev_preferred(dev2major(dn), dev2minor(dn), 1, prefer);
	}

	/* try to load a superblock */
	if (st) for (d = 0; d < max_disks; d++) {
		mdu_disk_info_t disk;
		char *dv;
		int fd2;
		int err;
		disk.number = d;
		if (ioctl(fd, GET_DISK_INFO, &disk) < 0)
			continue;
		if (d >= array.raid_disks &&
		    disk.major == 0 &&
		    disk.minor == 0)
			continue;

		if (array.raid_disks > 0 &&
		    (disk.state & (1 << MD_DISK_ACTIVE)) == 0)
			continue;

		dv = map_dev(disk.major, disk.minor, 1);
		if (!dv)
			continue;

		fd2 = dev_open(dv, O_RDONLY);
		if (fd2 < 0)
			continue;

		if (st->sb)
			st->ss->free_super(st);

		err = st->ss->load_super(st, fd2, NULL);
		close(fd2);
		if (err)
			continue;
		if (info)
			free(info);
		if (subarray)
			info = st->ss->container_content(st, subarray);
		else {
			info = malloc(sizeof(*info));
			st->ss->getinfo_super(st, info, NULL);
		}
		if (!info)
			continue;

		if (array.raid_disks != 0 && /* container */
		    (info->array.ctime != array.ctime ||
		     info->array.level != array.level)) {
			st->ss->free_super(st);
			continue;
		}
		/* some formats (imsm) have free-floating-spares
		 * with a uuid of uuid_zero, they don't
		 * have very good info about the rest of the
		 * container, so keep searching when
		 * encountering such a device.  Otherwise, stop
		 * after the first successful call to
		 * ->load_super.
		 */
		if (memcmp(uuid_zero,
			   info->uuid,
			   sizeof(uuid_zero)) == 0) {
			st->ss->free_super(st);
			continue;
		}
		break;
	}

	/* Ok, we have some info to print... */
	c = map_num(pers, array.level);

	if (export) {
		if (array.raid_disks) {
			if (c)
				printf("MD_LEVEL=%s\n", c);
			printf("MD_DEVICES=%d\n", array.raid_disks);
		} else {
			printf("MD_LEVEL=container\n");
			printf("MD_DEVICES=%d\n", array.nr_disks);
		}
		if (container) {
			printf("MD_CONTAINER=%s\n", container);
			printf("MD_MEMBER=%s\n", member);
		} else {
			if (sra && sra->array.major_version < 0)
				printf("MD_METADATA=%s\n", sra->text_version);
			else
				printf("MD_METADATA=%d.%d\n",
				       array.major_version, array.minor_version);
		}
		
		if (st && st->sb && info) {
			char nbuf[64];
			struct map_ent *mp, *map = NULL;

			fname_from_uuid(st, info, nbuf, ':');
			printf("MD_UUID=%s\n", nbuf+5);
			mp = map_by_uuid(&map, info->uuid);
			if (mp && mp->path &&
			    strncmp(mp->path, "/dev/md/", 8) == 0)
				printf("MD_DEVNAME=%s\n", mp->path+8);

			if (st->ss->export_detail_super)
				st->ss->export_detail_super(st);
		} else {
			struct map_ent *mp, *map = NULL;
			char nbuf[64];
			mp = map_by_devnum(&map, fd2devnum(fd));
			if (mp) {
				__fname_from_uuid(mp->uuid, 0, nbuf, ':');
				printf("MD_UUID=%s\n", nbuf+5);
			}
			if (mp && mp->path &&
			    strncmp(mp->path, "/dev/md/", 8) == 0)
				printf("MD_DEVNAME=%s\n", mp->path+8);
		}
		goto out;
	}

	disks = malloc(max_disks * sizeof(mdu_disk_info_t));
	for (d=0; d<max_disks; d++) {
		disks[d].state = (1<<MD_DISK_REMOVED);
		disks[d].major = disks[d].minor = 0;
		disks[d].number = disks[d].raid_disk = d;
	}

	next = array.raid_disks;
	for (d=0; d < max_disks; d++) {
		mdu_disk_info_t disk;
		disk.number = d;
		if (ioctl(fd, GET_DISK_INFO, &disk) < 0) {
			if (d < array.raid_disks)
				fprintf(stderr, Name ": cannot get device detail for device %d: %s\n",
					d, strerror(errno));
			continue;
		}
		if (disk.major == 0 && disk.minor == 0)
			continue;
		if (disk.raid_disk >= 0 && disk.raid_disk < array.raid_disks)
			disks[disk.raid_disk] = disk;
		else if (next < max_disks)
			disks[next++] = disk;
	}

	avail = calloc(array.raid_disks, 1);

	for (d= 0; d < array.raid_disks; d++) {
		mdu_disk_info_t disk = disks[d];

		if ((disk.state & (1<<MD_DISK_SYNC))) {
			avail_disks ++;
			avail[d] = 1;
		}
	}

	if (brief) {
		mdu_bitmap_file_t bmf;
		printf("ARRAY %s", dev);
		if (brief > 1) {
			if (array.raid_disks)
				printf(" level=%s num-devices=%d",
				       c?c:"-unknown-",
				       array.raid_disks );
			else
				printf(" level=container num-devices=%d",
				       array.nr_disks);
		}
		if (container) {
			printf(" container=%s", container);
			printf(" member=%s", member);
		} else {
			if (sra && sra->array.major_version < 0)
				printf(" metadata=%s", sra->text_version);
			else
				printf(" metadata=%d.%d",
				       array.major_version, array.minor_version);
		}

		/* Only try GET_BITMAP_FILE for 0.90.01 and later */
		if (vers >= 9001 &&
		    ioctl(fd, GET_BITMAP_FILE, &bmf) == 0 &&
		    bmf.pathname[0]) {
			printf(" bitmap=%s", bmf.pathname);
		}
	} else {
		mdu_bitmap_file_t bmf;
		unsigned long long larray_size;
		struct mdstat_ent *ms = mdstat_read(0, 0);
		struct mdstat_ent *e;
		int devnum = array.md_minor;
		if (major(stb.st_rdev) == (unsigned)get_mdp_major())
			devnum = -1 - devnum;

		for (e=ms; e; e=e->next)
			if (e->devnum == devnum)
				break;
		if (!get_dev_size(fd, NULL, &larray_size))
			larray_size = 0;

		printf("%s:\n", dev);

		if (container)
			printf("      Container : %s, member %s\n", container, member);
		else {
		if (sra && sra->array.major_version < 0)
			printf("        Version : %s\n", sra->text_version);
		else
			printf("        Version : %d.%d\n",
			       array.major_version, array.minor_version);
		}

		atime = array.ctime;
		if (atime)
			printf("  Creation Time : %.24s\n", ctime(&atime));
		if (array.raid_disks == 0) c = "container";
		printf("     Raid Level : %s\n", c?c:"-unknown-");
		if (larray_size)
			printf("     Array Size : %llu%s\n", (larray_size>>10), human_size(larray_size));
		if (array.level >= 1) {
			if (array.major_version != 0 &&
			    (larray_size >= 0xFFFFFFFFULL|| array.size == 0)) {
				unsigned long long dsize = get_component_size(fd);
				if (dsize > 0)
					printf("  Used Dev Size : %llu%s\n",
					       dsize/2,
					 human_size((long long)dsize<<9));
				else
					printf("  Used Dev Size : unknown\n");
			} else
				printf("  Used Dev Size : %d%s\n", array.size,
				       human_size((long long)array.size<<10));
		}
		if (array.raid_disks)
			printf("   Raid Devices : %d\n", array.raid_disks);
		printf("  Total Devices : %d\n", array.nr_disks);
		if (!container && 
		    ((sra == NULL && array.major_version == 0) ||
		     (sra && sra->array.major_version == 0)))
			printf("Preferred Minor : %d\n", array.md_minor);
		if (sra == NULL || sra->array.major_version >= 0)
			printf("    Persistence : Superblock is %spersistent\n",
			       array.not_persistent?"not ":"");
		printf("\n");
		/* Only try GET_BITMAP_FILE for 0.90.01 and later */
		if (vers >= 9001 &&
		    ioctl(fd, GET_BITMAP_FILE, &bmf) == 0 &&
		    bmf.pathname[0]) {
			printf("  Intent Bitmap : %s\n", bmf.pathname);
			printf("\n");
		} else if (array.state & (1<<MD_SB_BITMAP_PRESENT))
			printf("  Intent Bitmap : Internal\n\n");
		atime = array.utime;
		if (atime)
			printf("    Update Time : %.24s\n", ctime(&atime));
		if (array.raid_disks) {
			static char *sync_action[] = {", recovering",", resyncing",", reshaping",", checking"};
			char *st;
			if (avail_disks == array.raid_disks)
				st = "";
			else if (!enough(array.level, array.raid_disks,
					 array.layout, 1, avail))
				st = ", FAILED";
			else
				st = ", degraded";

			printf("          State : %s%s%s%s%s%s \n",
			       (array.state&(1<<MD_SB_CLEAN))?"clean":"active", st,
			       (!e || (e->percent < 0 && e->percent != PROCESS_PENDING &&
			       e->percent != PROCESS_DELAYED)) ? "" : sync_action[e->resync],
			       larray_size ? "": ", Not Started",
			       e->percent == PROCESS_DELAYED ? " (DELAYED)": "",
			       e->percent == PROCESS_PENDING ? " (PENDING)": "");
		}
		if (array.raid_disks)
			printf(" Active Devices : %d\n", array.active_disks);
		printf("Working Devices : %d\n", array.working_disks);
		if (array.raid_disks) {
			printf(" Failed Devices : %d\n", array.failed_disks);
			printf("  Spare Devices : %d\n", array.spare_disks);
		}
		printf("\n");
		if (array.level == 5) {
			c = map_num(r5layout, array.layout);
			printf("         Layout : %s\n", c?c:"-unknown-");
		}
		if (array.level == 6) {
			c = map_num(r6layout, array.layout);
			printf("         Layout : %s\n", c?c:"-unknown-");
		}
		if (array.level == 10) {
			printf("         Layout :");
			print_r10_layout(array.layout);
			printf("\n");
		}
		switch (array.level) {
		case 0:
		case 4:
		case 5:
		case 10:
		case 6:
			if (array.chunk_size)
				printf("     Chunk Size : %dK\n\n",
				       array.chunk_size/1024);
			break;
		case -1:
			printf("       Rounding : %dK\n\n", array.chunk_size/1024);
			break;
		default: break;
		}

		if (e && e->percent >= 0) {
			static char *sync_action[] = {"Rebuild", "Resync", "Reshape", "Check"};
			printf(" %7s Status : %d%% complete\n", sync_action[e->resync], e->percent);
			is_rebuilding = 1;
		}
		free_mdstat(ms);

		if (st->sb && info->reshape_active) {
#if 0
This is pretty boring
			printf("  Reshape pos'n : %llu%s\n", (unsigned long long) info->reshape_progress<<9,
			       human_size((unsigned long long)info->reshape_progress<<9));
#endif
			if (info->delta_disks != 0)
				printf("  Delta Devices : %d, (%d->%d)\n",
				       info->delta_disks, array.raid_disks - info->delta_disks, array.raid_disks);
			if (info->new_level != array.level) {
				char *c = map_num(pers, info->new_level);
				printf("      New Level : %s\n", c?c:"-unknown-");
			}
			if (info->new_level != array.level ||
			    info->new_layout != array.layout) {
				if (info->new_level == 5) {
					char *c = map_num(r5layout, info->new_layout);
					printf("     New Layout : %s\n",
					       c?c:"-unknown-");
				}
				if (info->new_level == 6) {
					char *c = map_num(r6layout, info->new_layout);
					printf("     New Layout : %s\n",
					       c?c:"-unknown-");
				}
				if (info->new_level == 10) {
					printf("     New Layout : near=%d, %s=%d\n",
					       info->new_layout&255,
					       (info->new_layout&0x10000)?"offset":"far",
					       (info->new_layout>>8)&255);
				}
			}
			if (info->new_chunk != array.chunk_size)
				printf("  New Chunksize : %dK\n", info->new_chunk/1024);
			printf("\n");
		} else if (e && e->percent >= 0)
			printf("\n");
		if (st && st->sb)
			st->ss->detail_super(st, homehost);

		if (array.raid_disks == 0 && sra && sra->array.major_version == -1
		    && sra->array.minor_version == -2 && sra->text_version[0] != '/') {
			/* This looks like a container.  Find any active arrays
			 * That claim to be a member.
			 */
			DIR *dir = opendir("/sys/block");
			struct dirent *de;

			printf("  Member Arrays :");

			while (dir && (de = readdir(dir)) != NULL) {
				char path[200];
				char vbuf[1024];
				int nlen = strlen(sra->sys_name);
				int dn;
				if (de->d_name[0] == '.')
					continue;
				sprintf(path, "/sys/block/%s/md/metadata_version",
					de->d_name);
				if (load_sys(path, vbuf) < 0)
					continue;
				if (strncmp(vbuf, "external:", 9) != 0 ||
				    !is_subarray(vbuf+9) ||
				    strncmp(vbuf+10, sra->sys_name, nlen) != 0 ||
				    vbuf[10+nlen] != '/')
					continue;
				dn = devname2devnum(de->d_name);
				printf(" %s", map_dev_preferred(
					       dev2major(dn),
					       dev2minor(dn), 1, prefer));
			}
			if (dir)
				closedir(dir);
			printf("\n\n");
		}

		if (array.raid_disks)
			printf("    Number   Major   Minor   RaidDevice State\n");
		else
			printf("    Number   Major   Minor   RaidDevice\n");
	}
	free(info);

	for (d= 0; d < max_disks; d++) {
		char *dv;
		mdu_disk_info_t disk = disks[d];

		if (d >= array.raid_disks &&
		    disk.major == 0 &&
		    disk.minor == 0)
			continue;
		if (!brief) {
			if (d == array.raid_disks) printf("\n");
			if (disk.raid_disk < 0)
				printf("   %5d   %5d    %5d        -     ",
				       disk.number, disk.major, disk.minor);
			else
				printf("   %5d   %5d    %5d    %5d     ",
				       disk.number, disk.major, disk.minor, disk.raid_disk);
		}
		if (!brief && array.raid_disks) {

			if (disk.state & (1<<MD_DISK_FAULTY)) {
				printf(" faulty");
				if (disk.raid_disk < array.raid_disks &&
				    disk.raid_disk >= 0)
					failed++;
			}
			if (disk.state & (1<<MD_DISK_ACTIVE)) printf(" active");
			if (disk.state & (1<<MD_DISK_SYNC)) printf(" sync");
			if (disk.state & (1<<MD_DISK_REMOVED)) printf(" removed");
			if (disk.state & (1<<MD_DISK_WRITEMOSTLY)) printf(" writemostly");
			if ((disk.state &
			     ((1<<MD_DISK_ACTIVE)|(1<<MD_DISK_SYNC)|(1<<MD_DISK_REMOVED)))
			    == 0) {
				printf(" spare");
				if (is_26) {
					if (disk.raid_disk < array.raid_disks && disk.raid_disk >= 0)
						printf(" rebuilding");
				} else if (is_rebuilding && failed) {
					/* Taking a bit of a risk here, we remove the
					 * device from the array, and then put it back.
					 * If this fails, we are rebuilding
					 */
					int err = ioctl(fd, HOT_REMOVE_DISK, makedev(disk.major, disk.minor));
					if (err == 0) ioctl(fd, HOT_ADD_DISK, makedev(disk.major, disk.minor));
					if (err && errno ==  EBUSY)
						printf(" rebuilding");
				}
			}
		}
		if (disk.state == 0) spares++;
		if (test && d < array.raid_disks
		    && !(disk.state & (1<<MD_DISK_SYNC)))
			rv |= 1;
		if ((dv=map_dev_preferred(disk.major, disk.minor, 0, prefer))) {
			if (brief) {
				if (devices) {
					devices = realloc(devices,
							  strlen(devices)+1+strlen(dv)+1);
					strcat(strcat(devices,","),dv);
				} else
					devices = strdup(dv);
			} else
				printf("   %s", dv);
		}
		if (!brief) printf("\n");
	}
	if (spares && brief && array.raid_disks) printf(" spares=%d", spares);
	if (brief && st && st->sb)
		st->ss->brief_detail_super(st);
	st->ss->free_super(st);

	if (brief > 1 && devices) printf("\n   devices=%s", devices);
	if (brief) printf("\n");
	if (test &&
	    !enough(array.level, array.raid_disks, array.layout,
		    1, avail))
		rv = 2;

	free(disks);
out:
	close(fd);
	free(subarray);
	free(avail);
	sysfs_free(sra);
	return rv;
}

int Detail_Platform(struct superswitch *ss, int scan, int verbose)
{
	/* display platform capabilities for the given metadata format
	 * 'scan' in this context means iterate over all metadata types
	 */
	int i;
	int err = 1;

	if (ss && ss->detail_platform)
		err = ss->detail_platform(verbose, 0);
	else if (ss) {
		if (verbose)
			fprintf(stderr, Name ": %s metadata is platform independent\n",
				ss->name ? : "[no name]");
	} else if (!scan) {
		if (verbose)
			fprintf(stderr, Name ": specify a metadata type or --scan\n");
	}

	if (!scan)
		return err;

	for (i = 0; superlist[i]; i++) {
		struct superswitch *meta = superlist[i];

		if (meta == ss)
			continue;
		if (verbose)
			fprintf(stderr, Name ": checking metadata %s\n",
				meta->name ? : "[no name]");
		if (!meta->detail_platform) {
			if (verbose)
				fprintf(stderr, Name ": %s metadata is platform independent\n",
					meta->name ? : "[no name]");
		} else
			err |= meta->detail_platform(verbose, 0);
	}

	return err;
}
