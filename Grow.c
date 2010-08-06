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
#include	"dlink.h"
#include	<sys/mman.h>

#if ! defined(__BIG_ENDIAN) && ! defined(__LITTLE_ENDIAN)
#error no endian defined
#endif
#include	"md_u.h"
#include	"md_p.h"

#ifndef offsetof
#define offsetof(t,f) ((size_t)&(((t*)0)->f))
#endif

int Grow_Add_device(char *devname, int fd, char *newdev)
{
	/* Add a device to an active array.
	 * Currently, just extend a linear array.
	 * This requires writing a new superblock on the
	 * new device, calling the kernel to add the device,
	 * and if that succeeds, update the superblock on
	 * all other devices.
	 * This means that we need to *find* all other devices.
	 */
	struct mdinfo info;

	struct stat stb;
	int nfd, fd2;
	int d, nd;
	struct supertype *st = NULL;


	if (ioctl(fd, GET_ARRAY_INFO, &info.array) < 0) {
		fprintf(stderr, Name ": cannot get array info for %s\n", devname);
		return 1;
	}

	st = super_by_fd(fd);
	if (!st) {
		fprintf(stderr, Name ": cannot handle arrays with superblock version %d\n", info.array.major_version);
		return 1;
	}

	if (info.array.level != -1) {
		fprintf(stderr, Name ": can only add devices to linear arrays\n");
		return 1;
	}

	nfd = open(newdev, O_RDWR|O_EXCL|O_DIRECT);
	if (nfd < 0) {
		fprintf(stderr, Name ": cannot open %s\n", newdev);
		return 1;
	}
	fstat(nfd, &stb);
	if ((stb.st_mode & S_IFMT) != S_IFBLK) {
		fprintf(stderr, Name ": %s is not a block device!\n", newdev);
		close(nfd);
		return 1;
	}
	/* now check out all the devices and make sure we can read the superblock */
	for (d=0 ; d < info.array.raid_disks ; d++) {
		mdu_disk_info_t disk;
		char *dv;

		disk.number = d;
		if (ioctl(fd, GET_DISK_INFO, &disk) < 0) {
			fprintf(stderr, Name ": cannot get device detail for device %d\n",
				d);
			return 1;
		}
		dv = map_dev(disk.major, disk.minor, 1);
		if (!dv) {
			fprintf(stderr, Name ": cannot find device file for device %d\n",
				d);
			return 1;
		}
		fd2 = dev_open(dv, O_RDWR);
		if (!fd2) {
			fprintf(stderr, Name ": cannot open device file %s\n", dv);
			return 1;
		}
		st->ss->free_super(st);

		if (st->ss->load_super(st, fd2, NULL)) {
			fprintf(stderr, Name ": cannot find super block on %s\n", dv);
			close(fd2);
			return 1;
		}
		close(fd2);
	}
	/* Ok, looks good. Lets update the superblock and write it out to
	 * newdev.
	 */

	info.disk.number = d;
	info.disk.major = major(stb.st_rdev);
	info.disk.minor = minor(stb.st_rdev);
	info.disk.raid_disk = d;
	info.disk.state = (1 << MD_DISK_SYNC) | (1 << MD_DISK_ACTIVE);
	st->ss->update_super(st, &info, "linear-grow-new", newdev,
			     0, 0, NULL);

	if (st->ss->store_super(st, nfd)) {
		fprintf(stderr, Name ": Cannot store new superblock on %s\n",
			newdev);
		close(nfd);
		return 1;
	}
	close(nfd);

	if (ioctl(fd, ADD_NEW_DISK, &info.disk) != 0) {
		fprintf(stderr, Name ": Cannot add new disk to this array\n");
		return 1;
	}
	/* Well, that seems to have worked.
	 * Now go through and update all superblocks
	 */

	if (ioctl(fd, GET_ARRAY_INFO, &info.array) < 0) {
		fprintf(stderr, Name ": cannot get array info for %s\n", devname);
		return 1;
	}

	nd = d;
	for (d=0 ; d < info.array.raid_disks ; d++) {
		mdu_disk_info_t disk;
		char *dv;

		disk.number = d;
		if (ioctl(fd, GET_DISK_INFO, &disk) < 0) {
			fprintf(stderr, Name ": cannot get device detail for device %d\n",
				d);
			return 1;
		}
		dv = map_dev(disk.major, disk.minor, 1);
		if (!dv) {
			fprintf(stderr, Name ": cannot find device file for device %d\n",
				d);
			return 1;
		}
		fd2 = dev_open(dv, O_RDWR);
		if (fd2 < 0) {
			fprintf(stderr, Name ": cannot open device file %s\n", dv);
			return 1;
		}
		if (st->ss->load_super(st, fd2, NULL)) {
			fprintf(stderr, Name ": cannot find super block on %s\n", dv);
			close(fd);
			return 1;
		}
		info.array.raid_disks = nd+1;
		info.array.nr_disks = nd+1;
		info.array.active_disks = nd+1;
		info.array.working_disks = nd+1;

		st->ss->update_super(st, &info, "linear-grow-update", dv,
				     0, 0, NULL);

		if (st->ss->store_super(st, fd2)) {
			fprintf(stderr, Name ": Cannot store new superblock on %s\n", dv);
			close(fd2);
			return 1;
		}
		close(fd2);
	}

	return 0;
}

int Grow_addbitmap(char *devname, int fd, char *file, int chunk, int delay, int write_behind, int force)
{
	/*
	 * First check that array doesn't have a bitmap
	 * Then create the bitmap
	 * Then add it
	 *
	 * For internal bitmaps, we need to check the version,
	 * find all the active devices, and write the bitmap block
	 * to all devices
	 */
	mdu_bitmap_file_t bmf;
	mdu_array_info_t array;
	struct supertype *st;
	int major = BITMAP_MAJOR_HI;
	int vers = md_get_version(fd);
	unsigned long long bitmapsize, array_size;

	if (vers < 9003) {
		major = BITMAP_MAJOR_HOSTENDIAN;
#ifdef __BIG_ENDIAN
		fprintf(stderr, Name ": Warning - bitmaps created on this kernel are not portable\n"
			"  between different architectured.  Consider upgrading the Linux kernel.\n");
#endif
	}

	if (ioctl(fd, GET_BITMAP_FILE, &bmf) != 0) {
		if (errno == ENOMEM)
			fprintf(stderr, Name ": Memory allocation failure.\n");
		else
			fprintf(stderr, Name ": bitmaps not supported by this kernel.\n");
		return 1;
	}
	if (bmf.pathname[0]) {
		if (strcmp(file,"none")==0) {
			if (ioctl(fd, SET_BITMAP_FILE, -1)!= 0) {
				fprintf(stderr, Name ": failed to remove bitmap %s\n",
					bmf.pathname);
				return 1;
			}
			return 0;
		}
		fprintf(stderr, Name ": %s already has a bitmap (%s)\n",
			devname, bmf.pathname);
		return 1;
	}
	if (ioctl(fd, GET_ARRAY_INFO, &array) != 0) {
		fprintf(stderr, Name ": cannot get array status for %s\n", devname);
		return 1;
	}
	if (array.state & (1<<MD_SB_BITMAP_PRESENT)) {
		if (strcmp(file, "none")==0) {
			array.state &= ~(1<<MD_SB_BITMAP_PRESENT);
			if (ioctl(fd, SET_ARRAY_INFO, &array)!= 0) {
				fprintf(stderr, Name ": failed to remove internal bitmap.\n");
				return 1;
			}
			return 0;
		}
		fprintf(stderr, Name ": Internal bitmap already present on %s\n",
			devname);
		return 1;
	}
	if (array.level <= 0) {
		fprintf(stderr, Name ": Bitmaps not meaningful with level %s\n",
			map_num(pers, array.level)?:"of this array");
		return 1;
	}
	bitmapsize = array.size;
	bitmapsize <<= 1;
	if (get_dev_size(fd, NULL, &array_size) &&
	    array_size > (0x7fffffffULL<<9)) {
		/* Array is big enough that we cannot trust array.size
		 * try other approaches
		 */
		bitmapsize = get_component_size(fd);
	}
	if (bitmapsize == 0) {
		fprintf(stderr, Name ": Cannot reliably determine size of array to create bitmap - sorry.\n");
		return 1;
	}

	if (array.level == 10) {
		int ncopies = (array.layout&255)*((array.layout>>8)&255);
		bitmapsize = bitmapsize * array.raid_disks / ncopies;
	}

	st = super_by_fd(fd);
	if (!st) {
		fprintf(stderr, Name ": Cannot understand version %d.%d\n",
			array.major_version, array.minor_version);
		return 1;
	}
	if (strcmp(file, "none") == 0) {
		fprintf(stderr, Name ": no bitmap found on %s\n", devname);
		return 1;
	} else if (strcmp(file, "internal") == 0) {
		int d;
		if (st->ss->add_internal_bitmap == NULL) {
			fprintf(stderr, Name ": Internal bitmaps not supported "
				"with %s metadata\n", st->ss->name);
			return 1;
		}
		for (d=0; d< st->max_devs; d++) {
			mdu_disk_info_t disk;
			char *dv;
			disk.number = d;
			if (ioctl(fd, GET_DISK_INFO, &disk) < 0)
				continue;
			if (disk.major == 0 &&
			    disk.minor == 0)
				continue;
			if ((disk.state & (1<<MD_DISK_SYNC))==0)
				continue;
			dv = map_dev(disk.major, disk.minor, 1);
			if (dv) {
				int fd2 = dev_open(dv, O_RDWR);
				if (fd2 < 0)
					continue;
				if (st->ss->load_super(st, fd2, NULL)==0) {
					if (st->ss->add_internal_bitmap(
						    st,
						    &chunk, delay, write_behind,
						    bitmapsize, 0, major)
						)
						st->ss->write_bitmap(st, fd2);
					else {
						fprintf(stderr, Name ": failed to create internal bitmap - chunksize problem.\n");
						close(fd2);
						return 1;
					}
				}
				close(fd2);
			}
		}
		array.state |= (1<<MD_SB_BITMAP_PRESENT);
		if (ioctl(fd, SET_ARRAY_INFO, &array)!= 0) {
			fprintf(stderr, Name ": failed to set internal bitmap.\n");
			return 1;
		}
	} else {
		int uuid[4];
		int bitmap_fd;
		int d;
		int max_devs = st->max_devs;

		/* try to load a superblock */
		for (d=0; d<max_devs; d++) {
			mdu_disk_info_t disk;
			char *dv;
			int fd2;
			disk.number = d;
			if (ioctl(fd, GET_DISK_INFO, &disk) < 0)
				continue;
			if ((disk.major==0 && disk.minor==0) ||
			    (disk.state & (1<<MD_DISK_REMOVED)))
				continue;
			dv = map_dev(disk.major, disk.minor, 1);
			if (!dv) continue;
			fd2 = dev_open(dv, O_RDONLY);
			if (fd2 >= 0 &&
			    st->ss->load_super(st, fd2, NULL) == 0) {
				close(fd2);
				st->ss->uuid_from_super(st, uuid);
				break;
			}
			close(fd2);
		}
		if (d == max_devs) {
			fprintf(stderr, Name ": cannot find UUID for array!\n");
			return 1;
		}
		if (CreateBitmap(file, force, (char*)uuid, chunk,
				 delay, write_behind, bitmapsize, major)) {
			return 1;
		}
		bitmap_fd = open(file, O_RDWR);
		if (bitmap_fd < 0) {
			fprintf(stderr, Name ": weird: %s cannot be opened\n",
				file);
			return 1;
		}
		if (ioctl(fd, SET_BITMAP_FILE, bitmap_fd) < 0) {
			fprintf(stderr, Name ": Cannot set bitmap file for %s: %s\n",
				devname, strerror(errno));
			return 1;
		}
	}

	return 0;
}


/*
 * When reshaping an array we might need to backup some data.
 * This is written to all spares with a 'super_block' describing it.
 * The superblock goes 4K from the end of the used space on the
 * device.
 * It if written after the backup is complete.
 * It has the following structure.
 */

static struct mdp_backup_super {
	char	magic[16];  /* md_backup_data-1 or -2 */
	__u8	set_uuid[16];
	__u64	mtime;
	/* start/sizes in 512byte sectors */
	__u64	devstart;	/* address on backup device/file of data */
	__u64	arraystart;
	__u64	length;
	__u32	sb_csum;	/* csum of preceeding bytes. */
	__u32   pad1;
	__u64	devstart2;	/* offset in to data of second section */
	__u64	arraystart2;
	__u64	length2;
	__u32	sb_csum2;	/* csum of preceeding bytes. */
	__u8 pad[512-68-32];
} __attribute__((aligned(512))) bsb, bsb2;

__u32 bsb_csum(char *buf, int len)
{
	int i;
	int csum = 0;
	for (i=0; i<len; i++)
		csum = (csum<<3) + buf[0];
	return __cpu_to_le32(csum);
}

static int child_grow(int afd, struct mdinfo *sra, unsigned long blocks,
		      int *fds, unsigned long long *offsets,
		      int disks, int chunk, int level, int layout, int data,
		      int dests, int *destfd, unsigned long long *destoffsets);
static int child_shrink(int afd, struct mdinfo *sra, unsigned long blocks,
			int *fds, unsigned long long *offsets,
			int disks, int chunk, int level, int layout, int data,
			int dests, int *destfd, unsigned long long *destoffsets);
static int child_same_size(int afd, struct mdinfo *sra, unsigned long blocks,
			   int *fds, unsigned long long *offsets,
			   unsigned long long start,
			   int disks, int chunk, int level, int layout, int data,
			   int dests, int *destfd, unsigned long long *destoffsets);

int freeze_array(struct mdinfo *sra)
{
	/* Try to freeze resync on this array.
	 * Return -1 if the array is busy,
	 * return 0 if this kernel doesn't support 'frozen'
	 * return 1 if it worked.
	 */
	char buf[20];
	if (sysfs_get_str(sra, NULL, "sync_action", buf, 20) <= 0)
		return 0;
	if (strcmp(buf, "idle\n") != 0 &&
	    strcmp(buf, "frozen\n") != 0)
		return -1;
	if (sysfs_set_str(sra, NULL, "sync_action", "frozen") < 0)
		return 0;
	return 1;
}

void unfreeze_array(struct mdinfo *sra, int frozen)
{
	/* If 'frozen' is 1, unfreeze the array */
	if (frozen > 0)
		sysfs_set_str(sra, NULL, "sync_action", "idle");
}

void wait_reshape(struct mdinfo *sra)
{
	int fd = sysfs_get_fd(sra, NULL, "sync_action");
	char action[20];

	do {
		fd_set rfds;
		FD_ZERO(&rfds);
		FD_SET(fd, &rfds);
		select(fd+1, NULL, NULL, &rfds, NULL);
		
		if (sysfs_fd_get_str(fd, action, 20) < 0) {
			close(fd);
			return;
		}
	} while  (strncmp(action, "reshape", 7) == 0);
}
			
		
int Grow_reshape(char *devname, int fd, int quiet, char *backup_file,
		 long long size,
		 int level, char *layout_str, int chunksize, int raid_disks)
{
	/* Make some changes in the shape of an array.
	 * The kernel must support the change.
	 *
	 * There are three different changes.  Each can trigger
	 * a resync or recovery so we freeze that until we have
	 * requested everything (if kernel supports freezing - 2.6.30).
	 * The steps are:
	 *  - change size (i.e. component_size)
	 *  - change level
	 *  - change layout/chunksize/ndisks
	 *
	 * The last can require a reshape.  It is different on different
	 * levels so we need to check the level before actioning it.
	 * Some times the level change needs to be requested after the
	 * reshape (e.g. raid6->raid5, raid5->raid0)
	 *
	 */
	struct mdu_array_info_s array, orig;
	char *c;
	int rv = 0;
	struct supertype *st;

	int nchunk, ochunk;
	int nlayout, olayout;
	int ndisks, odisks;
	unsigned int ndata, odata;
	int orig_level = UnSet;
	char alt_layout[40];
	int *fdlist;
	unsigned long long *offsets;
	int d, i;
	int nrdisks;
	int err;
	int frozen;
	unsigned long a,b, blocks, stripes;
	unsigned long cache;
	unsigned long long array_size;
	int changed = 0;
	int done;

	struct mdinfo *sra;
	struct mdinfo *sd;

	if (ioctl(fd, GET_ARRAY_INFO, &array) < 0) {
		fprintf(stderr, Name ": %s is not an active md array - aborting\n",
			devname);
		return 1;
	}

	if (size >= 0 &&
	    (chunksize || level!= UnSet || layout_str || raid_disks)) {
		fprintf(stderr, Name ": cannot change component size at the same time "
			"as other changes.\n"
			"   Change size first, then check data is intact before "
			"making other changes.\n");
		return 1;
	}

	if (raid_disks && raid_disks < array.raid_disks && array.level > 1 &&
	    get_linux_version() < 2006032 &&
	    !check_env("MDADM_FORCE_FEWER")) {
		fprintf(stderr, Name ": reducing the number of devices is not safe before Linux 2.6.32\n"
			"       Please use a newer kernel\n");
		return 1;
	}
	sra = sysfs_read(fd, 0, GET_LEVEL);
	if (sra)
		frozen = freeze_array(sra);
	else {
		fprintf(stderr, Name ": failed to read sysfs parameters for %s\n",
			devname);
		return 1;
	}
	if (frozen < 0) {
		fprintf(stderr, Name ": %s is performing resync/recovery and cannot"
			" be reshaped\n", devname);
		return 1;
	}

	/* ========= set size =============== */
	if (size >= 0 && (size == 0 || size != array.size)) {
		array.size = size;
		if (array.size != size) {
			/* got truncated to 32bit, write to
			 * component_size instead
			 */
			if (sra)
				rv = sysfs_set_num(sra, NULL,
						   "component_size", size);
			else
				rv = -1;
		} else
			rv = ioctl(fd, SET_ARRAY_INFO, &array);
		if (rv != 0) {
			int err = errno;
			fprintf(stderr, Name ": Cannot set device size for %s: %s\n",
				devname, strerror(err));
			if (err == EBUSY && 
			    (array.state & (1<<MD_SB_BITMAP_PRESENT)))
				fprintf(stderr, "       Bitmap must be removed before size can be changed\n");
			rv = 1;
			goto release;
		}
		ioctl(fd, GET_ARRAY_INFO, &array);
		size = get_component_size(fd)/2;
		if (size == 0)
			size = array.size;
		if (!quiet)
			fprintf(stderr, Name ": component size of %s has been set to %lluK\n",
				devname, size);
		changed = 1;
	} else {
		size = get_component_size(fd)/2;
		if (size == 0)
			size = array.size;
	}

	/* ======= set level =========== */
	if (level != UnSet && level != array.level) {
		/* Trying to change the level.
		 * We might need to change layout first and schedule a
		 * level change for later.
		 * Level changes that can happen immediately are:
		 * 0->4,5,6  1->5  4->5,6  5->1,6
		 * Level changes that need a layout change first are:
		 * 6->5,4,0 : need a -6 layout, or parity-last
		 * 5->4,0   : need parity-last
		 */
		if ((array.level == 6 || array.level == 5) &&
		    (level == 5 || level == 4 || level == 0)) {
			/* Don't change level yet, but choose intermediate
			 * layout
			 */
			if (level == 5) {
				if (layout_str == NULL)
					switch (array.layout) {
					case ALGORITHM_LEFT_ASYMMETRIC:
					case ALGORITHM_LEFT_ASYMMETRIC_6:
					case ALGORITHM_ROTATING_N_RESTART:
						layout_str = "left-asymmetric-6";
						break;
					case ALGORITHM_LEFT_SYMMETRIC:
					case ALGORITHM_LEFT_SYMMETRIC_6:
					case ALGORITHM_ROTATING_N_CONTINUE:
						layout_str = "left-symmetric-6";
						break;
					case ALGORITHM_RIGHT_ASYMMETRIC:
					case ALGORITHM_RIGHT_ASYMMETRIC_6:
					case ALGORITHM_ROTATING_ZERO_RESTART:
						layout_str = "right-asymmetric-6";
						break;
					case ALGORITHM_RIGHT_SYMMETRIC:
					case ALGORITHM_RIGHT_SYMMETRIC_6:
						layout_str = "right-symmetric-6";
						break;
					case ALGORITHM_PARITY_0:
					case ALGORITHM_PARITY_0_6:
						layout_str = "parity-first-6";
						break;
					case ALGORITHM_PARITY_N:
						layout_str = "parity-last";
						break;
					default:
						fprintf(stderr, Name ": %s: cannot"
							"convert layout to RAID5 equivalent\n",
							devname);
						rv = 1;
						goto release;
					}
				else {
					int l = map_name(r5layout, layout_str);
					if (l == UnSet) {
						fprintf(stderr, Name ": %s: layout '%s' not recognised\n",
							devname, layout_str);
						rv = 1;
						goto release;
					}
					if (l != ALGORITHM_PARITY_N) {
						/* need the -6 version */
						char *ls = map_num(r5layout, l);
						strcat(strcpy(alt_layout, ls),
						       "-6");
						layout_str = alt_layout;
					}
				}
				if (raid_disks)
					/* The final raid6->raid5 conversion
					 * will reduce the number of disks,
					 * so now we need to aim higher
					 */
					raid_disks++;
			} else
				layout_str = "parity-last";
		} else {
			c = map_num(pers, level);
			if (c == NULL) {
				rv = 1;/* not possible */
				goto release;
			}
			err = sysfs_set_str(sra, NULL, "level", c);
			if (err) {
				err = errno;
				fprintf(stderr, Name ": %s: could not set level to %s\n",
					devname, c);
				if (err == EBUSY && 
				    (array.state & (1<<MD_SB_BITMAP_PRESENT)))
					fprintf(stderr, "       Bitmap must be removed before level can be changed\n");
				rv = 1;
				goto release;
			}
			orig = array;
			orig_level = orig.level;
			ioctl(fd, GET_ARRAY_INFO, &array);
			if (layout_str == NULL &&
			    orig.level == 5 && level == 6 &&
			    array.layout != orig.layout)
				layout_str = map_num(r5layout, orig.layout);
			if (!quiet)
				fprintf(stderr, Name " level of %s changed to %s\n",
					devname, c);
			changed = 1;
		}
	}

	/* ========= set shape (chunk_size / layout / ndisks)  ============== */
	/* Check if layout change is a no-op */
	if (layout_str) switch(array.level) {
	case 5:
		if (array.layout == map_name(r5layout, layout_str))
			layout_str = NULL;
		break;
	case 6:
		if (layout_str == NULL &&
		    ((chunksize && chunksize * 1024 != array.chunk_size) ||
		     (raid_disks && raid_disks != array.raid_disks)) &&
		    array.layout >= 16) {
			fprintf(stderr, Name
				": %s has a non-standard layout.  If you wish to preserve this\n"
				"      during the reshape, please specify --layout=preserve\n"
				"      If you want to change it, specify a layout or use --layout=normalise\n",
				devname);
			rv = 1;
			goto release;
		}
		if (strcmp(layout_str, "normalise") == 0 ||
		    strcmp(layout_str, "normalize") == 0) {
			char *hyphen;
			strcpy(alt_layout, map_num(r6layout, array.layout));
			hyphen = strrchr(alt_layout, '-');
			if (hyphen && strcmp(hyphen, "-6") == 0) {
				*hyphen = 0;
				layout_str = alt_layout;
			}
		}

		if (array.layout == map_name(r6layout, layout_str))
			layout_str = NULL;
		if (layout_str && strcmp(layout_str, "preserve") == 0)
			layout_str = NULL;
		break;
	}
	if (layout_str == NULL
	    && (chunksize == 0 || chunksize*1024 == array.chunk_size)
	    && (raid_disks == 0 || raid_disks == array.raid_disks)) {
		rv = 0;
		if (level != UnSet && level != array.level) {
			/* Looks like this level change doesn't need
			 * a reshape after all.
			 */
			c = map_num(pers, level);
			if (c) {
				rv = sysfs_set_str(sra, NULL, "level", c);
				if (rv) {
					int err = errno;
					fprintf(stderr, Name ": %s: could not set level to %s\n",
						devname, c);
					if (err == EBUSY && 
					    (array.state & (1<<MD_SB_BITMAP_PRESENT)))
						fprintf(stderr, "       Bitmap must be removed before level can be changed\n");
					rv = 1;
				}
			}
		} else if (!changed && !quiet)
			fprintf(stderr, Name ": %s: no change requested\n",
				devname);
		goto release;
	}

	c = map_num(pers, array.level);
	if (c == NULL) c = "-unknown-";
	switch(array.level) {
	default: /* raid0, linear, multipath cannot be reconfigured */
		fprintf(stderr, Name ": %s array %s cannot be reshaped.\n",
			c, devname);
		rv = 1;
		break;

	case LEVEL_FAULTY: /* only 'layout' change is permitted */

		if (chunksize  || raid_disks) {
			fprintf(stderr, Name ": %s: Cannot change chunksize or disks of a 'faulty' array\n",
				devname);
			rv = 1;
			break;
		}
		if (layout_str == NULL)
			break; /* nothing to do.... */

		array.layout = parse_layout_faulty(layout_str);
		if (array.layout < 0) {
			fprintf(stderr, Name ": %s: layout %s not understood for 'faulty' array\n",
				devname, layout_str);
			rv = 1;
			break;
		}
		if (ioctl(fd, SET_ARRAY_INFO, &array) != 0) {
			fprintf(stderr, Name ": Cannot set layout for %s: %s\n",
				devname, strerror(errno));
			rv = 1;
		} else if (!quiet)
			printf("layout for %s set to %d\n", devname, array.layout);
		break;

	case 1: /* only raid_disks can each be changed. */

		if (chunksize || layout_str != NULL) {
			fprintf(stderr, Name ": %s: Cannot change chunk size or layout for a RAID1 array.\n",
				devname);
			rv = 1;
			break;
		}
		if (raid_disks > 0) {
			array.raid_disks = raid_disks;
			if (ioctl(fd, SET_ARRAY_INFO, &array) != 0) {
				fprintf(stderr, Name ": Cannot set raid-devices for %s: %s\n",
					devname, strerror(errno));
				rv = 1;
			}
		}
		break;

	case 4:
	case 5:
	case 6:

		/*
		 * layout/chunksize/raid_disks can be changed
		 * though the kernel may not support it all.
		 */
		st = super_by_fd(fd);

		/*
		 * There are three possibilities.
		 * 1/ The array will shrink.
		 *    We need to ensure the reshape will pause before reaching
		 *    the 'critical section'.  We also need to fork and wait for
		 *    that to happen.  When it does we 
		 *       suspend/backup/complete/unfreeze
		 *
		 * 2/ The array will not change size.
		 *    This requires that we keep a backup of a sliding window
		 *    so that we can restore data after a crash.  So we need
		 *    to fork and monitor progress.
		 *
		 * 3/ The array will grow. This is relatively easy.
		 *    However the kernel's restripe routines will cheerfully
		 *    overwrite some early data before it is safe.  So we
		 *    need to make a backup of the early parts of the array
		 *    and be ready to restore it if rebuild aborts very early.
		 *
		 *    We backup data by writing it to one spare, or to a
		 *    file which was given on command line.
		 *
		 *    [FOLLOWING IS OLD AND PARTLY WRONG]
		 *    So: we enumerate the devices in the array and
		 *    make sure we can open all of them.
		 *    Then we freeze the early part of the array and
		 *    backup to the various spares.
		 *    Then we request changes and start the reshape.
		 *    Monitor progress until it has passed the danger zone.
		 *    and finally invalidate the copied data and unfreeze the
		 *    start of the array.
		 *
		 * In each case, we first make sure that storage is available
		 * for the required backup.
		 * Then we:
		 *   -  request the shape change.
		 *   -  for to handle backup etc.
		 */
		nchunk = ochunk = array.chunk_size;
		nlayout = olayout = array.layout;
		ndisks = odisks = array.raid_disks;

		if (chunksize) {
			nchunk = chunksize * 1024;
			if (size % chunksize) {
				fprintf(stderr, Name ": component size %lluK is not"
					" a multiple of chunksize %dK\n",
					size, chunksize);
				break;
			}
		}
		if (layout_str != NULL)
			switch(array.level) {
			case 4: /* ignore layout */
				break;
			case 5:
				nlayout = map_name(r5layout, layout_str);
				if (nlayout == UnSet) {
					fprintf(stderr, Name ": layout %s not understood for raid5.\n",
						layout_str);
					rv = 1;
					goto release;
				}
				break;

			case 6:
				nlayout = map_name(r6layout, layout_str);
				if (nlayout == UnSet) {
					fprintf(stderr, Name ": layout %s not understood for raid6.\n",
						layout_str);
					rv = 1;
					goto release;
				}
				break;
			}
		if (raid_disks) ndisks = raid_disks;

		odata = odisks-1;
		ndata = ndisks-1;
		if (array.level == 6) {
			odata--; /* number of data disks */
			ndata--;
		}

		if (odata == ndata &&
		    get_linux_version() < 2006032) {
			fprintf(stderr, Name ": in-place reshape is not safe before 2.6.32, sorry.\n");
			break;
		}

		/* Check that we can hold all the data */
		get_dev_size(fd, NULL, &array_size);
		if (ndata * (unsigned long long)size < (array_size/1024)) {
			fprintf(stderr, Name ": this change will reduce the size of the array.\n"
				"       use --grow --array-size first to truncate array.\n"
				"       e.g. mdadm --grow %s --array-size %llu\n",
				devname, ndata * size);
			rv = 1;
			break;
		}

		/* So how much do we need to backup.
		 * We need an amount of data which is both a whole number of
		 * old stripes and a whole number of new stripes.
		 * So LCM for (chunksize*datadisks).
		 */
		a = (ochunk/512) * odata;
		b = (nchunk/512) * ndata;
		/* Find GCD */
		while (a != b) {
			if (a < b)
				b -= a;
			if (b < a)
				a -= b;
		}
		/* LCM == product / GCD */
		blocks = (ochunk/512) * (nchunk/512) * odata * ndata / a;

		sysfs_free(sra);
		sra = sysfs_read(fd, 0,
				 GET_COMPONENT|GET_DEVS|GET_OFFSET|GET_STATE|
				 GET_CACHE);

		if (!sra) {
			fprintf(stderr, Name ": %s: Cannot get array details from sysfs\n",
				devname);
			rv = 1;
			break;
		}

		if (ndata == odata) {
			/* Make 'blocks' bigger for better throughput, but
			 * not so big that we reject it below.
			 * Try for 16 megabytes
			 */
			while (blocks * 32 < sra->component_size &&
			       blocks < 16*1024*2)
			       blocks *= 2;
		} else
			fprintf(stderr, Name ": Need to backup %luK of critical "
				"section..\n", blocks/2);

		if (blocks >= sra->component_size/2) {
			fprintf(stderr, Name ": %s: Something wrong - reshape aborted\n",
				devname);
			rv = 1;
			break;
		}
		nrdisks = array.raid_disks + sra->array.spare_disks;
		/* Now we need to open all these devices so we can read/write.
		 */
		fdlist = malloc((1+nrdisks) * sizeof(int));
		offsets = malloc((1+nrdisks) * sizeof(offsets[0]));
		if (!fdlist || !offsets) {
			fprintf(stderr, Name ": malloc failed: grow aborted\n");
			rv = 1;
			break;
		}
		for (d=0; d <= nrdisks; d++)
			fdlist[d] = -1;
		d = array.raid_disks;
		for (sd = sra->devs; sd; sd=sd->next) {
			if (sd->disk.state & (1<<MD_DISK_FAULTY))
				continue;
			if (sd->disk.state & (1<<MD_DISK_SYNC)) {
				char *dn = map_dev(sd->disk.major,
						   sd->disk.minor, 1);
				fdlist[sd->disk.raid_disk]
					= dev_open(dn, O_RDONLY);
				offsets[sd->disk.raid_disk] = sd->data_offset*512;
				if (fdlist[sd->disk.raid_disk] < 0) {
					fprintf(stderr, Name ": %s: cannot open component %s\n",
						devname, dn?dn:"-unknown-");
					rv = 1;
					goto release;
				}
			} else if (backup_file == NULL) {
				/* spare */
				char *dn = map_dev(sd->disk.major,
						   sd->disk.minor, 1);
				fdlist[d] = dev_open(dn, O_RDWR);
				offsets[d] = (sd->data_offset + sra->component_size - blocks - 8)*512;
				if (fdlist[d]<0) {
					fprintf(stderr, Name ": %s: cannot open component %s\n",
						devname, dn?dn:"-unknown");
					rv = 1;
					goto release;
				}
				d++;
			}
		}
		if (backup_file == NULL) {
			if (ndata <= odata) {
				fprintf(stderr, Name ": %s: Cannot grow - need backup-file\n",
					devname);
				rv = 1;
				break;
			} else if (sra->array.spare_disks == 0) {
				fprintf(stderr, Name ": %s: Cannot grow - need a spare or "
					"backup-file to backup critical section\n",
					devname);
				rv = 1;
				break;
			}
			if (d == array.raid_disks) {
				fprintf(stderr, Name ": %s: No spare device for backup\n",
					devname);
				rv = 1;
				break;
			}
		} else {
			/* need to check backup file is large enough */
			char buf[512];
			fdlist[d] = open(backup_file, O_RDWR|O_CREAT|O_EXCL,
				     S_IRUSR | S_IWUSR);
			offsets[d] = 8 * 512;
			if (fdlist[d] < 0) {
				fprintf(stderr, Name ": %s: cannot create backup file %s: %s\n",
					devname, backup_file, strerror(errno));
				rv = 1;
				break;
			}
			memset(buf, 0, 512);
			for (i=0; i < (signed)blocks + 1 ; i++) {
				if (write(fdlist[d], buf, 512) != 512) {
					fprintf(stderr, Name ": %s: cannot create backup file %s: %s\n",
						devname, backup_file, strerror(errno));
					rv = 1;
					break;
				}
			}
			if (fsync(fdlist[d]) != 0) {
				fprintf(stderr, Name ": %s: cannot create backup file %s: %s\n",
					devname, backup_file, strerror(errno));
				rv = 1;
				break;
			}
			d++;
		}

		/* lastly, check that the internal stripe cache is
		 * large enough, or it won't work.
		 */
		
		cache = (nchunk < ochunk) ? ochunk : nchunk;
		cache = cache * 4 / 4096;
		if (cache < blocks / 8 / odisks + 16)
			/* Make it big enough to hold 'blocks' */
			cache = blocks / 8 / odisks + 16;
		if (sra->cache_size < cache)
			sysfs_set_num(sra, NULL, "stripe_cache_size",
				      cache+1);
		/* Right, everything seems fine. Let's kick things off.
		 * If only changing raid_disks, use ioctl, else use
		 * sysfs.
		 */
		if (ochunk == nchunk && olayout == nlayout) {
			array.raid_disks = ndisks;
			if (ioctl(fd, SET_ARRAY_INFO, &array) != 0) {
				int err = errno;
				rv = 1;
				fprintf(stderr, Name ": Cannot set device shape for %s: %s\n",
					devname, strerror(errno));
				if (ndisks < odisks &&
				    get_linux_version() < 2006030)
					fprintf(stderr, Name ": linux 2.6.30 or later required\n");
				if (err == EBUSY && 
				    (array.state & (1<<MD_SB_BITMAP_PRESENT)))
					fprintf(stderr, "       Bitmap must be removed before shape can be changed\n");

				break;
			}
		} else {
			/* set them all just in case some old 'new_*' value
			 * persists from some earlier problem
			 */
			int err = err; /* only used if rv==1, and always set if
					* rv==1, so initialisation not needed,
					* despite gcc warning
					*/
			if (sysfs_set_num(sra, NULL, "chunk_size", nchunk) < 0)
				rv = 1, err = errno;
			if (!rv && sysfs_set_num(sra, NULL, "layout", nlayout) < 0)
				rv = 1, err = errno;
			if (!rv && sysfs_set_num(sra, NULL, "raid_disks", ndisks) < 0)
				rv = 1, err = errno;
			if (rv) {
				fprintf(stderr, Name ": Cannot set device shape for %s\n",
					devname);
				if (get_linux_version() < 2006030)
					fprintf(stderr, Name ": linux 2.6.30 or later required\n");
				if (err == EBUSY && 
				    (array.state & (1<<MD_SB_BITMAP_PRESENT)))
					fprintf(stderr, "       Bitmap must be removed before shape can be changed\n");
				break;
			}
		}

		if (ndisks == 2 && odisks == 2) {
			/* No reshape is needed in this trivial case */
			rv = 0;
			break;
		}

		/* set up the backup-super-block.  This requires the
		 * uuid from the array.
		 */
		/* Find a superblock */
		for (sd = sra->devs; sd; sd = sd->next) {
			char *dn;
			int devfd;
			int ok;
			if (sd->disk.state & (1<<MD_DISK_FAULTY))
				continue;
			dn = map_dev(sd->disk.major, sd->disk.minor, 1);
			devfd = dev_open(dn, O_RDONLY);
			if (devfd < 0)
				continue;
			ok = st->ss->load_super(st, devfd, NULL);
			close(devfd);
			if (ok >= 0)
				break;
		}
		if (!sd) {
			fprintf(stderr, Name ": %s: Cannot find a superblock\n",
				devname);
			rv = 1;
			break;
		}

		memset(&bsb, 0, 512);
		memcpy(bsb.magic, "md_backup_data-1", 16);
		st->ss->uuid_from_super(st, (int*)&bsb.set_uuid);
		bsb.mtime = __cpu_to_le64(time(0));
		bsb.devstart2 = blocks;
		stripes = blocks / (ochunk/512) / odata;
		/* Now we just need to kick off the reshape and watch, while
		 * handling backups of the data...
		 * This is all done by a forked background process.
		 */
		switch(fork()) {
		case 0:
			close(fd);
			if (check_env("MDADM_GROW_VERIFY"))
				fd = open(devname, O_RDONLY | O_DIRECT);
			else
				fd = -1;
			mlockall(MCL_FUTURE);

			if (odata < ndata)
				done = child_grow(fd, sra, stripes,
						  fdlist, offsets,
						  odisks, ochunk, array.level, olayout, odata,
						  d - odisks, fdlist+odisks, offsets+odisks);
			else if (odata > ndata)
				done = child_shrink(fd, sra, stripes,
						    fdlist, offsets,
						    odisks, ochunk, array.level, olayout, odata,
						    d - odisks, fdlist+odisks, offsets+odisks);
			else
				done = child_same_size(fd, sra, stripes,
						       fdlist, offsets,
						       0,
						       odisks, ochunk, array.level, olayout, odata,
						       d - odisks, fdlist+odisks, offsets+odisks);
			if (backup_file && done)
				unlink(backup_file);
			if (level != UnSet && level != array.level) {
				/* We need to wait for the reshape to finish
				 * (which will have happened unless odata < ndata)
				 * and then set the level
				 */

				c = map_num(pers, level);
				if (c == NULL)
					exit(0);/* not possible */

				if (odata < ndata)
					wait_reshape(sra);
				err = sysfs_set_str(sra, NULL, "level", c);
				if (err)
					fprintf(stderr, Name ": %s: could not set level to %s\n",
						devname, c);
			}
			exit(0);
		case -1:
			fprintf(stderr, Name ": Cannot run child to monitor reshape: %s\n",
				strerror(errno));
			rv = 1;
			break;
		default:
			/* The child will take care of unfreezing the array */
			frozen = 0;
			break;
		}
		break;

	}

 release:
	if (rv && orig_level != UnSet && sra) {
		c = map_num(pers, orig_level);
		if (c && sysfs_set_str(sra, NULL, "level", c) == 0)
			fprintf(stderr, Name ": aborting level change\n");
	}
	if (sra)
		unfreeze_array(sra, frozen);
	return rv;
}

/*
 * We run a child process in the background which performs the following
 * steps:
 *   - wait for resync to reach a certain point
 *   - suspend io to the following section
 *   - backup that section
 *   - allow resync to proceed further
 *   - resume io
 *   - discard the backup.
 *
 * When are combined in slightly different ways in the three cases.
 * Grow:
 *   - suspend/backup/allow/wait/resume/discard
 * Shrink:
 *   - allow/wait/suspend/backup/allow/wait/resume/discard
 * same-size:
 *   - wait/resume/discard/suspend/backup/allow
 *
 * suspend/backup/allow always come together
 * wait/resume/discard do too.
 * For the same-size case we have two backups to improve flow.
 * 
 */

/* FIXME return status is never checked */
int grow_backup(struct mdinfo *sra,
		unsigned long long offset, /* per device */
		unsigned long stripes, /* per device */
		int *sources, unsigned long long *offsets,
		int disks, int chunk, int level, int layout,
		int dests, int *destfd, unsigned long long *destoffsets,
		int part, int *degraded,
		char *buf)
{
	/* Backup 'blocks' sectors at 'offset' on each device of the array,
	 * to storage 'destfd' (offset 'destoffsets'), after first
	 * suspending IO.  Then allow resync to continue
	 * over the suspended section.
	 * Use part 'part' of the backup-super-block.
	 */
	int odata = disks;
	int rv = 0;
	int i;
	unsigned long long ll;
	int new_degraded;
	//printf("offset %llu\n", offset);
	if (level >= 4)
		odata--;
	if (level == 6)
		odata--;
	sysfs_set_num(sra, NULL, "suspend_hi", (offset + stripes * (chunk/512)) * odata);
	/* Check that array hasn't become degraded, else we might backup the wrong data */
	sysfs_get_ll(sra, NULL, "degraded", &ll);
	new_degraded = (int)ll;
	if (new_degraded != *degraded) {
		/* check each device to ensure it is still working */
		struct mdinfo *sd;
		for (sd = sra->devs ; sd ; sd = sd->next) {
			if (sd->disk.state & (1<<MD_DISK_FAULTY))
				continue;
			if (sd->disk.state & (1<<MD_DISK_SYNC)) {
				char sbuf[20];
				if (sysfs_get_str(sra, sd, "state", sbuf, 20) < 0 ||
				    strstr(sbuf, "faulty") ||
				    strstr(sbuf, "in_sync") == NULL) {
					/* this device is dead */
					sd->disk.state = (1<<MD_DISK_FAULTY);
					if (sd->disk.raid_disk >= 0 &&
					    sources[sd->disk.raid_disk] >= 0) {
						close(sources[sd->disk.raid_disk]);
						sources[sd->disk.raid_disk] = -1;
					}
				}
			}
		}
		*degraded = new_degraded;
	}
	if (part) {
		bsb.arraystart2 = __cpu_to_le64(offset * odata);
		bsb.length2 = __cpu_to_le64(stripes * (chunk/512) * odata);
	} else {
		bsb.arraystart = __cpu_to_le64(offset * odata);
		bsb.length = __cpu_to_le64(stripes * (chunk/512) * odata);
	}
	if (part)
		bsb.magic[15] = '2';
	for (i = 0; i < dests; i++)
		if (part)
			lseek64(destfd[i], destoffsets[i] + __le64_to_cpu(bsb.devstart2)*512, 0);
		else
			lseek64(destfd[i], destoffsets[i], 0);

	rv = save_stripes(sources, offsets, 
			  disks, chunk, level, layout,
			  dests, destfd,
			  offset*512*odata, stripes * chunk * odata,
			  buf);

	if (rv)
		return rv;
	bsb.mtime = __cpu_to_le64(time(0));
	for (i = 0; i < dests; i++) {
		bsb.devstart = __cpu_to_le64(destoffsets[i]/512);

		bsb.sb_csum = bsb_csum((char*)&bsb, ((char*)&bsb.sb_csum)-((char*)&bsb));
		if (memcmp(bsb.magic, "md_backup_data-2", 16) == 0)
			bsb.sb_csum2 = bsb_csum((char*)&bsb,
						((char*)&bsb.sb_csum2)-((char*)&bsb));

		rv = -1;
		if ((unsigned long long)lseek64(destfd[i], destoffsets[i] - 4096, 0)
		    != destoffsets[i] - 4096)
			break;
		if (write(destfd[i], &bsb, 512) != 512)
			break;
		if (destoffsets[i] > 4096) {
			if ((unsigned long long)lseek64(destfd[i], destoffsets[i]+stripes*chunk*odata, 0) !=
			    destoffsets[i]+stripes*chunk*odata)
				break;
			if (write(destfd[i], &bsb, 512) != 512)
				break;
		}
		fsync(destfd[i]);
		rv = 0;
	}

	return rv;
}

/* in 2.6.30, the value reported by sync_completed can be
 * less that it should be by one stripe.
 * This only happens when reshape hits sync_max and pauses.
 * So allow wait_backup to either extent sync_max further
 * than strictly necessary, or return before the
 * sync has got quite as far as we would really like.
 * This is what 'blocks2' is for.
 * The various caller give appropriate values so that
 * every works.
 */
/* FIXME return value is often ignored */
int wait_backup(struct mdinfo *sra,
		unsigned long long offset, /* per device */
		unsigned long long blocks, /* per device */
		unsigned long long blocks2, /* per device - hack */
		int dests, int *destfd, unsigned long long *destoffsets,
		int part)
{
	/* Wait for resync to pass the section that was backed up
	 * then erase the backup and allow IO
	 */
	int fd = sysfs_get_fd(sra, NULL, "sync_completed");
	unsigned long long completed;
	int i;
	int rv;

	if (fd < 0)
		return -1;
	sysfs_set_num(sra, NULL, "sync_max", offset + blocks + blocks2);
	if (offset == 0)
		sysfs_set_str(sra, NULL, "sync_action", "reshape");
	do {
		char action[20];
		fd_set rfds;
		FD_ZERO(&rfds);
		FD_SET(fd, &rfds);
		select(fd+1, NULL, NULL, &rfds, NULL);
		if (sysfs_fd_get_ll(fd, &completed) < 0) {
			close(fd);
			return -1;
		}
		if (sysfs_get_str(sra, NULL, "sync_action",
				  action, 20) > 0 &&
		    strncmp(action, "reshape", 7) != 0)
			break;
	} while (completed < offset + blocks);
	close(fd);

	if (part) {
		bsb.arraystart2 = __cpu_to_le64(0);
		bsb.length2 = __cpu_to_le64(0);
	} else {
		bsb.arraystart = __cpu_to_le64(0);
		bsb.length = __cpu_to_le64(0);
	}
	bsb.mtime = __cpu_to_le64(time(0));
	rv = 0;
	for (i = 0; i < dests; i++) {
		bsb.devstart = __cpu_to_le64(destoffsets[i]/512);
		bsb.sb_csum = bsb_csum((char*)&bsb, ((char*)&bsb.sb_csum)-((char*)&bsb));
		if (memcmp(bsb.magic, "md_backup_data-2", 16) == 0)
			bsb.sb_csum2 = bsb_csum((char*)&bsb,
						((char*)&bsb.sb_csum2)-((char*)&bsb));
		if ((unsigned long long)lseek64(destfd[i], destoffsets[i]-4096, 0) !=
		    destoffsets[i]-4096)
			rv = -1;
		if (rv == 0 && 
		    write(destfd[i], &bsb, 512) != 512)
			rv = -1;
		fsync(destfd[i]);
	}
	return rv;
}

static void fail(char *msg)
{
	int rv;
	rv = (write(2, msg, strlen(msg)) != (int)strlen(msg));
	rv |= (write(2, "\n", 1) != 1);
	exit(rv ? 1 : 2);
}

static char *abuf, *bbuf;
static unsigned long long abuflen;
static void validate(int afd, int bfd, unsigned long long offset)
{
	/* check that the data in the backup against the array.
	 * This is only used for regression testing and should not
	 * be used while the array is active
	 */
	if (afd < 0)
		return;
	lseek64(bfd, offset - 4096, 0);
	if (read(bfd, &bsb2, 512) != 512)
		fail("cannot read bsb");
	if (bsb2.sb_csum != bsb_csum((char*)&bsb2,
				     ((char*)&bsb2.sb_csum)-((char*)&bsb2)))
		fail("first csum bad");
	if (memcmp(bsb2.magic, "md_backup_data", 14) != 0)
		fail("magic is bad");
	if (memcmp(bsb2.magic, "md_backup_data-2", 16) == 0 &&
	    bsb2.sb_csum2 != bsb_csum((char*)&bsb2,
				     ((char*)&bsb2.sb_csum2)-((char*)&bsb2)))
		fail("second csum bad");

	if (__le64_to_cpu(bsb2.devstart)*512 != offset)
		fail("devstart is wrong");

	if (bsb2.length) {
		unsigned long long len = __le64_to_cpu(bsb2.length)*512;

		if (abuflen < len) {
			free(abuf);
			free(bbuf);
			abuflen = len;
			if (posix_memalign((void**)&abuf, 4096, abuflen) ||
			    posix_memalign((void**)&bbuf, 4096, abuflen)) {
				abuflen = 0;
				/* just stop validating on mem-alloc failure */
				return;
			}
		}

		lseek64(bfd, offset, 0);
		if ((unsigned long long)read(bfd, bbuf, len) != len) {
			//printf("len %llu\n", len);
			fail("read first backup failed");
		}
		lseek64(afd, __le64_to_cpu(bsb2.arraystart)*512, 0);
		if ((unsigned long long)read(afd, abuf, len) != len)
			fail("read first from array failed");
		if (memcmp(bbuf, abuf, len) != 0) {
			#if 0
			int i;
			printf("offset=%llu len=%llu\n",
			       (unsigned long long)__le64_to_cpu(bsb2.arraystart)*512, len);
			for (i=0; i<len; i++)
				if (bbuf[i] != abuf[i]) {
					printf("first diff byte %d\n", i);
					break;
				}
			#endif
			fail("data1 compare failed");
		}
	}
	if (bsb2.length2) {
		unsigned long long len = __le64_to_cpu(bsb2.length2)*512;

		if (abuflen < len) {
			free(abuf);
			free(bbuf);
			abuflen = len;
			abuf = malloc(abuflen);
			bbuf = malloc(abuflen);
		}

		lseek64(bfd, offset+__le64_to_cpu(bsb2.devstart2)*512, 0);
		if ((unsigned long long)read(bfd, bbuf, len) != len)
			fail("read second backup failed");
		lseek64(afd, __le64_to_cpu(bsb2.arraystart2)*512, 0);
		if ((unsigned long long)read(afd, abuf, len) != len)
			fail("read second from array failed");
		if (memcmp(bbuf, abuf, len) != 0)
			fail("data2 compare failed");
	}
}

static int child_grow(int afd, struct mdinfo *sra, unsigned long stripes,
		      int *fds, unsigned long long *offsets,
		      int disks, int chunk, int level, int layout, int data,
		      int dests, int *destfd, unsigned long long *destoffsets)
{
	char *buf;
	int degraded = 0;

	if (posix_memalign((void**)&buf, 4096, disks * chunk))
		/* Don't start the 'reshape' */
		return 0;
	sysfs_set_num(sra, NULL, "suspend_hi", 0);
	sysfs_set_num(sra, NULL, "suspend_lo", 0);
	grow_backup(sra, 0, stripes,
		    fds, offsets, disks, chunk, level, layout,
		    dests, destfd, destoffsets,
		    0, &degraded, buf);
	validate(afd, destfd[0], destoffsets[0]);
	wait_backup(sra, 0, stripes * (chunk / 512), stripes * (chunk / 512),
		    dests, destfd, destoffsets,
		    0);
	sysfs_set_num(sra, NULL, "suspend_lo", (stripes * (chunk/512)) * data);
	free(buf);
	/* FIXME this should probably be numeric */
	sysfs_set_str(sra, NULL, "sync_max", "max");
	return 1;
}

static int child_shrink(int afd, struct mdinfo *sra, unsigned long stripes,
			int *fds, unsigned long long *offsets,
			int disks, int chunk, int level, int layout, int data,
			int dests, int *destfd, unsigned long long *destoffsets)
{
	char *buf;
	unsigned long long start;
	int rv;
	int degraded = 0;

	if (posix_memalign((void**)&buf, 4096, disks * chunk))
		return 0;
	start = sra->component_size - stripes * (chunk/512);
	sysfs_set_num(sra, NULL, "sync_max", start);
	sysfs_set_str(sra, NULL, "sync_action", "reshape");
	sysfs_set_num(sra, NULL, "suspend_lo", 0);
	sysfs_set_num(sra, NULL, "suspend_hi", 0);
	rv = wait_backup(sra, 0, start - stripes * (chunk/512), stripes * (chunk/512),
			 dests, destfd, destoffsets, 0);
	if (rv < 0)
		return 0;
	grow_backup(sra, 0, stripes,
		    fds, offsets,
		    disks, chunk, level, layout,
		    dests, destfd, destoffsets,
		    0, &degraded, buf);
	validate(afd, destfd[0], destoffsets[0]);
	wait_backup(sra, start, stripes*(chunk/512), 0,
		    dests, destfd, destoffsets, 0);
	sysfs_set_num(sra, NULL, "suspend_lo", (stripes * (chunk/512)) * data);
	free(buf);
	/* FIXME this should probably be numeric */
	sysfs_set_str(sra, NULL, "sync_max", "max");
	return 1;
}

static int child_same_size(int afd, struct mdinfo *sra, unsigned long stripes,
			   int *fds, unsigned long long *offsets,
			   unsigned long long start,
			   int disks, int chunk, int level, int layout, int data,
			   int dests, int *destfd, unsigned long long *destoffsets)
{
	unsigned long long size;
	unsigned long tailstripes = stripes;
	int part;
	char *buf;
	unsigned long long speed;
	int degraded = 0;


	if (posix_memalign((void**)&buf, 4096, disks * chunk))
		return 0;

	sysfs_set_num(sra, NULL, "suspend_lo", 0);
	sysfs_set_num(sra, NULL, "suspend_hi", 0);

	sysfs_get_ll(sra, NULL, "sync_speed_min", &speed);
	sysfs_set_num(sra, NULL, "sync_speed_min", 200000);

	grow_backup(sra, start, stripes,
		    fds, offsets,
		    disks, chunk, level, layout,
		    dests, destfd, destoffsets,
		    0, &degraded, buf);
	grow_backup(sra, (start + stripes) * (chunk/512), stripes,
		    fds, offsets,
		    disks, chunk, level, layout,
		    dests, destfd, destoffsets,
		    1, &degraded, buf);
	validate(afd, destfd[0], destoffsets[0]);
	part = 0;
	start += stripes * 2; /* where to read next */
	size = sra->component_size / (chunk/512);
	while (start < size) {
		if (wait_backup(sra, (start-stripes*2)*(chunk/512),
				stripes*(chunk/512), 0,
				dests, destfd, destoffsets,
				part) < 0)
			return 0;
		sysfs_set_num(sra, NULL, "suspend_lo", start*(chunk/512) * data);
		if (start + stripes > size)
			tailstripes = (size - start);

		grow_backup(sra, start*(chunk/512), tailstripes,
			    fds, offsets,
			    disks, chunk, level, layout,
			    dests, destfd, destoffsets,
			    part, &degraded, buf);
		start += stripes;
		part = 1 - part;
		validate(afd, destfd[0], destoffsets[0]);
	}
	if (wait_backup(sra, (start-stripes*2) * (chunk/512), stripes * (chunk/512), 0,
			dests, destfd, destoffsets,
			part) < 0)
		return 0;
	sysfs_set_num(sra, NULL, "suspend_lo", ((start-stripes)*(chunk/512)) * data);
	wait_backup(sra, (start-stripes) * (chunk/512), tailstripes * (chunk/512), 0,
		    dests, destfd, destoffsets,
		    1-part);
	sysfs_set_num(sra, NULL, "suspend_lo", (size*(chunk/512)) * data);
	sysfs_set_num(sra, NULL, "sync_speed_min", speed);
	free(buf);
	return 1;
}

/*
 * If any spare contains md_back_data-1 which is recent wrt mtime,
 * write that data into the array and update the super blocks with
 * the new reshape_progress
 */
int Grow_restart(struct supertype *st, struct mdinfo *info, int *fdlist, int cnt,
		 char *backup_file, int verbose)
{
	int i, j;
	int old_disks;
	unsigned long long *offsets;
	unsigned long long  nstripe, ostripe;
	int ndata, odata;

	if (info->new_level != info->array.level)
		return 1; /* Cannot handle level changes (they are instantaneous) */

	odata = info->array.raid_disks - info->delta_disks - 1;
	if (info->array.level == 6) odata--; /* number of data disks */
	ndata = info->array.raid_disks - 1;
	if (info->new_level == 6) ndata--;

	old_disks = info->array.raid_disks - info->delta_disks;

	if (info->delta_disks <= 0)
		/* Didn't grow, so the backup file must have
		 * been used
		 */
		old_disks = cnt;
	for (i=old_disks-(backup_file?1:0); i<cnt; i++) {
		struct mdinfo dinfo;
		int fd;
		int bsbsize;
		char *devname, namebuf[20];

		/* This was a spare and may have some saved data on it.
		 * Load the superblock, find and load the
		 * backup_super_block.
		 * If either fail, go on to next device.
		 * If the backup contains no new info, just return
		 * else restore data and update all superblocks
		 */
		if (i == old_disks-1) {
			fd = open(backup_file, O_RDONLY);
			if (fd<0) {
				fprintf(stderr, Name ": backup file %s inaccessible: %s\n",
					backup_file, strerror(errno));
				continue;
			}
			devname = backup_file;
		} else {
			fd = fdlist[i];
			if (fd < 0)
				continue;
			if (st->ss->load_super(st, fd, NULL))
				continue;

			st->ss->getinfo_super(st, &dinfo);
			st->ss->free_super(st);

			if (lseek64(fd,
				    (dinfo.data_offset + dinfo.component_size - 8) <<9,
				    0) < 0) {
				fprintf(stderr, Name ": Cannot seek on device %d\n", i);
				continue; /* Cannot seek */
			}
			sprintf(namebuf, "device-%d", i);
			devname = namebuf;
		}
		if (read(fd, &bsb, sizeof(bsb)) != sizeof(bsb)) {
			if (verbose)
				fprintf(stderr, Name ": Cannot read from %s\n", devname);
			continue; /* Cannot read */
		}
		if (memcmp(bsb.magic, "md_backup_data-1", 16) != 0 &&
		    memcmp(bsb.magic, "md_backup_data-2", 16) != 0) {
			if (verbose)
				fprintf(stderr, Name ": No backup metadata on %s\n", devname);
			continue;
		}
		if (bsb.sb_csum != bsb_csum((char*)&bsb, ((char*)&bsb.sb_csum)-((char*)&bsb))) {
			if (verbose)
				fprintf(stderr, Name ": Bad backup-metadata checksum on %s\n", devname);
			continue; /* bad checksum */
		}
		if (memcmp(bsb.magic, "md_backup_data-2", 16) == 0 &&
		    bsb.sb_csum2 != bsb_csum((char*)&bsb, ((char*)&bsb.sb_csum2)-((char*)&bsb))) {
			if (verbose)
				fprintf(stderr, Name ": Bad backup-metadata checksum2 on %s\n", devname);
			continue; /* Bad second checksum */
		}
		if (memcmp(bsb.set_uuid,info->uuid, 16) != 0) {
			if (verbose)
				fprintf(stderr, Name ": Wrong uuid on backup-metadata on %s\n", devname);
			continue; /* Wrong uuid */
		}

		/* array utime and backup-mtime should be updated at much the same time, but it seems that
		 * sometimes they aren't... So allow considerable flexability in matching, and allow
		 * this test to be overridden by an environment variable.
		 */
		if (info->array.utime > (int)__le64_to_cpu(bsb.mtime) + 2*60*60 ||
		    info->array.utime < (int)__le64_to_cpu(bsb.mtime) - 10*60) {
			if (check_env("MDADM_GROW_ALLOW_OLD")) {
				fprintf(stderr, Name ": accepting backup with timestamp %lu "
					"for array with timestamp %lu\n",
					(unsigned long)__le64_to_cpu(bsb.mtime),
					(unsigned long)info->array.utime);
			} else {
				if (verbose)
					fprintf(stderr, Name ": too-old timestamp on "
						"backup-metadata on %s\n", devname);
				continue; /* time stamp is too bad */
			}
		}

		if (bsb.magic[15] == '1') {
		if (info->delta_disks >= 0) {
			/* reshape_progress is increasing */
			if (__le64_to_cpu(bsb.arraystart) + __le64_to_cpu(bsb.length) <
			    info->reshape_progress) {
			nonew:
				if (verbose)
					fprintf(stderr, Name ": backup-metadata found on %s but is not needed\n", devname);
				continue; /* No new data here */
			}
		} else {
			/* reshape_progress is decreasing */
			if (__le64_to_cpu(bsb.arraystart) >=
			    info->reshape_progress)
				goto nonew; /* No new data here */
		}
		} else {
		if (info->delta_disks >= 0) {
			/* reshape_progress is increasing */
			if (__le64_to_cpu(bsb.arraystart) + __le64_to_cpu(bsb.length) <
			    info->reshape_progress &&
			    __le64_to_cpu(bsb.arraystart2) + __le64_to_cpu(bsb.length2) <
			    info->reshape_progress)
				goto nonew; /* No new data here */
		} else {
			/* reshape_progress is decreasing */
			if (__le64_to_cpu(bsb.arraystart) >=
			    info->reshape_progress &&
			    __le64_to_cpu(bsb.arraystart2) >=
			    info->reshape_progress)
				goto nonew; /* No new data here */
		}
		}
		if (lseek64(fd, __le64_to_cpu(bsb.devstart)*512, 0)< 0) {
		second_fail:
			if (verbose)
				fprintf(stderr, Name ": Failed to verify secondary backup-metadata block on %s\n",
					devname);
			continue; /* Cannot seek */
		}
		/* There should be a duplicate backup superblock 4k before here */
		if (lseek64(fd, -4096, 1) < 0 ||
		    read(fd, &bsb2, sizeof(bsb2)) != sizeof(bsb2))
			goto second_fail; /* Cannot find leading superblock */
		if (bsb.magic[15] == '1')
			bsbsize = offsetof(struct mdp_backup_super, pad1);
		else
			bsbsize = offsetof(struct mdp_backup_super, pad);
		if (memcmp(&bsb2, &bsb, bsbsize) != 0)
			goto second_fail; /* Cannot find leading superblock */

		/* Now need the data offsets for all devices. */
		offsets = malloc(sizeof(*offsets)*info->array.raid_disks);
		for(j=0; j<info->array.raid_disks; j++) {
			if (fdlist[j] < 0)
				continue;
			if (st->ss->load_super(st, fdlist[j], NULL))
				/* FIXME should be this be an error */
				continue;
			st->ss->getinfo_super(st, &dinfo);
			st->ss->free_super(st);
			offsets[j] = dinfo.data_offset * 512;
		}
		printf(Name ": restoring critical section\n");

		if (restore_stripes(fdlist, offsets,
				    info->array.raid_disks,
				    info->new_chunk,
				    info->new_level,
				    info->new_layout,
				    fd, __le64_to_cpu(bsb.devstart)*512,
				    __le64_to_cpu(bsb.arraystart)*512,
				    __le64_to_cpu(bsb.length)*512)) {
			/* didn't succeed, so giveup */
			if (verbose)
				fprintf(stderr, Name ": Error restoring backup from %s\n",
					devname);
			return 1;
		}
		
		if (bsb.magic[15] == '2' &&
		    restore_stripes(fdlist, offsets,
				    info->array.raid_disks,
				    info->new_chunk,
				    info->new_level,
				    info->new_layout,
				    fd, __le64_to_cpu(bsb.devstart)*512 +
				    __le64_to_cpu(bsb.devstart2)*512,
				    __le64_to_cpu(bsb.arraystart2)*512,
				    __le64_to_cpu(bsb.length2)*512)) {
			/* didn't succeed, so giveup */
			if (verbose)
				fprintf(stderr, Name ": Error restoring second backup from %s\n",
					devname);
			return 1;
		}


		/* Ok, so the data is restored. Let's update those superblocks. */

		if (info->delta_disks >= 0) {
			info->reshape_progress = __le64_to_cpu(bsb.arraystart) +
				__le64_to_cpu(bsb.length);
			if (bsb.magic[15] == '2') {
				unsigned long long p2 = __le64_to_cpu(bsb.arraystart2) +
					__le64_to_cpu(bsb.length2);
				if (p2 > info->reshape_progress)
					info->reshape_progress = p2;
			}
		} else {
			info->reshape_progress = __le64_to_cpu(bsb.arraystart);
			if (bsb.magic[15] == '2') {
				unsigned long long p2 = __le64_to_cpu(bsb.arraystart2);
				if (p2 < info->reshape_progress)
					info->reshape_progress = p2;
			}
		}
		for (j=0; j<info->array.raid_disks; j++) {
			if (fdlist[j] < 0) continue;
			if (st->ss->load_super(st, fdlist[j], NULL))
				continue;
			st->ss->getinfo_super(st, &dinfo);
			dinfo.reshape_progress = info->reshape_progress;
			st->ss->update_super(st, &dinfo,
					     "_reshape_progress",
					     NULL,0, 0, NULL);
			st->ss->store_super(st, fdlist[j]);
			st->ss->free_super(st);
		}
		return 0;
	}
	/* Didn't find any backup data, try to see if any
	 * was needed.
	 */
	if (info->delta_disks < 0) {
		/* When shrinking, the critical section is at the end.
		 * So see if we are before the critical section.
		 */
		unsigned long long first_block;
		nstripe = ostripe = 0;
		first_block = 0;
		while (ostripe >= nstripe) {
			ostripe += info->array.chunk_size / 512;
			first_block = ostripe * odata;
			nstripe = first_block / ndata / (info->new_chunk/512) *
				(info->new_chunk/512);
		}

		if (info->reshape_progress >= first_block)
			return 0;
	}
	if (info->delta_disks > 0) {
		/* See if we are beyond the critical section. */
		unsigned long long last_block;
		nstripe = ostripe = 0;
		last_block = 0;
		while (nstripe >= ostripe) {
			nstripe += info->new_chunk / 512;
			last_block = nstripe * ndata;
			ostripe = last_block / odata / (info->array.chunk_size/512) *
				(info->array.chunk_size/512);
		}

		if (info->reshape_progress >= last_block)
			return 0;
	}
	/* needed to recover critical section! */
	if (verbose)
		fprintf(stderr, Name ": Failed to find backup of critical section\n");
	return 1;
}

int Grow_continue(int mdfd, struct supertype *st, struct mdinfo *info,
		  char *backup_file)
{
	/* Array is assembled and ready to be started, but
	 * monitoring is probably required.
	 * So:
	 *   - start read-only
	 *   - set upper bound for resync
	 *   - initialise the 'suspend' boundaries
	 *   - switch to read-write
	 *   - fork and continue monitoring
	 */
	int err;
	int backup_list[1];
	unsigned long long backup_offsets[1];
	int odisks, ndisks, ochunk, nchunk,odata,ndata;
	unsigned long a,b,blocks,stripes;
	int backup_fd;
	int *fds;
	unsigned long long *offsets;
	int d;
	struct mdinfo *sra, *sd;
	int rv;
	unsigned long cache;
	int done = 0;

	err = sysfs_set_str(info, NULL, "array_state", "readonly");
	if (err)
		return err;

	/* make sure reshape doesn't progress until we are ready */
	sysfs_set_str(info, NULL, "sync_max", "0");
	sysfs_set_str(info, NULL, "array_state", "active"); /* FIXME or clean */

	sra = sysfs_read(-1, devname2devnum(info->sys_name),
			 GET_COMPONENT|GET_DEVS|GET_OFFSET|GET_STATE|
			 GET_CACHE);
	if (!sra)
		return 1;

	/* ndisks is not growing, so raid_disks is old and +delta is new */
	odisks = info->array.raid_disks;
	ndisks = odisks + info->delta_disks;
	odata = odisks - 1;
	ndata = ndisks - 1;
	if (info->array.level == 6) {
		odata--;
		ndata--;
	}
	ochunk = info->array.chunk_size;
	nchunk = info->new_chunk;

	a = (ochunk/512) * odata;
	b = (nchunk/512) * ndata;
	/* Find GCD */
	while (a != b) {
		if (a < b)
			b -= a;
		if (b < a)
			a -= b;
	}
	/* LCM == product / GCD */
	blocks = (ochunk/512) * (nchunk/512) * odata * ndata / a;

	if (ndata == odata)
		while (blocks * 32 < sra->component_size &&
		       blocks < 16*1024*2)
			blocks *= 2;
	stripes = blocks / (info->array.chunk_size/512) / odata;

	/* check that the internal stripe cache is
	 * large enough, or it won't work.
	 */
	cache = (nchunk < ochunk) ? ochunk : nchunk;
	cache = cache * 4 / 4096;
	if (cache < blocks / 8 / odisks + 16)
		/* Make it big enough to hold 'blocks' */
		cache = blocks / 8 / odisks + 16;
	if (sra->cache_size < cache)
		sysfs_set_num(sra, NULL, "stripe_cache_size",
			      cache+1);

	memset(&bsb, 0, 512);
	memcpy(bsb.magic, "md_backup_data-1", 16);
	memcpy(&bsb.set_uuid, info->uuid, 16);
	bsb.mtime = __cpu_to_le64(time(0));
	bsb.devstart2 = blocks;

	backup_fd = open(backup_file, O_RDWR|O_CREAT, S_IRUSR | S_IWUSR);
	backup_list[0] = backup_fd;
	backup_offsets[0] = 8 * 512;
	fds = malloc(odisks * sizeof(fds[0]));
	offsets = malloc(odisks * sizeof(offsets[0]));
	for (d=0; d<odisks; d++)
		fds[d] = -1;

	for (sd = sra->devs; sd; sd = sd->next) {
		if (sd->disk.state & (1<<MD_DISK_FAULTY))
			continue;
		if (sd->disk.state & (1<<MD_DISK_SYNC)) {
			char *dn = map_dev(sd->disk.major,
					   sd->disk.minor, 1);
			fds[sd->disk.raid_disk]
				= dev_open(dn, O_RDONLY);
			offsets[sd->disk.raid_disk] = sd->data_offset*512;
			if (fds[sd->disk.raid_disk] < 0) {
				fprintf(stderr, Name ": %s: cannot open component %s\n",
					info->sys_name, dn?dn:"-unknown-");
				rv = 1;
				goto release;
			}
			free(dn);
		}
	}

	switch(fork()) {
	case 0:
		close(mdfd);
		mlockall(MCL_FUTURE);
		if (info->delta_disks < 0)
			done = child_shrink(-1, info, stripes,
					    fds, offsets,
					    info->array.raid_disks,
					    info->array.chunk_size,
					    info->array.level, info->array.layout,
					    odata,
					    1, backup_list, backup_offsets);
		else if (info->delta_disks == 0) {
			/* The 'start' is a per-device stripe number.
			 * reshape_progress is a per-array sector number.
			 * So divide by ndata * chunk_size
			 */
			unsigned long long start = info->reshape_progress / ndata;
			start /= (info->array.chunk_size/512);
			done = child_same_size(-1, info, stripes,
					       fds, offsets,
					       start,
					       info->array.raid_disks,
					       info->array.chunk_size,
					       info->array.level, info->array.layout,
					       odata,
					       1, backup_list, backup_offsets);
		}
		if (backup_file && done)
			unlink(backup_file);
		/* FIXME should I intuit a level change */
		exit(0);
	case -1:
		fprintf(stderr, Name ": Cannot run child to continue monitoring reshape: %s\n",
			strerror(errno));
		return 1;
	default:
		break;
	}
release:
	return 0;
}


