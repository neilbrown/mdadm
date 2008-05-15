/*
 * mdadm - Intel(R) Matrix Storage Manager Support
 *
 * Copyright (C) 2002-2007 Intel Corporation
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms and conditions of the GNU General Public License,
 * version 2, as published by the Free Software Foundation.
 *
 * This program is distributed in the hope it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St - Fifth Floor, Boston, MA 02110-1301 USA.
 */

#include "mdadm.h"
#include "mdmon.h"
#include <values.h>
#include <scsi/sg.h>
#include <ctype.h>

/* MPB == Metadata Parameter Block */
#define MPB_SIGNATURE "Intel Raid ISM Cfg Sig. "
#define MPB_SIG_LEN (strlen(MPB_SIGNATURE))
#define MPB_VERSION_RAID0 "1.0.00"
#define MPB_VERSION_RAID1 "1.1.00"
#define MPB_VERSION_RAID5 "1.2.02"
#define MAX_SIGNATURE_LENGTH  32
#define MAX_RAID_SERIAL_LEN   16

/* Disk configuration info. */
#define IMSM_MAX_DEVICES 255
struct imsm_disk {
	__u8 serial[MAX_RAID_SERIAL_LEN];/* 0xD8 - 0xE7 ascii serial number */
	__u32 total_blocks;		 /* 0xE8 - 0xEB total blocks */
	__u32 scsi_id;			 /* 0xEC - 0xEF scsi ID */
	__u32 status;			 /* 0xF0 - 0xF3 */
#define SPARE_DISK      0x01  /* Spare */
#define CONFIGURED_DISK 0x02  /* Member of some RaidDev */
#define FAILED_DISK     0x04  /* Permanent failure */
#define USABLE_DISK     0x08  /* Fully usable unless FAILED_DISK is set */

#define	IMSM_DISK_FILLERS	5
	__u32 filler[IMSM_DISK_FILLERS]; /* 0xF4 - 0x107 MPB_DISK_FILLERS for future expansion */
};

/* RAID map configuration infos. */
struct imsm_map {
	__u32 pba_of_lba0;	/* start address of partition */
	__u32 blocks_per_member;/* blocks per member */
	__u32 num_data_stripes;	/* number of data stripes */
	__u16 blocks_per_strip;
	__u8  map_state;	/* Normal, Uninitialized, Degraded, Failed */
#define IMSM_T_STATE_NORMAL 0
#define IMSM_T_STATE_UNINITIALIZED 1
#define IMSM_T_STATE_DEGRADED 2 /* FIXME: is this correct? */
#define IMSM_T_STATE_FAILED 3 /* FIXME: is this correct? */
	__u8  raid_level;
#define IMSM_T_RAID0 0
#define IMSM_T_RAID1 1
#define IMSM_T_RAID5 5		/* since metadata version 1.2.02 ? */
	__u8  num_members;	/* number of member disks */
	__u8  reserved[3];
	__u32 filler[7];	/* expansion area */
	__u32 disk_ord_tbl[1];	/* disk_ord_tbl[num_members],
				   top byte special */
} __attribute__ ((packed));

struct imsm_vol {
	__u32 reserved[2];
	__u8  migr_state;	/* Normal or Migrating */
	__u8  migr_type;	/* Initializing, Rebuilding, ... */
	__u8  dirty;
	__u8  fill[1];
	__u32 filler[5];
	struct imsm_map map[1];
	/* here comes another one if migr_state */
} __attribute__ ((packed));

struct imsm_dev {
	__u8	volume[MAX_RAID_SERIAL_LEN];
	__u32 size_low;
	__u32 size_high;
	__u32 status;	/* Persistent RaidDev status */
	__u32 reserved_blocks; /* Reserved blocks at beginning of volume */
#define IMSM_DEV_FILLERS 12
	__u32 filler[IMSM_DEV_FILLERS];
	struct imsm_vol vol;
} __attribute__ ((packed));

struct imsm_super {
	__u8 sig[MAX_SIGNATURE_LENGTH];	/* 0x00 - 0x1F */
	__u32 check_sum;		/* 0x20 - 0x23 MPB Checksum */
	__u32 mpb_size;			/* 0x24 - 0x27 Size of MPB */
	__u32 family_num;		/* 0x28 - 0x2B Checksum from first time this config was written */
	__u32 generation_num;		/* 0x2C - 0x2F Incremented each time this array's MPB is written */
	__u32 reserved[2];		/* 0x30 - 0x37 */
	__u8 num_disks;			/* 0x38 Number of configured disks */
	__u8 num_raid_devs;		/* 0x39 Number of configured volumes */
	__u8 fill[2];			/* 0x3A - 0x3B */
#define IMSM_FILLERS 39
	__u32 filler[IMSM_FILLERS];	/* 0x3C - 0xD7 RAID_MPB_FILLERS */
	struct imsm_disk disk[1];	/* 0xD8 diskTbl[numDisks] */
	/* here comes imsm_dev[num_raid_devs] */
} __attribute__ ((packed));

#ifndef MDASSEMBLE
static char *map_state_str[] = { "normal", "uninitialized", "degraded", "failed" };
#endif

static unsigned long long mpb_sectors(struct imsm_super *mpb)
{
	__u32 size = __le32_to_cpu(mpb->mpb_size);

	return ((size + (512-1)) & (~(512-1))) / 512;
}

/* internal representation of IMSM metadata */
struct intel_super {
	union {
		struct imsm_super *mpb;
		void *buf;
	};
	int updates_pending;
	struct dl {
		struct dl *next;
		int index;
		__u8 serial[MAX_RAID_SERIAL_LEN];
		int major, minor;
		char *devname;
		int fd;
	} *disks;
};

static struct supertype *match_metadata_desc_imsm(char *arg)
{
	struct supertype *st;

	if (strcmp(arg, "imsm") != 0 &&
	    strcmp(arg, "default") != 0
		)
		return NULL;

	st = malloc(sizeof(*st));
	st->ss = &super_imsm;
	st->max_devs = IMSM_MAX_DEVICES;
	st->minor_version = 0;
	st->sb = NULL;
	return st;
}

static struct supertype *match_metadata_desc_imsm_raid(char *arg)
{
	struct supertype *st;

	if (strcmp(arg, "imsm/raid") != 0 &&
	    strcmp(arg, "raid") != 0 &&
	    strcmp(arg, "default") != 0
		)
		return NULL;

	st = malloc(sizeof(*st));
	st->ss = &super_imsm_raid;
	st->max_devs = IMSM_MAX_DEVICES;
	st->minor_version = 0;
	st->sb = NULL;
	return st;
}

static __u8 *get_imsm_version(struct imsm_super *mpb)
{
	return &mpb->sig[MPB_SIG_LEN];
}

static struct imsm_disk *get_imsm_disk(struct imsm_super *mpb, __u8 index)
{
	if (index > mpb->num_disks - 1)
		return NULL;
	return &mpb->disk[index];
}

static __u32 gen_imsm_checksum(struct imsm_super *mpb)
{
	__u32 end = mpb->mpb_size / sizeof(end);
	__u32 *p = (__u32 *) mpb;
	__u32 sum = 0;

        while (end--)
                sum += __le32_to_cpu(*p++);

        return sum - __le32_to_cpu(mpb->check_sum);
}

static size_t sizeof_imsm_dev(struct imsm_dev *dev)
{
	size_t size = sizeof(*dev);

	/* each map has disk_ord_tbl[num_members - 1] additional space */
	size += sizeof(__u32) * (dev->vol.map[0].num_members - 1);

	/* migrating means an additional map */
	if (dev->vol.migr_state) {
		size += sizeof(struct imsm_map);
		size += sizeof(__u32) * (dev->vol.map[1].num_members - 1);
	}

	return size;
}

static struct imsm_dev *get_imsm_dev(struct imsm_super *mpb, __u8 index)
{
	int offset;
	int i;
	void *_mpb = mpb;

	if (index > mpb->num_raid_devs - 1)
		return NULL;

	/* devices start after all disks */
	offset = ((void *) &mpb->disk[mpb->num_disks]) - _mpb;

	for (i = 0; i <= index; i++)
		if (i == index)
			return _mpb + offset;
		else
			offset += sizeof_imsm_dev(_mpb + offset);

	return NULL;
}

static __u32 get_imsm_disk_idx(struct imsm_map *map, int slot)
{
	__u32 *ord_tbl = &map->disk_ord_tbl[slot];

	/* top byte is 'special' */
	return __le32_to_cpu(*ord_tbl & ~(0xff << 24));
}

static int get_imsm_raid_level(struct imsm_map *map)
{
	if (map->raid_level == 1) {
		if (map->num_members == 2)
			return 1;
		else
			return 10;
	}

	return map->raid_level;
}

#ifndef MDASSEMBLE
static void print_imsm_dev(struct imsm_dev *dev, int index)
{
	__u64 sz;
	int slot;
	struct imsm_map *map = dev->vol.map;

	printf("\n");
	printf("[%s]:\n", dev->volume);
	printf("     RAID Level : %d\n", get_imsm_raid_level(map));
	printf("        Members : %d\n", map->num_members);
	for (slot = 0; slot < map->num_members; slot++)
		if (index == get_imsm_disk_idx(map, slot))
			break;
	if (slot < map->num_members)
		printf("      This Slot : %d\n", slot);
	else
		printf("      This Slot : ?\n");
	sz = __le32_to_cpu(dev->size_high);
	sz <<= 32;
	sz += __le32_to_cpu(dev->size_low);
	printf("     Array Size : %llu%s\n", (unsigned long long)sz,
	       human_size(sz * 512));
	sz = __le32_to_cpu(map->blocks_per_member);
	printf("   Per Dev Size : %llu%s\n", (unsigned long long)sz,
	       human_size(sz * 512));
	printf("  Sector Offset : %u\n",
		__le32_to_cpu(map->pba_of_lba0));
	printf("    Num Stripes : %u\n",
		__le32_to_cpu(map->num_data_stripes));
	printf("     Chunk Size : %u KiB\n",
		__le16_to_cpu(map->blocks_per_strip) / 2);
	printf("       Reserved : %d\n", __le32_to_cpu(dev->reserved_blocks));
	printf("  Migrate State : %s\n", dev->vol.migr_state ? "migrating" : "idle");
	printf("    Dirty State : %s\n", dev->vol.dirty ? "dirty" : "clean");
	printf("      Map State : %s\n", map_state_str[map->map_state]);
}

static void print_imsm_disk(struct imsm_super *mpb, int index)
{
	struct imsm_disk *disk = get_imsm_disk(mpb, index);
	char str[MAX_RAID_SERIAL_LEN];
	__u32 s;
	__u64 sz;

	printf("\n");
	snprintf(str, MAX_RAID_SERIAL_LEN, "%s", disk->serial);
	printf("  Disk%02d Serial : %s\n", index, str);
	s = __le32_to_cpu(disk->status);
	printf("          State :%s%s%s%s\n", s&SPARE_DISK ? " spare" : "",
					      s&CONFIGURED_DISK ? " active" : "",
					      s&FAILED_DISK ? " failed" : "",
					      s&USABLE_DISK ? " usable" : "");
	printf("             Id : %08x\n", __le32_to_cpu(disk->scsi_id));
	sz = __le32_to_cpu(disk->total_blocks) - mpb_sectors(mpb);
	printf("    Usable Size : %llu%s\n", (unsigned long long)sz,
	       human_size(sz * 512));
}

static void examine_super_imsm(struct supertype *st, char *homehost)
{
	struct intel_super *super = st->sb;
	struct imsm_super *mpb = super->mpb;
	char str[MAX_SIGNATURE_LENGTH];
	int i;
	__u32 sum;

	snprintf(str, MPB_SIG_LEN, "%s", mpb->sig);
	printf("          Magic : %s\n", str);
	snprintf(str, strlen(MPB_VERSION_RAID0), "%s", get_imsm_version(mpb));
	printf("        Version : %s\n", get_imsm_version(mpb));
	printf("         Family : %08x\n", __le32_to_cpu(mpb->family_num));
	printf("     Generation : %08x\n", __le32_to_cpu(mpb->generation_num));
	sum = __le32_to_cpu(mpb->check_sum);
	printf("       Checksum : %08x %s\n", sum,
		gen_imsm_checksum(mpb) == sum ? "correct" : "incorrect");
	printf("          Disks : %d\n", mpb->num_disks);
	printf("   RAID Devices : %d\n", mpb->num_raid_devs);
	print_imsm_disk(mpb, super->disks->index);
	for (i = 0; i < mpb->num_raid_devs; i++)
		print_imsm_dev(get_imsm_dev(mpb, i), super->disks->index);
	for (i = 0; i < mpb->num_disks; i++) {
		if (i == super->disks->index)
			continue;
		print_imsm_disk(mpb, i);
	}
}

static void brief_examine_super_imsm(struct supertype *st)
{
	struct intel_super *super = st->sb;
	struct imsm_super *mpb = super->mpb;

	printf("ARRAY /dev/imsm family=%08x metadata=external:imsm\n",
		__le32_to_cpu(mpb->family_num));
}

static void detail_super_imsm(struct supertype *st, char *homehost)
{
	printf("%s\n", __FUNCTION__);
}

static void brief_detail_super_imsm(struct supertype *st)
{
	printf("%s\n", __FUNCTION__);
}
#endif

static int match_home_imsm(struct supertype *st, char *homehost)
{
	printf("%s\n", __FUNCTION__);

	return 0;
}

static void uuid_from_super_imsm(struct supertype *st, int uuid[4])
{
	printf("%s\n", __FUNCTION__);
}

static void
get_imsm_numerical_version(struct imsm_super *mpb, int *m, int *p)
{
	__u8 *v = get_imsm_version(mpb);
	__u8 *end = mpb->sig + MAX_SIGNATURE_LENGTH;
	char major[] = { 0, 0, 0 };
	char minor[] = { 0 ,0, 0 };
	char patch[] = { 0, 0, 0 };
	char *ver_parse[] = { major, minor, patch };
	int i, j;

	i = j = 0;
	while (*v != '\0' && v < end) {
		if (*v != '.' && j < 2)
			ver_parse[i][j++] = *v;
		else {
			i++;
			j = 0;
		}
		v++;
	}

	*m = strtol(minor, NULL, 0);
	*p = strtol(patch, NULL, 0);
}

static void getinfo_super_imsm(struct supertype *st, struct mdinfo *info)
{
	struct intel_super *super = st->sb;
	struct imsm_super *mpb = super->mpb;
	struct imsm_disk *disk;
	__u32 s;
	int i, j;

	info->array.major_version = 2000;
	get_imsm_numerical_version(mpb, &info->array.minor_version,
				   &info->array.patch_version);
	info->array.raid_disks    = mpb->num_disks;
	info->array.level         = LEVEL_CONTAINER;
	info->array.layout        = 0;
	info->array.md_minor      = -1;
	info->array.ctime         = __le32_to_cpu(mpb->generation_num); //??
	info->array.utime         = 0;
	info->array.chunk_size    = 0;

	info->disk.major = 0;
	info->disk.minor = 0;
	info->disk.number = super->disks->index;
	info->disk.raid_disk = -1;
	/* is this disk a member of a raid device? */
	for (i = 0; i < mpb->num_raid_devs; i++) {
		struct imsm_dev *dev = get_imsm_dev(mpb, i);
		struct imsm_map *map = dev->vol.map;

		for (j = 0; j < map->num_members; j++) {
			__u32 index = get_imsm_disk_idx(map, j);

			if (index == super->disks->index) {
				info->disk.raid_disk = super->disks->index;
				break;
			}
		}
		if (info->disk.raid_disk != -1)
			break;
	}
	disk = get_imsm_disk(mpb, super->disks->index);
	s = __le32_to_cpu(disk->status);
	info->disk.state  = s & CONFIGURED_DISK ? (1 << MD_DISK_ACTIVE) : 0;
	info->disk.state |= s & FAILED_DISK ? (1 << MD_DISK_FAULTY) : 0;
	info->disk.state |= s & USABLE_DISK ? (1 << MD_DISK_SYNC) : 0;
	info->reshape_active = 0;
}

static void getinfo_super_imsm_raid(struct supertype *st, struct mdinfo *info)
{
	printf("%s\n", __FUNCTION__);
}

static int update_super_imsm(struct supertype *st, struct mdinfo *info,
			     char *update, char *devname, int verbose,
			     int uuid_set, char *homehost)
{
	/* FIXME */

	/* For 'assemble' and 'force' we need to return non-zero if any
	 * change was made.  For others, the return value is ignored.
	 * Update options are:
	 *  force-one : This device looks a bit old but needs to be included,
	 *        update age info appropriately.
	 *  assemble: clear any 'faulty' flag to allow this device to
	 *		be assembled.
	 *  force-array: Array is degraded but being forced, mark it clean
	 *	   if that will be needed to assemble it.
	 *
	 *  newdev:  not used ????
	 *  grow:  Array has gained a new device - this is currently for
	 *		linear only
	 *  resync: mark as dirty so a resync will happen.
	 *  name:  update the name - preserving the homehost
	 *
	 * Following are not relevant for this imsm:
	 *  sparc2.2 : update from old dodgey metadata
	 *  super-minor: change the preferred_minor number
	 *  summaries:  update redundant counters.
	 *  uuid:  Change the uuid of the array to match watch is given
	 *  homehost:  update the recorded homehost
	 *  _reshape_progress: record new reshape_progress position.
	 */
	int rv = 0;
	//struct intel_super *super = st->sb;
	//struct imsm_super *mpb = super->mpb;

	if (strcmp(update, "grow") == 0) {
	}
	if (strcmp(update, "resync") == 0) {
		/* dev->vol.dirty = 1; */
	}

	/* IMSM has no concept of UUID or homehost */

	return rv;
}

static __u64 avail_size_imsm(struct supertype *st, __u64 size)
{
	printf("%s\n", __FUNCTION__);

	return 0;
}

static int compare_super_imsm(struct supertype *st, struct supertype *tst)
{
	/*
	 * return:
	 *  0 same, or first was empty, and second was copied
	 *  1 second had wrong number
	 *  2 wrong uuid
	 *  3 wrong other info
	 */
	struct intel_super *first = st->sb;
	struct intel_super *sec = tst->sb;

        if (!first) {
                st->sb = tst->sb;
                tst->sb = NULL;
                return 0;
        }

	if (memcmp(first->mpb->sig, sec->mpb->sig, MAX_SIGNATURE_LENGTH) != 0)
		return 3;
	if (first->mpb->family_num != sec->mpb->family_num)
		return 3;
	if (first->mpb->mpb_size != sec->mpb->mpb_size)
		return 3;
	if (first->mpb->check_sum != sec->mpb->check_sum)
		return 3;

	return 0;
}

extern int scsi_get_serial(int fd, void *buf, size_t buf_len);

static int imsm_read_serial(int fd, char *devname,
			    __u8 serial[MAX_RAID_SERIAL_LEN])
{
	unsigned char scsi_serial[255];
	struct stat stb;
	int sg_fd;
	int minor;
	char sg_path[20];
	int rv;
	int rsp_len;
	int i, cnt;

	memset(scsi_serial, 0, sizeof(scsi_serial));
	fstat(fd, &stb);
	minor = minor(stb.st_rdev);
	minor /= 16;

	sprintf(sg_path, "/dev/sg%d", minor);
	sg_fd = open(sg_path, O_RDONLY);
	if (sg_fd < 0) {
		if (devname)
			fprintf(stderr,
				Name ": Failed to open %s for %s: %s\n",
				sg_path,  devname, strerror(errno));
		return 1;
	}

	rv = scsi_get_serial(sg_fd, scsi_serial, sizeof(scsi_serial));
	close(sg_fd);

	if (rv != 0) {
		if (devname)
			fprintf(stderr,
				Name ": Failed to retrieve serial for %s\n",
				devname);
		return rv;
	}

	rsp_len = scsi_serial[3];
	for (i = 0, cnt = 0; i < rsp_len; i++) {
		if (!isspace(scsi_serial[4 + i]))
			serial[cnt++] = scsi_serial[4 + i];
		if (cnt == MAX_RAID_SERIAL_LEN)
			break;
	}

	serial[MAX_RAID_SERIAL_LEN - 1] = '\0';

	return 0;
}

static int
load_imsm_disk(int fd, struct intel_super *super, char *devname, int keep_fd)
{
	struct imsm_super *mpb = super->mpb;
	struct dl *dl;
	struct stat stb;
	struct imsm_disk *disk;
	int rv;
	int i;

	dl = malloc(sizeof(*dl));
	if (!dl) {
		if (devname)
			fprintf(stderr,
				Name ": failed to allocate disk buffer for %s\n",
				devname);
		return 2;
	}
	memset(dl, 0, sizeof(*dl));

	fstat(fd, &stb);
	dl->major = major(stb.st_rdev);
	dl->minor = minor(stb.st_rdev);
	dl->next = super->disks;
	dl->fd = keep_fd ? fd : -1;
	dl->devname = devname ? strdup(devname) : NULL;
	dl->index = -1;
	super->disks = dl;
	rv = imsm_read_serial(fd, devname, dl->serial);

	if (rv != 0)
		return 2;

	/* look up this disk's index */
	for (i = 0; i < mpb->num_disks; i++) {
		disk = get_imsm_disk(mpb, i);

		if (memcmp(disk->serial, dl->serial, MAX_RAID_SERIAL_LEN) == 0)
			break;
	}

	if (i > mpb->num_disks)
		return 2;

	dl->index = i;

	return 0;
}

/* load_imsm_mpb - read matrix metadata
 * allocates super->mpb to be freed by free_super
 */
static int load_imsm_mpb(int fd, struct intel_super *super, char *devname)
{
	unsigned long long dsize;
	size_t len, mpb_size;
	unsigned long long sectors;
	struct stat;
	struct imsm_super anchor;
	__u32 check_sum;

	memset(super, 0, sizeof(*super));
	get_dev_size(fd, NULL, &dsize);

	if (lseek64(fd, dsize - (512 * 2), SEEK_SET) < 0) {
		if (devname)
			fprintf(stderr,
				Name ": Cannot seek to anchor block on %s: %s\n",
				devname, strerror(errno));
		return 1;
	}

	len = sizeof(anchor);
	if (read(fd, &anchor, len) != len) {
		if (devname)
			fprintf(stderr,
				Name ": Cannot read anchor block on %s: %s\n",
				devname, strerror(errno));
		return 1;
	}

	if (strncmp((char *) anchor.sig, MPB_SIGNATURE, MPB_SIG_LEN) != 0) {
		if (devname)
			fprintf(stderr,
				Name ": no IMSM anchor on %s\n", devname);
		return 2;
	}

	mpb_size = __le32_to_cpu(anchor.mpb_size);
	super->mpb = malloc(mpb_size < 512 ? 512 : mpb_size);
	if (!super->mpb) {
		if (devname)
			fprintf(stderr,
				Name ": unable to allocate %zu byte mpb buffer\n",
				mpb_size);
		return 2;
	}
	memcpy(super->buf, &anchor, sizeof(anchor));

	/* read the rest of the first block */
	len = 512 - sizeof(anchor);
	if (read(fd, super->buf + sizeof(anchor), len) != len) {
		if (devname)
			fprintf(stderr,
				Name ": Cannot read anchor remainder on %s: %s\n",
				devname, strerror(errno));
		return 2;
	}

	sectors = mpb_sectors(&anchor) - 1;
	if (!sectors)
		return load_imsm_disk(fd, super, devname, 0);

	/* read the extended mpb */
	if (lseek64(fd, dsize - (512 * (2 + sectors)), SEEK_SET) < 0) {
		if (devname)
			fprintf(stderr,
				Name ": Cannot seek to extended mpb on %s: %s\n",
				devname, strerror(errno));
		return 1;
	}

	len = mpb_size - 512;
	if (read(fd, super->buf + 512, len) != len) {
		if (devname)
			fprintf(stderr,
				Name ": Cannot read extended mpb on %s: %s\n",
				devname, strerror(errno));
		return 2;
	}

	check_sum = gen_imsm_checksum(super->mpb);
	if (check_sum != __le32_to_cpu(super->mpb->check_sum)) {
		if (devname)
			fprintf(stderr,
				Name ": IMSM checksum %x != %x on %s\n",
				check_sum, __le32_to_cpu(super->mpb->check_sum),
				devname);
		return 2;
	}

	return load_imsm_disk(fd, super, devname, 0);
}

struct superswitch super_imsm_container;

static void free_imsm_disks(struct intel_super *super)
{
	while (super->disks) {
		struct dl *d = super->disks;

		super->disks = d->next;
		if (d->fd >= 0)
			close(d->fd);
		if (d->devname)
			free(d->devname);
		free(d);
	}
}

static void free_imsm(struct intel_super *super)
{
	if (super->mpb)
		free(super->mpb);
	free_imsm_disks(super);
	free(super);
}


static void free_super_imsm(struct supertype *st)
{
	struct intel_super *super = st->sb;

	if (!super)
		return;

	free_imsm(super);
	st->sb = NULL;
}

#ifndef MDASSEMBLE
static int load_super_imsm_all(struct supertype *st, int fd, void **sbp,
			       char *devname, int keep_fd)
{
	struct mdinfo *sra;
	struct intel_super *super;
	struct mdinfo *sd, *best = NULL;
	__u32 bestgen = 0;
	__u32 gen;
	char nm[20];
	int dfd;
	int rv;

	/* check if this disk is a member of an active array */
	sra = sysfs_read(fd, 0, GET_LEVEL|GET_VERSION|GET_DEVS|GET_STATE);
	if (!sra)
		return 1;

	if (sra->array.major_version != -1 ||
	    sra->array.minor_version != -2 ||
	    strcmp(sra->text_version, "imsm") != 0)
		return 1;

	super = malloc(sizeof(*super));
	if (!super)
		return 1;

	/* find the most up to date disk in this array */
	for (sd = sra->devs; sd; sd = sd->next) {
		sprintf(nm, "%d:%d", sd->disk.major, sd->disk.minor);
		dfd = dev_open(nm, keep_fd ? O_RDWR : O_RDONLY);
		if (!dfd) {
			free_imsm(super);
			return 2;
		}
		rv = load_imsm_mpb(dfd, super, NULL);
		if (!keep_fd)
			close(dfd);
		if (rv == 0) {
			gen = __le32_to_cpu(super->mpb->generation_num);
			if (!best || gen > bestgen) {
				bestgen = gen;
				best = sd;
			}
		} else {
			free_imsm(super);
			return 2;
		}
	}

	if (!best) {
		free_imsm(super);
		return 1;
	}

	/* load the most up to date anchor */
	sprintf(nm, "%d:%d", best->disk.major, best->disk.minor);
	dfd = dev_open(nm, O_RDONLY);
	if (!dfd) {
		free_imsm(super);
		return 1;
	}
	rv = load_imsm_mpb(dfd, super, NULL);
	close(dfd);
	if (rv != 0) {
		free_imsm(super);
		return 2;
	}

	/* reset the disk list */
	free_imsm_disks(super);

	/* populate disk list */
	for (sd = sra->devs ; sd ; sd = sd->next) {
		sprintf(nm, "%d:%d", sd->disk.major, sd->disk.minor);
		dfd = dev_open(nm, keep_fd? O_RDWR : O_RDONLY);
		if (!dfd) {
			free_imsm(super);
			return 2;
		}
		load_imsm_disk(dfd, super, NULL, keep_fd);
		if (!keep_fd)
			close(dfd);
	}

	*sbp = super;
	if (st->ss == NULL) {
		st->ss = &super_imsm_container;
		st->minor_version = 0;
		st->max_devs = IMSM_MAX_DEVICES;
	}

	return 0;
}
#endif

static int load_super_imsm(struct supertype *st, int fd, char *devname)
{
	struct intel_super *super;
	int rv;

#ifndef MDASSEMBLE
	if (load_super_imsm_all(st, fd, &st->sb, devname, 1) == 0)
		return 0;
#endif

	super = malloc(sizeof(*super));
	if (!super) {
		fprintf(stderr,
			Name ": malloc of %zu failed.\n",
			sizeof(*super));
		return 1;
	}

	rv = load_imsm_mpb(fd, super, devname);

	if (rv) {
		if (devname)
			fprintf(stderr,
				Name ": Failed to load all information "
				"sections on %s\n", devname);
		free_imsm(super);
		return rv;
	}

	st->sb = super;
	if (st->ss == NULL) {
		st->ss = &super_imsm;
		st->minor_version = 0;
		st->max_devs = IMSM_MAX_DEVICES;
	}

	return 0;
}

static int init_zero_imsm(struct supertype *st, mdu_array_info_t *info,
			  unsigned long long size, char *name,
			  char *homehost, int *uuid)
{
	printf("%s\n", __FUNCTION__);

	return 0;
}

static int init_super_imsm(struct supertype *st, mdu_array_info_t *info,
			   unsigned long long size, char *name,
			   char *homehost, int *uuid)
{
	printf("%s\n", __FUNCTION__);

	return 0;
}

static int init_super_imsm_raid(struct supertype *st, mdu_array_info_t *info,
				unsigned long long size, char *name,
				char *homehost, int *uuid)
{
	printf("%s\n", __FUNCTION__);

	return 0;
}

static void add_to_super_imsm(struct supertype *st, mdu_disk_info_t *dinfo,
			      int fd, char *devname)
{
	printf("%s\n", __FUNCTION__);
}

static void add_to_super_imsm_raid(struct supertype *st, mdu_disk_info_t *dinfo,
				   int fd, char *devname)
{
	printf("%s\n", __FUNCTION__);
}

static int write_init_super_imsm(struct supertype *st)
{
	printf("%s\n", __FUNCTION__);

	return 0;
}

static int store_zero_imsm(struct supertype *st, int fd)
{
	printf("%s\n", __FUNCTION__);

	return 0;
}

static void getinfo_super_n_imsm_container(struct supertype *st, struct mdinfo *info)
{
	/* just need offset and size...
	 * of the metadata??
	 */
	struct intel_super *super = st->sb;
	struct imsm_super *mpb = super->mpb;
	struct imsm_disk *disk = get_imsm_disk(mpb, info->disk.number);
	int sect = mpb_sectors(mpb);

	info->data_offset = __le32_to_cpu(disk->total_blocks) - (2 + sect - 1);
	info->component_size = sect;
}

static void getinfo_super_n_raid(struct supertype *st, struct mdinfo *info)
{
	printf("%s\n", __FUNCTION__);
}

static int validate_geometry_imsm(struct supertype *st, int level, int layout,
				  int raiddisks, int chunk, unsigned long long size,
				  char *subdev, unsigned long long *freesize)
{
	printf("%s\n", __FUNCTION__);

	return 0;
}

static int validate_geometry_imsm_container(struct supertype *st, int level,
					    int layout, int raiddisks, int chunk,
					    unsigned long long size, char *subdev,
					    unsigned long long *freesize)
{
	printf("%s\n", __FUNCTION__);

	return 0;
}

static int validate_geometry_imsm_raid(struct supertype *st, int level,
				       int layout, int raiddisks, int chunk,
				       unsigned long long size, char *subdev,
				       unsigned long long *freesize)
{
	printf("%s\n", __FUNCTION__);

	return 0;
}

static struct mdinfo *container_content_imsm(struct supertype *st)
{
	/* Given a container loaded by load_super_imsm_all,
	 * extract information about all the arrays into
	 * an mdinfo tree.
	 *
	 * For each imsm_dev create an mdinfo, fill it in,
	 *  then look for matching devices in super->disks
	 *  and create appropriate device mdinfo.
	 */
	struct intel_super *super = st->sb;
	struct imsm_super *mpb = super->mpb;
	struct mdinfo *rest = NULL;
	int i;

	for (i = 0; i < mpb->num_raid_devs; i++) {
		struct imsm_dev *dev = get_imsm_dev(mpb, i);
		struct imsm_vol *vol = &dev->vol;
		struct imsm_map *map = vol->map;
		struct mdinfo *this;
		__u64 sz;
		int slot;

		this = malloc(sizeof(*this));
		memset(this, 0, sizeof(*this));
		this->next = rest;
		rest = this;

		this->array.major_version = 2000;
		get_imsm_numerical_version(mpb, &this->array.minor_version,
					   &this->array.patch_version);
		this->array.level = get_imsm_raid_level(map);
		this->array.raid_disks = map->num_members;
		switch(this->array.level) {
		case 0:
		case 1:
			this->array.layout = 0;
			break;
		case 5:
		case 6:
			this->array.layout = ALGORITHM_LEFT_SYMMETRIC;
			break;
		case 10:
			this->array.layout = 0x102; //FIXME is this correct?
			break;
		default:
			this->array.layout = -1; // FIXME
		}
		this->array.md_minor = -1;
		this->array.ctime = 0;
		this->array.utime = 0;
		this->array.chunk_size = __le16_to_cpu(map->blocks_per_strip) << 9;
		this->array.state = !vol->dirty;
		this->container_member = i;
		if (map->map_state == IMSM_T_STATE_UNINITIALIZED || dev->vol.dirty)
			this->resync_start = 0;
		else
			this->resync_start = ~0ULL;

		strncpy(this->name, (char *) dev->volume, MAX_RAID_SERIAL_LEN);
		this->name[MAX_RAID_SERIAL_LEN] = 0;

		memset(this->uuid, 0, sizeof(this->uuid));

		sz = __le32_to_cpu(dev->size_high);
		sz <<= 32;
		sz += __le32_to_cpu(dev->size_low);
		this->component_size = sz;
		this->array.size = this->component_size / 2;

		for (slot = 0 ; slot <  map->num_members; slot++) {
			struct imsm_disk *disk;
			struct mdinfo *info_d;
			struct dl *d;
			int idx;
			__u32 s;

			idx = __le32_to_cpu(map->disk_ord_tbl[slot] & ~(0xff << 24));
			for (d = super->disks; d ; d = d->next)
				if (d->index == idx)
                                        break;

			if (d == NULL)
				break; /* shouldn't this be continue ?? */

			info_d = malloc(sizeof(*info_d));
			if (!info_d)
				break; /* ditto ?? */
			memset(info_d, 0, sizeof(*info_d));
			info_d->next = this->devs;
			this->devs = info_d;

			disk = get_imsm_disk(mpb, idx);
			s = __le32_to_cpu(disk->status);

			info_d->disk.number = d->index;
			info_d->disk.major = d->major;
			info_d->disk.minor = d->minor;
			info_d->disk.raid_disk = slot;
			info_d->disk.state  = s & CONFIGURED_DISK ? (1 << MD_DISK_ACTIVE) : 0;
			info_d->disk.state |= s & FAILED_DISK ? (1 << MD_DISK_FAULTY) : 0;
			info_d->disk.state |= s & USABLE_DISK ? (1 << MD_DISK_SYNC) : 0;

			this->array.working_disks++;

			info_d->events = __le32_to_cpu(mpb->generation_num);
			info_d->data_offset = __le32_to_cpu(map->pba_of_lba0);
			info_d->component_size = __le32_to_cpu(map->blocks_per_member);
			if (d->devname)
				strcpy(info_d->name, d->devname);
		}
	}

	return rest;
}


static int imsm_open_new(struct supertype *c, struct active_array *a, int inst)
{
	fprintf(stderr, "imsm: open_new %d\n", inst);
	return 0;
}

static void imsm_mark_clean(struct active_array *a, unsigned long long sync_pos)
{
	int inst = a->info.container_member;
	struct intel_super *super = a->container->sb;
	struct imsm_dev *dev = get_imsm_dev(super->mpb, inst);

	if (dev->vol.dirty) {
		fprintf(stderr, "imsm: mark clean %llu\n", sync_pos);
		dev->vol.dirty = 0;
		super->updates_pending++;
	}
}

static void imsm_mark_dirty(struct active_array *a)
{
	int inst = a->info.container_member;
	struct intel_super *super = a->container->sb;
	struct imsm_dev *dev = get_imsm_dev(super->mpb, inst);

	if (!dev->vol.dirty) {
		fprintf(stderr, "imsm: mark dirty\n");
		dev->vol.dirty = 1;
		super->updates_pending++;
	}
}

static __u8 imsm_check_degraded(struct imsm_super *mpb, int n, int failed)
{
	struct imsm_dev *dev = get_imsm_dev(mpb, n);
	struct imsm_map *map = dev->vol.map;

	if (!failed)
		return map->map_state;

	switch (get_imsm_raid_level(map)) {
	case 0:
		return IMSM_T_STATE_FAILED;
		break;
	case 1:
		if (failed < map->num_members)
			return IMSM_T_STATE_DEGRADED;
		else
			return IMSM_T_STATE_FAILED;
		break;
	case 10:
	{
		/**
		 * check to see if any mirrors have failed,
		 * otherwise we are degraded
		 */
		int device_per_mirror = 2; /* FIXME is this always the case?
					    * and are they always adjacent?
					    */
		int failed = 0;
		int i;

		for (i = 0; i < map->num_members; i++) {
			int idx = get_imsm_disk_idx(map, i);
			struct imsm_disk *disk = get_imsm_disk(mpb, idx);

			if (__le32_to_cpu(disk->status) & FAILED_DISK)
				failed++;

			if (failed >= device_per_mirror)
				return IMSM_T_STATE_FAILED;

			/* reset 'failed' for next mirror set */
			if (!((i + 1) % device_per_mirror))
				failed = 0;
		}

		return IMSM_T_STATE_DEGRADED;
	}
	case 5:
		if (failed < 2)
			return IMSM_T_STATE_DEGRADED;
		else
			return IMSM_T_STATE_FAILED;
		break;
	default:
		break;
	}

	return map->map_state;
}

static int imsm_count_failed(struct imsm_super *mpb, struct imsm_map *map)
{
	int i;
	int failed = 0;
	struct imsm_disk *disk;

	for (i = 0; i < map->num_members; i++) {
		int idx = get_imsm_disk_idx(map, i);

		disk = get_imsm_disk(mpb, idx);
		if (__le32_to_cpu(disk->status) & FAILED_DISK)
			failed++;
	}

	return failed;
}

static void imsm_mark_sync(struct active_array *a, unsigned long long resync)
{
	int inst = a->info.container_member;
	struct intel_super *super = a->container->sb;
	struct imsm_dev *dev = get_imsm_dev(super->mpb, inst);
	struct imsm_map *map = dev->vol.map;
	int failed;
	__u8 map_state;

	if (resync != ~0ULL)
		return;

	fprintf(stderr, "imsm: mark sync\n");

	failed = imsm_count_failed(super->mpb, map);
	map_state = imsm_check_degraded(super->mpb, inst, failed);
	if (!failed)
		map_state = IMSM_T_STATE_NORMAL;
	if (map->map_state != map_state) {
		map->map_state = map_state;
		super->updates_pending++;
	}
}

static void imsm_set_disk(struct active_array *a, int n, int state)
{
	int inst = a->info.container_member;
	struct intel_super *super = a->container->sb;
	struct imsm_dev *dev = get_imsm_dev(super->mpb, inst);
	struct imsm_map *map = dev->vol.map;
	struct imsm_disk *disk;
	__u32 status;
	int failed = 0;
	int new_failure = 0;

	if (n > map->num_members)
		fprintf(stderr, "imsm: set_disk %d out of range 0..%d\n",
			n, map->num_members - 1);

	if (n < 0)
		return;

	fprintf(stderr, "imsm: set_disk %d:%x\n", n, state);

	disk = get_imsm_disk(super->mpb, get_imsm_disk_idx(map, n));

	/* check if we have seen this failure before */
	status = __le32_to_cpu(disk->status);
	if ((state & DS_FAULTY) && !(status & FAILED_DISK)) {
		status |= FAILED_DISK;
		disk->status = __cpu_to_le32(status);
		new_failure = 1;
	}

	/**
	 * the number of failures have changed, count up 'failed' to determine
	 * degraded / failed status
	 */
	if (new_failure && map->map_state != IMSM_T_STATE_FAILED)
		failed = imsm_count_failed(super->mpb, map);

	if (failed)
		map->map_state = imsm_check_degraded(super->mpb, inst, failed);

	if (new_failure)
		super->updates_pending++;
}

static int store_imsm_mpb(int fd, struct intel_super *super)
{
	struct imsm_super *mpb = super->mpb;
	__u32 mpb_size = __le32_to_cpu(mpb->mpb_size);
	unsigned long long dsize;
	unsigned long long sectors;

	get_dev_size(fd, NULL, &dsize);

	/* first block is stored on second to last sector of the disk */
	if (lseek64(fd, dsize - (512 * 2), SEEK_SET) < 0)
		return 1;

	if (write(fd, super->buf, 512) != 512)
		return 1;

	if (mpb_size <= 512)
		return 0;

	/* -1 because we already wrote a sector */
	sectors = mpb_sectors(mpb) - 1;

	/* write the extended mpb to the sectors preceeding the anchor */
	if (lseek64(fd, dsize - (512 * (2 + sectors)), SEEK_SET) < 0)
		return 1;

	if (write(fd, super->buf + 512, mpb_size - 512) != mpb_size - 512)
		return 1;

	fsync(fd);

	return 0;
}

static void imsm_sync_metadata(struct active_array *a)
{
	struct intel_super *super = a->container->sb;
	struct imsm_super *mpb = super->mpb;
	struct dl *d;
	__u32 generation;
	__u32 sum;

	if (!super->updates_pending)
		return;

	fprintf(stderr, "imsm: sync_metadata\n");

	/* 'generation' is incremented everytime the metadata is written */
	generation = __le32_to_cpu(mpb->generation_num);
	generation++;
	mpb->generation_num = __cpu_to_le32(generation);

	/* recalculate checksum */
	sum = gen_imsm_checksum(mpb);
	mpb->check_sum = __cpu_to_le32(sum);

	for (d = super->disks; d ; d = d->next)
		if (store_imsm_mpb(d->fd, super))
			fprintf(stderr, "%s: failed for device %d:%d %s\n",
				__func__, d->major, d->minor, strerror(errno));

	super->updates_pending = 0;
}

struct superswitch super_imsm = {
#ifndef	MDASSEMBLE
	.examine_super	= examine_super_imsm,
	.brief_examine_super = brief_examine_super_imsm,
	.detail_super	= detail_super_imsm,
	.brief_detail_super = brief_detail_super_imsm,
#endif
	.match_home	= match_home_imsm,
	.uuid_from_super= uuid_from_super_imsm,
	.getinfo_super  = getinfo_super_imsm,
	.update_super	= update_super_imsm,

	.avail_size	= avail_size_imsm,

	.compare_super	= compare_super_imsm,

	.load_super	= load_super_imsm,
	.init_super	= init_zero_imsm,
	.store_super	= store_zero_imsm,
	.free_super	= free_super_imsm,
	.match_metadata_desc = match_metadata_desc_imsm,
	.getinfo_super_n  = getinfo_super_n_imsm_container,

	.validate_geometry = validate_geometry_imsm,
	.major		= 2000,
	.swapuuid	= 0,
	.external	= 1,
	.text_version	= "imsm",

/* for mdmon */
	.open_new	= imsm_open_new,
	.load_super	= load_super_imsm,
	.mark_clean	= imsm_mark_clean,
	.mark_dirty	= imsm_mark_dirty,
	.mark_sync	= imsm_mark_sync,
	.set_disk	= imsm_set_disk,
	.sync_metadata	= imsm_sync_metadata,
};

/* super_imsm_container is set by validate_geometry_imsm when given a
 * device that is not part of any array
 */
struct superswitch super_imsm_container = {

	.validate_geometry = validate_geometry_imsm_container,
	.init_super	= init_super_imsm,
	.add_to_super	= add_to_super_imsm,
	.write_init_super = write_init_super_imsm,
	.getinfo_super  = getinfo_super_imsm,
	.getinfo_super_n  = getinfo_super_n_imsm_container,
	.load_super	= load_super_imsm,

#ifndef MDASSEMBLE
	.examine_super	= examine_super_imsm,
	.brief_examine_super = brief_examine_super_imsm,
	.detail_super	= detail_super_imsm,
	.brief_detail_super = brief_detail_super_imsm,
#endif

	.free_super	= free_super_imsm,

	.container_content = container_content_imsm,

	.major		= 2000,
	.swapuuid	= 0,
	.external	= 1,
	.text_version	= "imsm",
};

struct superswitch super_imsm_raid = {
	.update_super	= update_super_imsm,
	.init_super	= init_super_imsm_raid,
	.add_to_super	= add_to_super_imsm_raid,
	.getinfo_super  = getinfo_super_imsm_raid,
	.getinfo_super_n  = getinfo_super_n_raid,
	.write_init_super = write_init_super_imsm,

	.load_super	= load_super_imsm,
	.free_super	= free_super_imsm,
	.match_metadata_desc = match_metadata_desc_imsm_raid,


	.validate_geometry = validate_geometry_imsm_raid,
	.major		= 2001,
	.swapuuid	= 0,
	.external	= 2,
	.text_version	= "imsm",
};
