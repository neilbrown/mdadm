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
#define MPB_SECTOR_CNT 418
#define IMSM_RESERVED_SECTORS 4096

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

static unsigned int sector_count(__u32 bytes)
{
	return ((bytes + (512-1)) & (~(512-1))) / 512;
}

static unsigned int mpb_sectors(struct imsm_super *mpb)
{
	return sector_count(__le32_to_cpu(mpb->mpb_size));
}

/* internal representation of IMSM metadata */
struct intel_super {
	union {
		struct imsm_super *mpb;
		void *buf;
	};
	int updates_pending; /* count of pending updates for mdmon */
	int creating_imsm; /* flag to indicate container creation */
	int current_vol; /* index of raid device undergoing creation */
	struct dl {
		struct dl *next;
		int index;
		__u8 serial[MAX_RAID_SERIAL_LEN];
		int major, minor;
		char *devname;
		int fd;
	} *disks;
};

struct extent {
	unsigned long long start, size;
};

static struct supertype *match_metadata_desc_imsm(char *arg)
{
	struct supertype *st;

	if (strcmp(arg, "imsm") != 0 &&
	    strcmp(arg, "default") != 0
		)
		return NULL;

	st = malloc(sizeof(*st));
	memset(st, 0, sizeof(*st));
	st->ss = &super_imsm;
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

static int cmp_extent(const void *av, const void *bv)
{
	const struct extent *a = av;
	const struct extent *b = bv;
	if (a->start < b->start)
		return -1;
	if (a->start > b->start)
		return 1;
	return 0;
}

static struct extent *get_extents(struct intel_super *super, struct dl *dl)
{
	/* find a list of used extents on the given physical device */
	struct imsm_super *mpb = super->mpb;
	struct imsm_disk *disk;
	struct extent *rv, *e;
	int i, j;
	int memberships = 0;

	disk = get_imsm_disk(mpb, dl->index);
	if (!disk)
		return NULL;

	for (i = 0; i < mpb->num_raid_devs; i++) {
		struct imsm_dev *dev = get_imsm_dev(mpb, i);
		struct imsm_map *map = dev->vol.map;

		for (j = 0; j < map->num_members; j++) {
			__u32 index = get_imsm_disk_idx(map, j);

			if (index == dl->index)
				memberships++;
		}
	}
	rv = malloc(sizeof(struct extent) * (memberships + 1));
	if (!rv)
		return NULL;
	e = rv;

	for (i = 0; i < mpb->num_raid_devs; i++) {
		struct imsm_dev *dev = get_imsm_dev(mpb, i);
		struct imsm_map *map = dev->vol.map;

		for (j = 0; j < map->num_members; j++) {
			__u32 index = get_imsm_disk_idx(map, j);

			if (index == dl->index) {
				e->start = __le32_to_cpu(map->pba_of_lba0);
				e->size = __le32_to_cpu(map->blocks_per_member);
				e++;
			}
		}
	}
	qsort(rv, memberships, sizeof(*rv), cmp_extent);

	e->start = __le32_to_cpu(disk->total_blocks) -
		   (MPB_SECTOR_CNT + IMSM_RESERVED_SECTORS);
	e->size = 0;
	return rv;
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
	sz = __le32_to_cpu(disk->total_blocks) -
	     (MPB_SECTOR_CNT + IMSM_RESERVED_SECTORS * mpb->num_raid_devs);
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
	printf("    MPB Sectors : %d\n", mpb_sectors(mpb));
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

#if 0
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
#endif

static int imsm_level_to_layout(int level)
{
	switch (level) {
	case 0:
	case 1:
		return 0;
	case 5:
	case 6:
		return ALGORITHM_LEFT_SYMMETRIC;
	case 10:
		return 0x102; //FIXME is this correct?
	}
	return -1;
}

static void getinfo_super_imsm_volume(struct supertype *st, struct mdinfo *info)
{
	struct intel_super *super = st->sb;
	struct imsm_super *mpb = super->mpb;
	struct imsm_dev *dev = get_imsm_dev(mpb, super->current_vol);
	struct imsm_map *map = &dev->vol.map[0];

	info->container_member	  = super->current_vol;
	info->array.raid_disks    = map->num_members;
	info->array.level	  = get_imsm_raid_level(map);
	info->array.layout	  = imsm_level_to_layout(info->array.level);
	info->array.md_minor	  = -1;
	info->array.ctime	  = 0;
	info->array.utime	  = 0;
	info->array.chunk_size	  = __le16_to_cpu(map->blocks_per_strip * 512);

	info->data_offset	  = __le32_to_cpu(map->pba_of_lba0);
	info->component_size	  = __le32_to_cpu(map->blocks_per_member);

	info->disk.major = 0;
	info->disk.minor = 0;

	sprintf(info->text_version, "/%s/%d",
		devnum2devname(st->container_dev),
		info->container_member);
}


static void getinfo_super_imsm(struct supertype *st, struct mdinfo *info)
{
	struct intel_super *super = st->sb;
	struct imsm_super *mpb = super->mpb;
	struct imsm_disk *disk;
	__u32 s;

	if (super->current_vol >= 0) {
		getinfo_super_imsm_volume(st, info);
		return;
	}
	info->array.raid_disks    = mpb->num_disks;
	info->array.level         = LEVEL_CONTAINER;
	info->array.layout        = 0;
	info->array.md_minor      = -1;
	info->array.ctime         = 0; /* N/A for imsm */ 
	info->array.utime         = 0;
	info->array.chunk_size    = 0;

	info->disk.major = 0;
	info->disk.minor = 0;
	info->disk.raid_disk = -1;
	info->reshape_active = 0;
	strcpy(info->text_version, "imsm");
	info->disk.number = -1;
	info->disk.state = 0;

	if (super->disks) {
		disk = get_imsm_disk(mpb, super->disks->index);
		info->disk.number = super->disks->index;
		info->disk.raid_disk = super->disks->index;
		info->data_offset = __le32_to_cpu(disk->total_blocks) -
				    (MPB_SECTOR_CNT + IMSM_RESERVED_SECTORS);
		info->component_size = MPB_SECTOR_CNT + IMSM_RESERVED_SECTORS;
		s = __le32_to_cpu(disk->status);
		info->disk.state  = s & CONFIGURED_DISK ? (1 << MD_DISK_ACTIVE) : 0;
		info->disk.state |= s & FAILED_DISK ? (1 << MD_DISK_FAULTY) : 0;
		info->disk.state |= s & USABLE_DISK ? (1 << MD_DISK_SYNC) : 0;
	}
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

static size_t disks_to_mpb_size(int disks)
{
	size_t size;

	size = sizeof(struct imsm_super);
	size += (disks - 1) * sizeof(struct imsm_disk);
	size += 2 * sizeof(struct imsm_dev);
	/* up to 2 maps per raid device (-2 for imsm_maps in imsm_dev */
	size += (4 - 2) * sizeof(struct imsm_map);
	/* 4 possible disk_ord_tbl's */
	size += 4 * (disks - 1) * sizeof(__u32);

	return size;
}

static __u64 avail_size_imsm(struct supertype *st, __u64 devsize)
{
	if (devsize < (MPB_SECTOR_CNT + IMSM_RESERVED_SECTORS))
		return 0;

	return devsize - (MPB_SECTOR_CNT + IMSM_RESERVED_SECTORS);
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
	int sg_fd;
	int rv;
	int rsp_len;
	int i, cnt;

	memset(scsi_serial, 0, sizeof(scsi_serial));

	sg_fd = sysfs_disk_to_sg(fd);
	if (sg_fd < 0) {
		if (devname)
			fprintf(stderr,
				Name ": Failed to open sg interface for %s: %s\n",
				devname, strerror(errno));
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
	struct imsm_super *anchor;
	__u32 check_sum;

	get_dev_size(fd, NULL, &dsize);

	if (lseek64(fd, dsize - (512 * 2), SEEK_SET) < 0) {
		if (devname)
			fprintf(stderr,
				Name ": Cannot seek to anchor block on %s: %s\n",
				devname, strerror(errno));
		return 1;
	}

	len = 512;
	if (posix_memalign((void**)&anchor, 512, len) != 0) {
		if (devname)
			fprintf(stderr,
				Name ": Failed to allocate imsm anchor buffer"
				" on %s\n", devname);
		return 1;
	}
	if (read(fd, anchor, len) != len) {
		if (devname)
			fprintf(stderr,
				Name ": Cannot read anchor block on %s: %s\n",
				devname, strerror(errno));
		free(anchor);
		return 1;
	}

	if (strncmp((char *) anchor->sig, MPB_SIGNATURE, MPB_SIG_LEN) != 0) {
		if (devname)
			fprintf(stderr,
				Name ": no IMSM anchor on %s\n", devname);
		free(anchor);
		return 2;
	}

	mpb_size = __le32_to_cpu(anchor->mpb_size);
	mpb_size = ROUND_UP(mpb_size, 512);
	if (posix_memalign((void**)&super->mpb, 512, mpb_size) != 0) {
		if (devname)
			fprintf(stderr,
				Name ": unable to allocate %zu byte mpb buffer\n",
				mpb_size);
		free(anchor);
		return 2;
	}
	memcpy(super->buf, anchor, len);

	sectors = mpb_sectors(anchor) - 1;
	free(anchor);
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

static struct intel_super *alloc_super(int creating_imsm)
{
	struct intel_super *super = malloc(sizeof(*super));

	if (super) {
		memset(super, 0, sizeof(*super));
		super->creating_imsm = creating_imsm;
		super->current_vol = -1;
	}

	return super;
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

	super = alloc_super(0);
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

	if (st->subarray[0]) {
		if (atoi(st->subarray) <= super->mpb->num_raid_devs)
			super->current_vol = atoi(st->subarray);
		else
			return 1;
	}

	*sbp = super;
	if (st->ss == NULL) {
		st->ss = &super_imsm;
		st->minor_version = 0;
		st->max_devs = IMSM_MAX_DEVICES;
		st->container_dev = fd2devnum(fd);
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
	if (st->subarray[0])
		return 1; /* FIXME */

	super = alloc_super(0);
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

static int init_super_imsm_volume(struct supertype *st, mdu_array_info_t *info,
				  unsigned long long size, char *name,
				  char *homehost, int *uuid)
{
	/* We are creating a volume inside a pre-existing container.
	 * so st->sb is already set.
	 */
	struct intel_super *super = st->sb;
	struct imsm_super *mpb = super->mpb;
	struct imsm_dev *dev;
	struct imsm_vol *vol;
	struct imsm_map *map;
	int idx = mpb->num_raid_devs;
	int i;
	unsigned long long array_blocks;
	unsigned long long sz;
	__u32 offset = 0;
	size_t size_old, size_new;

	if (mpb->num_raid_devs >= 2) {
		fprintf(stderr, Name": This imsm-container already has the "
			"maximum of 2 volumes\n");
		return 0;
	}

	/* ensure the mpb is large enough for the new data */
	size_old = __le32_to_cpu(mpb->mpb_size);
	size_new = disks_to_mpb_size(info->nr_disks);
	if (size_new > size_old) {
		void *mpb_new;
		size_t size_round = ROUND_UP(size_new, 512);

		if (posix_memalign(&mpb_new, 512, size_round) != 0) {
			fprintf(stderr, Name": could not allocate new mpb\n");
			return 0;
		}
		memcpy(mpb_new, mpb, size_old);
		free(mpb);
		mpb = mpb_new;
		super->mpb = mpb_new;
		mpb->mpb_size = __cpu_to_le32(size_new);
		memset(mpb_new + size_old, 0, size_round - size_old);
	}
	super->current_vol = idx;
	sprintf(st->subarray, "%d", idx);
	mpb->num_raid_devs++;
	dev = get_imsm_dev(mpb, idx);
	strncpy((char *) dev->volume, name, MAX_RAID_SERIAL_LEN);
	array_blocks = calc_array_size(info->level, info->raid_disks,
				       info->layout, info->chunk_size,
				       info->size*2);
	dev->size_low = __cpu_to_le32((__u32) array_blocks);
	dev->size_high = __cpu_to_le32((__u32) (array_blocks >> 32));
	dev->status = __cpu_to_le32(0);
	dev->reserved_blocks = __cpu_to_le32(0);
	vol = &dev->vol;
	vol->migr_state = 0;
	vol->migr_type = 0;
	vol->dirty = 0;
	for (i = 0; i < idx; i++) {
		struct imsm_dev *prev = get_imsm_dev(mpb, i);
		struct imsm_map *pmap = &prev->vol.map[0];

		offset += __le32_to_cpu(pmap->blocks_per_member);
		offset += IMSM_RESERVED_SECTORS;
	}
	map = &vol->map[0];
	map->pba_of_lba0 = __cpu_to_le32(offset);
	sz = info->size * 2;
	map->blocks_per_member = __cpu_to_le32(sz);
	map->blocks_per_strip = __cpu_to_le16(info->chunk_size >> 9);
	map->num_data_stripes = __cpu_to_le32(sz / (info->chunk_size >> 9));
	map->map_state = info->level ? IMSM_T_STATE_UNINITIALIZED :
				       IMSM_T_STATE_NORMAL;
	if (info->level == 10)
		map->raid_level = 1;
	else
		map->raid_level = info->level;
	map->num_members = info->raid_disks;
	for (i = 0; i < map->num_members; i++) {
		/* initialized in add_to_super */
		map->disk_ord_tbl[i] = __cpu_to_le32(0);
	}

	return 1;
}

static int init_super_imsm(struct supertype *st, mdu_array_info_t *info,
			   unsigned long long size, char *name,
			   char *homehost, int *uuid)
{
	/* This is primarily called by Create when creating a new array.
	 * We will then get add_to_super called for each component, and then
	 * write_init_super called to write it out to each device.
	 * For IMSM, Create can create on fresh devices or on a pre-existing
	 * array.
	 * To create on a pre-existing array a different method will be called.
	 * This one is just for fresh drives.
	 */
	struct intel_super *super;
	struct imsm_super *mpb;
	size_t mpb_size;

	if (!info) {
		st->sb = NULL;
		return 0;
	}
	if (st->sb)
		return init_super_imsm_volume(st, info, size, name, homehost,
					      uuid);

	super = alloc_super(1);
	if (!super)
		return 0;
	mpb_size = disks_to_mpb_size(info->nr_disks);
	if (posix_memalign((void**)&mpb, 512, mpb_size) != 0) {
		free(super);
		return 0;
	}
	memset(mpb, 0, mpb_size); 

	memcpy(mpb->sig, MPB_SIGNATURE, strlen(MPB_SIGNATURE));
	memcpy(mpb->sig + strlen(MPB_SIGNATURE), MPB_VERSION_RAID5,
	       strlen(MPB_VERSION_RAID5)); 
	mpb->mpb_size = mpb_size;

	super->mpb = mpb;
	st->sb = super;
	return 1;
}

static void add_to_super_imsm_volume(struct supertype *st, mdu_disk_info_t *dk,
				     int fd, char *devname)
{
	struct intel_super *super = st->sb;
	struct imsm_super *mpb = super->mpb;
	struct dl *dl;
	struct imsm_dev *dev;
	struct imsm_map *map;
	struct imsm_disk *disk;
	__u32 status;

	dev = get_imsm_dev(mpb, super->current_vol);
	map = &dev->vol.map[0];

	for (dl = super->disks; dl ; dl = dl->next)
		if (dl->major == dk->major &&
		    dl->minor == dk->minor)
			break;
	if (!dl || ! (dk->state & (1<<MD_DISK_SYNC)))
		return;

	map->disk_ord_tbl[dk->number] = __cpu_to_le32(dl->index);

	disk = get_imsm_disk(mpb, dl->index);
	status = CONFIGURED_DISK | USABLE_DISK;
	disk->status = __cpu_to_le32(status);
}

static void add_to_super_imsm(struct supertype *st, mdu_disk_info_t *dk,
			      int fd, char *devname)
{
	struct intel_super *super = st->sb;
	struct imsm_super *mpb = super->mpb;
	struct imsm_disk *disk;
	struct dl *dd;
	unsigned long long size;
	__u32 status, id;
	int rv;
	struct stat stb;

	if (super->current_vol >= 0) {
		add_to_super_imsm_volume(st, dk, fd, devname);
		return;
	}

	fstat(fd, &stb);
	dd = malloc(sizeof(*dd));
	if (!dd) {
		fprintf(stderr,
			Name ": malloc failed %s:%d.\n", __func__, __LINE__);
		abort();
	}
	memset(dd, 0, sizeof(*dd));
	dd->major = major(stb.st_rdev);
	dd->minor = minor(stb.st_rdev);
	dd->index = dk->number;
	dd->devname = devname ? strdup(devname) : NULL;
	dd->next = super->disks;
	dd->fd = fd;
	rv = imsm_read_serial(fd, devname, dd->serial);
	if (rv) {
		fprintf(stderr,
			Name ": failed to retrieve scsi serial "
			"using \'%s\' instead\n", devname);
		strcpy((char *) dd->serial, devname);
	}

	if (mpb->num_disks <= dk->number)
		mpb->num_disks = dk->number + 1;

	disk = get_imsm_disk(mpb, dk->number);
	get_dev_size(fd, NULL, &size);
	size /= 512;
	status = USABLE_DISK | SPARE_DISK;
	strcpy((char *) disk->serial, (char *) dd->serial);
	disk->total_blocks = __cpu_to_le32(size);
	disk->status = __cpu_to_le32(status);
	if (sysfs_disk_to_scsi_id(fd, &id) == 0)
		disk->scsi_id = __cpu_to_le32(id);
	else
		disk->scsi_id = __cpu_to_le32(0);

	/* update the family number if we are creating a container */
	if (super->creating_imsm)
		mpb->family_num = __cpu_to_le32(gen_imsm_checksum(mpb));
	
	super->disks = dd;
}

static int store_imsm_mpb(int fd, struct intel_super *super);

static int write_super_imsm(struct intel_super *super, int doclose)
{
	struct imsm_super *mpb = super->mpb;
	struct dl *d;
	__u32 generation;
	__u32 sum;

	/* 'generation' is incremented everytime the metadata is written */
	generation = __le32_to_cpu(mpb->generation_num);
	generation++;
	mpb->generation_num = __cpu_to_le32(generation);

	/* recalculate checksum */
	sum = gen_imsm_checksum(mpb);
	mpb->check_sum = __cpu_to_le32(sum);

	for (d = super->disks; d ; d = d->next) {
		if (store_imsm_mpb(d->fd, super)) {
			fprintf(stderr, "%s: failed for device %d:%d %s\n",
				__func__, d->major, d->minor, strerror(errno));
			return 0;
		}
		if (doclose) {
			close(d->fd);
			d->fd = -1;
		}
	}

	return 1;
}

static int write_init_super_imsm(struct supertype *st)
{
	return write_super_imsm(st->sb, 1);
}

static int store_zero_imsm(struct supertype *st, int fd)
{
	unsigned long long dsize;
	void *buf;

	get_dev_size(fd, NULL, &dsize);

	/* first block is stored on second to last sector of the disk */
	if (lseek64(fd, dsize - (512 * 2), SEEK_SET) < 0)
		return 1;

	if (posix_memalign(&buf, 512, 512) != 0)
		return 1;

	memset(buf, 0, sizeof(buf));
	if (write(fd, buf, sizeof(buf)) != sizeof(buf))
		return 1;
	return 0;
}

static int validate_geometry_imsm_container(struct supertype *st, int level,
					    int layout, int raiddisks, int chunk,
					    unsigned long long size, char *dev,
					    unsigned long long *freesize,
					    int verbose)
{
	int fd;
	unsigned long long ldsize;

	if (level != LEVEL_CONTAINER)
		return 0;
	if (!dev)
		return 1;

	fd = open(dev, O_RDONLY|O_EXCL, 0);
	if (fd < 0) {
		if (verbose)
			fprintf(stderr, Name ": imsm: Cannot open %s: %s\n",
				dev, strerror(errno));
		return 0;
	}
	if (!get_dev_size(fd, dev, &ldsize)) {
		close(fd);
		return 0;
	}
	close(fd);

	*freesize = avail_size_imsm(st, ldsize >> 9);

	return 1;
}

/* validate_geometry_imsm_volume - lifted from validate_geometry_ddf_bvd 
 * FIX ME add ahci details
 */
static int validate_geometry_imsm_volume(struct supertype *st, int level,
					 int layout, int raiddisks, int chunk,
					 unsigned long long size, char *dev,
					 unsigned long long *freesize,
					 int verbose)
{
	struct stat stb;
	struct intel_super *super = st->sb;
	struct dl *dl;
	unsigned long long pos = 0;
	unsigned long long maxsize;
	struct extent *e;
	int i;

	if (level == LEVEL_CONTAINER)
		return 0;

	if (level == 1 && raiddisks > 2) {
		if (verbose)
			fprintf(stderr, Name ": imsm does not support more "
				"than 2 in a raid1 configuration\n");
		return 0;
	}

	/* We must have the container info already read in. */
	if (!super)
		return 0;

	if (!dev) {
		/* General test:  make sure there is space for
		 * 'raiddisks' device extents of size 'size'.
		 */
		unsigned long long minsize = size*2 /* convert to blocks */;
		int dcnt = 0;
		if (minsize == 0)
			minsize = MPB_SECTOR_CNT + IMSM_RESERVED_SECTORS;
		for (dl = super->disks; dl ; dl = dl->next) {
			int found = 0;

			pos = 0;
			i = 0;
			e = get_extents(super, dl);
			if (!e) continue;
			do {
				unsigned long long esize;
				esize = e[i].start - pos;
				if (esize >= minsize)
					found = 1;
				pos = e[i].start + e[i].size;
				i++;
			} while (e[i-1].size);
			if (found)
				dcnt++;
			free(e);
		}
		if (dcnt < raiddisks) {
			if (verbose)
				fprintf(stderr, Name ": imsm: Not enough "
					"devices with space for this array "
					"(%d < %d)\n",
					dcnt, raiddisks);
			return 0;
		}
		return 1;
	}
	/* This device must be a member of the set */
	if (stat(dev, &stb) < 0)
		return 0;
	if ((S_IFMT & stb.st_mode) != S_IFBLK)
		return 0;
	for (dl = super->disks ; dl ; dl = dl->next) {
		if (dl->major == major(stb.st_rdev) &&
		    dl->minor == minor(stb.st_rdev))
			break;
	}
	if (!dl) {
		if (verbose)
			fprintf(stderr, Name ": %s is not in the "
				"same imsm set\n", dev);
		return 0;
	}
	e = get_extents(super, dl);
	maxsize = 0;
	i = 0;
	if (e) do {
		unsigned long long esize;
		esize = e[i].start - pos;
		if (esize >= maxsize)
			maxsize = esize;
		pos = e[i].start + e[i].size;
		i++;
	} while (e[i-1].size);
	*freesize = maxsize;

	return 1;
}

static int validate_geometry_imsm(struct supertype *st, int level, int layout,
				  int raiddisks, int chunk, unsigned long long size,
				  char *dev, unsigned long long *freesize,
				  int verbose)
{
	int fd, cfd;
	struct mdinfo *sra;

	/* if given unused devices create a container 
	 * if given given devices in a container create a member volume
	 */
	if (level == LEVEL_CONTAINER) {
		/* Must be a fresh device to add to a container */
		return validate_geometry_imsm_container(st, level, layout,
							raiddisks, chunk, size,
							dev, freesize,
							verbose);
	}
	
	if (st->sb) {
		/* creating in a given container */
		return validate_geometry_imsm_volume(st, level, layout,
						     raiddisks, chunk, size,
						     dev, freesize, verbose);
	}

	/* limit creation to the following levels */
	if (!dev)
		switch (level) {
		case 0:
		case 1:
		case 10:
		case 5:
			break;
		default:
			return 1;
		}

	/* This device needs to be a device in an 'imsm' container */
	fd = open(dev, O_RDONLY|O_EXCL, 0);
	if (fd >= 0) {
		if (verbose)
			fprintf(stderr,
				Name ": Cannot create this array on device %s\n",
				dev);
		close(fd);
		return 0;
	}
	if (errno != EBUSY || (fd = open(dev, O_RDONLY, 0)) < 0) {
		if (verbose)
			fprintf(stderr, Name ": Cannot open %s: %s\n",
				dev, strerror(errno));
		return 0;
	}
	/* Well, it is in use by someone, maybe an 'imsm' container. */
	cfd = open_container(fd);
	if (cfd < 0) {
		close(fd);
		if (verbose)
			fprintf(stderr, Name ": Cannot use %s: It is busy\n",
				dev);
		return 0;
	}
	sra = sysfs_read(cfd, 0, GET_VERSION);
	close(fd);
	if (sra && sra->array.major_version == -1 &&
	    strcmp(sra->text_version, "imsm") == 0) {
		/* This is a member of a imsm container.  Load the container
		 * and try to create a volume
		 */
		struct intel_super *super;

		if (load_super_imsm_all(st, cfd, (void **) &super, NULL, 1) == 0) {
			st->sb = super;
			st->container_dev = fd2devnum(cfd);
			close(cfd);
			return validate_geometry_imsm_volume(st, level, layout,
							     raiddisks, chunk,
							     size, dev,
							     freesize, verbose);
		}
		close(cfd);
	} else /* may belong to another container */
		return 0;

	return 1;
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

		this->array.level = get_imsm_raid_level(map);
		this->array.raid_disks = map->num_members;
		this->array.layout = imsm_level_to_layout(this->array.level);
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

		sprintf(this->text_version, "/%s/%d",
			devnum2devname(st->container_dev),
			this->container_member);

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


static int imsm_open_new(struct supertype *c, struct active_array *a,
			 char *inst)
{
	dprintf("imsm: open_new %s\n", inst);
	a->info.container_member = atoi(inst);
	return 0;
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

static void imsm_set_array_state(struct active_array *a, int consistent)
{
	int inst = a->info.container_member;
	struct intel_super *super = a->container->sb;
	struct imsm_dev *dev = get_imsm_dev(super->mpb, inst);
	struct imsm_map *map = &dev->vol.map[0];
	int dirty = !consistent;
	int failed;
	__u8 map_state;

	if (a->resync_start == ~0ULL) {
		failed = imsm_count_failed(super->mpb, map);
		map_state = imsm_check_degraded(super->mpb, inst, failed);
		if (!failed)
			map_state = IMSM_T_STATE_NORMAL;
		if (map->map_state != map_state) {
			dprintf("imsm: map_state %d: %d\n",
				inst, map_state);
			map->map_state = map_state;
			super->updates_pending++;
		}
	}

	if (dev->vol.dirty != dirty) {
		dprintf("imsm: mark '%s' (%llu)\n",
			dirty?"dirty":"clean", a->resync_start);

		dev->vol.dirty = dirty;
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

	dprintf("imsm: set_disk %d:%x\n", n, state);

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

	if (mpb_size > 512) {
		/* -1 to account for anchor */
		sectors = mpb_sectors(mpb) - 1;

		/* write the extended mpb to the sectors preceeding the anchor */
		if (lseek64(fd, dsize - (512 * (2 + sectors)), SEEK_SET) < 0)
			return 1;

		if (write(fd, super->buf + 512, 512 * sectors) != 512 * sectors)
			return 1;
	}

	/* first block is stored on second to last sector of the disk */
	if (lseek64(fd, dsize - (512 * 2), SEEK_SET) < 0)
		return 1;

	if (write(fd, super->buf, 512) != 512)
		return 1;

	fsync(fd);

	return 0;
}

static void imsm_sync_metadata(struct supertype *container)
{
	struct intel_super *super = container->sb;

	if (!super->updates_pending)
		return;

	write_super_imsm(super, 0);

	super->updates_pending = 0;
}

struct superswitch super_imsm = {
#ifndef	MDASSEMBLE
	.examine_super	= examine_super_imsm,
	.brief_examine_super = brief_examine_super_imsm,
	.detail_super	= detail_super_imsm,
	.brief_detail_super = brief_detail_super_imsm,
	.write_init_super = write_init_super_imsm,
#endif
	.match_home	= match_home_imsm,
	.uuid_from_super= uuid_from_super_imsm,
	.getinfo_super  = getinfo_super_imsm,
	.update_super	= update_super_imsm,

	.avail_size	= avail_size_imsm,

	.compare_super	= compare_super_imsm,

	.load_super	= load_super_imsm,
	.init_super	= init_super_imsm,
	.add_to_super	= add_to_super_imsm,
	.store_super	= store_zero_imsm,
	.free_super	= free_super_imsm,
	.match_metadata_desc = match_metadata_desc_imsm,
	.container_content = container_content_imsm,

	.validate_geometry = validate_geometry_imsm,
	.external	= 1,

/* for mdmon */
	.open_new	= imsm_open_new,
	.load_super	= load_super_imsm,
	.set_array_state= imsm_set_array_state,
	.set_disk	= imsm_set_disk,
	.sync_metadata	= imsm_sync_metadata,
};
