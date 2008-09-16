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
#define IMSM_ORD_REBUILD (1 << 24)
	__u32 disk_ord_tbl[1];	/* disk_ord_tbl[num_members],
				 * top byte contains some flags
				 */
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
	__u32 error_log_size;		/* 0x30 - 0x33 in bytes */
	__u32 attributes;		/* 0x34 - 0x37 */
	__u8 num_disks;			/* 0x38 Number of configured disks */
	__u8 num_raid_devs;		/* 0x39 Number of configured volumes */
	__u8 error_log_pos;		/* 0x3A  */
	__u8 fill[1];			/* 0x3B */
	__u32 cache_size;		/* 0x3c - 0x40 in mb */
	__u32 orig_family_num;		/* 0x40 - 0x43 original family num */
	__u32 pwr_cycle_count;		/* 0x44 - 0x47 simulated power cycle count for array */
	__u32 bbm_log_size;		/* 0x48 - 0x4B - size of bad Block Mgmt Log in bytes */
#define IMSM_FILLERS 35
	__u32 filler[IMSM_FILLERS];	/* 0x4C - 0xD7 RAID_MPB_FILLERS */
	struct imsm_disk disk[1];	/* 0xD8 diskTbl[numDisks] */
	/* here comes imsm_dev[num_raid_devs] */
	/* here comes BBM logs */
} __attribute__ ((packed));

#define BBM_LOG_MAX_ENTRIES 254

struct bbm_log_entry {
	__u64 defective_block_start;
#define UNREADABLE 0xFFFFFFFF
	__u32 spare_block_offset;
	__u16 remapped_marked_count;
	__u16 disk_ordinal;
} __attribute__ ((__packed__));

struct bbm_log {
	__u32 signature; /* 0xABADB10C */
	__u32 entry_count;
	__u32 reserved_spare_block_count; /* 0 */
	__u32 reserved; /* 0xFFFF */
	__u64 first_spare_lba;
	struct bbm_log_entry mapped_block_entries[BBM_LOG_MAX_ENTRIES];
} __attribute__ ((__packed__));


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
		void *buf; /* O_DIRECT buffer for reading/writing metadata */
		struct imsm_super *anchor; /* immovable parameters */
	};
	size_t len; /* size of the 'buf' allocation */
	void *next_buf; /* for realloc'ing buf from the manager */
	size_t next_len;
	int updates_pending; /* count of pending updates for mdmon */
	int creating_imsm; /* flag to indicate container creation */
	int current_vol; /* index of raid device undergoing creation */
	#define IMSM_MAX_RAID_DEVS 2
	struct imsm_dev *dev_tbl[IMSM_MAX_RAID_DEVS];
	struct dl {
		struct dl *next;
		int index;
		__u8 serial[MAX_RAID_SERIAL_LEN];
		int major, minor;
		char *devname;
		struct imsm_disk disk;
		int fd;
	} *disks;
	struct dl *add; /* list of disks to add while mdmon active */
	struct bbm_log *bbm_log;
};

struct extent {
	unsigned long long start, size;
};

/* definition of messages passed to imsm_process_update */
enum imsm_update_type {
	update_activate_spare,
	update_create_array,
	update_add_disk,
};

struct imsm_update_activate_spare {
	enum imsm_update_type type;
	struct dl *dl;
	int slot;
	int array;
	struct imsm_update_activate_spare *next;
};

struct imsm_update_create_array {
	enum imsm_update_type type;
	int dev_idx;
	struct imsm_dev dev;
};

struct imsm_update_add_disk {
	enum imsm_update_type type;
};

static int imsm_env_devname_as_serial(void)
{
	char *val = getenv("IMSM_DEVNAME_AS_SERIAL");

	if (val && atoi(val) == 1)
		return 1;

	return 0;
}


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

/* retrieve a disk directly from the anchor when the anchor is known to be
 * up-to-date, currently only at load time
 */
static struct imsm_disk *__get_imsm_disk(struct imsm_super *mpb, __u8 index)
{
	if (index >= mpb->num_disks)
		return NULL;
	return &mpb->disk[index];
}

/* retrieve a disk from the parsed metadata */
static struct imsm_disk *get_imsm_disk(struct intel_super *super, __u8 index)
{
	struct dl *d;

	for (d = super->disks; d; d = d->next)
		if (d->index == index)
			return &d->disk;
	
	return NULL;
}

/* generate a checksum directly from the anchor when the anchor is known to be
 * up-to-date, currently only at load or write_super after coalescing
 */
static __u32 __gen_imsm_checksum(struct imsm_super *mpb)
{
	__u32 end = mpb->mpb_size / sizeof(end);
	__u32 *p = (__u32 *) mpb;
	__u32 sum = 0;

        while (end--)
                sum += __le32_to_cpu(*p++);

        return sum - __le32_to_cpu(mpb->check_sum);
}

static size_t sizeof_imsm_map(struct imsm_map *map)
{
	return sizeof(struct imsm_map) + sizeof(__u32) * (map->num_members - 1);
}

struct imsm_map *get_imsm_map(struct imsm_dev *dev, int second_map)
{
	struct imsm_map *map = &dev->vol.map[0];

	if (second_map && !dev->vol.migr_state)
		return NULL;
	else if (second_map) {
		void *ptr = map;

		return ptr + sizeof_imsm_map(map);
	} else
		return map;
		
}

/* return the size of the device.
 * migr_state increases the returned size if map[0] were to be duplicated
 */
static size_t sizeof_imsm_dev(struct imsm_dev *dev, int migr_state)
{
	size_t size = sizeof(*dev) - sizeof(struct imsm_map) +
		      sizeof_imsm_map(get_imsm_map(dev, 0));

	/* migrating means an additional map */
	if (dev->vol.migr_state)
		size += sizeof_imsm_map(get_imsm_map(dev, 1));
	else if (migr_state)
		size += sizeof_imsm_map(get_imsm_map(dev, 0));

	return size;
}

static struct imsm_dev *__get_imsm_dev(struct imsm_super *mpb, __u8 index)
{
	int offset;
	int i;
	void *_mpb = mpb;

	if (index >= mpb->num_raid_devs)
		return NULL;

	/* devices start after all disks */
	offset = ((void *) &mpb->disk[mpb->num_disks]) - _mpb;

	for (i = 0; i <= index; i++)
		if (i == index)
			return _mpb + offset;
		else
			offset += sizeof_imsm_dev(_mpb + offset, 0);

	return NULL;
}

static struct imsm_dev *get_imsm_dev(struct intel_super *super, __u8 index)
{
	if (index >= super->anchor->num_raid_devs)
		return NULL;
	return super->dev_tbl[index];
}

static __u32 get_imsm_disk_idx(struct imsm_map *map, int slot)
{
	__u32 *ord_tbl = &map->disk_ord_tbl[slot];

	/* top byte identifies disk under rebuild
	 * why not just use the USABLE bit... oh well.
	 */
	return __le32_to_cpu(*ord_tbl & ~(0xff << 24));
}

static __u32 get_imsm_ord_tbl_ent(struct imsm_dev *dev, int slot)
{
	struct imsm_map *map;

	if (dev->vol.migr_state)
		map = get_imsm_map(dev, 1);
	else
		map = get_imsm_map(dev, 0);

	return map->disk_ord_tbl[slot];
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
	struct extent *rv, *e;
	int i, j;
	int memberships = 0;

	for (i = 0; i < super->anchor->num_raid_devs; i++) {
		struct imsm_dev *dev = get_imsm_dev(super, i);
		struct imsm_map *map = get_imsm_map(dev, 0);

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

	for (i = 0; i < super->anchor->num_raid_devs; i++) {
		struct imsm_dev *dev = get_imsm_dev(super, i);
		struct imsm_map *map = get_imsm_map(dev, 0);

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

	e->start = __le32_to_cpu(dl->disk.total_blocks) -
		   (MPB_SECTOR_CNT + IMSM_RESERVED_SECTORS);
	e->size = 0;
	return rv;
}

#ifndef MDASSEMBLE
static void print_imsm_dev(struct imsm_dev *dev, int index)
{
	__u64 sz;
	int slot;
	struct imsm_map *map = get_imsm_map(dev, 0);

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
	printf("  Migrate State : %s", dev->vol.migr_state ? "migrating" : "idle");
	if (dev->vol.migr_state)
		printf(": %s", dev->vol.migr_type ? "rebuilding" : "initializing");
	printf("\n");
	printf("      Map State : %s", map_state_str[map->map_state]);
	if (dev->vol.migr_state) {
		struct imsm_map *map = get_imsm_map(dev, 1);
		printf(", %s", map_state_str[map->map_state]);
	}
	printf("\n");
	printf("    Dirty State : %s\n", dev->vol.dirty ? "dirty" : "clean");
}

static void print_imsm_disk(struct imsm_super *mpb, int index)
{
	struct imsm_disk *disk = __get_imsm_disk(mpb, index);
	char str[MAX_RAID_SERIAL_LEN];
	__u32 s;
	__u64 sz;

	if (index < 0)
		return;

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
	struct imsm_super *mpb = super->anchor;
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
		__gen_imsm_checksum(mpb) == sum ? "correct" : "incorrect");
	printf("    MPB Sectors : %d\n", mpb_sectors(mpb));
	printf("          Disks : %d\n", mpb->num_disks);
	printf("   RAID Devices : %d\n", mpb->num_raid_devs);
	print_imsm_disk(mpb, super->disks->index);
	if (super->bbm_log) {
		struct bbm_log *log = super->bbm_log;

		printf("\n");
		printf("Bad Block Management Log:\n");
		printf("       Log Size : %d\n", __le32_to_cpu(mpb->bbm_log_size));
		printf("      Signature : %x\n", __le32_to_cpu(log->signature));
		printf("    Entry Count : %d\n", __le32_to_cpu(log->entry_count));
		printf("   Spare Blocks : %d\n",  __le32_to_cpu(log->reserved_spare_block_count));
		printf("    First Spare : %llx\n", __le64_to_cpu(log->first_spare_lba));
	}
	for (i = 0; i < mpb->num_raid_devs; i++)
		print_imsm_dev(__get_imsm_dev(mpb, i), super->disks->index);
	for (i = 0; i < mpb->num_disks; i++) {
		if (i == super->disks->index)
			continue;
		print_imsm_disk(mpb, i);
	}
}

static void brief_examine_super_imsm(struct supertype *st)
{
	printf("ARRAY /dev/imsm metadata=imsm\n");
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
	/* imsm does not track uuid's so just make sure we never return
	 * the same value twice to break uuid matching in Manage_subdevs
	 * FIXME what about the use of uuid's with bitmap's?
	 */
	static int dummy_id = 0;

	uuid[0] = dummy_id++;
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
		return ALGORITHM_LEFT_ASYMMETRIC;
	case 10:
		return 0x102; //FIXME is this correct?
	}
	return -1;
}

static void getinfo_super_imsm_volume(struct supertype *st, struct mdinfo *info)
{
	struct intel_super *super = st->sb;
	struct imsm_dev *dev = get_imsm_dev(super, super->current_vol);
	struct imsm_map *map = get_imsm_map(dev, 0);

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
	struct imsm_disk *disk;
	__u32 s;

	if (super->current_vol >= 0) {
		getinfo_super_imsm_volume(st, info);
		return;
	}

	/* Set raid_disks to zero so that Assemble will always pull in valid
	 * spares
	 */
	info->array.raid_disks    = 0;
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
		disk = &super->disks->disk;
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

	if (memcmp(first->anchor->sig, sec->anchor->sig, MAX_SIGNATURE_LENGTH) != 0)
		return 3;

	/* if an anchor does not have num_raid_devs set then it is a free
	 * floating spare
	 */
	if (first->anchor->num_raid_devs > 0 &&
	    sec->anchor->num_raid_devs > 0) {
		if (first->anchor->family_num != sec->anchor->family_num)
			return 3;
	}

	return 0;
}

static void fd2devname(int fd, char *name)
{
	struct stat st;
	char path[256];
	char dname[100];
	char *nm;
	int rv;

	name[0] = '\0';
	if (fstat(fd, &st) != 0)
		return;
	sprintf(path, "/sys/dev/block/%d:%d",
		major(st.st_rdev), minor(st.st_rdev));

	rv = readlink(path, dname, sizeof(dname));
	if (rv <= 0)
		return;
	
	dname[rv] = '\0';
	nm = strrchr(dname, '/');
	nm++;
	snprintf(name, MAX_RAID_SERIAL_LEN, "/dev/%s", nm);
}


extern int scsi_get_serial(int fd, void *buf, size_t buf_len);

static int imsm_read_serial(int fd, char *devname,
			    __u8 serial[MAX_RAID_SERIAL_LEN])
{
	unsigned char scsi_serial[255];
	int rv;
	int rsp_len;
	int i, cnt;

	memset(scsi_serial, 0, sizeof(scsi_serial));

	if (imsm_env_devname_as_serial()) {
		char name[MAX_RAID_SERIAL_LEN];
		
		fd2devname(fd, name);
		strcpy((char *) serial, name);
		return 0;
	}

	rv = scsi_get_serial(fd, scsi_serial, sizeof(scsi_serial));

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
	struct dl *dl;
	struct stat stb;
	int rv;
	int i;
	int alloc = 1;
	__u8 serial[MAX_RAID_SERIAL_LEN];

	rv = imsm_read_serial(fd, devname, serial);

	if (rv != 0)
		return 2;

	/* check if this is a disk we have seen before.  it may be a spare in
	 * super->disks while the current anchor believes it is a raid member,
	 * check if we need to update dl->index
	 */
	for (dl = super->disks; dl; dl = dl->next)
		if (memcmp(dl->serial, serial, MAX_RAID_SERIAL_LEN) == 0)
			break;

	if (!dl)
		dl = malloc(sizeof(*dl));
	else
		alloc = 0;

	if (!dl) {
		if (devname)
			fprintf(stderr,
				Name ": failed to allocate disk buffer for %s\n",
				devname);
		return 2;
	}

	if (alloc) {
		fstat(fd, &stb);
		dl->major = major(stb.st_rdev);
		dl->minor = minor(stb.st_rdev);
		dl->next = super->disks;
		dl->fd = keep_fd ? fd : -1;
		dl->devname = devname ? strdup(devname) : NULL;
		strncpy((char *) dl->serial, (char *) serial, MAX_RAID_SERIAL_LEN);
		dl->index = -2;
	} else if (keep_fd) {
		close(dl->fd);
		dl->fd = fd;
	}

	/* look up this disk's index in the current anchor */
	for (i = 0; i < super->anchor->num_disks; i++) {
		struct imsm_disk *disk_iter;

		disk_iter = __get_imsm_disk(super->anchor, i);

		if (memcmp(disk_iter->serial, dl->serial,
			   MAX_RAID_SERIAL_LEN) == 0) {
			__u32 status;

			dl->disk = *disk_iter;
			status = __le32_to_cpu(dl->disk.status);
			/* only set index on disks that are a member of a
			 * populated contianer, i.e. one with raid_devs
			 */
			if (status & FAILED_DISK)
				dl->index = -2;
			else if (status & SPARE_DISK)
				dl->index = -1;
			else
				dl->index = i;

			break;
		}
	}

	if (alloc)
		super->disks = dl;

	return 0;
}

static void imsm_copy_dev(struct imsm_dev *dest, struct imsm_dev *src)
{
	memcpy(dest, src, sizeof_imsm_dev(src, 0));
}

static void dup_map(struct imsm_dev *dev)
{
	struct imsm_map *dest = get_imsm_map(dev, 1);
	struct imsm_map *src = get_imsm_map(dev, 0);

	memcpy(dest, src, sizeof_imsm_map(src));
}

static int parse_raid_devices(struct intel_super *super)
{
	int i;
	struct imsm_dev *dev_new;
	size_t len, len_migr;
	size_t space_needed = 0;
	struct imsm_super *mpb = super->anchor;

	for (i = 0; i < super->anchor->num_raid_devs; i++) {
		struct imsm_dev *dev_iter = __get_imsm_dev(super->anchor, i);

		len = sizeof_imsm_dev(dev_iter, 0);
		len_migr = sizeof_imsm_dev(dev_iter, 1);
		if (len_migr > len)
			space_needed += len_migr - len;
		
		dev_new = malloc(len_migr);
		if (!dev_new)
			return 1;
		imsm_copy_dev(dev_new, dev_iter);
		super->dev_tbl[i] = dev_new;
	}

	/* ensure that super->buf is large enough when all raid devices
	 * are migrating
	 */
	if (__le32_to_cpu(mpb->mpb_size) + space_needed > super->len) {
		void *buf;

		len = ROUND_UP(__le32_to_cpu(mpb->mpb_size) + space_needed, 512);
		if (posix_memalign(&buf, 512, len) != 0)
			return 1;

		memcpy(buf, super->buf, len);
		free(super->buf);
		super->buf = buf;
		super->len = len;
	}
		
	return 0;
}

/* retrieve a pointer to the bbm log which starts after all raid devices */
struct bbm_log *__get_imsm_bbm_log(struct imsm_super *mpb)
{
	void *ptr = NULL;

	if (__le32_to_cpu(mpb->bbm_log_size)) {
		ptr = mpb;
		ptr += mpb->mpb_size - __le32_to_cpu(mpb->bbm_log_size);
	} 

	return ptr;
}

static void __free_imsm(struct intel_super *super, int free_disks);

/* load_imsm_mpb - read matrix metadata
 * allocates super->mpb to be freed by free_super
 */
static int load_imsm_mpb(int fd, struct intel_super *super, char *devname)
{
	unsigned long long dsize;
	unsigned long long sectors;
	struct stat;
	struct imsm_super *anchor;
	__u32 check_sum;
	int rc;

	get_dev_size(fd, NULL, &dsize);

	if (lseek64(fd, dsize - (512 * 2), SEEK_SET) < 0) {
		if (devname)
			fprintf(stderr,
				Name ": Cannot seek to anchor block on %s: %s\n",
				devname, strerror(errno));
		return 1;
	}

	if (posix_memalign((void**)&anchor, 512, 512) != 0) {
		if (devname)
			fprintf(stderr,
				Name ": Failed to allocate imsm anchor buffer"
				" on %s\n", devname);
		return 1;
	}
	if (read(fd, anchor, 512) != 512) {
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

	__free_imsm(super, 0);
	super->len = ROUND_UP(anchor->mpb_size, 512);
	if (posix_memalign(&super->buf, 512, super->len) != 0) {
		if (devname)
			fprintf(stderr,
				Name ": unable to allocate %zu byte mpb buffer\n",
				super->len);
		free(anchor);
		return 2;
	}
	memcpy(super->buf, anchor, 512);

	sectors = mpb_sectors(anchor) - 1;
	free(anchor);
	if (!sectors) {
		rc = load_imsm_disk(fd, super, devname, 0);
		if (rc == 0)
			rc = parse_raid_devices(super);
		return rc;
	}

	/* read the extended mpb */
	if (lseek64(fd, dsize - (512 * (2 + sectors)), SEEK_SET) < 0) {
		if (devname)
			fprintf(stderr,
				Name ": Cannot seek to extended mpb on %s: %s\n",
				devname, strerror(errno));
		return 1;
	}

	if (read(fd, super->buf + 512, super->len - 512) != super->len - 512) {
		if (devname)
			fprintf(stderr,
				Name ": Cannot read extended mpb on %s: %s\n",
				devname, strerror(errno));
		return 2;
	}

	check_sum = __gen_imsm_checksum(super->anchor);
	if (check_sum != __le32_to_cpu(super->anchor->check_sum)) {
		if (devname)
			fprintf(stderr,
				Name ": IMSM checksum %x != %x on %s\n",
				check_sum, __le32_to_cpu(super->anchor->check_sum),
				devname);
		return 2;
	}

	/* FIXME the BBM log is disk specific so we cannot use this global
	 * buffer for all disks.  Ok for now since we only look at the global
	 * bbm_log_size parameter to gate assembly
	 */
	super->bbm_log = __get_imsm_bbm_log(super->anchor);

	rc = load_imsm_disk(fd, super, devname, 0);
	if (rc == 0)
		rc = parse_raid_devices(super);

	return rc;
}

static void __free_imsm_disk(struct dl *d)
{
	if (d->fd >= 0)
		close(d->fd);
	if (d->devname)
		free(d->devname);
	free(d);

}
static void free_imsm_disks(struct intel_super *super)
{
	while (super->disks) {
		struct dl *d = super->disks;

		super->disks = d->next;
		__free_imsm_disk(d);
	}
}

/* free all the pieces hanging off of a super pointer */
static void __free_imsm(struct intel_super *super, int free_disks)
{
	int i;

	if (super->buf) {
		free(super->buf);
		super->buf = NULL;
	}
	if (free_disks)
		free_imsm_disks(super);
	for (i = 0; i < IMSM_MAX_RAID_DEVS; i++)
		if (super->dev_tbl[i]) {
			free(super->dev_tbl[i]);
			super->dev_tbl[i] = NULL;
		}
}

static void free_imsm(struct intel_super *super)
{
	__free_imsm(super, 1);
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

	/* find the most up to date disk in this array, skipping spares */
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
			if (super->anchor->num_raid_devs == 0)
				gen = 0;
			else
				gen = __le32_to_cpu(super->anchor->generation_num);
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

	/* re-parse the disk list with the current anchor */
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
		if (atoi(st->subarray) <= super->anchor->num_raid_devs)
			super->current_vol = atoi(st->subarray);
		else
			return 1;
	}

	*sbp = super;
	st->container_dev = fd2devnum(fd);
	if (st->ss == NULL) {
		st->ss = &super_imsm;
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

static __u16 info_to_blocks_per_strip(mdu_array_info_t *info)
{
	if (info->level == 1)
		return 128;
	return info->chunk_size >> 9;
}

static __u32 info_to_num_data_stripes(mdu_array_info_t *info)
{
	__u32 num_stripes;

	num_stripes = (info->size * 2) / info_to_blocks_per_strip(info);
	if (info->level == 1)
		num_stripes /= 2;

	return num_stripes;
}

static __u32 info_to_blocks_per_member(mdu_array_info_t *info)
{
	return (info->size * 2) & ~(info_to_blocks_per_strip(info) - 1);
}

static int init_super_imsm_volume(struct supertype *st, mdu_array_info_t *info,
				  unsigned long long size, char *name,
				  char *homehost, int *uuid)
{
	/* We are creating a volume inside a pre-existing container.
	 * so st->sb is already set.
	 */
	struct intel_super *super = st->sb;
	struct imsm_super *mpb = super->anchor;
	struct imsm_dev *dev;
	struct imsm_vol *vol;
	struct imsm_map *map;
	int idx = mpb->num_raid_devs;
	int i;
	unsigned long long array_blocks;
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
		super->anchor = mpb_new;
		mpb->mpb_size = __cpu_to_le32(size_new);
		memset(mpb_new + size_old, 0, size_round - size_old);
	}
	super->current_vol = idx;
	/* when creating the first raid device in this container set num_disks
	 * to zero, i.e. delete this spare and add raid member devices in
	 * add_to_super_imsm_volume()
	 */
	if (super->current_vol == 0)
		mpb->num_disks = 0;
	sprintf(st->subarray, "%d", idx);
	dev = malloc(sizeof(*dev) + sizeof(__u32) * (info->raid_disks - 1));
	if (!dev) {
		fprintf(stderr, Name": could not allocate raid device\n");
		return 0;
	}
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
		struct imsm_dev *prev = get_imsm_dev(super, i);
		struct imsm_map *pmap = get_imsm_map(prev, 0);

		offset += __le32_to_cpu(pmap->blocks_per_member);
		offset += IMSM_RESERVED_SECTORS;
	}
	map = get_imsm_map(dev, 0);
	map->pba_of_lba0 = __cpu_to_le32(offset);
	map->blocks_per_member = __cpu_to_le32(info_to_blocks_per_member(info));
	map->blocks_per_strip = __cpu_to_le16(info_to_blocks_per_strip(info));
	map->num_data_stripes = __cpu_to_le32(info_to_num_data_stripes(info));
	map->map_state = info->level ? IMSM_T_STATE_UNINITIALIZED :
				       IMSM_T_STATE_NORMAL;

	if (info->level == 1 && info->raid_disks > 2) {
		fprintf(stderr, Name": imsm does not support more than 2 disks"
				"in a raid1 volume\n");
		return 0;
	}
	if (info->level == 10)
		map->raid_level = 1;
	else
		map->raid_level = info->level;

	map->num_members = info->raid_disks;
	for (i = 0; i < map->num_members; i++) {
		/* initialized in add_to_super */
		map->disk_ord_tbl[i] = __cpu_to_le32(0);
	}
	mpb->num_raid_devs++;
	super->dev_tbl[super->current_vol] = dev;

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
	if (posix_memalign(&super->buf, 512, mpb_size) != 0) {
		free(super);
		return 0;
	}
	mpb = super->buf;
	memset(mpb, 0, mpb_size); 

	memcpy(mpb->sig, MPB_SIGNATURE, strlen(MPB_SIGNATURE));
	memcpy(mpb->sig + strlen(MPB_SIGNATURE), MPB_VERSION_RAID5,
	       strlen(MPB_VERSION_RAID5)); 
	mpb->mpb_size = mpb_size;

	st->sb = super;
	return 1;
}

static void add_to_super_imsm_volume(struct supertype *st, mdu_disk_info_t *dk,
				     int fd, char *devname)
{
	struct intel_super *super = st->sb;
	struct imsm_super *mpb = super->anchor;
	struct dl *dl;
	struct imsm_dev *dev;
	struct imsm_map *map;
	__u32 status;

	dev = get_imsm_dev(super, super->current_vol);
	map = get_imsm_map(dev, 0);

	for (dl = super->disks; dl ; dl = dl->next)
		if (dl->major == dk->major &&
		    dl->minor == dk->minor)
			break;

	if (!dl || ! (dk->state & (1<<MD_DISK_SYNC)))
		return;

	/* add a pristine spare to the metadata */
	if (dl->index < 0) {
		dl->index = super->anchor->num_disks;
		super->anchor->num_disks++;
	}
	map->disk_ord_tbl[dk->number] = __cpu_to_le32(dl->index);
	status = CONFIGURED_DISK | USABLE_DISK;
	dl->disk.status = __cpu_to_le32(status);

	/* if we are creating the first raid device update the family number */
	if (super->current_vol == 0) {
		__u32 sum;
		struct imsm_dev *_dev = __get_imsm_dev(mpb, 0);
		struct imsm_disk *_disk = __get_imsm_disk(mpb, dl->index);

		*_dev = *dev;
		*_disk = dl->disk;
		sum = __gen_imsm_checksum(mpb);
		mpb->family_num = __cpu_to_le32(sum);
	}
}

static void add_to_super_imsm(struct supertype *st, mdu_disk_info_t *dk,
			      int fd, char *devname)
{
	struct intel_super *super = st->sb;
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
	dd->index = -1;
	dd->devname = devname ? strdup(devname) : NULL;
	dd->fd = fd;
	rv = imsm_read_serial(fd, devname, dd->serial);
	if (rv) {
		fprintf(stderr,
			Name ": failed to retrieve scsi serial, aborting\n");
		free(dd);
		abort();
	}

	get_dev_size(fd, NULL, &size);
	size /= 512;
	status = USABLE_DISK | SPARE_DISK;
	strcpy((char *) dd->disk.serial, (char *) dd->serial);
	dd->disk.total_blocks = __cpu_to_le32(size);
	dd->disk.status = __cpu_to_le32(status);
	if (sysfs_disk_to_scsi_id(fd, &id) == 0)
		dd->disk.scsi_id = __cpu_to_le32(id);
	else
		dd->disk.scsi_id = __cpu_to_le32(0);

	if (st->update_tail) {
		dd->next = super->add;
		super->add = dd;
	} else {
		dd->next = super->disks;
		super->disks = dd;
	}
}

static int store_imsm_mpb(int fd, struct intel_super *super);

/* spare records have their own family number and do not have any defined raid
 * devices
 */
static int write_super_imsm_spares(struct intel_super *super, int doclose)
{
	struct imsm_super mpb_save;
	struct imsm_super *mpb = super->anchor;
	__u32 sum;
	struct dl *d;

	mpb_save = *mpb;
	mpb->num_raid_devs = 0;
	mpb->num_disks = 1;
	mpb->mpb_size = sizeof(struct imsm_super);
	mpb->generation_num = __cpu_to_le32(1UL);

	for (d = super->disks; d; d = d->next) {
		if (d->index != -1)
			continue;

		mpb->disk[0] = d->disk;
		sum = __gen_imsm_checksum(mpb);
		mpb->family_num = __cpu_to_le32(sum);
		sum = __gen_imsm_checksum(mpb);
		mpb->check_sum = __cpu_to_le32(sum);

		if (store_imsm_mpb(d->fd, super)) {
			fprintf(stderr, "%s: failed for device %d:%d %s\n",
				__func__, d->major, d->minor, strerror(errno));
			*mpb = mpb_save;
			return 1;
		}
		if (doclose) {
			close(d->fd);
			d->fd = -1;
		}
	}

	*mpb = mpb_save;
	return 0;
}

static int write_super_imsm(struct intel_super *super, int doclose)
{
	struct imsm_super *mpb = super->anchor;
	struct dl *d;
	__u32 generation;
	__u32 sum;
	int spares = 0;
	int i;
	__u32 mpb_size = sizeof(struct imsm_super) - sizeof(struct imsm_disk);

	/* 'generation' is incremented everytime the metadata is written */
	generation = __le32_to_cpu(mpb->generation_num);
	generation++;
	mpb->generation_num = __cpu_to_le32(generation);

	for (d = super->disks; d; d = d->next) {
		if (d->index == -1)
			spares++;
		else {
			mpb->disk[d->index] = d->disk;
			mpb_size += sizeof(struct imsm_disk);
		}
	}

	for (i = 0; i < mpb->num_raid_devs; i++) {
		struct imsm_dev *dev = __get_imsm_dev(mpb, i);

		imsm_copy_dev(dev, super->dev_tbl[i]);
		mpb_size += sizeof_imsm_dev(dev, 0);
	}
	mpb_size += __le32_to_cpu(mpb->bbm_log_size);
	mpb->mpb_size = __cpu_to_le32(mpb_size);

	/* recalculate checksum */
	sum = __gen_imsm_checksum(mpb);
	mpb->check_sum = __cpu_to_le32(sum);

	/* write the mpb for disks that compose raid devices */
	for (d = super->disks; d ; d = d->next) {
		if (d->index < 0)
			continue;
		if (store_imsm_mpb(d->fd, super))
			fprintf(stderr, "%s: failed for device %d:%d %s\n",
				__func__, d->major, d->minor, strerror(errno));
		if (doclose) {
			close(d->fd);
			d->fd = -1;
		}
	}

	if (spares)
		return write_super_imsm_spares(super, doclose);

	return 0;
}

static int create_array(struct supertype *st)
{
	size_t len;
	struct imsm_update_create_array *u;
	struct intel_super *super = st->sb;
	struct imsm_dev *dev = get_imsm_dev(super, super->current_vol);

	len = sizeof(*u) - sizeof(*dev) + sizeof_imsm_dev(dev, 0);
	u = malloc(len);
	if (!u) {
		fprintf(stderr, "%s: failed to allocate update buffer\n",
			__func__);
		return 1;
	}

	u->type = update_create_array;
	u->dev_idx = super->current_vol;
	imsm_copy_dev(&u->dev, dev);
	append_metadata_update(st, u, len);

	return 0;
}

static int add_disk(struct supertype *st)
{
	struct intel_super *super = st->sb;
	size_t len;
	struct imsm_update_add_disk *u;

	if (!super->add)
		return 0;

	len = sizeof(*u);
	u = malloc(len);
	if (!u) {
		fprintf(stderr, "%s: failed to allocate update buffer\n",
			__func__);
		return 1;
	}

	u->type = update_add_disk;
	append_metadata_update(st, u, len);

	return 0;
}

static int write_init_super_imsm(struct supertype *st)
{
	if (st->update_tail) {
		/* queue the recently created array / added disk
		 * as a metadata update */
		struct intel_super *super = st->sb;
		struct dl *d;
		int rv;

		/* determine if we are creating a volume or adding a disk */
		if (super->current_vol < 0) {
			/* in the add disk case we are running in mdmon
			 * context, so don't close fd's
			 */
			return add_disk(st);
		} else
			rv = create_array(st);

		for (d = super->disks; d ; d = d->next) {
			close(d->fd);
			d->fd = -1;
		}

		return rv;
	} else
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

	memset(buf, 0, 512);
	if (write(fd, buf, 512) != 512)
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
		 * 'raiddisks' device extents of size 'size' at a given
		 * offset
		 */
		unsigned long long minsize = size*2 /* convert to blocks */;
		unsigned long long start_offset = ~0ULL;
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
				if (found && start_offset == ~0ULL) {
					start_offset = pos;
					break;
				} else if (found && pos != start_offset) {
					found = 0;
					break;
				}
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

int imsm_bbm_log_size(struct imsm_super *mpb)
{
	return __le32_to_cpu(mpb->bbm_log_size);
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
	struct imsm_super *mpb = super->anchor;
	struct mdinfo *rest = NULL;
	int i;

	/* do not assemble arrays that might have bad blocks */
	if (imsm_bbm_log_size(super->anchor)) {
		fprintf(stderr, Name ": BBM log found in metadata. "
				"Cannot activate array(s).\n");
		return NULL;
	}

	for (i = 0; i < mpb->num_raid_devs; i++) {
		struct imsm_dev *dev = get_imsm_dev(super, i);
		struct imsm_vol *vol = &dev->vol;
		struct imsm_map *map = get_imsm_map(dev, 0);
		struct mdinfo *this;
		int slot;

		this = malloc(sizeof(*this));
		memset(this, 0, sizeof(*this));
		this->next = rest;

		this->array.level = get_imsm_raid_level(map);
		this->array.raid_disks = map->num_members;
		this->array.layout = imsm_level_to_layout(this->array.level);
		this->array.md_minor = -1;
		this->array.ctime = 0;
		this->array.utime = 0;
		this->array.chunk_size = __le16_to_cpu(map->blocks_per_strip) << 9;
		this->array.state = !vol->dirty;
		this->container_member = i;
		if (map->map_state == IMSM_T_STATE_UNINITIALIZED ||
		    dev->vol.dirty || dev->vol.migr_state)
			this->resync_start = 0;
		else
			this->resync_start = ~0ULL;

		strncpy(this->name, (char *) dev->volume, MAX_RAID_SERIAL_LEN);
		this->name[MAX_RAID_SERIAL_LEN] = 0;

		sprintf(this->text_version, "/%s/%d",
			devnum2devname(st->container_dev),
			this->container_member);

		memset(this->uuid, 0, sizeof(this->uuid));

		this->component_size = __le32_to_cpu(map->blocks_per_member);

		for (slot = 0 ; slot <  map->num_members; slot++) {
			struct mdinfo *info_d;
			struct dl *d;
			int idx;
			int skip;
			__u32 s;
			__u32 ord;

			skip = 0;
			idx = get_imsm_disk_idx(map, slot);
			ord = get_imsm_ord_tbl_ent(dev, slot); 
			for (d = super->disks; d ; d = d->next)
				if (d->index == idx)
                                        break;

			if (d == NULL)
				skip = 1;

			s = d ? __le32_to_cpu(d->disk.status) : 0;
			if (s & FAILED_DISK)
				skip = 1;
			if (!(s & USABLE_DISK))
				skip = 1;
			if (ord & IMSM_ORD_REBUILD)
				skip = 1;

			/* 
			 * if we skip some disks the array will be assmebled degraded;
			 * reset resync start to avoid a dirty-degraded situation
			 *
			 * FIXME handle dirty degraded
			 */
			if (skip && !dev->vol.dirty)
				this->resync_start = ~0ULL;
			if (skip)
				continue;

			info_d = malloc(sizeof(*info_d));
			if (!info_d) {
				fprintf(stderr, Name ": failed to allocate disk"
					" for volume %s\n", (char *) dev->volume);
				free(this);
				this = rest;
				break;
			}
			memset(info_d, 0, sizeof(*info_d));
			info_d->next = this->devs;
			this->devs = info_d;

			info_d->disk.number = d->index;
			info_d->disk.major = d->major;
			info_d->disk.minor = d->minor;
			info_d->disk.raid_disk = slot;

			this->array.working_disks++;

			info_d->events = __le32_to_cpu(mpb->generation_num);
			info_d->data_offset = __le32_to_cpu(map->pba_of_lba0);
			info_d->component_size = __le32_to_cpu(map->blocks_per_member);
			if (d->devname)
				strcpy(info_d->name, d->devname);
		}
		rest = this;
	}

	return rest;
}


static int imsm_open_new(struct supertype *c, struct active_array *a,
			 char *inst)
{
	struct intel_super *super = c->sb;
	struct imsm_super *mpb = super->anchor;
	
	if (atoi(inst) >= mpb->num_raid_devs) {
		fprintf(stderr, "%s: subarry index %d, out of range\n",
			__func__, atoi(inst));
		return -ENODEV;
	}

	dprintf("imsm: open_new %s\n", inst);
	a->info.container_member = atoi(inst);
	return 0;
}

static __u8 imsm_check_degraded(struct intel_super *super, int n, int failed)
{
	struct imsm_dev *dev = get_imsm_dev(super, n);
	struct imsm_map *map = get_imsm_map(dev, 0);

	if (!failed)
		return map->map_state == IMSM_T_STATE_UNINITIALIZED ? 
			IMSM_T_STATE_UNINITIALIZED : IMSM_T_STATE_NORMAL;

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
		int r10fail = 0;
		int i;

		for (i = 0; i < map->num_members; i++) {
			int idx = get_imsm_disk_idx(map, i);
			struct imsm_disk *disk = get_imsm_disk(super, idx);

			if (!disk)
				r10fail++;
			else if (__le32_to_cpu(disk->status) & FAILED_DISK)
				r10fail++;

			if (r10fail >= device_per_mirror)
				return IMSM_T_STATE_FAILED;

			/* reset 'r10fail' for next mirror set */
			if (!((i + 1) % device_per_mirror))
				r10fail = 0;
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

static int imsm_count_failed(struct intel_super *super, struct imsm_map *map)
{
	int i;
	int failed = 0;
	struct imsm_disk *disk;

	for (i = 0; i < map->num_members; i++) {
		int idx = get_imsm_disk_idx(map, i);

		disk = get_imsm_disk(super, idx);
		if (!disk)
			failed++;
		else if (__le32_to_cpu(disk->status) & FAILED_DISK)
			failed++;
		else if (!(__le32_to_cpu(disk->status) & USABLE_DISK))
			failed++;
	}

	return failed;
}

static int imsm_set_array_state(struct active_array *a, int consistent)
{
	int inst = a->info.container_member;
	struct intel_super *super = a->container->sb;
	struct imsm_dev *dev = get_imsm_dev(super, inst);
	struct imsm_map *map = get_imsm_map(dev, 0);
	int dirty = !consistent;
	int failed;
	__u8 map_state;

	failed = imsm_count_failed(super, map);
	map_state = imsm_check_degraded(super, inst, failed);

	if (consistent && !dev->vol.dirty &&
	    (dev->vol.migr_state || map_state != IMSM_T_STATE_NORMAL))
		a->resync_start = 0ULL;
	if (consistent == 2 && a->resync_start != ~0ULL)
		consistent = 0;

	if (a->resync_start == ~0ULL) {
		/* complete recovery or initial resync */
		if (map->map_state != map_state) {
			dprintf("imsm: map_state %d: %d\n",
				inst, map_state);
			map->map_state = map_state;
			super->updates_pending++;
		}
		if (dev->vol.migr_state) {
			dprintf("imsm: mark resync complete\n");
			dev->vol.migr_state = 0;
			dev->vol.migr_type = 0;
			super->updates_pending++;
		}
	} else if (!dev->vol.migr_state) {
		dprintf("imsm: mark '%s' (%llu)\n",
			failed ? "rebuild" : "initializing", a->resync_start);
		/* mark that we are rebuilding */
		map->map_state = failed ? map_state : IMSM_T_STATE_NORMAL;
		dev->vol.migr_state = 1;
		dev->vol.migr_type = failed ? 1 : 0;
		dup_map(dev);
		a->check_degraded = 1;
		super->updates_pending++;
	}

	/* mark dirty / clean */
	if (dirty != dev->vol.dirty) {
		dprintf("imsm: mark '%s' (%llu)\n",
			dirty ? "dirty" : "clean", a->resync_start);
		dev->vol.dirty = dirty;
		super->updates_pending++;
	}
	return consistent;
}

static void imsm_set_disk(struct active_array *a, int n, int state)
{
	int inst = a->info.container_member;
	struct intel_super *super = a->container->sb;
	struct imsm_dev *dev = get_imsm_dev(super, inst);
	struct imsm_map *map = get_imsm_map(dev, 0);
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

	disk = get_imsm_disk(super, get_imsm_disk_idx(map, n));

	/* check for new failures */
	status = __le32_to_cpu(disk->status);
	if ((state & DS_FAULTY) && !(status & FAILED_DISK)) {
		status |= FAILED_DISK;
		disk->status = __cpu_to_le32(status);
		disk->scsi_id = __cpu_to_le32(~0UL);
		memmove(&disk->serial[0], &disk->serial[1], MAX_RAID_SERIAL_LEN - 1);
		new_failure = 1;
		super->updates_pending++;
	}
	/* check if in_sync */
	if ((state & DS_INSYNC) && !(status & USABLE_DISK)) {
		status |= USABLE_DISK;
		disk->status = __cpu_to_le32(status);
		super->updates_pending++;
	}

	/* the number of failures have changed, count up 'failed' to determine
	 * degraded / failed status
	 */
	if (new_failure && map->map_state != IMSM_T_STATE_FAILED)
		failed = imsm_count_failed(super, map);

	/* determine map_state based on failed or in_sync count */
	if (failed)
		map->map_state = imsm_check_degraded(super, inst, failed);
	else if (map->map_state == IMSM_T_STATE_DEGRADED) {
		struct mdinfo *d;
		int working = 0;

		for (d = a->info.devs ; d ; d = d->next)
			if (d->curr_state & DS_INSYNC)
				working++;

		if (working == a->info.array.raid_disks) {
			map->map_state = IMSM_T_STATE_NORMAL;
			dev->vol.migr_state = 0;
			dev->vol.migr_type = 0;
			super->updates_pending++;
		}
	}
}

static int store_imsm_mpb(int fd, struct intel_super *super)
{
	struct imsm_super *mpb = super->anchor;
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

static struct dl *imsm_readd(struct intel_super *super, int idx, struct active_array *a)
{
	struct imsm_dev *dev = get_imsm_dev(super, a->info.container_member);
	struct imsm_map *map = get_imsm_map(dev, 0);
	int i = get_imsm_disk_idx(map, idx);
	struct dl *dl;

	for (dl = super->disks; dl; dl = dl->next)
		if (dl->index == i)
			break;

	if (dl && __le32_to_cpu(dl->disk.status) & FAILED_DISK)
		dl = NULL;

	if (dl)
		dprintf("%s: found %x:%x\n", __func__, dl->major, dl->minor);

	return dl;
}

static struct dl *imsm_add_spare(struct intel_super *super, int idx, struct active_array *a)
{
	struct imsm_dev *dev = get_imsm_dev(super, a->info.container_member);
	struct imsm_map *map = get_imsm_map(dev, 0);
	unsigned long long esize;
	unsigned long long pos;
	struct mdinfo *d;
	struct extent *ex;
	int j;
	int found;
	__u32 array_start;
	__u32 status;
	struct dl *dl;

	for (dl = super->disks; dl; dl = dl->next) {
		/* If in this array, skip */
		for (d = a->info.devs ; d ; d = d->next)
			if (d->disk.major == dl->major &&
			    d->disk.minor == dl->minor) {
				dprintf("%x:%x already in array\n", dl->major, dl->minor);
				break;
			}
		if (d)
			continue;

		/* skip marked in use or failed drives */
		status = __le32_to_cpu(dl->disk.status);
		if (status & FAILED_DISK || status & CONFIGURED_DISK) {
			dprintf("%x:%x status ( %s%s)\n",
			dl->major, dl->minor,
			status & FAILED_DISK ? "failed " : "",
			status & CONFIGURED_DISK ? "configured " : "");
			continue;
		}

		/* Does this unused device have the requisite free space?
		 * We need a->info.component_size sectors
		 */
		ex = get_extents(super, dl);
		if (!ex) {
			dprintf("cannot get extents\n");
			continue;
		}
		found = 0;
		j = 0;
		pos = 0;
		array_start = __le32_to_cpu(map->pba_of_lba0);

		do {
			/* check that we can start at pba_of_lba0 with
			 * a->info.component_size of space
			 */
			esize = ex[j].start - pos;
			if (array_start >= pos &&
			    array_start + a->info.component_size < ex[j].start) {
				found = 1;
				break;
			}
			pos = ex[j].start + ex[j].size;
			j++;
			    
		} while (ex[j-1].size);

		free(ex);
		if (!found) {
			dprintf("%x:%x does not have %llu at %d\n",
				dl->major, dl->minor,
				a->info.component_size,
				__le32_to_cpu(map->pba_of_lba0));
			/* No room */
			continue;
		} else
			break;
	}

	return dl;
}

static struct mdinfo *imsm_activate_spare(struct active_array *a,
					  struct metadata_update **updates)
{
	/**
	 * Find a device with unused free space and use it to replace a
	 * failed/vacant region in an array.  We replace failed regions one a
	 * array at a time.  The result is that a new spare disk will be added
	 * to the first failed array and after the monitor has finished
	 * propagating failures the remainder will be consumed.
	 *
	 * FIXME add a capability for mdmon to request spares from another
	 * container.
	 */

	struct intel_super *super = a->container->sb;
	int inst = a->info.container_member;
	struct imsm_dev *dev = get_imsm_dev(super, inst);
	struct imsm_map *map = get_imsm_map(dev, 0);
	int failed = a->info.array.raid_disks;
	struct mdinfo *rv = NULL;
	struct mdinfo *d;
	struct mdinfo *di;
	struct metadata_update *mu;
	struct dl *dl;
	struct imsm_update_activate_spare *u;
	int num_spares = 0;
	int i;

	for (d = a->info.devs ; d ; d = d->next) {
		if ((d->curr_state & DS_FAULTY) &&
			d->state_fd >= 0)
			/* wait for Removal to happen */
			return NULL;
		if (d->state_fd >= 0)
			failed--;
	}

	dprintf("imsm: activate spare: inst=%d failed=%d (%d) level=%d\n",
		inst, failed, a->info.array.raid_disks, a->info.array.level);
	if (imsm_check_degraded(super, inst, failed) != IMSM_T_STATE_DEGRADED)
		return NULL;

	/* For each slot, if it is not working, find a spare */
	for (i = 0; i < a->info.array.raid_disks; i++) {
		for (d = a->info.devs ; d ; d = d->next)
			if (d->disk.raid_disk == i)
				break;
		dprintf("found %d: %p %x\n", i, d, d?d->curr_state:0);
		if (d && (d->state_fd >= 0))
			continue;

		/*
		 * OK, this device needs recovery.  Try to re-add the previous
		 * occupant of this slot, if this fails add a new spare
		 */
		dl = imsm_readd(super, i, a);
		if (!dl)
			dl = imsm_add_spare(super, i, a);
		if (!dl)
			continue;
 
		/* found a usable disk with enough space */
		di = malloc(sizeof(*di));
		memset(di, 0, sizeof(*di));

		/* dl->index will be -1 in the case we are activating a
		 * pristine spare.  imsm_process_update() will create a
		 * new index in this case.  Once a disk is found to be
		 * failed in all member arrays it is kicked from the
		 * metadata
		 */
		di->disk.number = dl->index;

		/* (ab)use di->devs to store a pointer to the device
		 * we chose
		 */
		di->devs = (struct mdinfo *) dl;

		di->disk.raid_disk = i;
		di->disk.major = dl->major;
		di->disk.minor = dl->minor;
		di->disk.state = 0;
		di->data_offset = __le32_to_cpu(map->pba_of_lba0);
		di->component_size = a->info.component_size;
		di->container_member = inst;
		di->next = rv;
		rv = di;
		num_spares++;
		dprintf("%x:%x to be %d at %llu\n", dl->major, dl->minor,
			i, di->data_offset);

		break;
	}

	if (!rv)
		/* No spares found */
		return rv;
	/* Now 'rv' has a list of devices to return.
	 * Create a metadata_update record to update the
	 * disk_ord_tbl for the array
	 */
	mu = malloc(sizeof(*mu));
	mu->buf = malloc(sizeof(struct imsm_update_activate_spare) * num_spares);
	mu->space = NULL;
	mu->len = sizeof(struct imsm_update_activate_spare) * num_spares;
	mu->next = *updates;
	u = (struct imsm_update_activate_spare *) mu->buf;

	for (di = rv ; di ; di = di->next) {
		u->type = update_activate_spare;
		u->dl = (struct dl *) di->devs;
		di->devs = NULL;
		u->slot = di->disk.raid_disk;
		u->array = inst;
		u->next = u + 1;
		u++;
	}
	(u-1)->next = NULL;
	*updates = mu;

	return rv;
}

static int disks_overlap(struct imsm_map *m1, struct imsm_map *m2)
{
	int i;
	int j;
	int idx;

	for (i = 0; i < m1->num_members; i++) {
		idx = get_imsm_disk_idx(m1, i);
		for (j = 0; j < m2->num_members; j++)
			if (idx == get_imsm_disk_idx(m2, j))
				return 1;
	}

	return 0;
}

static void imsm_delete(struct intel_super *super, struct dl **dlp);

static void imsm_process_update(struct supertype *st,
			        struct metadata_update *update)
{
	/**
	 * crack open the metadata_update envelope to find the update record
	 * update can be one of:
	 * 	update_activate_spare - a spare device has replaced a failed
	 * 	device in an array, update the disk_ord_tbl.  If this disk is
	 * 	present in all member arrays then also clear the SPARE_DISK
	 * 	flag
	 */
	struct intel_super *super = st->sb;
	struct imsm_super *mpb;
	enum imsm_update_type type = *(enum imsm_update_type *) update->buf;

	/* update requires a larger buf but the allocation failed */
	if (super->next_len && !super->next_buf) {
		super->next_len = 0;
		return;
	}

	if (super->next_buf) {
		memcpy(super->next_buf, super->buf, super->len);
		free(super->buf);
		super->len = super->next_len;
		super->buf = super->next_buf;

		super->next_len = 0;
		super->next_buf = NULL;
	}

	mpb = super->anchor;

	switch (type) {
	case update_activate_spare: {
		struct imsm_update_activate_spare *u = (void *) update->buf; 
		struct imsm_dev *dev = get_imsm_dev(super, u->array);
		struct imsm_map *map = get_imsm_map(dev, 0);
		struct active_array *a;
		struct imsm_disk *disk;
		__u32 status;
		struct dl *dl;
		unsigned int found;
		int victim;
		int i;

		for (dl = super->disks; dl; dl = dl->next)
			if (dl == u->dl)
				break;

		if (!dl) {
			fprintf(stderr, "error: imsm_activate_spare passed "
				"an unknown disk (index: %d serial: %s)\n",
				u->dl->index, u->dl->serial);
			return;
		}

		super->updates_pending++;

		/* adding a pristine spare, assign a new index */
		if (dl->index < 0) {
			dl->index = super->anchor->num_disks;
			super->anchor->num_disks++;
		}
		victim = get_imsm_disk_idx(map, u->slot);
		map->disk_ord_tbl[u->slot] = __cpu_to_le32(dl->index);
		disk = &dl->disk;
		status = __le32_to_cpu(disk->status);
		status |= CONFIGURED_DISK;
		status &= ~(SPARE_DISK | USABLE_DISK);
		disk->status = __cpu_to_le32(status);

		/* count arrays using the victim in the metadata */
		found = 0;
		for (a = st->arrays; a ; a = a->next) {
			dev = get_imsm_dev(super, a->info.container_member);
			map = get_imsm_map(dev, 0);
			for (i = 0; i < map->num_members; i++)
				if (victim == get_imsm_disk_idx(map, i))
					found++;
		}

		/* clear some flags if the victim is no longer being
		 * utilized anywhere
		 */
		if (!found) {
			struct dl **dlp;
			for (dlp = &super->disks; *dlp; )
				if ((*dlp)->index == victim)
					break;
			disk = &(*dlp)->disk;
			status = __le32_to_cpu(disk->status);
			status &= ~(CONFIGURED_DISK | USABLE_DISK);
			disk->status = __cpu_to_le32(status);
			/* We know that 'manager' isn't touching anything,
			 * so it is safe to:
			 */
			imsm_delete(super, dlp);
		}
		break;
	}
	case update_create_array: {
		/* someone wants to create a new array, we need to be aware of
		 * a few races/collisions:
		 * 1/ 'Create' called by two separate instances of mdadm
		 * 2/ 'Create' versus 'activate_spare': mdadm has chosen
		 *     devices that have since been assimilated via
		 *     activate_spare.
		 * In the event this update can not be carried out mdadm will
		 * (FIX ME) notice that its update did not take hold.
		 */
		struct imsm_update_create_array *u = (void *) update->buf;
		struct imsm_dev *dev;
		struct imsm_map *map, *new_map;
		unsigned long long start, end;
		unsigned long long new_start, new_end;
		int i;
		int overlap = 0;

		/* handle racing creates: first come first serve */
		if (u->dev_idx < mpb->num_raid_devs) {
			dprintf("%s: subarray %d already defined\n",
				__func__, u->dev_idx);
			return;
		}

		/* check update is next in sequence */
		if (u->dev_idx != mpb->num_raid_devs) {
			dprintf("%s: can not create array %d expected index %d\n",
				__func__, u->dev_idx, mpb->num_raid_devs);
			return;
		}

		new_map = get_imsm_map(&u->dev, 0);
		new_start = __le32_to_cpu(new_map->pba_of_lba0);
		new_end = new_start + __le32_to_cpu(new_map->blocks_per_member);

		/* handle activate_spare versus create race:
		 * check to make sure that overlapping arrays do not include
		 * overalpping disks
		 */
		for (i = 0; i < mpb->num_raid_devs; i++) {
			dev = get_imsm_dev(super, i);
			map = get_imsm_map(dev, 0);
			start = __le32_to_cpu(map->pba_of_lba0);
			end = start + __le32_to_cpu(map->blocks_per_member);
			if ((new_start >= start && new_start <= end) ||
			    (start >= new_start && start <= new_end))
				overlap = 1;
			if (overlap && disks_overlap(map, new_map)) {
				dprintf("%s: arrays overlap\n", __func__);
				return;
			}
		}
		/* check num_members sanity */
		if (new_map->num_members > mpb->num_disks) {
			dprintf("%s: num_disks out of range\n", __func__);
			return;
		}

		/* check that prepare update was successful */
		if (!update->space) {
			dprintf("%s: prepare update failed\n", __func__);
			return;
		}

		super->updates_pending++;
		dev = update->space;
		update->space = NULL;
		imsm_copy_dev(dev, &u->dev);
		map = get_imsm_map(dev, 0);
		super->dev_tbl[u->dev_idx] = dev;
		mpb->num_raid_devs++;

		/* fix up flags */
		for (i = 0; i < map->num_members; i++) {
			struct imsm_disk *disk;
			__u32 status;

			disk = get_imsm_disk(super, get_imsm_disk_idx(map, i));
			status = __le32_to_cpu(disk->status);
			status |= CONFIGURED_DISK;
			status &= ~SPARE_DISK;
			disk->status = __cpu_to_le32(status);
		}
		break;
	}
	case update_add_disk:

		/* we may be able to repair some arrays if disks are
		 * being added */
		if (super->add) {
			struct active_array *a;
 			for (a = st->arrays; a; a = a->next)
				a->check_degraded = 1;
		}
		/* check if we can add / replace some disks in the
		 * metadata */
		while (super->add) {
			struct dl **dlp, *dl, *al;
			al = super->add;
			super->add = al->next;
			for (dlp = &super->disks; *dlp ; ) {
				if (memcmp(al->serial, (*dlp)->serial,
					   MAX_RAID_SERIAL_LEN) == 0) {
					dl = *dlp;
					*dlp = (*dlp)->next;
					__free_imsm_disk(dl);
					break;
				} else
					dlp = &(*dlp)->next;
			}
			al->next = super->disks;
			super->disks = al;
		}

		break;
	}
}

static void imsm_prepare_update(struct supertype *st,
				struct metadata_update *update)
{
	/**
	 * Allocate space to hold new disk entries, raid-device entries or a new
	 * mpb if necessary.  The manager synchronously waits for updates to
	 * complete in the monitor, so new mpb buffers allocated here can be
	 * integrated by the monitor thread without worrying about live pointers
	 * in the manager thread.
	 */
	enum imsm_update_type type = *(enum imsm_update_type *) update->buf;
	struct intel_super *super = st->sb;
	struct imsm_super *mpb = super->anchor;
	size_t buf_len;
	size_t len = 0;

	switch (type) {
	case update_create_array: {
		struct imsm_update_create_array *u = (void *) update->buf;

		len = sizeof_imsm_dev(&u->dev, 1);
		update->space = malloc(len);
		break;
	default:
		break;
	}
	}

	/* check if we need a larger metadata buffer */
	if (super->next_buf)
		buf_len = super->next_len;
	else
		buf_len = super->len;

	if (__le32_to_cpu(mpb->mpb_size) + len > buf_len) {
		/* ok we need a larger buf than what is currently allocated
		 * if this allocation fails process_update will notice that
		 * ->next_len is set and ->next_buf is NULL
		 */
		buf_len = ROUND_UP(__le32_to_cpu(mpb->mpb_size) + len, 512);
		if (super->next_buf)
			free(super->next_buf);

		super->next_len = buf_len;
		if (posix_memalign(&super->next_buf, buf_len, 512) != 0)
			super->next_buf = NULL;
	}
}

/* must be called while manager is quiesced */
static void imsm_delete(struct intel_super *super, struct dl **dlp)
{
	struct imsm_super *mpb = super->anchor;
	struct dl *dl = *dlp;
	struct dl *iter;
	struct imsm_dev *dev;
	struct imsm_map *map;
	int i, j;

	dprintf("%s: deleting device %x:%x from imsm_super\n",
		__func__, dl->major, dl->minor);

	/* shift all indexes down one */
	for (iter = super->disks; iter; iter = iter->next)
		if (iter->index > dl->index)
			iter->index--;

	for (i = 0; i < mpb->num_raid_devs; i++) {
		dev = get_imsm_dev(super, i);
		map = get_imsm_map(dev, 0);

		for (j = 0; j < map->num_members; j++) {
			int idx = get_imsm_disk_idx(map, j);

			if (idx > dl->index)
				map->disk_ord_tbl[j] = __cpu_to_le32(idx - 1);
		}
	}

	mpb->num_disks--;
	super->updates_pending++;
	*dlp = (*dlp)->next;
	__free_imsm_disk(dl);
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
	.activate_spare = imsm_activate_spare,
	.process_update = imsm_process_update,
	.prepare_update = imsm_prepare_update,
};
