/*
 * mdadm - Intel(R) Matrix Storage Manager Support
 *
 * Copyright (C) 2002-2008 Intel Corporation
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

#define HAVE_STDINT_H 1
#include "mdadm.h"
#include "mdmon.h"
#include "sha1.h"
#include "platform-intel.h"
#include <values.h>
#include <scsi/sg.h>
#include <ctype.h>
#include <dirent.h>

/* MPB == Metadata Parameter Block */
#define MPB_SIGNATURE "Intel Raid ISM Cfg Sig. "
#define MPB_SIG_LEN (strlen(MPB_SIGNATURE))
#define MPB_VERSION_RAID0 "1.0.00"
#define MPB_VERSION_RAID1 "1.1.00"
#define MPB_VERSION_MANY_VOLUMES_PER_ARRAY "1.2.00"
#define MPB_VERSION_3OR4_DISK_ARRAY "1.2.01"
#define MPB_VERSION_RAID5 "1.2.02"
#define MPB_VERSION_5OR6_DISK_ARRAY "1.2.04"
#define MPB_VERSION_CNG "1.2.06"
#define MPB_VERSION_ATTRIBS "1.3.00"
#define MAX_SIGNATURE_LENGTH  32
#define MAX_RAID_SERIAL_LEN   16

#define MPB_ATTRIB_CHECKSUM_VERIFY __cpu_to_le32(0x80000000)
#define MPB_ATTRIB_PM      __cpu_to_le32(0x40000000)
#define MPB_ATTRIB_2TB     __cpu_to_le32(0x20000000)
#define MPB_ATTRIB_RAID0   __cpu_to_le32(0x00000001)
#define MPB_ATTRIB_RAID1   __cpu_to_le32(0x00000002)
#define MPB_ATTRIB_RAID10  __cpu_to_le32(0x00000004)
#define MPB_ATTRIB_RAID1E  __cpu_to_le32(0x00000008)
#define MPB_ATTRIB_RAID5   __cpu_to_le32(0x00000010)
#define MPB_ATTRIB_RAIDCNG __cpu_to_le32(0x00000020)

#define MPB_SECTOR_CNT 418
#define IMSM_RESERVED_SECTORS 4096
#define SECT_PER_MB_SHIFT 11

/* Disk configuration info. */
#define IMSM_MAX_DEVICES 255
struct imsm_disk {
	__u8 serial[MAX_RAID_SERIAL_LEN];/* 0xD8 - 0xE7 ascii serial number */
	__u32 total_blocks;		 /* 0xE8 - 0xEB total blocks */
	__u32 scsi_id;			 /* 0xEC - 0xEF scsi ID */
#define SPARE_DISK      __cpu_to_le32(0x01)  /* Spare */
#define CONFIGURED_DISK __cpu_to_le32(0x02)  /* Member of some RaidDev */
#define FAILED_DISK     __cpu_to_le32(0x04)  /* Permanent failure */
	__u32 status;			 /* 0xF0 - 0xF3 */
	__u32 owner_cfg_num; /* which config 0,1,2... owns this disk */ 
#define	IMSM_DISK_FILLERS	4
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
#define IMSM_T_STATE_DEGRADED 2
#define IMSM_T_STATE_FAILED 3
	__u8  raid_level;
#define IMSM_T_RAID0 0
#define IMSM_T_RAID1 1
#define IMSM_T_RAID5 5		/* since metadata version 1.2.02 ? */
	__u8  num_members;	/* number of member disks */
	__u8  num_domains;	/* number of parity domains */
	__u8  failed_disk_num;  /* valid only when state is degraded */
	__u8  ddf;
	__u32 filler[7];	/* expansion area */
#define IMSM_ORD_REBUILD (1 << 24)
	__u32 disk_ord_tbl[1];	/* disk_ord_tbl[num_members],
				 * top byte contains some flags
				 */
} __attribute__ ((packed));

struct imsm_vol {
	__u32 curr_migr_unit;
	__u32 checkpoint_id;	/* id to access curr_migr_unit */
	__u8  migr_state;	/* Normal or Migrating */
#define MIGR_INIT 0
#define MIGR_REBUILD 1
#define MIGR_VERIFY 2 /* analagous to echo check > sync_action */
#define MIGR_GEN_MIGR 3
#define MIGR_STATE_CHANGE 4
#define MIGR_REPAIR 5
	__u8  migr_type;	/* Initializing, Rebuilding, ... */
	__u8  dirty;
	__u8  fs_state;		/* fast-sync state for CnG (0xff == disabled) */
	__u16 verify_errors;	/* number of mismatches */
	__u16 bad_blocks;	/* number of bad blocks during verify */
	__u32 filler[4];
	struct imsm_map map[1];
	/* here comes another one if migr_state */
} __attribute__ ((packed));

struct imsm_dev {
	__u8  volume[MAX_RAID_SERIAL_LEN];
	__u32 size_low;
	__u32 size_high;
#define DEV_BOOTABLE		__cpu_to_le32(0x01)
#define DEV_BOOT_DEVICE		__cpu_to_le32(0x02)
#define DEV_READ_COALESCING	__cpu_to_le32(0x04)
#define DEV_WRITE_COALESCING	__cpu_to_le32(0x08)
#define DEV_LAST_SHUTDOWN_DIRTY	__cpu_to_le32(0x10)
#define DEV_HIDDEN_AT_BOOT	__cpu_to_le32(0x20)
#define DEV_CURRENTLY_HIDDEN	__cpu_to_le32(0x40)
#define DEV_VERIFY_AND_FIX	__cpu_to_le32(0x80)
#define DEV_MAP_STATE_UNINIT	__cpu_to_le32(0x100)
#define DEV_NO_AUTO_RECOVERY	__cpu_to_le32(0x200)
#define DEV_CLONE_N_GO		__cpu_to_le32(0x400)
#define DEV_CLONE_MAN_SYNC	__cpu_to_le32(0x800)
#define DEV_CNG_MASTER_DISK_NUM	__cpu_to_le32(0x1000)
	__u32 status;	/* Persistent RaidDev status */
	__u32 reserved_blocks; /* Reserved blocks at beginning of volume */
	__u8  migr_priority;
	__u8  num_sub_vols;
	__u8  tid;
	__u8  cng_master_disk;
	__u16 cache_policy;
	__u8  cng_state;
	__u8  cng_sub_state;
#define IMSM_DEV_FILLERS 10
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

static __u8 migr_type(struct imsm_dev *dev)
{
	if (dev->vol.migr_type == MIGR_VERIFY &&
	    dev->status & DEV_VERIFY_AND_FIX)
		return MIGR_REPAIR;
	else
		return dev->vol.migr_type;
}

static void set_migr_type(struct imsm_dev *dev, __u8 migr_type)
{
	/* for compatibility with older oroms convert MIGR_REPAIR, into
	 * MIGR_VERIFY w/ DEV_VERIFY_AND_FIX status
	 */
	if (migr_type == MIGR_REPAIR) {
		dev->vol.migr_type = MIGR_VERIFY;
		dev->status |= DEV_VERIFY_AND_FIX;
	} else {
		dev->vol.migr_type = migr_type;
		dev->status &= ~DEV_VERIFY_AND_FIX;
	}
}

static unsigned int sector_count(__u32 bytes)
{
	return ((bytes + (512-1)) & (~(512-1))) / 512;
}

static unsigned int mpb_sectors(struct imsm_super *mpb)
{
	return sector_count(__le32_to_cpu(mpb->mpb_size));
}

struct intel_dev {
	struct imsm_dev *dev;
	struct intel_dev *next;
	unsigned index;
};

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
	int current_vol; /* index of raid device undergoing creation */
	__u32 create_offset; /* common start for 'current_vol' */
	__u32 random; /* random data for seeding new family numbers */
	struct intel_dev *devlist;
	struct dl {
		struct dl *next;
		int index;
		__u8 serial[MAX_RAID_SERIAL_LEN];
		int major, minor;
		char *devname;
		struct imsm_disk disk;
		int fd;
		int extent_cnt;
		struct extent *e; /* for determining freespace @ create */
		int raiddisk; /* slot to fill in autolayout */
	} *disks;
	struct dl *add; /* list of disks to add while mdmon active */
	struct dl *missing; /* disks removed while we weren't looking */
	struct bbm_log *bbm_log;
	const char *hba; /* device path of the raid controller for this metadata */
	const struct imsm_orom *orom; /* platform firmware support */
	struct intel_super *next; /* (temp) list for disambiguating family_num */
};

struct intel_disk {
	struct imsm_disk disk;
	#define IMSM_UNKNOWN_OWNER (-1)
	int owner;
	struct intel_disk *next;
};

struct extent {
	unsigned long long start, size;
};

/* definition of messages passed to imsm_process_update */
enum imsm_update_type {
	update_activate_spare,
	update_create_array,
	update_kill_array,
	update_rename_array,
	update_add_disk,
};

struct imsm_update_activate_spare {
	enum imsm_update_type type;
	struct dl *dl;
	int slot;
	int array;
	struct imsm_update_activate_spare *next;
};

struct disk_info {
	__u8 serial[MAX_RAID_SERIAL_LEN];
};

struct imsm_update_create_array {
	enum imsm_update_type type;
	int dev_idx;
	struct imsm_dev dev;
};

struct imsm_update_kill_array {
	enum imsm_update_type type;
	int dev_idx;
};

struct imsm_update_rename_array {
	enum imsm_update_type type;
	__u8 name[MAX_RAID_SERIAL_LEN];
	int dev_idx;
};

struct imsm_update_add_disk {
	enum imsm_update_type type;
};

static struct supertype *match_metadata_desc_imsm(char *arg)
{
	struct supertype *st;

	if (strcmp(arg, "imsm") != 0 &&
	    strcmp(arg, "default") != 0
		)
		return NULL;

	st = malloc(sizeof(*st));
	if (!st)
		return NULL;
	memset(st, 0, sizeof(*st));
	st->ss = &super_imsm;
	st->max_devs = IMSM_MAX_DEVICES;
	st->minor_version = 0;
	st->sb = NULL;
	return st;
}

#ifndef MDASSEMBLE
static __u8 *get_imsm_version(struct imsm_super *mpb)
{
	return &mpb->sig[MPB_SIG_LEN];
}
#endif 

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

        while (end--) {
                sum += __le32_to_cpu(*p);
		p++;
	}

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

#ifndef MDASSEMBLE
/* retrieve disk serial number list from a metadata update */
static struct disk_info *get_disk_info(struct imsm_update_create_array *update)
{
	void *u = update;
	struct disk_info *inf;

	inf = u + sizeof(*update) - sizeof(struct imsm_dev) +
	      sizeof_imsm_dev(&update->dev, 0);

	return inf;
}
#endif

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
	struct intel_dev *dv;

	if (index >= super->anchor->num_raid_devs)
		return NULL;
	for (dv = super->devlist; dv; dv = dv->next)
		if (dv->index == index)
			return dv->dev;
	return NULL;
}

static __u32 get_imsm_ord_tbl_ent(struct imsm_dev *dev, int slot)
{
	struct imsm_map *map;

	if (dev->vol.migr_state)
		map = get_imsm_map(dev, 1);
	else
		map = get_imsm_map(dev, 0);

	/* top byte identifies disk under rebuild */
	return __le32_to_cpu(map->disk_ord_tbl[slot]);
}

#define ord_to_idx(ord) (((ord) << 8) >> 8)
static __u32 get_imsm_disk_idx(struct imsm_dev *dev, int slot)
{
	__u32 ord = get_imsm_ord_tbl_ent(dev, slot);

	return ord_to_idx(ord);
}

static void set_imsm_ord_tbl_ent(struct imsm_map *map, int slot, __u32 ord)
{
	map->disk_ord_tbl[slot] = __cpu_to_le32(ord);
}

static int get_imsm_disk_slot(struct imsm_map *map, unsigned idx)
{
	int slot;
	__u32 ord;

	for (slot = 0; slot < map->num_members; slot++) {
		ord = __le32_to_cpu(map->disk_ord_tbl[slot]);
		if (ord_to_idx(ord) == idx)
			return slot;
	}

	return -1;
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

static int count_memberships(struct dl *dl, struct intel_super *super)
{
	int memberships = 0;
	int i;

	for (i = 0; i < super->anchor->num_raid_devs; i++) {
		struct imsm_dev *dev = get_imsm_dev(super, i);
		struct imsm_map *map = get_imsm_map(dev, 0);

		if (get_imsm_disk_slot(map, dl->index) >= 0)
			memberships++;
	}

	return memberships;
}

static struct extent *get_extents(struct intel_super *super, struct dl *dl)
{
	/* find a list of used extents on the given physical device */
	struct extent *rv, *e;
	int i;
	int memberships = count_memberships(dl, super);
	__u32 reservation = MPB_SECTOR_CNT + IMSM_RESERVED_SECTORS;

	rv = malloc(sizeof(struct extent) * (memberships + 1));
	if (!rv)
		return NULL;
	e = rv;

	for (i = 0; i < super->anchor->num_raid_devs; i++) {
		struct imsm_dev *dev = get_imsm_dev(super, i);
		struct imsm_map *map = get_imsm_map(dev, 0);

		if (get_imsm_disk_slot(map, dl->index) >= 0) {
			e->start = __le32_to_cpu(map->pba_of_lba0);
			e->size = __le32_to_cpu(map->blocks_per_member);
			e++;
		}
	}
	qsort(rv, memberships, sizeof(*rv), cmp_extent);

	/* determine the start of the metadata 
	 * when no raid devices are defined use the default
	 * ...otherwise allow the metadata to truncate the value
	 * as is the case with older versions of imsm
	 */
	if (memberships) {
		struct extent *last = &rv[memberships - 1];
		__u32 remainder;

		remainder = __le32_to_cpu(dl->disk.total_blocks) - 
			    (last->start + last->size);
		/* round down to 1k block to satisfy precision of the kernel
		 * 'size' interface
		 */
		remainder &= ~1UL;
		/* make sure remainder is still sane */
		if (remainder < (unsigned)ROUND_UP(super->len, 512) >> 9)
			remainder = ROUND_UP(super->len, 512) >> 9;
		if (reservation > remainder)
			reservation = remainder;
	}
	e->start = __le32_to_cpu(dl->disk.total_blocks) - reservation;
	e->size = 0;
	return rv;
}

/* try to determine how much space is reserved for metadata from
 * the last get_extents() entry, otherwise fallback to the
 * default
 */
static __u32 imsm_reserved_sectors(struct intel_super *super, struct dl *dl)
{
	struct extent *e;
	int i;
	__u32 rv;

	/* for spares just return a minimal reservation which will grow
	 * once the spare is picked up by an array
	 */
	if (dl->index == -1)
		return MPB_SECTOR_CNT;

	e = get_extents(super, dl);
	if (!e)
		return MPB_SECTOR_CNT + IMSM_RESERVED_SECTORS;

	/* scroll to last entry */
	for (i = 0; e[i].size; i++)
		continue;

	rv = __le32_to_cpu(dl->disk.total_blocks) - e[i].start;

	free(e);

	return rv;
}

static int is_spare(struct imsm_disk *disk)
{
	return (disk->status & SPARE_DISK) == SPARE_DISK;
}

static int is_configured(struct imsm_disk *disk)
{
	return (disk->status & CONFIGURED_DISK) == CONFIGURED_DISK;
}

static int is_failed(struct imsm_disk *disk)
{
	return (disk->status & FAILED_DISK) == FAILED_DISK;
}

#ifndef MDASSEMBLE
static __u64 blocks_per_migr_unit(struct imsm_dev *dev);

static void print_imsm_dev(struct imsm_dev *dev, char *uuid, int disk_idx)
{
	__u64 sz;
	int slot, i;
	struct imsm_map *map = get_imsm_map(dev, 0);
	__u32 ord;

	printf("\n");
	printf("[%.16s]:\n", dev->volume);
	printf("           UUID : %s\n", uuid);
	printf("     RAID Level : %d\n", get_imsm_raid_level(map));
	printf("        Members : %d\n", map->num_members);
	printf("          Slots : [");
	for (i = 0; i < map->num_members; i++) {
		ord = get_imsm_ord_tbl_ent(dev, i);
		printf("%s", ord & IMSM_ORD_REBUILD ? "_" : "U");
	}
	printf("]\n");
	slot = get_imsm_disk_slot(map, disk_idx);
	if (slot >= 0) {
		ord = get_imsm_ord_tbl_ent(dev, slot);
		printf("      This Slot : %d%s\n", slot,
		       ord & IMSM_ORD_REBUILD ? " (out-of-sync)" : "");
	} else
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
	printf("  Migrate State : ");
	if (dev->vol.migr_state) {
		if (migr_type(dev) == MIGR_INIT)
			printf("initialize\n");
		else if (migr_type(dev) == MIGR_REBUILD)
			printf("rebuild\n");
		else if (migr_type(dev) == MIGR_VERIFY)
			printf("check\n");
		else if (migr_type(dev) == MIGR_GEN_MIGR)
			printf("general migration\n");
		else if (migr_type(dev) == MIGR_STATE_CHANGE)
			printf("state change\n");
		else if (migr_type(dev) == MIGR_REPAIR)
			printf("repair\n");
		else
			printf("<unknown:%d>\n", migr_type(dev));
	} else
		printf("idle\n");
	printf("      Map State : %s", map_state_str[map->map_state]);
	if (dev->vol.migr_state) {
		struct imsm_map *map = get_imsm_map(dev, 1);

		printf(" <-- %s", map_state_str[map->map_state]);
		printf("\n     Checkpoint : %u (%llu)",
		       __le32_to_cpu(dev->vol.curr_migr_unit),
		       (unsigned long long)blocks_per_migr_unit(dev));
	}
	printf("\n");
	printf("    Dirty State : %s\n", dev->vol.dirty ? "dirty" : "clean");
}

static void print_imsm_disk(struct imsm_super *mpb, int index, __u32 reserved)
{
	struct imsm_disk *disk = __get_imsm_disk(mpb, index);
	char str[MAX_RAID_SERIAL_LEN + 1];
	__u64 sz;

	if (index < 0 || !disk)
		return;

	printf("\n");
	snprintf(str, MAX_RAID_SERIAL_LEN + 1, "%s", disk->serial);
	printf("  Disk%02d Serial : %s\n", index, str);
	printf("          State :%s%s%s\n", is_spare(disk) ? " spare" : "",
					    is_configured(disk) ? " active" : "",
					    is_failed(disk) ? " failed" : "");
	printf("             Id : %08x\n", __le32_to_cpu(disk->scsi_id));
	sz = __le32_to_cpu(disk->total_blocks) - reserved;
	printf("    Usable Size : %llu%s\n", (unsigned long long)sz,
	       human_size(sz * 512));
}

static void getinfo_super_imsm(struct supertype *st, struct mdinfo *info);

static void examine_super_imsm(struct supertype *st, char *homehost)
{
	struct intel_super *super = st->sb;
	struct imsm_super *mpb = super->anchor;
	char str[MAX_SIGNATURE_LENGTH];
	int i;
	struct mdinfo info;
	char nbuf[64];
	__u32 sum;
	__u32 reserved = imsm_reserved_sectors(super, super->disks);


	snprintf(str, MPB_SIG_LEN, "%s", mpb->sig);
	printf("          Magic : %s\n", str);
	snprintf(str, strlen(MPB_VERSION_RAID0), "%s", get_imsm_version(mpb));
	printf("        Version : %s\n", get_imsm_version(mpb));
	printf("    Orig Family : %08x\n", __le32_to_cpu(mpb->orig_family_num));
	printf("         Family : %08x\n", __le32_to_cpu(mpb->family_num));
	printf("     Generation : %08x\n", __le32_to_cpu(mpb->generation_num));
	getinfo_super_imsm(st, &info);
	fname_from_uuid(st, &info, nbuf, ':');
	printf("           UUID : %s\n", nbuf + 5);
	sum = __le32_to_cpu(mpb->check_sum);
	printf("       Checksum : %08x %s\n", sum,
		__gen_imsm_checksum(mpb) == sum ? "correct" : "incorrect");
	printf("    MPB Sectors : %d\n", mpb_sectors(mpb));
	printf("          Disks : %d\n", mpb->num_disks);
	printf("   RAID Devices : %d\n", mpb->num_raid_devs);
	print_imsm_disk(mpb, super->disks->index, reserved);
	if (super->bbm_log) {
		struct bbm_log *log = super->bbm_log;

		printf("\n");
		printf("Bad Block Management Log:\n");
		printf("       Log Size : %d\n", __le32_to_cpu(mpb->bbm_log_size));
		printf("      Signature : %x\n", __le32_to_cpu(log->signature));
		printf("    Entry Count : %d\n", __le32_to_cpu(log->entry_count));
		printf("   Spare Blocks : %d\n",  __le32_to_cpu(log->reserved_spare_block_count));
		printf("    First Spare : %llx\n",
		       (unsigned long long) __le64_to_cpu(log->first_spare_lba));
	}
	for (i = 0; i < mpb->num_raid_devs; i++) {
		struct mdinfo info;
		struct imsm_dev *dev = __get_imsm_dev(mpb, i);

		super->current_vol = i;
		getinfo_super_imsm(st, &info);
		fname_from_uuid(st, &info, nbuf, ':');
		print_imsm_dev(dev, nbuf + 5, super->disks->index);
	}
	for (i = 0; i < mpb->num_disks; i++) {
		if (i == super->disks->index)
			continue;
		print_imsm_disk(mpb, i, reserved);
	}
}

static void brief_examine_super_imsm(struct supertype *st, int verbose)
{
	/* We just write a generic IMSM ARRAY entry */
	struct mdinfo info;
	char nbuf[64];
	struct intel_super *super = st->sb;

	if (!super->anchor->num_raid_devs) {
		printf("ARRAY metadata=imsm\n");
		return;
	}

	getinfo_super_imsm(st, &info);
	fname_from_uuid(st, &info, nbuf, ':');
	printf("ARRAY metadata=imsm UUID=%s\n", nbuf + 5);
}

static void brief_examine_subarrays_imsm(struct supertype *st, int verbose)
{
	/* We just write a generic IMSM ARRAY entry */
	struct mdinfo info;
	char nbuf[64];
	char nbuf1[64];
	struct intel_super *super = st->sb;
	int i;

	if (!super->anchor->num_raid_devs)
		return;

	getinfo_super_imsm(st, &info);
	fname_from_uuid(st, &info, nbuf, ':');
	for (i = 0; i < super->anchor->num_raid_devs; i++) {
		struct imsm_dev *dev = get_imsm_dev(super, i);

		super->current_vol = i;
		getinfo_super_imsm(st, &info);
		fname_from_uuid(st, &info, nbuf1, ':');
		printf("ARRAY /dev/md/%.16s container=%s member=%d UUID=%s\n",
		       dev->volume, nbuf + 5, i, nbuf1 + 5);
	}
}

static void export_examine_super_imsm(struct supertype *st)
{
	struct intel_super *super = st->sb;
	struct imsm_super *mpb = super->anchor;
	struct mdinfo info;
	char nbuf[64];

	getinfo_super_imsm(st, &info);
	fname_from_uuid(st, &info, nbuf, ':');
	printf("MD_METADATA=imsm\n");
	printf("MD_LEVEL=container\n");
	printf("MD_UUID=%s\n", nbuf+5);
	printf("MD_DEVICES=%u\n", mpb->num_disks);
}

static void detail_super_imsm(struct supertype *st, char *homehost)
{
	struct mdinfo info;
	char nbuf[64];

	getinfo_super_imsm(st, &info);
	fname_from_uuid(st, &info, nbuf, ':');
	printf("\n           UUID : %s\n", nbuf + 5);
}

static void brief_detail_super_imsm(struct supertype *st)
{
	struct mdinfo info;
	char nbuf[64];
	getinfo_super_imsm(st, &info);
	fname_from_uuid(st, &info, nbuf, ':');
	printf(" UUID=%s", nbuf + 5);
}

static int imsm_read_serial(int fd, char *devname, __u8 *serial);
static void fd2devname(int fd, char *name);

static int imsm_enumerate_ports(const char *hba_path, int port_count, int host_base, int verbose)
{
	/* dump an unsorted list of devices attached to ahci, as well as
	 * non-connected ports
	 */
	int hba_len = strlen(hba_path) + 1;
	struct dirent *ent;
	DIR *dir;
	char *path = NULL;
	int err = 0;
	unsigned long port_mask = (1 << port_count) - 1;

	if (port_count > (int)sizeof(port_mask) * 8) {
		if (verbose)
			fprintf(stderr, Name ": port_count %d out of range\n", port_count);
		return 2;
	}

	/* scroll through /sys/dev/block looking for devices attached to
	 * this hba
	 */
	dir = opendir("/sys/dev/block");
	for (ent = dir ? readdir(dir) : NULL; ent; ent = readdir(dir)) {
		int fd;
		char model[64];
		char vendor[64];
		char buf[1024];
		int major, minor;
		char *device;
		char *c;
		int port;
		int type;

		if (sscanf(ent->d_name, "%d:%d", &major, &minor) != 2)
			continue;
		path = devt_to_devpath(makedev(major, minor));
		if (!path)
			continue;
		if (!path_attached_to_hba(path, hba_path)) {
			free(path);
			path = NULL;
			continue;
		}

		/* retrieve the scsi device type */
		if (asprintf(&device, "/sys/dev/block/%d:%d/device/xxxxxxx", major, minor) < 0) {
			if (verbose)
				fprintf(stderr, Name ": failed to allocate 'device'\n");
			err = 2;
			break;
		}
		sprintf(device, "/sys/dev/block/%d:%d/device/type", major, minor);
		if (load_sys(device, buf) != 0) {
			if (verbose)
				fprintf(stderr, Name ": failed to read device type for %s\n",
					path);
			err = 2;
			free(device);
			break;
		}
		type = strtoul(buf, NULL, 10);

		/* if it's not a disk print the vendor and model */
		if (!(type == 0 || type == 7 || type == 14)) {
			vendor[0] = '\0';
			model[0] = '\0';
			sprintf(device, "/sys/dev/block/%d:%d/device/vendor", major, minor);
			if (load_sys(device, buf) == 0) {
				strncpy(vendor, buf, sizeof(vendor));
				vendor[sizeof(vendor) - 1] = '\0';
				c = (char *) &vendor[sizeof(vendor) - 1];
				while (isspace(*c) || *c == '\0')
					*c-- = '\0';

			}
			sprintf(device, "/sys/dev/block/%d:%d/device/model", major, minor);
			if (load_sys(device, buf) == 0) {
				strncpy(model, buf, sizeof(model));
				model[sizeof(model) - 1] = '\0';
				c = (char *) &model[sizeof(model) - 1];
				while (isspace(*c) || *c == '\0')
					*c-- = '\0';
			}

			if (vendor[0] && model[0])
				sprintf(buf, "%.64s %.64s", vendor, model);
			else
				switch (type) { /* numbers from hald/linux/device.c */
				case 1: sprintf(buf, "tape"); break;
				case 2: sprintf(buf, "printer"); break;
				case 3: sprintf(buf, "processor"); break;
				case 4:
				case 5: sprintf(buf, "cdrom"); break;
				case 6: sprintf(buf, "scanner"); break;
				case 8: sprintf(buf, "media_changer"); break;
				case 9: sprintf(buf, "comm"); break;
				case 12: sprintf(buf, "raid"); break;
				default: sprintf(buf, "unknown");
				}
		} else
			buf[0] = '\0';
		free(device);

		/* chop device path to 'host%d' and calculate the port number */
		c = strchr(&path[hba_len], '/');
		if (!c) {
			if (verbose)
				fprintf(stderr, Name ": %s - invalid path name\n", path + hba_len);
			err = 2;
			break;
		}
		*c = '\0';
		if (sscanf(&path[hba_len], "host%d", &port) == 1)
			port -= host_base;
		else {
			if (verbose) {
				*c = '/'; /* repair the full string */
				fprintf(stderr, Name ": failed to determine port number for %s\n",
					path);
			}
			err = 2;
			break;
		}

		/* mark this port as used */
		port_mask &= ~(1 << port);

		/* print out the device information */
		if (buf[0]) {
			printf("          Port%d : - non-disk device (%s) -\n", port, buf);
			continue;
		}

		fd = dev_open(ent->d_name, O_RDONLY);
		if (fd < 0)
			printf("          Port%d : - disk info unavailable -\n", port);
		else {
			fd2devname(fd, buf);
			printf("          Port%d : %s", port, buf);
			if (imsm_read_serial(fd, NULL, (__u8 *) buf) == 0)
				printf(" (%s)\n", buf);
			else
				printf("()\n");
		}
		close(fd);
		free(path);
		path = NULL;
	}
	if (path)
		free(path);
	if (dir)
		closedir(dir);
	if (err == 0) {
		int i;

		for (i = 0; i < port_count; i++)
			if (port_mask & (1 << i))
				printf("          Port%d : - no device attached -\n", i);
	}

	return err;
}

static int detail_platform_imsm(int verbose, int enumerate_only)
{
	/* There are two components to imsm platform support, the ahci SATA
	 * controller and the option-rom.  To find the SATA controller we
	 * simply look in /sys/bus/pci/drivers/ahci to see if an ahci
	 * controller with the Intel vendor id is present.  This approach
	 * allows mdadm to leverage the kernel's ahci detection logic, with the
	 * caveat that if ahci.ko is not loaded mdadm will not be able to
	 * detect platform raid capabilities.  The option-rom resides in a
	 * platform "Adapter ROM".  We scan for its signature to retrieve the
	 * platform capabilities.  If raid support is disabled in the BIOS the
	 * option-rom capability structure will not be available.
	 */
	const struct imsm_orom *orom;
	struct sys_dev *list, *hba;
	DIR *dir;
	struct dirent *ent;
	const char *hba_path;
	int host_base = 0;
	int port_count = 0;

	if (enumerate_only) {
		if (check_env("IMSM_NO_PLATFORM") || find_imsm_orom())
			return 0;
		return 2;
	}

	list = find_driver_devices("pci", "ahci");
	for (hba = list; hba; hba = hba->next)
		if (devpath_to_vendor(hba->path) == 0x8086)
			break;

	if (!hba) {
		if (verbose)
			fprintf(stderr, Name ": unable to find active ahci controller\n");
		free_sys_dev(&list);
		return 2;
	} else if (verbose)
		fprintf(stderr, Name ": found Intel SATA AHCI Controller\n");
	hba_path = hba->path;
	hba->path = NULL;
	free_sys_dev(&list);

	orom = find_imsm_orom();
	if (!orom) {
		if (verbose)
			fprintf(stderr, Name ": imsm option-rom not found\n");
		return 2;
	}

	printf("       Platform : Intel(R) Matrix Storage Manager\n");
	printf("        Version : %d.%d.%d.%d\n", orom->major_ver, orom->minor_ver,
	       orom->hotfix_ver, orom->build);
	printf("    RAID Levels :%s%s%s%s%s\n",
	       imsm_orom_has_raid0(orom) ? " raid0" : "",
	       imsm_orom_has_raid1(orom) ? " raid1" : "",
	       imsm_orom_has_raid1e(orom) ? " raid1e" : "",
	       imsm_orom_has_raid10(orom) ? " raid10" : "",
	       imsm_orom_has_raid5(orom) ? " raid5" : "");
	printf("    Chunk Sizes :%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s\n",
	       imsm_orom_has_chunk(orom, 2) ? " 2k" : "",
	       imsm_orom_has_chunk(orom, 4) ? " 4k" : "",
	       imsm_orom_has_chunk(orom, 8) ? " 8k" : "",
	       imsm_orom_has_chunk(orom, 16) ? " 16k" : "",
	       imsm_orom_has_chunk(orom, 32) ? " 32k" : "",
	       imsm_orom_has_chunk(orom, 64) ? " 64k" : "",
	       imsm_orom_has_chunk(orom, 128) ? " 128k" : "",
	       imsm_orom_has_chunk(orom, 256) ? " 256k" : "",
	       imsm_orom_has_chunk(orom, 512) ? " 512k" : "",
	       imsm_orom_has_chunk(orom, 1024*1) ? " 1M" : "",
	       imsm_orom_has_chunk(orom, 1024*2) ? " 2M" : "",
	       imsm_orom_has_chunk(orom, 1024*4) ? " 4M" : "",
	       imsm_orom_has_chunk(orom, 1024*8) ? " 8M" : "",
	       imsm_orom_has_chunk(orom, 1024*16) ? " 16M" : "",
	       imsm_orom_has_chunk(orom, 1024*32) ? " 32M" : "",
	       imsm_orom_has_chunk(orom, 1024*64) ? " 64M" : "");
	printf("      Max Disks : %d\n", orom->tds);
	printf("    Max Volumes : %d\n", orom->vpa);
	printf(" I/O Controller : %s\n", hba_path);

	/* find the smallest scsi host number to determine a port number base */
	dir = opendir(hba_path);
	for (ent = dir ? readdir(dir) : NULL; ent; ent = readdir(dir)) {
		int host;

		if (sscanf(ent->d_name, "host%d", &host) != 1)
			continue;
		if (port_count == 0)
			host_base = host;
		else if (host < host_base)
			host_base = host;

		if (host + 1 > port_count + host_base)
			port_count = host + 1 - host_base;

	}
	if (dir)
		closedir(dir);

	if (!port_count || imsm_enumerate_ports(hba_path, port_count,
						host_base, verbose) != 0) {
		if (verbose)
			fprintf(stderr, Name ": failed to enumerate ports\n");
		return 2;
	}

	return 0;
}
#endif

static int match_home_imsm(struct supertype *st, char *homehost)
{
	/* the imsm metadata format does not specify any host
	 * identification information.  We return -1 since we can never
	 * confirm nor deny whether a given array is "meant" for this
	 * host.  We rely on compare_super and the 'family_num' fields to
	 * exclude member disks that do not belong, and we rely on
	 * mdadm.conf to specify the arrays that should be assembled.
	 * Auto-assembly may still pick up "foreign" arrays.
	 */

	return -1;
}

static void uuid_from_super_imsm(struct supertype *st, int uuid[4])
{
	/* The uuid returned here is used for:
	 *  uuid to put into bitmap file (Create, Grow)
	 *  uuid for backup header when saving critical section (Grow)
	 *  comparing uuids when re-adding a device into an array
	 *    In these cases the uuid required is that of the data-array,
	 *    not the device-set.
	 *  uuid to recognise same set when adding a missing device back
	 *    to an array.   This is a uuid for the device-set.
	 *  
	 * For each of these we can make do with a truncated
	 * or hashed uuid rather than the original, as long as
	 * everyone agrees.
	 * In each case the uuid required is that of the data-array,
	 * not the device-set.
	 */
	/* imsm does not track uuid's so we synthesis one using sha1 on
	 * - The signature (Which is constant for all imsm array, but no matter)
	 * - the orig_family_num of the container
	 * - the index number of the volume
	 * - the 'serial' number of the volume.
	 * Hopefully these are all constant.
	 */
	struct intel_super *super = st->sb;

	char buf[20];
	struct sha1_ctx ctx;
	struct imsm_dev *dev = NULL;
	__u32 family_num;

	/* some mdadm versions failed to set ->orig_family_num, in which
	 * case fall back to ->family_num.  orig_family_num will be
	 * fixed up with the first metadata update.
	 */
	family_num = super->anchor->orig_family_num;
	if (family_num == 0)
		family_num = super->anchor->family_num;
	sha1_init_ctx(&ctx);
	sha1_process_bytes(super->anchor->sig, MPB_SIG_LEN, &ctx);
	sha1_process_bytes(&family_num, sizeof(__u32), &ctx);
	if (super->current_vol >= 0)
		dev = get_imsm_dev(super, super->current_vol);
	if (dev) {
		__u32 vol = super->current_vol;
		sha1_process_bytes(&vol, sizeof(vol), &ctx);
		sha1_process_bytes(dev->volume, MAX_RAID_SERIAL_LEN, &ctx);
	}
	sha1_finish_ctx(&ctx, buf);
	memcpy(uuid, buf, 4*4);
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

static __u32 migr_strip_blocks_resync(struct imsm_dev *dev)
{
	/* migr_strip_size when repairing or initializing parity */
	struct imsm_map *map = get_imsm_map(dev, 0);
	__u32 chunk = __le32_to_cpu(map->blocks_per_strip);

	switch (get_imsm_raid_level(map)) {
	case 5:
	case 10:
		return chunk;
	default:
		return 128*1024 >> 9;
	}
}

static __u32 migr_strip_blocks_rebuild(struct imsm_dev *dev)
{
	/* migr_strip_size when rebuilding a degraded disk, no idea why
	 * this is different than migr_strip_size_resync(), but it's good
	 * to be compatible
	 */
	struct imsm_map *map = get_imsm_map(dev, 1);
	__u32 chunk = __le32_to_cpu(map->blocks_per_strip);

	switch (get_imsm_raid_level(map)) {
	case 1:
	case 10:
		if (map->num_members % map->num_domains == 0)
			return 128*1024 >> 9;
		else
			return chunk;
	case 5:
		return max((__u32) 64*1024 >> 9, chunk);
	default:
		return 128*1024 >> 9;
	}
}

static __u32 num_stripes_per_unit_resync(struct imsm_dev *dev)
{
	struct imsm_map *lo = get_imsm_map(dev, 0);
	struct imsm_map *hi = get_imsm_map(dev, 1);
	__u32 lo_chunk = __le32_to_cpu(lo->blocks_per_strip);
	__u32 hi_chunk = __le32_to_cpu(hi->blocks_per_strip);

	return max((__u32) 1, hi_chunk / lo_chunk);
}

static __u32 num_stripes_per_unit_rebuild(struct imsm_dev *dev)
{
	struct imsm_map *lo = get_imsm_map(dev, 0);
	int level = get_imsm_raid_level(lo);

	if (level == 1 || level == 10) {
		struct imsm_map *hi = get_imsm_map(dev, 1);

		return hi->num_domains;
	} else
		return num_stripes_per_unit_resync(dev);
}

static __u8 imsm_num_data_members(struct imsm_dev *dev)
{
	/* named 'imsm_' because raid0, raid1 and raid10
	 * counter-intuitively have the same number of data disks
	 */
	struct imsm_map *map = get_imsm_map(dev, 0);

	switch (get_imsm_raid_level(map)) {
	case 0:
	case 1:
	case 10:
		return map->num_members;
	case 5:
		return map->num_members - 1;
	default:
		dprintf("%s: unsupported raid level\n", __func__);
		return 0;
	}
}

static __u32 parity_segment_depth(struct imsm_dev *dev)
{
	struct imsm_map *map = get_imsm_map(dev, 0);
	__u32 chunk =  __le32_to_cpu(map->blocks_per_strip);

	switch(get_imsm_raid_level(map)) {
	case 1:
	case 10:
		return chunk * map->num_domains;
	case 5:
		return chunk * map->num_members;
	default:
		return chunk;
	}
}

static __u32 map_migr_block(struct imsm_dev *dev, __u32 block)
{
	struct imsm_map *map = get_imsm_map(dev, 1);
	__u32 chunk = __le32_to_cpu(map->blocks_per_strip);
	__u32 strip = block / chunk;

	switch (get_imsm_raid_level(map)) {
	case 1:
	case 10: {
		__u32 vol_strip = (strip * map->num_domains) + 1;
		__u32 vol_stripe = vol_strip / map->num_members;

		return vol_stripe * chunk + block % chunk;
	} case 5: {
		__u32 stripe = strip / (map->num_members - 1);

		return stripe * chunk + block % chunk;
	}
	default:
		return 0;
	}
}

static __u64 blocks_per_migr_unit(struct imsm_dev *dev)
{
	/* calculate the conversion factor between per member 'blocks'
	 * (md/{resync,rebuild}_start) and imsm migration units, return
	 * 0 for the 'not migrating' and 'unsupported migration' cases
	 */
	if (!dev->vol.migr_state)
		return 0;

	switch (migr_type(dev)) {
	case MIGR_VERIFY:
	case MIGR_REPAIR:
	case MIGR_INIT: {
		struct imsm_map *map = get_imsm_map(dev, 0);
		__u32 stripes_per_unit;
		__u32 blocks_per_unit;
		__u32 parity_depth;
		__u32 migr_chunk;
		__u32 block_map;
		__u32 block_rel;
		__u32 segment;
		__u32 stripe;
		__u8  disks;

		/* yes, this is really the translation of migr_units to
		 * per-member blocks in the 'resync' case
		 */
		stripes_per_unit = num_stripes_per_unit_resync(dev);
		migr_chunk = migr_strip_blocks_resync(dev);
		disks = imsm_num_data_members(dev);
		blocks_per_unit = stripes_per_unit * migr_chunk * disks;
		stripe = __le32_to_cpu(map->blocks_per_strip) * disks;
		segment = blocks_per_unit / stripe;
		block_rel = blocks_per_unit - segment * stripe;
		parity_depth = parity_segment_depth(dev);
		block_map = map_migr_block(dev, block_rel);
		return block_map + parity_depth * segment;
	}
	case MIGR_REBUILD: {
		__u32 stripes_per_unit;
		__u32 migr_chunk;

		stripes_per_unit = num_stripes_per_unit_rebuild(dev);
		migr_chunk = migr_strip_blocks_rebuild(dev);
		return migr_chunk * stripes_per_unit;
	}
	case MIGR_GEN_MIGR:
	case MIGR_STATE_CHANGE:
	default:
		return 0;
	}
}

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
		return 0x102;
	}
	return UnSet;
}

static void getinfo_super_imsm_volume(struct supertype *st, struct mdinfo *info)
{
	struct intel_super *super = st->sb;
	struct imsm_dev *dev = get_imsm_dev(super, super->current_vol);
	struct imsm_map *map = get_imsm_map(dev, 0);
	struct dl *dl;
	char *devname;

	for (dl = super->disks; dl; dl = dl->next)
		if (dl->raiddisk == info->disk.raid_disk)
			break;
	info->container_member	  = super->current_vol;
	info->array.raid_disks    = map->num_members;
	info->array.level	  = get_imsm_raid_level(map);
	info->array.layout	  = imsm_level_to_layout(info->array.level);
	info->array.md_minor	  = -1;
	info->array.ctime	  = 0;
	info->array.utime	  = 0;
	info->array.chunk_size	  = __le16_to_cpu(map->blocks_per_strip) << 9;
	info->array.state	  = !dev->vol.dirty;
	info->custom_array_size   = __le32_to_cpu(dev->size_high);
	info->custom_array_size   <<= 32;
	info->custom_array_size   |= __le32_to_cpu(dev->size_low);

	info->disk.major = 0;
	info->disk.minor = 0;
	if (dl) {
		info->disk.major = dl->major;
		info->disk.minor = dl->minor;
	}

	info->data_offset	  = __le32_to_cpu(map->pba_of_lba0);
	info->component_size	  = __le32_to_cpu(map->blocks_per_member);
	memset(info->uuid, 0, sizeof(info->uuid));
	info->recovery_start = MaxSector;
	info->reshape_active = 0;

	if (map->map_state == IMSM_T_STATE_UNINITIALIZED || dev->vol.dirty) {
		info->resync_start = 0;
	} else if (dev->vol.migr_state) {
		switch (migr_type(dev)) {
		case MIGR_REPAIR:
		case MIGR_INIT: {
			__u64 blocks_per_unit = blocks_per_migr_unit(dev);
			__u64 units = __le32_to_cpu(dev->vol.curr_migr_unit);

			info->resync_start = blocks_per_unit * units;
			break;
		}
		case MIGR_VERIFY:
			/* we could emulate the checkpointing of
			 * 'sync_action=check' migrations, but for now
			 * we just immediately complete them
			 */
		case MIGR_REBUILD:
			/* this is handled by container_content_imsm() */
		case MIGR_GEN_MIGR:
		case MIGR_STATE_CHANGE:
			/* FIXME handle other migrations */
		default:
			/* we are not dirty, so... */
			info->resync_start = MaxSector;
		}
	} else
		info->resync_start = MaxSector;

	strncpy(info->name, (char *) dev->volume, MAX_RAID_SERIAL_LEN);
	info->name[MAX_RAID_SERIAL_LEN] = 0;

	info->array.major_version = -1;
	info->array.minor_version = -2;
	devname = devnum2devname(st->container_dev);
	*info->text_version = '\0';
	if (devname)
		sprintf(info->text_version, "/%s/%d", devname, info->container_member);
	free(devname);
	info->safe_mode_delay = 4000;  /* 4 secs like the Matrix driver */
	uuid_from_super_imsm(st, info->uuid);
}

/* check the config file to see if we can return a real uuid for this spare */
static void fixup_container_spare_uuid(struct mdinfo *inf)
{
	struct mddev_ident_s *array_list;

	if (inf->array.level != LEVEL_CONTAINER ||
	    memcmp(inf->uuid, uuid_match_any, sizeof(int[4])) != 0)
		return;

	array_list = conf_get_ident(NULL);

	for (; array_list; array_list = array_list->next) {
		if (array_list->uuid_set) {
			struct supertype *_sst; /* spare supertype */
			struct supertype *_cst; /* container supertype */

			_cst = array_list->st;
			if (_cst)
				_sst = _cst->ss->match_metadata_desc(inf->text_version);
			else
				_sst = NULL;

			if (_sst) {
				memcpy(inf->uuid, array_list->uuid, sizeof(int[4]));
				free(_sst);
				break;
			}
		}
	}
}


static __u8 imsm_check_degraded(struct intel_super *super, struct imsm_dev *dev, int failed);
static int imsm_count_failed(struct intel_super *super, struct imsm_dev *dev);

static struct imsm_disk *get_imsm_missing(struct intel_super *super, __u8 index)
{
	struct dl *d;

	for (d = super->missing; d; d = d->next)
		if (d->index == index)
			return &d->disk;
	return NULL;
}

static void getinfo_super_imsm(struct supertype *st, struct mdinfo *info)
{
	struct intel_super *super = st->sb;
	struct imsm_disk *disk;

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
	info->array.major_version = -1;
	info->array.minor_version = -2;
	strcpy(info->text_version, "imsm");
	info->safe_mode_delay = 0;
	info->disk.number = -1;
	info->disk.state = 0;
	info->name[0] = 0;
	info->recovery_start = MaxSector;

	/* do we have the all the insync disks that we expect? */
	if (st->loaded_container) {
		struct imsm_super *mpb = super->anchor;
		int max_enough = -1, i;

		for (i = 0; i < mpb->num_raid_devs; i++) {
			struct imsm_dev *dev = get_imsm_dev(super, i);
			int failed, enough, j, missing = 0;
			struct imsm_map *map;
			__u8 state;

			failed = imsm_count_failed(super, dev);
			state = imsm_check_degraded(super, dev, failed);
			map = get_imsm_map(dev, dev->vol.migr_state);

			/* any newly missing disks?
			 * (catches single-degraded vs double-degraded)
			 */
			for (j = 0; j < map->num_members; j++) {
				__u32 ord = get_imsm_ord_tbl_ent(dev, i);
				__u32 idx = ord_to_idx(ord);

				if (!(ord & IMSM_ORD_REBUILD) &&
				    get_imsm_missing(super, idx)) {
					missing = 1;
					break;
				}
			}

			if (state == IMSM_T_STATE_FAILED)
				enough = -1;
			else if (state == IMSM_T_STATE_DEGRADED &&
				 (state != map->map_state || missing))
				enough = 0;
			else /* we're normal, or already degraded */
				enough = 1;

			/* in the missing/failed disk case check to see
			 * if at least one array is runnable
			 */
			max_enough = max(max_enough, enough);
		}
		dprintf("%s: enough: %d\n", __func__, max_enough);
		info->container_enough = max_enough;
	} else
		info->container_enough = -1;

	if (super->disks) {
		__u32 reserved = imsm_reserved_sectors(super, super->disks);

		disk = &super->disks->disk;
		info->data_offset = __le32_to_cpu(disk->total_blocks) - reserved;
		info->component_size = reserved;
		info->disk.state  = is_configured(disk) ? (1 << MD_DISK_ACTIVE) : 0;
		/* we don't change info->disk.raid_disk here because
		 * this state will be finalized in mdmon after we have
		 * found the 'most fresh' version of the metadata
		 */
		info->disk.state |= is_failed(disk) ? (1 << MD_DISK_FAULTY) : 0;
		info->disk.state |= is_spare(disk) ? 0 : (1 << MD_DISK_SYNC);
	}

	/* only call uuid_from_super_imsm when this disk is part of a populated container,
	 * ->compare_super may have updated the 'num_raid_devs' field for spares
	 */
	if (info->disk.state & (1 << MD_DISK_SYNC) || super->anchor->num_raid_devs)
		uuid_from_super_imsm(st, info->uuid);
	else {
		memcpy(info->uuid, uuid_match_any, sizeof(int[4]));
		fixup_container_spare_uuid(info);
	}
}

static int update_super_imsm(struct supertype *st, struct mdinfo *info,
			     char *update, char *devname, int verbose,
			     int uuid_set, char *homehost)
{
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
	 *  uuid:  Change the uuid of the array to match watch is given
	 *
	 * Following are not relevant for this imsm:
	 *  sparc2.2 : update from old dodgey metadata
	 *  super-minor: change the preferred_minor number
	 *  summaries:  update redundant counters.
	 *  homehost:  update the recorded homehost
	 *  _reshape_progress: record new reshape_progress position.
	 */
	int rv = 1;
	struct intel_super *super = st->sb;
	struct imsm_super *mpb;

	/* we can only update container info */
	if (!super || super->current_vol >= 0 || !super->anchor)
		return 1;

	mpb = super->anchor;

	if (strcmp(update, "uuid") == 0 && uuid_set && !info->update_private)
		fprintf(stderr,
			Name ": '--uuid' not supported for imsm metadata\n");
	else if (strcmp(update, "uuid") == 0 && uuid_set && info->update_private) {
		mpb->orig_family_num = *((__u32 *) info->update_private);
		rv = 0;
	} else if (strcmp(update, "uuid") == 0) {
		__u32 *new_family = malloc(sizeof(*new_family));

		/* update orig_family_number with the incoming random
		 * data, report the new effective uuid, and store the
		 * new orig_family_num for future updates.
		 */
		if (new_family) {
			memcpy(&mpb->orig_family_num, info->uuid, sizeof(__u32));
			uuid_from_super_imsm(st, info->uuid);
			*new_family = mpb->orig_family_num;
			info->update_private = new_family;
			rv = 0;
		}
	} else if (strcmp(update, "assemble") == 0)
		rv = 0;
	else
		fprintf(stderr,
			Name ": '--update=%s' not supported for imsm metadata\n",
			update);

	/* successful update? recompute checksum */
	if (rv == 0)
		mpb->check_sum = __le32_to_cpu(__gen_imsm_checksum(mpb));

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

static void free_devlist(struct intel_super *super)
{
	struct intel_dev *dv;

	while (super->devlist) {
		dv = super->devlist->next;
		free(super->devlist->dev);
		free(super->devlist);
		super->devlist = dv;
	}
}

static void imsm_copy_dev(struct imsm_dev *dest, struct imsm_dev *src)
{
	memcpy(dest, src, sizeof_imsm_dev(src, 0));
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

	/* if an anchor does not have num_raid_devs set then it is a free
	 * floating spare
	 */
	if (first->anchor->num_raid_devs > 0 &&
	    sec->anchor->num_raid_devs > 0) {
		/* Determine if these disks might ever have been
		 * related.  Further disambiguation can only take place
		 * in load_super_imsm_all
		 */
		__u32 first_family = first->anchor->orig_family_num;
		__u32 sec_family = sec->anchor->orig_family_num;

		if (memcmp(first->anchor->sig, sec->anchor->sig,
			   MAX_SIGNATURE_LENGTH) != 0)
			return 3;

		if (first_family == 0)
			first_family = first->anchor->family_num;
		if (sec_family == 0)
			sec_family = sec->anchor->family_num;

		if (first_family != sec_family)
			return 3;

	}


	/* if 'first' is a spare promote it to a populated mpb with sec's
	 * family number
	 */
	if (first->anchor->num_raid_devs == 0 &&
	    sec->anchor->num_raid_devs > 0) {
		int i;
		struct intel_dev *dv;
		struct imsm_dev *dev;

		/* we need to copy raid device info from sec if an allocation
		 * fails here we don't associate the spare
		 */
		for (i = 0; i < sec->anchor->num_raid_devs; i++) {
			dv = malloc(sizeof(*dv));
			if (!dv)
				break;
			dev = malloc(sizeof_imsm_dev(get_imsm_dev(sec, i), 1));
			if (!dev) {
				free(dv);
				break;
			}
			dv->dev = dev;
			dv->index = i;
			dv->next = first->devlist;
			first->devlist = dv;
		}
		if (i < sec->anchor->num_raid_devs) {
			/* allocation failure */
			free_devlist(first);
			fprintf(stderr, "imsm: failed to associate spare\n"); 
			return 3;
		}
		first->anchor->num_raid_devs = sec->anchor->num_raid_devs;
		first->anchor->orig_family_num = sec->anchor->orig_family_num;
		first->anchor->family_num = sec->anchor->family_num;
		memcpy(first->anchor->sig, sec->anchor->sig, MAX_SIGNATURE_LENGTH);
		for (i = 0; i < sec->anchor->num_raid_devs; i++)
			imsm_copy_dev(get_imsm_dev(first, i), get_imsm_dev(sec, i));
	}

	return 0;
}

static void fd2devname(int fd, char *name)
{
	struct stat st;
	char path[256];
	char dname[PATH_MAX];
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
	int len;
	char *dest;
	char *src;
	char *rsp_buf;
	int i;

	memset(scsi_serial, 0, sizeof(scsi_serial));

	rv = scsi_get_serial(fd, scsi_serial, sizeof(scsi_serial));

	if (rv && check_env("IMSM_DEVNAME_AS_SERIAL")) {
		memset(serial, 0, MAX_RAID_SERIAL_LEN);
		fd2devname(fd, (char *) serial);
		return 0;
	}

	if (rv != 0) {
		if (devname)
			fprintf(stderr,
				Name ": Failed to retrieve serial for %s\n",
				devname);
		return rv;
	}

	rsp_len = scsi_serial[3];
	if (!rsp_len) {
		if (devname)
			fprintf(stderr,
				Name ": Failed to retrieve serial for %s\n",
				devname);
		return 2;
	}
	rsp_buf = (char *) &scsi_serial[4];

	/* trim all whitespace and non-printable characters and convert
	 * ':' to ';'
	 */
	for (i = 0, dest = rsp_buf; i < rsp_len; i++) {
		src = &rsp_buf[i];
		if (*src > 0x20) {
			/* ':' is reserved for use in placeholder serial
			 * numbers for missing disks
			 */
			if (*src == ':')
				*dest++ = ';';
			else
				*dest++ = *src;
		}
	}
	len = dest - rsp_buf;
	dest = rsp_buf;

	/* truncate leading characters */
	if (len > MAX_RAID_SERIAL_LEN) {
		dest += len - MAX_RAID_SERIAL_LEN;
		len = MAX_RAID_SERIAL_LEN;
	}

	memset(serial, 0, MAX_RAID_SERIAL_LEN);
	memcpy(serial, dest, len);

	return 0;
}

static int serialcmp(__u8 *s1, __u8 *s2)
{
	return strncmp((char *) s1, (char *) s2, MAX_RAID_SERIAL_LEN);
}

static void serialcpy(__u8 *dest, __u8 *src)
{
	strncpy((char *) dest, (char *) src, MAX_RAID_SERIAL_LEN);
}

#ifndef MDASSEMBLE
static struct dl *serial_to_dl(__u8 *serial, struct intel_super *super)
{
	struct dl *dl;

	for (dl = super->disks; dl; dl = dl->next)
		if (serialcmp(dl->serial, serial) == 0)
			break;

	return dl;
}
#endif

static struct imsm_disk *
__serial_to_disk(__u8 *serial, struct imsm_super *mpb, int *idx)
{
	int i;

	for (i = 0; i < mpb->num_disks; i++) {
		struct imsm_disk *disk = __get_imsm_disk(mpb, i);

		if (serialcmp(disk->serial, serial) == 0) {
			if (idx)
				*idx = i;
			return disk;
		}
	}

	return NULL;
}

static int
load_imsm_disk(int fd, struct intel_super *super, char *devname, int keep_fd)
{
	struct imsm_disk *disk;
	struct dl *dl;
	struct stat stb;
	int rv;
	char name[40];
	__u8 serial[MAX_RAID_SERIAL_LEN];

	rv = imsm_read_serial(fd, devname, serial);

	if (rv != 0)
		return 2;

	dl = calloc(1, sizeof(*dl));
	if (!dl) {
		if (devname)
			fprintf(stderr,
				Name ": failed to allocate disk buffer for %s\n",
				devname);
		return 2;
	}

	fstat(fd, &stb);
	dl->major = major(stb.st_rdev);
	dl->minor = minor(stb.st_rdev);
	dl->next = super->disks;
	dl->fd = keep_fd ? fd : -1;
	assert(super->disks == NULL);
	super->disks = dl;
	serialcpy(dl->serial, serial);
	dl->index = -2;
	dl->e = NULL;
	fd2devname(fd, name);
	if (devname)
		dl->devname = strdup(devname);
	else
		dl->devname = strdup(name);

	/* look up this disk's index in the current anchor */
	disk = __serial_to_disk(dl->serial, super->anchor, &dl->index);
	if (disk) {
		dl->disk = *disk;
		/* only set index on disks that are a member of a
		 * populated contianer, i.e. one with raid_devs
		 */
		if (is_failed(&dl->disk))
			dl->index = -2;
		else if (is_spare(&dl->disk))
			dl->index = -1;
	}

	return 0;
}

#ifndef MDASSEMBLE
/* When migrating map0 contains the 'destination' state while map1
 * contains the current state.  When not migrating map0 contains the
 * current state.  This routine assumes that map[0].map_state is set to
 * the current array state before being called.
 *
 * Migration is indicated by one of the following states
 * 1/ Idle (migr_state=0 map0state=normal||unitialized||degraded||failed)
 * 2/ Initialize (migr_state=1 migr_type=MIGR_INIT map0state=normal
 *    map1state=unitialized)
 * 3/ Repair (Resync) (migr_state=1 migr_type=MIGR_REPAIR  map0state=normal
 *    map1state=normal)
 * 4/ Rebuild (migr_state=1 migr_type=MIGR_REBUILD map0state=normal
 *    map1state=degraded)
 */
static void migrate(struct imsm_dev *dev, __u8 to_state, int migr_type)
{
	struct imsm_map *dest;
	struct imsm_map *src = get_imsm_map(dev, 0);

	dev->vol.migr_state = 1;
	set_migr_type(dev, migr_type);
	dev->vol.curr_migr_unit = 0;
	dest = get_imsm_map(dev, 1);

	/* duplicate and then set the target end state in map[0] */
	memcpy(dest, src, sizeof_imsm_map(src));
	if (migr_type == MIGR_REBUILD) {
		__u32 ord;
		int i;

		for (i = 0; i < src->num_members; i++) {
			ord = __le32_to_cpu(src->disk_ord_tbl[i]);
			set_imsm_ord_tbl_ent(src, i, ord_to_idx(ord));
		}
	}

	src->map_state = to_state;
}

static void end_migration(struct imsm_dev *dev, __u8 map_state)
{
	struct imsm_map *map = get_imsm_map(dev, 0);
	struct imsm_map *prev = get_imsm_map(dev, dev->vol.migr_state);
	int i;

	/* merge any IMSM_ORD_REBUILD bits that were not successfully
	 * completed in the last migration.
	 *
	 * FIXME add support for online capacity expansion and
	 * raid-level-migration
	 */
	for (i = 0; i < prev->num_members; i++)
		map->disk_ord_tbl[i] |= prev->disk_ord_tbl[i];

	dev->vol.migr_state = 0;
	dev->vol.curr_migr_unit = 0;
	map->map_state = map_state;
}
#endif

static int parse_raid_devices(struct intel_super *super)
{
	int i;
	struct imsm_dev *dev_new;
	size_t len, len_migr;
	size_t space_needed = 0;
	struct imsm_super *mpb = super->anchor;

	for (i = 0; i < super->anchor->num_raid_devs; i++) {
		struct imsm_dev *dev_iter = __get_imsm_dev(super->anchor, i);
		struct intel_dev *dv;

		len = sizeof_imsm_dev(dev_iter, 0);
		len_migr = sizeof_imsm_dev(dev_iter, 1);
		if (len_migr > len)
			space_needed += len_migr - len;
		
		dv = malloc(sizeof(*dv));
		if (!dv)
			return 1;
		dev_new = malloc(len_migr);
		if (!dev_new) {
			free(dv);
			return 1;
		}
		imsm_copy_dev(dev_new, dev_iter);
		dv->dev = dev_new;
		dv->index = i;
		dv->next = super->devlist;
		super->devlist = dv;
	}

	/* ensure that super->buf is large enough when all raid devices
	 * are migrating
	 */
	if (__le32_to_cpu(mpb->mpb_size) + space_needed > super->len) {
		void *buf;

		len = ROUND_UP(__le32_to_cpu(mpb->mpb_size) + space_needed, 512);
		if (posix_memalign(&buf, 512, len) != 0)
			return 1;

		memcpy(buf, super->buf, super->len);
		memset(buf + super->len, 0, len - super->len);
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
		check_sum = __gen_imsm_checksum(super->anchor);
		if (check_sum != __le32_to_cpu(super->anchor->check_sum)) {
			if (devname)
				fprintf(stderr,
					Name ": IMSM checksum %x != %x on %s\n",
					check_sum,
					__le32_to_cpu(super->anchor->check_sum),
					devname);
			return 2;
		}

		return 0;
	}

	/* read the extended mpb */
	if (lseek64(fd, dsize - (512 * (2 + sectors)), SEEK_SET) < 0) {
		if (devname)
			fprintf(stderr,
				Name ": Cannot seek to extended mpb on %s: %s\n",
				devname, strerror(errno));
		return 1;
	}

	if ((unsigned)read(fd, super->buf + 512, super->len - 512) != super->len - 512) {
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
		return 3;
	}

	/* FIXME the BBM log is disk specific so we cannot use this global
	 * buffer for all disks.  Ok for now since we only look at the global
	 * bbm_log_size parameter to gate assembly
	 */
	super->bbm_log = __get_imsm_bbm_log(super->anchor);

	return 0;
}

static int
load_and_parse_mpb(int fd, struct intel_super *super, char *devname, int keep_fd)
{
	int err;

	err = load_imsm_mpb(fd, super, devname);
	if (err)
		return err;
	err = load_imsm_disk(fd, super, devname, keep_fd);
	if (err)
		return err;
	err = parse_raid_devices(super);

	return err;
}

static void __free_imsm_disk(struct dl *d)
{
	if (d->fd >= 0)
		close(d->fd);
	if (d->devname)
		free(d->devname);
	if (d->e)
		free(d->e);
	free(d);

}
static void free_imsm_disks(struct intel_super *super)
{
	struct dl *d;

	while (super->disks) {
		d = super->disks;
		super->disks = d->next;
		__free_imsm_disk(d);
	}
	while (super->missing) {
		d = super->missing;
		super->missing = d->next;
		__free_imsm_disk(d);
	}

}

/* free all the pieces hanging off of a super pointer */
static void __free_imsm(struct intel_super *super, int free_disks)
{
	if (super->buf) {
		free(super->buf);
		super->buf = NULL;
	}
	if (free_disks)
		free_imsm_disks(super);
	free_devlist(super);
	if (super->hba) {
		free((void *) super->hba);
		super->hba = NULL;
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

static struct intel_super *alloc_super(void)
{
	struct intel_super *super = malloc(sizeof(*super));

	if (super) {
		memset(super, 0, sizeof(*super));
		super->current_vol = -1;
		super->create_offset = ~((__u32 ) 0);
		if (!check_env("IMSM_NO_PLATFORM"))
			super->orom = find_imsm_orom();
		if (super->orom && !check_env("IMSM_TEST_OROM")) {
			struct sys_dev *list, *ent;

			/* find the first intel ahci controller */
			list = find_driver_devices("pci", "ahci");
			for (ent = list; ent; ent = ent->next)
				if (devpath_to_vendor(ent->path) == 0x8086)
					break;
			if (ent) {
				super->hba = ent->path;
				ent->path = NULL;
			}
			free_sys_dev(&list);
		}
	}

	return super;
}

#ifndef MDASSEMBLE
/* find_missing - helper routine for load_super_imsm_all that identifies
 * disks that have disappeared from the system.  This routine relies on
 * the mpb being uptodate, which it is at load time.
 */
static int find_missing(struct intel_super *super)
{
	int i;
	struct imsm_super *mpb = super->anchor;
	struct dl *dl;
	struct imsm_disk *disk;

	for (i = 0; i < mpb->num_disks; i++) {
		disk = __get_imsm_disk(mpb, i);
		dl = serial_to_dl(disk->serial, super);
		if (dl)
			continue;

		dl = malloc(sizeof(*dl));
		if (!dl)
			return 1;
		dl->major = 0;
		dl->minor = 0;
		dl->fd = -1;
		dl->devname = strdup("missing");
		dl->index = i;
		serialcpy(dl->serial, disk->serial);
		dl->disk = *disk;
		dl->e = NULL;
		dl->next = super->missing;
		super->missing = dl;
	}

	return 0;
}

static struct intel_disk *disk_list_get(__u8 *serial, struct intel_disk *disk_list)
{
	struct intel_disk *idisk = disk_list;

	while (idisk) {
		if (serialcmp(idisk->disk.serial, serial) == 0)
			break;
		idisk = idisk->next;
	}

	return idisk;
}

static int __prep_thunderdome(struct intel_super **table, int tbl_size,
			      struct intel_super *super,
			      struct intel_disk **disk_list)
{
	struct imsm_disk *d = &super->disks->disk;
	struct imsm_super *mpb = super->anchor;
	int i, j;

	for (i = 0; i < tbl_size; i++) {
		struct imsm_super *tbl_mpb = table[i]->anchor;
		struct imsm_disk *tbl_d = &table[i]->disks->disk;

		if (tbl_mpb->family_num == mpb->family_num) {
			if (tbl_mpb->check_sum == mpb->check_sum) {
				dprintf("%s: mpb from %d:%d matches %d:%d\n",
					__func__, super->disks->major,
					super->disks->minor,
					table[i]->disks->major,
					table[i]->disks->minor);
				break;
			}

			if (((is_configured(d) && !is_configured(tbl_d)) ||
			     is_configured(d) == is_configured(tbl_d)) &&
			    tbl_mpb->generation_num < mpb->generation_num) {
				/* current version of the mpb is a
				 * better candidate than the one in
				 * super_table, but copy over "cross
				 * generational" status
				 */
				struct intel_disk *idisk;

				dprintf("%s: mpb from %d:%d replaces %d:%d\n",
					__func__, super->disks->major,
					super->disks->minor,
					table[i]->disks->major,
					table[i]->disks->minor);

				idisk = disk_list_get(tbl_d->serial, *disk_list);
				if (idisk && is_failed(&idisk->disk))
					tbl_d->status |= FAILED_DISK;
				break;
			} else {
				struct intel_disk *idisk;
				struct imsm_disk *disk;

				/* tbl_mpb is more up to date, but copy
				 * over cross generational status before
				 * returning
				 */
				disk = __serial_to_disk(d->serial, mpb, NULL);
				if (disk && is_failed(disk))
					d->status |= FAILED_DISK;

				idisk = disk_list_get(d->serial, *disk_list);
				if (idisk) {
					idisk->owner = i;
					if (disk && is_configured(disk))
						idisk->disk.status |= CONFIGURED_DISK;
				}

				dprintf("%s: mpb from %d:%d prefer %d:%d\n",
					__func__, super->disks->major,
					super->disks->minor,
					table[i]->disks->major,
					table[i]->disks->minor);

				return tbl_size;
			}
		}
	}

	if (i >= tbl_size)
		table[tbl_size++] = super;
	else
		table[i] = super;

	/* update/extend the merged list of imsm_disk records */
	for (j = 0; j < mpb->num_disks; j++) {
		struct imsm_disk *disk = __get_imsm_disk(mpb, j);
		struct intel_disk *idisk;

		idisk = disk_list_get(disk->serial, *disk_list);
		if (idisk) {
			idisk->disk.status |= disk->status;
			if (is_configured(&idisk->disk) ||
			    is_failed(&idisk->disk))
				idisk->disk.status &= ~(SPARE_DISK);
		} else {
			idisk = calloc(1, sizeof(*idisk));
			if (!idisk)
				return -1;
			idisk->owner = IMSM_UNKNOWN_OWNER;
			idisk->disk = *disk;
			idisk->next = *disk_list;
			*disk_list = idisk;
		}

		if (serialcmp(idisk->disk.serial, d->serial) == 0)
			idisk->owner = i;
	}

	return tbl_size;
}

static struct intel_super *
validate_members(struct intel_super *super, struct intel_disk *disk_list,
		 const int owner)
{
	struct imsm_super *mpb = super->anchor;
	int ok_count = 0;
	int i;

	for (i = 0; i < mpb->num_disks; i++) {
		struct imsm_disk *disk = __get_imsm_disk(mpb, i);
		struct intel_disk *idisk;

		idisk = disk_list_get(disk->serial, disk_list);
		if (idisk) {
			if (idisk->owner == owner ||
			    idisk->owner == IMSM_UNKNOWN_OWNER)
				ok_count++;
			else
				dprintf("%s: '%.16s' owner %d != %d\n",
					__func__, disk->serial, idisk->owner,
					owner);
		} else {
			dprintf("%s: unknown disk %x [%d]: %.16s\n",
				__func__, __le32_to_cpu(mpb->family_num), i,
				disk->serial);
			break;
		}
	}

	if (ok_count == mpb->num_disks)
		return super;
	return NULL;
}

static void show_conflicts(__u32 family_num, struct intel_super *super_list)
{
	struct intel_super *s;

	for (s = super_list; s; s = s->next) {
		if (family_num != s->anchor->family_num)
			continue;
		fprintf(stderr, "Conflict, offlining family %#x on '%s'\n",
			__le32_to_cpu(family_num), s->disks->devname);
	}
}

static struct intel_super *
imsm_thunderdome(struct intel_super **super_list, int len)
{
	struct intel_super *super_table[len];
	struct intel_disk *disk_list = NULL;
	struct intel_super *champion, *spare;
	struct intel_super *s, **del;
	int tbl_size = 0;
	int conflict;
	int i;

	memset(super_table, 0, sizeof(super_table));
	for (s = *super_list; s; s = s->next)
		tbl_size = __prep_thunderdome(super_table, tbl_size, s, &disk_list);

	for (i = 0; i < tbl_size; i++) {
		struct imsm_disk *d;
		struct intel_disk *idisk;
		struct imsm_super *mpb = super_table[i]->anchor;

		s = super_table[i];
		d = &s->disks->disk;

		/* 'd' must appear in merged disk list for its
		 * configuration to be valid
		 */
		idisk = disk_list_get(d->serial, disk_list);
		if (idisk && idisk->owner == i)
			s = validate_members(s, disk_list, i);
		else
			s = NULL;

		if (!s)
			dprintf("%s: marking family: %#x from %d:%d offline\n",
				__func__, mpb->family_num,
				super_table[i]->disks->major,
				super_table[i]->disks->minor);
		super_table[i] = s;
	}

	/* This is where the mdadm implementation differs from the Windows
	 * driver which has no strict concept of a container.  We can only
	 * assemble one family from a container, so when returning a prodigal
	 * array member to this system the code will not be able to disambiguate
	 * the container contents that should be assembled ("foreign" versus
	 * "local").  It requires user intervention to set the orig_family_num
	 * to a new value to establish a new container.  The Windows driver in
	 * this situation fixes up the volume name in place and manages the
	 * foreign array as an independent entity.
	 */
	s = NULL;
	spare = NULL;
	conflict = 0;
	for (i = 0; i < tbl_size; i++) {
		struct intel_super *tbl_ent = super_table[i];
		int is_spare = 0;

		if (!tbl_ent)
			continue;

		if (tbl_ent->anchor->num_raid_devs == 0) {
			spare = tbl_ent;
			is_spare = 1;
		}

		if (s && !is_spare) {
			show_conflicts(tbl_ent->anchor->family_num, *super_list);
			conflict++;
		} else if (!s && !is_spare)
			s = tbl_ent;
	}

	if (!s)
		s = spare;
	if (!s) {
		champion = NULL;
		goto out;
	}
	champion = s;

	if (conflict)
		fprintf(stderr, "Chose family %#x on '%s', "
			"assemble conflicts to new container with '--update=uuid'\n",
			__le32_to_cpu(s->anchor->family_num), s->disks->devname);

	/* collect all dl's onto 'champion', and update them to
	 * champion's version of the status
	 */
	for (s = *super_list; s; s = s->next) {
		struct imsm_super *mpb = champion->anchor;
		struct dl *dl = s->disks;

		if (s == champion)
			continue;

		for (i = 0; i < mpb->num_disks; i++) {
			struct imsm_disk *disk;

			disk = __serial_to_disk(dl->serial, mpb, &dl->index);
			if (disk) {
				dl->disk = *disk;
				/* only set index on disks that are a member of
				 * a populated contianer, i.e. one with
				 * raid_devs
				 */
				if (is_failed(&dl->disk))
					dl->index = -2;
				else if (is_spare(&dl->disk))
					dl->index = -1;
				break;
			}
		}

		if (i >= mpb->num_disks) {
			struct intel_disk *idisk;

			idisk = disk_list_get(dl->serial, disk_list);
			if (idisk && is_spare(&idisk->disk) &&
			    !is_failed(&idisk->disk) && !is_configured(&idisk->disk))
				dl->index = -1;
			else {
				dl->index = -2;
				continue;
			}
		}

		dl->next = champion->disks;
		champion->disks = dl;
		s->disks = NULL;
	}

	/* delete 'champion' from super_list */
	for (del = super_list; *del; ) {
		if (*del == champion) {
			*del = (*del)->next;
			break;
		} else
			del = &(*del)->next;
	}
	champion->next = NULL;

 out:
	while (disk_list) {
		struct intel_disk *idisk = disk_list;

		disk_list = disk_list->next;
		free(idisk);
	}

	return champion;
}

static int load_super_imsm_all(struct supertype *st, int fd, void **sbp,
			       char *devname, int keep_fd)
{
	struct mdinfo *sra;
	struct intel_super *super_list = NULL;
	struct intel_super *super = NULL;
	int devnum = fd2devnum(fd);
	struct mdinfo *sd;
	int retry;
	int err = 0;
	int i;

	/* check if 'fd' an opened container */
	sra = sysfs_read(fd, 0, GET_LEVEL|GET_VERSION|GET_DEVS|GET_STATE);
	if (!sra)
		return 1;

	if (sra->array.major_version != -1 ||
	    sra->array.minor_version != -2 ||
	    strcmp(sra->text_version, "imsm") != 0) {
		err = 1;
		goto error;
	}
	/* load all mpbs */
	for (sd = sra->devs, i = 0; sd; sd = sd->next, i++) {
		struct intel_super *s = alloc_super();
		char nm[32];
		int dfd;

		err = 1;
		if (!s)
			goto error;
		s->next = super_list;
		super_list = s;

		err = 2;
		sprintf(nm, "%d:%d", sd->disk.major, sd->disk.minor);
		dfd = dev_open(nm, keep_fd ? O_RDWR : O_RDONLY);
		if (dfd < 0)
			goto error;

		err = load_and_parse_mpb(dfd, s, NULL, keep_fd);

		/* retry the load if we might have raced against mdmon */
		if (err == 3 && mdmon_running(devnum))
			for (retry = 0; retry < 3; retry++) {
				usleep(3000);
				err = load_and_parse_mpb(dfd, s, NULL, keep_fd);
				if (err != 3)
					break;
			}
		if (!keep_fd)
			close(dfd);
		if (err)
			goto error;
	}

	/* all mpbs enter, maybe one leaves */
	super = imsm_thunderdome(&super_list, i);
	if (!super) {
		err = 1;
		goto error;
	}

	if (find_missing(super) != 0) {
		free_imsm(super);
		err = 2;
		goto error;
	}

	if (st->subarray[0]) {
		unsigned long val;
		char *ep;

		err = 1;
		val = strtoul(st->subarray, &ep, 10);
		if (*ep != '\0') {
			free_imsm(super);
			goto error;
		}

		if (val < super->anchor->num_raid_devs)
			super->current_vol = val;
		else {
			free_imsm(super);
			goto error;
		}
	}
	err = 0;

 error:
	while (super_list) {
		struct intel_super *s = super_list;

		super_list = super_list->next;
		free_imsm(s);
	}
	sysfs_free(sra);

	if (err)
		return err;

	*sbp = super;
	st->container_dev = devnum;
	if (err == 0 && st->ss == NULL) {
		st->ss = &super_imsm;
		st->minor_version = 0;
		st->max_devs = IMSM_MAX_DEVICES;
	}
	st->loaded_container = 1;

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

	if (test_partition(fd))
		/* IMSM not allowed on partitions */
		return 1;

	free_super_imsm(st);

	super = alloc_super();
	if (!super) {
		fprintf(stderr,
			Name ": malloc of %zu failed.\n",
			sizeof(*super));
		return 1;
	}

	rv = load_and_parse_mpb(fd, super, devname, 0);

	if (rv) {
		if (devname)
			fprintf(stderr,
				Name ": Failed to load all information "
				"sections on %s\n", devname);
		free_imsm(super);
		return rv;
	}

	if (st->subarray[0]) {
		unsigned long val;
		char *ep;

		val = strtoul(st->subarray, &ep, 10);
		if (*ep != '\0') {
			free_imsm(super);
			return 1;
		}

		if (val < super->anchor->num_raid_devs)
			super->current_vol = val;
		else {
			free_imsm(super);
			return 1;
		}
	}

	st->sb = super;
	if (st->ss == NULL) {
		st->ss = &super_imsm;
		st->minor_version = 0;
		st->max_devs = IMSM_MAX_DEVICES;
	}
	st->loaded_container = 0;

	return 0;
}

static __u16 info_to_blocks_per_strip(mdu_array_info_t *info)
{
	if (info->level == 1)
		return 128;
	return info->chunk_size >> 9;
}

static __u32 info_to_num_data_stripes(mdu_array_info_t *info, int num_domains)
{
	__u32 num_stripes;

	num_stripes = (info->size * 2) / info_to_blocks_per_strip(info);
	num_stripes /= num_domains;

	return num_stripes;
}

static __u32 info_to_blocks_per_member(mdu_array_info_t *info)
{
	if (info->level == 1)
		return info->size * 2;
	else
		return (info->size * 2) & ~(info_to_blocks_per_strip(info) - 1);
}

static void imsm_update_version_info(struct intel_super *super)
{
	/* update the version and attributes */
	struct imsm_super *mpb = super->anchor;
	char *version;
	struct imsm_dev *dev;
	struct imsm_map *map;
	int i;

	for (i = 0; i < mpb->num_raid_devs; i++) {
		dev = get_imsm_dev(super, i);
		map = get_imsm_map(dev, 0);
		if (__le32_to_cpu(dev->size_high) > 0)
			mpb->attributes |= MPB_ATTRIB_2TB;

		/* FIXME detect when an array spans a port multiplier */
		#if 0
		mpb->attributes |= MPB_ATTRIB_PM;
		#endif

		if (mpb->num_raid_devs > 1 ||
		    mpb->attributes != MPB_ATTRIB_CHECKSUM_VERIFY) {
			version = MPB_VERSION_ATTRIBS;
			switch (get_imsm_raid_level(map)) {
			case 0: mpb->attributes |= MPB_ATTRIB_RAID0; break;
			case 1: mpb->attributes |= MPB_ATTRIB_RAID1; break;
			case 10: mpb->attributes |= MPB_ATTRIB_RAID10; break;
			case 5: mpb->attributes |= MPB_ATTRIB_RAID5; break;
			}
		} else {
			if (map->num_members >= 5)
				version = MPB_VERSION_5OR6_DISK_ARRAY;
			else if (dev->status == DEV_CLONE_N_GO)
				version = MPB_VERSION_CNG;
			else if (get_imsm_raid_level(map) == 5)
				version = MPB_VERSION_RAID5;
			else if (map->num_members >= 3)
				version = MPB_VERSION_3OR4_DISK_ARRAY;
			else if (get_imsm_raid_level(map) == 1)
				version = MPB_VERSION_RAID1;
			else
				version = MPB_VERSION_RAID0;
		}
		strcpy(((char *) mpb->sig) + strlen(MPB_SIGNATURE), version);
	}
}

static int check_name(struct intel_super *super, char *name, int quiet)
{
	struct imsm_super *mpb = super->anchor;
	char *reason = NULL;
	int i;

	if (strlen(name) > MAX_RAID_SERIAL_LEN)
		reason = "must be 16 characters or less";

	for (i = 0; i < mpb->num_raid_devs; i++) {
		struct imsm_dev *dev = get_imsm_dev(super, i);

		if (strncmp((char *) dev->volume, name, MAX_RAID_SERIAL_LEN) == 0) {
			reason = "already exists";
			break;
		}
	}

	if (reason && !quiet)
		fprintf(stderr, Name ": imsm volume name %s\n", reason);

	return !reason;
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
	struct intel_dev *dv;
	struct imsm_dev *dev;
	struct imsm_vol *vol;
	struct imsm_map *map;
	int idx = mpb->num_raid_devs;
	int i;
	unsigned long long array_blocks;
	size_t size_old, size_new;
	__u32 num_data_stripes;

	if (super->orom && mpb->num_raid_devs >= super->orom->vpa) {
		fprintf(stderr, Name": This imsm-container already has the "
			"maximum of %d volumes\n", super->orom->vpa);
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

	if (!check_name(super, name, 0))
		return 0;
	sprintf(st->subarray, "%d", idx);
	dv = malloc(sizeof(*dv));
	if (!dv) {
		fprintf(stderr, Name ": failed to allocate device list entry\n");
		return 0;
	}
	dev = malloc(sizeof(*dev) + sizeof(__u32) * (info->raid_disks - 1));
	if (!dev) {
		free(dv);
		fprintf(stderr, Name": could not allocate raid device\n");
		return 0;
	}
	strncpy((char *) dev->volume, name, MAX_RAID_SERIAL_LEN);
	if (info->level == 1)
		array_blocks = info_to_blocks_per_member(info);
	else
		array_blocks = calc_array_size(info->level, info->raid_disks,
					       info->layout, info->chunk_size,
					       info->size*2);
	/* round array size down to closest MB */
	array_blocks = (array_blocks >> SECT_PER_MB_SHIFT) << SECT_PER_MB_SHIFT;

	dev->size_low = __cpu_to_le32((__u32) array_blocks);
	dev->size_high = __cpu_to_le32((__u32) (array_blocks >> 32));
	dev->status = __cpu_to_le32(0);
	dev->reserved_blocks = __cpu_to_le32(0);
	vol = &dev->vol;
	vol->migr_state = 0;
	set_migr_type(dev, MIGR_INIT);
	vol->dirty = 0;
	vol->curr_migr_unit = 0;
	map = get_imsm_map(dev, 0);
	map->pba_of_lba0 = __cpu_to_le32(super->create_offset);
	map->blocks_per_member = __cpu_to_le32(info_to_blocks_per_member(info));
	map->blocks_per_strip = __cpu_to_le16(info_to_blocks_per_strip(info));
	map->failed_disk_num = ~0;
	map->map_state = info->level ? IMSM_T_STATE_UNINITIALIZED :
				       IMSM_T_STATE_NORMAL;
	map->ddf = 1;

	if (info->level == 1 && info->raid_disks > 2) {
		free(dev);
		free(dv);
		fprintf(stderr, Name": imsm does not support more than 2 disks"
				"in a raid1 volume\n");
		return 0;
	}

	map->raid_level = info->level;
	if (info->level == 10) {
		map->raid_level = 1;
		map->num_domains = info->raid_disks / 2;
	} else if (info->level == 1)
		map->num_domains = info->raid_disks;
	else
		map->num_domains = 1;

	num_data_stripes = info_to_num_data_stripes(info, map->num_domains);
	map->num_data_stripes = __cpu_to_le32(num_data_stripes);

	map->num_members = info->raid_disks;
	for (i = 0; i < map->num_members; i++) {
		/* initialized in add_to_super */
		set_imsm_ord_tbl_ent(map, i, IMSM_ORD_REBUILD);
	}
	mpb->num_raid_devs++;

	dv->dev = dev;
	dv->index = super->current_vol;
	dv->next = super->devlist;
	super->devlist = dv;

	imsm_update_version_info(super);

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
	char *version;

	if (st->sb)
		return init_super_imsm_volume(st, info, size, name, homehost, uuid);

	if (info)
		mpb_size = disks_to_mpb_size(info->nr_disks);
	else
		mpb_size = 512;

	super = alloc_super();
	if (super && posix_memalign(&super->buf, 512, mpb_size) != 0) {
		free(super);
		super = NULL;
	}
	if (!super) {
		fprintf(stderr, Name
			": %s could not allocate superblock\n", __func__);
		return 0;
	}
	memset(super->buf, 0, mpb_size);
	mpb = super->buf;
	mpb->mpb_size = __cpu_to_le32(mpb_size);
	st->sb = super;

	if (info == NULL) {
		/* zeroing superblock */
		return 0;
	}

	mpb->attributes = MPB_ATTRIB_CHECKSUM_VERIFY;

	version = (char *) mpb->sig;
	strcpy(version, MPB_SIGNATURE);
	version += strlen(MPB_SIGNATURE);
	strcpy(version, MPB_VERSION_RAID0);

	return 1;
}

#ifndef MDASSEMBLE
static int add_to_super_imsm_volume(struct supertype *st, mdu_disk_info_t *dk,
				     int fd, char *devname)
{
	struct intel_super *super = st->sb;
	struct imsm_super *mpb = super->anchor;
	struct dl *dl;
	struct imsm_dev *dev;
	struct imsm_map *map;
	int slot;

	dev = get_imsm_dev(super, super->current_vol);
	map = get_imsm_map(dev, 0);

	if (! (dk->state & (1<<MD_DISK_SYNC))) {
		fprintf(stderr, Name ": %s: Cannot add spare devices to IMSM volume\n",
			devname);
		return 1;
	}

	if (fd == -1) {
		/* we're doing autolayout so grab the pre-marked (in
		 * validate_geometry) raid_disk
		 */
		for (dl = super->disks; dl; dl = dl->next)
			if (dl->raiddisk == dk->raid_disk)
				break;
	} else {
		for (dl = super->disks; dl ; dl = dl->next)
			if (dl->major == dk->major &&
			    dl->minor == dk->minor)
				break;
	}

	if (!dl) {
		fprintf(stderr, Name ": %s is not a member of the same container\n", devname);
		return 1;
	}

	/* add a pristine spare to the metadata */
	if (dl->index < 0) {
		dl->index = super->anchor->num_disks;
		super->anchor->num_disks++;
	}
	/* Check the device has not already been added */
	slot = get_imsm_disk_slot(map, dl->index);
	if (slot >= 0 &&
	    (get_imsm_ord_tbl_ent(dev, slot) & IMSM_ORD_REBUILD) == 0) {
		fprintf(stderr, Name ": %s has been included in this array twice\n",
			devname);
		return 1;
	}
	set_imsm_ord_tbl_ent(map, dk->number, dl->index);
	dl->disk.status = CONFIGURED_DISK;

	/* if we are creating the first raid device update the family number */
	if (super->current_vol == 0) {
		__u32 sum;
		struct imsm_dev *_dev = __get_imsm_dev(mpb, 0);
		struct imsm_disk *_disk = __get_imsm_disk(mpb, dl->index);

		if (!_dev || !_disk) {
			fprintf(stderr, Name ": BUG mpb setup error\n");
			return 1;
		}
		*_dev = *dev;
		*_disk = dl->disk;
		sum = random32();
		sum += __gen_imsm_checksum(mpb);
		mpb->family_num = __cpu_to_le32(sum);
		mpb->orig_family_num = mpb->family_num;
	}

	return 0;
}

static int add_to_super_imsm(struct supertype *st, mdu_disk_info_t *dk,
			      int fd, char *devname)
{
	struct intel_super *super = st->sb;
	struct dl *dd;
	unsigned long long size;
	__u32 id;
	int rv;
	struct stat stb;

	/* if we are on an RAID enabled platform check that the disk is
	 * attached to the raid controller
	 */
	if (super->hba && !disk_attached_to_hba(fd, super->hba)) {
		fprintf(stderr,
			Name ": %s is not attached to the raid controller: %s\n",
			devname ? : "disk", super->hba);
		return 1;
	}

	if (super->current_vol >= 0)
		return add_to_super_imsm_volume(st, dk, fd, devname);

	fstat(fd, &stb);
	dd = malloc(sizeof(*dd));
	if (!dd) {
		fprintf(stderr,
			Name ": malloc failed %s:%d.\n", __func__, __LINE__);
		return 1;
	}
	memset(dd, 0, sizeof(*dd));
	dd->major = major(stb.st_rdev);
	dd->minor = minor(stb.st_rdev);
	dd->index = -1;
	dd->devname = devname ? strdup(devname) : NULL;
	dd->fd = fd;
	dd->e = NULL;
	rv = imsm_read_serial(fd, devname, dd->serial);
	if (rv) {
		fprintf(stderr,
			Name ": failed to retrieve scsi serial, aborting\n");
		free(dd);
		abort();
	}

	get_dev_size(fd, NULL, &size);
	size /= 512;
	serialcpy(dd->disk.serial, dd->serial);
	dd->disk.total_blocks = __cpu_to_le32(size);
	dd->disk.status = SPARE_DISK;
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

	return 0;
}

static int store_imsm_mpb(int fd, struct imsm_super *mpb);

static union {
	char buf[512];
	struct imsm_super anchor;
} spare_record __attribute__ ((aligned(512)));

/* spare records have their own family number and do not have any defined raid
 * devices
 */
static int write_super_imsm_spares(struct intel_super *super, int doclose)
{
	struct imsm_super *mpb = super->anchor;
	struct imsm_super *spare = &spare_record.anchor;
	__u32 sum;
	struct dl *d;

	spare->mpb_size = __cpu_to_le32(sizeof(struct imsm_super)),
	spare->generation_num = __cpu_to_le32(1UL),
	spare->attributes = MPB_ATTRIB_CHECKSUM_VERIFY;
	spare->num_disks = 1,
	spare->num_raid_devs = 0,
	spare->cache_size = mpb->cache_size,
	spare->pwr_cycle_count = __cpu_to_le32(1),

	snprintf((char *) spare->sig, MAX_SIGNATURE_LENGTH,
		 MPB_SIGNATURE MPB_VERSION_RAID0);

	for (d = super->disks; d; d = d->next) {
		if (d->index != -1)
			continue;

		spare->disk[0] = d->disk;
		sum = __gen_imsm_checksum(spare);
		spare->family_num = __cpu_to_le32(sum);
		spare->orig_family_num = 0;
		sum = __gen_imsm_checksum(spare);
		spare->check_sum = __cpu_to_le32(sum);

		if (store_imsm_mpb(d->fd, spare)) {
			fprintf(stderr, "%s: failed for device %d:%d %s\n",
				__func__, d->major, d->minor, strerror(errno));
			return 1;
		}
		if (doclose) {
			close(d->fd);
			d->fd = -1;
		}
	}

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

	/* fix up cases where previous mdadm releases failed to set
	 * orig_family_num
	 */
	if (mpb->orig_family_num == 0)
		mpb->orig_family_num = mpb->family_num;

	mpb_size += sizeof(struct imsm_disk) * mpb->num_disks;
	for (d = super->disks; d; d = d->next) {
		if (d->index == -1)
			spares++;
		else
			mpb->disk[d->index] = d->disk;
	}
	for (d = super->missing; d; d = d->next)
		mpb->disk[d->index] = d->disk;

	for (i = 0; i < mpb->num_raid_devs; i++) {
		struct imsm_dev *dev = __get_imsm_dev(mpb, i);

		imsm_copy_dev(dev, get_imsm_dev(super, i));
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
		if (store_imsm_mpb(d->fd, mpb))
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


static int create_array(struct supertype *st, int dev_idx)
{
	size_t len;
	struct imsm_update_create_array *u;
	struct intel_super *super = st->sb;
	struct imsm_dev *dev = get_imsm_dev(super, dev_idx);
	struct imsm_map *map = get_imsm_map(dev, 0);
	struct disk_info *inf;
	struct imsm_disk *disk;
	int i;

	len = sizeof(*u) - sizeof(*dev) + sizeof_imsm_dev(dev, 0) +
	      sizeof(*inf) * map->num_members;
	u = malloc(len);
	if (!u) {
		fprintf(stderr, "%s: failed to allocate update buffer\n",
			__func__);
		return 1;
	}

	u->type = update_create_array;
	u->dev_idx = dev_idx;
	imsm_copy_dev(&u->dev, dev);
	inf = get_disk_info(u);
	for (i = 0; i < map->num_members; i++) {
		int idx = get_imsm_disk_idx(dev, i);

		disk = get_imsm_disk(super, idx);
		serialcpy(inf[i].serial, disk->serial);
	}
	append_metadata_update(st, u, len);

	return 0;
}

static int _add_disk(struct supertype *st)
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
	struct intel_super *super = st->sb;
	int current_vol = super->current_vol;

	/* we are done with current_vol reset it to point st at the container */
	super->current_vol = -1;

	if (st->update_tail) {
		/* queue the recently created array / added disk
		 * as a metadata update */
		struct dl *d;
		int rv;

		/* determine if we are creating a volume or adding a disk */
		if (current_vol < 0) {
			/* in the add disk case we are running in mdmon
			 * context, so don't close fd's
			 */
			return _add_disk(st);
		} else
			rv = create_array(st, current_vol);

		for (d = super->disks; d ; d = d->next) {
			close(d->fd);
			d->fd = -1;
		}

		return rv;
	} else {
		struct dl *d;
		for (d = super->disks; d; d = d->next)
			Kill(d->devname, NULL, 0, 1, 1);
		return write_super_imsm(st->sb, 1);
	}
}
#endif

static int store_super_imsm(struct supertype *st, int fd)
{
	struct intel_super *super = st->sb;
	struct imsm_super *mpb = super ? super->anchor : NULL;

	if (!mpb)
		return 1;

#ifndef MDASSEMBLE
	return store_imsm_mpb(fd, mpb);
#else
	return 1;
#endif
}

static int imsm_bbm_log_size(struct imsm_super *mpb)
{
	return __le32_to_cpu(mpb->bbm_log_size);
}

#ifndef MDASSEMBLE
static int validate_geometry_imsm_container(struct supertype *st, int level,
					    int layout, int raiddisks, int chunk,
					    unsigned long long size, char *dev,
					    unsigned long long *freesize,
					    int verbose)
{
	int fd;
	unsigned long long ldsize;
	const struct imsm_orom *orom;

	if (level != LEVEL_CONTAINER)
		return 0;
	if (!dev)
		return 1;

	if (check_env("IMSM_NO_PLATFORM"))
		orom = NULL;
	else
		orom = find_imsm_orom();
	if (orom && raiddisks > orom->tds) {
		if (verbose)
			fprintf(stderr, Name ": %d exceeds maximum number of"
				" platform supported disks: %d\n",
				raiddisks, orom->tds);
		return 0;
	}

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

static unsigned long long find_size(struct extent *e, int *idx, int num_extents)
{
	const unsigned long long base_start = e[*idx].start;
	unsigned long long end = base_start + e[*idx].size;
	int i;

	if (base_start == end)
		return 0;

	*idx = *idx + 1;
	for (i = *idx; i < num_extents; i++) {
		/* extend overlapping extents */
		if (e[i].start >= base_start &&
		    e[i].start <= end) {
			if (e[i].size == 0)
				return 0;
			if (e[i].start + e[i].size > end)
				end = e[i].start + e[i].size;
		} else if (e[i].start > end) {
			*idx = i;
			break;
		}
	}

	return end - base_start;
}

static unsigned long long merge_extents(struct intel_super *super, int sum_extents)
{
	/* build a composite disk with all known extents and generate a new
	 * 'maxsize' given the "all disks in an array must share a common start
	 * offset" constraint
	 */
	struct extent *e = calloc(sum_extents, sizeof(*e));
	struct dl *dl;
	int i, j;
	int start_extent;
	unsigned long long pos;
	unsigned long long start = 0;
	unsigned long long maxsize;
	unsigned long reserve;

	if (!e)
		return 0;

	/* coalesce and sort all extents. also, check to see if we need to
	 * reserve space between member arrays
	 */
	j = 0;
	for (dl = super->disks; dl; dl = dl->next) {
		if (!dl->e)
			continue;
		for (i = 0; i < dl->extent_cnt; i++)
			e[j++] = dl->e[i];
	}
	qsort(e, sum_extents, sizeof(*e), cmp_extent);

	/* merge extents */
	i = 0;
	j = 0;
	while (i < sum_extents) {
		e[j].start = e[i].start;
		e[j].size = find_size(e, &i, sum_extents);
		j++;
		if (e[j-1].size == 0)
			break;
	}

	pos = 0;
	maxsize = 0;
	start_extent = 0;
	i = 0;
	do {
		unsigned long long esize;

		esize = e[i].start - pos;
		if (esize >= maxsize) {
			maxsize = esize;
			start = pos;
			start_extent = i;
		}
		pos = e[i].start + e[i].size;
		i++;
	} while (e[i-1].size);
	free(e);

	if (maxsize == 0)
		return 0;

	/* FIXME assumes volume at offset 0 is the first volume in a
	 * container
	 */
	if (start_extent > 0)
		reserve = IMSM_RESERVED_SECTORS; /* gap between raid regions */
	else
		reserve = 0;

	if (maxsize < reserve)
		return 0;

	super->create_offset = ~((__u32) 0);
	if (start + reserve > super->create_offset)
		return 0; /* start overflows create_offset */
	super->create_offset = start + reserve;

	return maxsize - reserve;
}

static int is_raid_level_supported(const struct imsm_orom *orom, int level, int raiddisks)
{
	if (level < 0 || level == 6 || level == 4)
		return 0;

	/* if we have an orom prevent invalid raid levels */
	if (orom)
		switch (level) {
		case 0: return imsm_orom_has_raid0(orom);
		case 1:
			if (raiddisks > 2)
				return imsm_orom_has_raid1e(orom);
			return imsm_orom_has_raid1(orom) && raiddisks == 2;
		case 10: return imsm_orom_has_raid10(orom) && raiddisks == 4;
		case 5: return imsm_orom_has_raid5(orom) && raiddisks > 2;
		}
	else
		return 1; /* not on an Intel RAID platform so anything goes */

	return 0;
}

#define pr_vrb(fmt, arg...) (void) (verbose && fprintf(stderr, Name fmt, ##arg))
static int
validate_geometry_imsm_orom(struct intel_super *super, int level, int layout,
			    int raiddisks, int chunk, int verbose)
{
	if (!is_raid_level_supported(super->orom, level, raiddisks)) {
		pr_vrb(": platform does not support raid%d with %d disk%s\n",
			level, raiddisks, raiddisks > 1 ? "s" : "");
		return 0;
	}
	if (super->orom && level != 1 &&
	    !imsm_orom_has_chunk(super->orom, chunk)) {
		pr_vrb(": platform does not support a chunk size of: %d\n", chunk);
		return 0;
	}
	if (layout != imsm_level_to_layout(level)) {
		if (level == 5)
			pr_vrb(": imsm raid 5 only supports the left-asymmetric layout\n");
		else if (level == 10)
			pr_vrb(": imsm raid 10 only supports the n2 layout\n");
		else
			pr_vrb(": imsm unknown layout %#x for this raid level %d\n",
				layout, level);
		return 0;
	}

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
	struct imsm_super *mpb = super->anchor;
	struct dl *dl;
	unsigned long long pos = 0;
	unsigned long long maxsize;
	struct extent *e;
	int i;

	/* We must have the container info already read in. */
	if (!super)
		return 0;

	if (!validate_geometry_imsm_orom(super, level, layout, raiddisks, chunk, verbose))
		return 0;

	if (!dev) {
		/* General test:  make sure there is space for
		 * 'raiddisks' device extents of size 'size' at a given
		 * offset
		 */
		unsigned long long minsize = size;
		unsigned long long start_offset = MaxSector;
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
				if (found && start_offset == MaxSector) {
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
		if (dl->major == (int)major(stb.st_rdev) &&
		    dl->minor == (int)minor(stb.st_rdev))
			break;
	}
	if (!dl) {
		if (verbose)
			fprintf(stderr, Name ": %s is not in the "
				"same imsm set\n", dev);
		return 0;
	} else if (super->orom && dl->index < 0 && mpb->num_raid_devs) {
		/* If a volume is present then the current creation attempt
		 * cannot incorporate new spares because the orom may not
		 * understand this configuration (all member disks must be
		 * members of each array in the container).
		 */
		fprintf(stderr, Name ": %s is a spare and a volume"
			" is already defined for this container\n", dev);
		fprintf(stderr, Name ": The option-rom requires all member"
			" disks to be a member of all volumes\n");
		return 0;
	}

	/* retrieve the largest free space block */
	e = get_extents(super, dl);
	maxsize = 0;
	i = 0;
	if (e) {
		do {
			unsigned long long esize;

			esize = e[i].start - pos;
			if (esize >= maxsize)
				maxsize = esize;
			pos = e[i].start + e[i].size;
			i++;
		} while (e[i-1].size);
		dl->e = e;
		dl->extent_cnt = i;
	} else {
		if (verbose)
			fprintf(stderr, Name ": unable to determine free space for: %s\n",
				dev);
		return 0;
	}
	if (maxsize < size) {
		if (verbose)
			fprintf(stderr, Name ": %s not enough space (%llu < %llu)\n",
				dev, maxsize, size);
		return 0;
	}

	/* count total number of extents for merge */
	i = 0;
	for (dl = super->disks; dl; dl = dl->next)
		if (dl->e)
			i += dl->extent_cnt;

	maxsize = merge_extents(super, i);
	if (maxsize < size || maxsize == 0) {
		if (verbose)
			fprintf(stderr, Name ": not enough space after merge (%llu < %llu)\n",
				maxsize, size);
		return 0;
	}

	*freesize = maxsize;

	return 1;
}

static int reserve_space(struct supertype *st, int raiddisks,
			 unsigned long long size, int chunk,
			 unsigned long long *freesize)
{
	struct intel_super *super = st->sb;
	struct imsm_super *mpb = super->anchor;
	struct dl *dl;
	int i;
	int extent_cnt;
	struct extent *e;
	unsigned long long maxsize;
	unsigned long long minsize;
	int cnt;
	int used;

	/* find the largest common start free region of the possible disks */
	used = 0;
	extent_cnt = 0;
	cnt = 0;
	for (dl = super->disks; dl; dl = dl->next) {
		dl->raiddisk = -1;

		if (dl->index >= 0)
			used++;

		/* don't activate new spares if we are orom constrained
		 * and there is already a volume active in the container
		 */
		if (super->orom && dl->index < 0 && mpb->num_raid_devs)
			continue;

		e = get_extents(super, dl);
		if (!e)
			continue;
		for (i = 1; e[i-1].size; i++)
			;
		dl->e = e;
		dl->extent_cnt = i;
		extent_cnt += i;
		cnt++;
	}

	maxsize = merge_extents(super, extent_cnt);
	minsize = size;
	if (size == 0)
		minsize = chunk;

	if (cnt < raiddisks ||
	    (super->orom && used && used != raiddisks) ||
	    maxsize < minsize ||
	    maxsize == 0) {
		fprintf(stderr, Name ": not enough devices with space to create array.\n");
		return 0; /* No enough free spaces large enough */
	}

	if (size == 0) {
		size = maxsize;
		if (chunk) {
			size /= chunk;
			size *= chunk;
		}
	}

	cnt = 0;
	for (dl = super->disks; dl; dl = dl->next)
		if (dl->e)
			dl->raiddisk = cnt++;

	*freesize = size;

	return 1;
}

static int validate_geometry_imsm(struct supertype *st, int level, int layout,
				  int raiddisks, int chunk, unsigned long long size,
				  char *dev, unsigned long long *freesize,
				  int verbose)
{
	int fd, cfd;
	struct mdinfo *sra;
	int is_member = 0;

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
	
	if (!dev) {
		if (st->sb && freesize) {
			/* we are being asked to automatically layout a
			 * new volume based on the current contents of
			 * the container.  If the the parameters can be
			 * satisfied reserve_space will record the disks,
			 * start offset, and size of the volume to be
			 * created.  add_to_super and getinfo_super
			 * detect when autolayout is in progress.
			 */
			if (!validate_geometry_imsm_orom(st->sb, level, layout,
							 raiddisks, chunk,
							 verbose))
				return 0;
			return reserve_space(st, raiddisks, size, chunk, freesize);
		}
		return 1;
	}
	if (st->sb) {
		/* creating in a given container */
		return validate_geometry_imsm_volume(st, level, layout,
						     raiddisks, chunk, size,
						     dev, freesize, verbose);
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
	close(fd);
	if (cfd < 0) {
		if (verbose)
			fprintf(stderr, Name ": Cannot use %s: It is busy\n",
				dev);
		return 0;
	}
	sra = sysfs_read(cfd, 0, GET_VERSION);
	if (sra && sra->array.major_version == -1 &&
	    strcmp(sra->text_version, "imsm") == 0)
		is_member = 1;
	sysfs_free(sra);
	if (is_member) {
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
	}

	if (verbose)
		fprintf(stderr, Name ": failed container membership check\n");

	close(cfd);
	return 0;
}

static int default_chunk_imsm(struct supertype *st)
{
	struct intel_super *super = st->sb;

	if (!super->orom)
		return 0;

	return imsm_orom_default_chunk(super->orom);
}

static void handle_missing(struct intel_super *super, struct imsm_dev *dev);

static int kill_subarray_imsm(struct supertype *st)
{
	/* remove the subarray currently referenced by ->current_vol */
	__u8 i;
	struct intel_dev **dp;
	struct intel_super *super = st->sb;
	__u8 current_vol = super->current_vol;
	struct imsm_super *mpb = super->anchor;

	if (super->current_vol < 0)
		return 2;
	super->current_vol = -1; /* invalidate subarray cursor */

	/* block deletions that would change the uuid of active subarrays
	 *
	 * FIXME when immutable ids are available, but note that we'll
	 * also need to fixup the invalidated/active subarray indexes in
	 * mdstat
	 */
	for (i = 0; i < mpb->num_raid_devs; i++) {
		char subarray[4];

		if (i < current_vol)
			continue;
		sprintf(subarray, "%u", i);
		if (is_subarray_active(subarray, st->devname)) {
			fprintf(stderr,
				Name ": deleting subarray-%d would change the UUID of active subarray-%d, aborting\n",
				current_vol, i);

			return 2;
		}
	}

	if (st->update_tail) {
		struct imsm_update_kill_array *u = malloc(sizeof(*u));

		if (!u)
			return 2;
		u->type = update_kill_array;
		u->dev_idx = current_vol;
		append_metadata_update(st, u, sizeof(*u));

		return 0;
	}

	for (dp = &super->devlist; *dp;)
		if ((*dp)->index == current_vol) {
			*dp = (*dp)->next;
		} else {
			handle_missing(super, (*dp)->dev);
			if ((*dp)->index > current_vol)
				(*dp)->index--;
			dp = &(*dp)->next;
		}

	/* no more raid devices, all active components are now spares,
	 * but of course failed are still failed
	 */
	if (--mpb->num_raid_devs == 0) {
		struct dl *d;

		for (d = super->disks; d; d = d->next)
			if (d->index > -2) {
				d->index = -1;
				d->disk.status = SPARE_DISK;
			}
	}

	super->updates_pending++;

	return 0;
}

static int update_subarray_imsm(struct supertype *st, char *update, mddev_ident_t ident)
{
	/* update the subarray currently referenced by ->current_vol */
	struct intel_super *super = st->sb;
	struct imsm_super *mpb = super->anchor;

	if (super->current_vol < 0)
		return 2;

	if (strcmp(update, "name") == 0) {
		char *name = ident->name;

		if (is_subarray_active(st->subarray, st->devname)) {
			fprintf(stderr,
				Name ": Unable to update name of active subarray\n");
			return 2;
		}

		if (!check_name(super, name, 0))
			return 2;

		if (st->update_tail) {
			struct imsm_update_rename_array *u = malloc(sizeof(*u));

			if (!u)
				return 2;
			u->type = update_rename_array;
			u->dev_idx = super->current_vol;
			snprintf((char *) u->name, MAX_RAID_SERIAL_LEN, "%s", name);
			append_metadata_update(st, u, sizeof(*u));
		} else {
			struct imsm_dev *dev;
			int i;

			dev = get_imsm_dev(super, super->current_vol);
			snprintf((char *) dev->volume, MAX_RAID_SERIAL_LEN, "%s", name);
			for (i = 0; i < mpb->num_raid_devs; i++) {
				dev = get_imsm_dev(super, i);
				handle_missing(super, dev);
			}
			super->updates_pending++;
		}
	} else
		return 2;

	return 0;
}
#endif /* MDASSEMBLE */

static int is_rebuilding(struct imsm_dev *dev)
{
	struct imsm_map *migr_map;

	if (!dev->vol.migr_state)
		return 0;

	if (migr_type(dev) != MIGR_REBUILD)
		return 0;

	migr_map = get_imsm_map(dev, 1);

	if (migr_map->map_state == IMSM_T_STATE_DEGRADED)
		return 1;
	else
		return 0;
}

static void update_recovery_start(struct imsm_dev *dev, struct mdinfo *array)
{
	struct mdinfo *rebuild = NULL;
	struct mdinfo *d;
	__u32 units;

	if (!is_rebuilding(dev))
		return;

	/* Find the rebuild target, but punt on the dual rebuild case */
	for (d = array->devs; d; d = d->next)
		if (d->recovery_start == 0) {
			if (rebuild)
				return;
			rebuild = d;
		}

	if (!rebuild) {
		/* (?) none of the disks are marked with
		 * IMSM_ORD_REBUILD, so assume they are missing and the
		 * disk_ord_tbl was not correctly updated
		 */
		dprintf("%s: failed to locate out-of-sync disk\n", __func__);
		return;
	}

	units = __le32_to_cpu(dev->vol.curr_migr_unit);
	rebuild->recovery_start = units * blocks_per_migr_unit(dev);
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
		struct imsm_map *map = get_imsm_map(dev, 0);
		struct mdinfo *this;
		int slot;

		/* do not publish arrays that are in the middle of an
		 * unsupported migration
		 */
		if (dev->vol.migr_state &&
		    (migr_type(dev) == MIGR_GEN_MIGR ||
		     migr_type(dev) == MIGR_STATE_CHANGE)) {
			fprintf(stderr, Name ": cannot assemble volume '%.16s':"
				" unsupported migration in progress\n",
				dev->volume);
			continue;
		}

		this = malloc(sizeof(*this));
		if (!this) {
			fprintf(stderr, Name ": failed to allocate %zu bytes\n",
				sizeof(*this));
			break;
		}
		memset(this, 0, sizeof(*this));
		this->next = rest;

		super->current_vol = i;
		getinfo_super_imsm_volume(st, this);
		for (slot = 0 ; slot <  map->num_members; slot++) {
			unsigned long long recovery_start;
			struct mdinfo *info_d;
			struct dl *d;
			int idx;
			int skip;
			__u32 ord;

			skip = 0;
			idx = get_imsm_disk_idx(dev, slot);
			ord = get_imsm_ord_tbl_ent(dev, slot); 
			for (d = super->disks; d ; d = d->next)
				if (d->index == idx)
					break;

			recovery_start = MaxSector;
			if (d == NULL)
				skip = 1;
			if (d && is_failed(&d->disk))
				skip = 1;
			if (ord & IMSM_ORD_REBUILD)
				recovery_start = 0;

			/* 
			 * if we skip some disks the array will be assmebled degraded;
			 * reset resync start to avoid a dirty-degraded
			 * situation when performing the intial sync
			 *
			 * FIXME handle dirty degraded
			 */
			if ((skip || recovery_start == 0) && !dev->vol.dirty)
				this->resync_start = MaxSector;
			if (skip)
				continue;

			info_d = calloc(1, sizeof(*info_d));
			if (!info_d) {
				fprintf(stderr, Name ": failed to allocate disk"
					" for volume %.16s\n", dev->volume);
				info_d = this->devs;
				while (info_d) {
					struct mdinfo *d = info_d->next;

					free(info_d);
					info_d = d;
				}
				free(this);
				this = rest;
				break;
			}
			info_d->next = this->devs;
			this->devs = info_d;

			info_d->disk.number = d->index;
			info_d->disk.major = d->major;
			info_d->disk.minor = d->minor;
			info_d->disk.raid_disk = slot;
			info_d->recovery_start = recovery_start;

			if (info_d->recovery_start == MaxSector)
				this->array.working_disks++;

			info_d->events = __le32_to_cpu(mpb->generation_num);
			info_d->data_offset = __le32_to_cpu(map->pba_of_lba0);
			info_d->component_size = __le32_to_cpu(map->blocks_per_member);
		}
		/* now that the disk list is up-to-date fixup recovery_start */
		update_recovery_start(dev, this);
		rest = this;
	}

	return rest;
}


static __u8 imsm_check_degraded(struct intel_super *super, struct imsm_dev *dev, int failed)
{
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
		 * check to see if any mirrors have failed, otherwise we
		 * are degraded.  Even numbered slots are mirrored on
		 * slot+1
		 */
		int i;
		/* gcc -Os complains that this is unused */
		int insync = insync;

		for (i = 0; i < map->num_members; i++) {
			__u32 ord = get_imsm_ord_tbl_ent(dev, i);
			int idx = ord_to_idx(ord);
			struct imsm_disk *disk;

			/* reset the potential in-sync count on even-numbered
			 * slots.  num_copies is always 2 for imsm raid10 
			 */
			if ((i & 1) == 0)
				insync = 2;

			disk = get_imsm_disk(super, idx);
			if (!disk || is_failed(disk) || ord & IMSM_ORD_REBUILD)
				insync--;

			/* no in-sync disks left in this mirror the
			 * array has failed
			 */
			if (insync == 0)
				return IMSM_T_STATE_FAILED;
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

static int imsm_count_failed(struct intel_super *super, struct imsm_dev *dev)
{
	int i;
	int failed = 0;
	struct imsm_disk *disk;
	struct imsm_map *map = get_imsm_map(dev, 0);
	struct imsm_map *prev = get_imsm_map(dev, dev->vol.migr_state);
	__u32 ord;
	int idx;

	/* at the beginning of migration we set IMSM_ORD_REBUILD on
	 * disks that are being rebuilt.  New failures are recorded to
	 * map[0].  So we look through all the disks we started with and
	 * see if any failures are still present, or if any new ones
	 * have arrived
	 *
	 * FIXME add support for online capacity expansion and
	 * raid-level-migration
	 */
	for (i = 0; i < prev->num_members; i++) {
		ord = __le32_to_cpu(prev->disk_ord_tbl[i]);
		ord |= __le32_to_cpu(map->disk_ord_tbl[i]);
		idx = ord_to_idx(ord);

		disk = get_imsm_disk(super, idx);
		if (!disk || is_failed(disk) || ord & IMSM_ORD_REBUILD)
			failed++;
	}

	return failed;
}

#ifndef MDASSEMBLE
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

static int is_resyncing(struct imsm_dev *dev)
{
	struct imsm_map *migr_map;

	if (!dev->vol.migr_state)
		return 0;

	if (migr_type(dev) == MIGR_INIT ||
	    migr_type(dev) == MIGR_REPAIR)
		return 1;

	migr_map = get_imsm_map(dev, 1);

	if (migr_map->map_state == IMSM_T_STATE_NORMAL)
		return 1;
	else
		return 0;
}

/* return true if we recorded new information */
static int mark_failure(struct imsm_dev *dev, struct imsm_disk *disk, int idx)
{
	__u32 ord;
	int slot;
	struct imsm_map *map;

	/* new failures are always set in map[0] */
	map = get_imsm_map(dev, 0);

	slot = get_imsm_disk_slot(map, idx);
	if (slot < 0)
		return 0;

	ord = __le32_to_cpu(map->disk_ord_tbl[slot]);
	if (is_failed(disk) && (ord & IMSM_ORD_REBUILD))
		return 0;

	disk->status |= FAILED_DISK;
	disk->status &= ~CONFIGURED_DISK;
	set_imsm_ord_tbl_ent(map, slot, idx | IMSM_ORD_REBUILD);
	if (map->failed_disk_num == 0xff)
		map->failed_disk_num = slot;
	return 1;
}

static void mark_missing(struct imsm_dev *dev, struct imsm_disk *disk, int idx)
{
	mark_failure(dev, disk, idx);

	if (disk->scsi_id == __cpu_to_le32(~(__u32)0))
		return;

	disk->scsi_id = __cpu_to_le32(~(__u32)0);
	memmove(&disk->serial[0], &disk->serial[1], MAX_RAID_SERIAL_LEN - 1);
}

static void handle_missing(struct intel_super *super, struct imsm_dev *dev)
{
	__u8 map_state;
	struct dl *dl;
	int failed;

	if (!super->missing)
		return;
	failed = imsm_count_failed(super, dev);
	map_state = imsm_check_degraded(super, dev, failed);

	dprintf("imsm: mark missing\n");
	end_migration(dev, map_state);
	for (dl = super->missing; dl; dl = dl->next)
		mark_missing(dev, &dl->disk, dl->index);
	super->updates_pending++;
}

/* Handle dirty -> clean transititions and resync.  Degraded and rebuild
 * states are handled in imsm_set_disk() with one exception, when a
 * resync is stopped due to a new failure this routine will set the
 * 'degraded' state for the array.
 */
static int imsm_set_array_state(struct active_array *a, int consistent)
{
	int inst = a->info.container_member;
	struct intel_super *super = a->container->sb;
	struct imsm_dev *dev = get_imsm_dev(super, inst);
	struct imsm_map *map = get_imsm_map(dev, 0);
	int failed = imsm_count_failed(super, dev);
	__u8 map_state = imsm_check_degraded(super, dev, failed);
	__u32 blocks_per_unit;

	/* before we activate this array handle any missing disks */
	if (consistent == 2)
		handle_missing(super, dev);

	if (consistent == 2 &&
	    (!is_resync_complete(&a->info) ||
	     map_state != IMSM_T_STATE_NORMAL ||
	     dev->vol.migr_state))
		consistent = 0;

	if (is_resync_complete(&a->info)) {
		/* complete intialization / resync,
		 * recovery and interrupted recovery is completed in
		 * ->set_disk
		 */
		if (is_resyncing(dev)) {
			dprintf("imsm: mark resync done\n");
			end_migration(dev, map_state);
			super->updates_pending++;
			a->last_checkpoint = 0;
		}
	} else if (!is_resyncing(dev) && !failed) {
		/* mark the start of the init process if nothing is failed */
		dprintf("imsm: mark resync start\n");
		if (map->map_state == IMSM_T_STATE_UNINITIALIZED)
			migrate(dev, IMSM_T_STATE_NORMAL, MIGR_INIT);
		else
			migrate(dev, IMSM_T_STATE_NORMAL, MIGR_REPAIR);
		super->updates_pending++;
	}

	/* check if we can update curr_migr_unit from resync_start, recovery_start */
	blocks_per_unit = blocks_per_migr_unit(dev);
	if (blocks_per_unit) {
		__u32 units32;
		__u64 units;

		units = a->last_checkpoint / blocks_per_unit;
		units32 = units;

		/* check that we did not overflow 32-bits, and that
		 * curr_migr_unit needs updating
		 */
		if (units32 == units &&
		    __le32_to_cpu(dev->vol.curr_migr_unit) != units32) {
			dprintf("imsm: mark checkpoint (%u)\n", units32);
			dev->vol.curr_migr_unit = __cpu_to_le32(units32);
			super->updates_pending++;
		}
	}

	/* mark dirty / clean */
	if (dev->vol.dirty != !consistent) {
		dprintf("imsm: mark '%s'\n", consistent ? "clean" : "dirty");
		if (consistent)
			dev->vol.dirty = 0;
		else
			dev->vol.dirty = 1;
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
	int failed;
	__u32 ord;
	__u8 map_state;

	if (n > map->num_members)
		fprintf(stderr, "imsm: set_disk %d out of range 0..%d\n",
			n, map->num_members - 1);

	if (n < 0)
		return;

	dprintf("imsm: set_disk %d:%x\n", n, state);

	ord = get_imsm_ord_tbl_ent(dev, n);
	disk = get_imsm_disk(super, ord_to_idx(ord));

	/* check for new failures */
	if (state & DS_FAULTY) {
		if (mark_failure(dev, disk, ord_to_idx(ord)))
			super->updates_pending++;
	}

	/* check if in_sync */
	if (state & DS_INSYNC && ord & IMSM_ORD_REBUILD && is_rebuilding(dev)) {
		struct imsm_map *migr_map = get_imsm_map(dev, 1);

		set_imsm_ord_tbl_ent(migr_map, n, ord_to_idx(ord));
		super->updates_pending++;
	}

	failed = imsm_count_failed(super, dev);
	map_state = imsm_check_degraded(super, dev, failed);

	/* check if recovery complete, newly degraded, or failed */
	if (map_state == IMSM_T_STATE_NORMAL && is_rebuilding(dev)) {
		end_migration(dev, map_state);
		map = get_imsm_map(dev, 0);
		map->failed_disk_num = ~0;
		super->updates_pending++;
		a->last_checkpoint = 0;
	} else if (map_state == IMSM_T_STATE_DEGRADED &&
		   map->map_state != map_state &&
		   !dev->vol.migr_state) {
		dprintf("imsm: mark degraded\n");
		map->map_state = map_state;
		super->updates_pending++;
		a->last_checkpoint = 0;
	} else if (map_state == IMSM_T_STATE_FAILED &&
		   map->map_state != map_state) {
		dprintf("imsm: mark failed\n");
		end_migration(dev, map_state);
		super->updates_pending++;
		a->last_checkpoint = 0;
	}
}

static int store_imsm_mpb(int fd, struct imsm_super *mpb)
{
	void *buf = mpb;
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

		if ((unsigned long long)write(fd, buf + 512, 512 * sectors)
		    != 512 * sectors)
			return 1;
	}

	/* first block is stored on second to last sector of the disk */
	if (lseek64(fd, dsize - (512 * 2), SEEK_SET) < 0)
		return 1;

	if (write(fd, buf, 512) != 512)
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
	int i = get_imsm_disk_idx(dev, idx);
	struct dl *dl;

	for (dl = super->disks; dl; dl = dl->next)
		if (dl->index == i)
			break;

	if (dl && is_failed(&dl->disk))
		dl = NULL;

	if (dl)
		dprintf("%s: found %x:%x\n", __func__, dl->major, dl->minor);

	return dl;
}

static struct dl *imsm_add_spare(struct intel_super *super, int slot,
				 struct active_array *a, int activate_new)
{
	struct imsm_dev *dev = get_imsm_dev(super, a->info.container_member);
	int idx = get_imsm_disk_idx(dev, slot);
	struct imsm_super *mpb = super->anchor;
	struct imsm_map *map;
	unsigned long long pos;
	struct mdinfo *d;
	struct extent *ex;
	int i, j;
	int found;
	__u32 array_start = 0;
	__u32 array_end = 0;
	struct dl *dl;

	for (dl = super->disks; dl; dl = dl->next) {
		/* If in this array, skip */
		for (d = a->info.devs ; d ; d = d->next)
			if (d->state_fd >= 0 &&
			    d->disk.major == dl->major &&
			    d->disk.minor == dl->minor) {
				dprintf("%x:%x already in array\n", dl->major, dl->minor);
				break;
			}
		if (d)
			continue;

		/* skip in use or failed drives */
		if (is_failed(&dl->disk) || idx == dl->index ||
		    dl->index == -2) {
			dprintf("%x:%x status (failed: %d index: %d)\n",
				dl->major, dl->minor, is_failed(&dl->disk), idx);
			continue;
		}

		/* skip pure spares when we are looking for partially
		 * assimilated drives
		 */
		if (dl->index == -1 && !activate_new)
			continue;

		/* Does this unused device have the requisite free space?
		 * It needs to be able to cover all member volumes
		 */
		ex = get_extents(super, dl);
		if (!ex) {
			dprintf("cannot get extents\n");
			continue;
		}
		for (i = 0; i < mpb->num_raid_devs; i++) {
			dev = get_imsm_dev(super, i);
			map = get_imsm_map(dev, 0);

			/* check if this disk is already a member of
			 * this array
			 */
			if (get_imsm_disk_slot(map, dl->index) >= 0)
				continue;

			found = 0;
			j = 0;
			pos = 0;
			array_start = __le32_to_cpu(map->pba_of_lba0);
			array_end = array_start +
				    __le32_to_cpu(map->blocks_per_member) - 1;

			do {
				/* check that we can start at pba_of_lba0 with
				 * blocks_per_member of space
				 */
				if (array_start >= pos && array_end < ex[j].start) {
					found = 1;
					break;
				}
				pos = ex[j].start + ex[j].size;
				j++;
			} while (ex[j-1].size);

			if (!found)
				break;
		}

		free(ex);
		if (i < mpb->num_raid_devs) {
			dprintf("%x:%x does not have %u to %u available\n",
				dl->major, dl->minor, array_start, array_end);
			/* No room */
			continue;
		}
		return dl;
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
	if (imsm_check_degraded(super, dev, failed) != IMSM_T_STATE_DEGRADED)
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
		 * OK, this device needs recovery.  Try to re-add the
		 * previous occupant of this slot, if this fails see if
		 * we can continue the assimilation of a spare that was
		 * partially assimilated, finally try to activate a new
		 * spare.
		 */
		dl = imsm_readd(super, i, a);
		if (!dl)
			dl = imsm_add_spare(super, i, a, 0);
		if (!dl)
			dl = imsm_add_spare(super, i, a, 1);
		if (!dl)
			continue;
 
		/* found a usable disk with enough space */
		di = malloc(sizeof(*di));
		if (!di)
			continue;
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
		di->recovery_start = 0;
		di->data_offset = __le32_to_cpu(map->pba_of_lba0);
		di->component_size = a->info.component_size;
		di->container_member = inst;
		super->random = random32();
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
	if (mu) {
		mu->buf = malloc(sizeof(struct imsm_update_activate_spare) * num_spares);
		if (mu->buf == NULL) {
			free(mu);
			mu = NULL;
		}
	}
	if (!mu) {
		while (rv) {
			struct mdinfo *n = rv->next;

			free(rv);
			rv = n;
		}
		return NULL;
	}
			
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

static int disks_overlap(struct intel_super *super, int idx, struct imsm_update_create_array *u)
{
	struct imsm_dev *dev = get_imsm_dev(super, idx);
	struct imsm_map *map = get_imsm_map(dev, 0);
	struct imsm_map *new_map = get_imsm_map(&u->dev, 0);
	struct disk_info *inf = get_disk_info(u);
	struct imsm_disk *disk;
	int i;
	int j;

	for (i = 0; i < map->num_members; i++) {
		disk = get_imsm_disk(super, get_imsm_disk_idx(dev, i));
		for (j = 0; j < new_map->num_members; j++)
			if (serialcmp(disk->serial, inf[j].serial) == 0)
				return 1;
	}

	return 0;
}

static void imsm_delete(struct intel_super *super, struct dl **dlp, unsigned index);

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
		struct imsm_map *migr_map;
		struct active_array *a;
		struct imsm_disk *disk;
		__u8 to_state;
		struct dl *dl;
		unsigned int found;
		int failed;
		int victim = get_imsm_disk_idx(dev, u->slot);
		int i;

		for (dl = super->disks; dl; dl = dl->next)
			if (dl == u->dl)
				break;

		if (!dl) {
			fprintf(stderr, "error: imsm_activate_spare passed "
				"an unknown disk (index: %d)\n",
				u->dl->index);
			return;
		}

		super->updates_pending++;

		/* count failures (excluding rebuilds and the victim)
		 * to determine map[0] state
		 */
		failed = 0;
		for (i = 0; i < map->num_members; i++) {
			if (i == u->slot)
				continue;
			disk = get_imsm_disk(super, get_imsm_disk_idx(dev, i));
			if (!disk || is_failed(disk))
				failed++;
		}

		/* adding a pristine spare, assign a new index */
		if (dl->index < 0) {
			dl->index = super->anchor->num_disks;
			super->anchor->num_disks++;
		}
		disk = &dl->disk;
		disk->status |= CONFIGURED_DISK;
		disk->status &= ~SPARE_DISK;

		/* mark rebuild */
		to_state = imsm_check_degraded(super, dev, failed);
		map->map_state = IMSM_T_STATE_DEGRADED;
		migrate(dev, to_state, MIGR_REBUILD);
		migr_map = get_imsm_map(dev, 1);
		set_imsm_ord_tbl_ent(map, u->slot, dl->index);
		set_imsm_ord_tbl_ent(migr_map, u->slot, dl->index | IMSM_ORD_REBUILD);

		/* update the family_num to mark a new container
		 * generation, being careful to record the existing
		 * family_num in orig_family_num to clean up after
		 * earlier mdadm versions that neglected to set it.
		 */
		if (mpb->orig_family_num == 0)
			mpb->orig_family_num = mpb->family_num;
		mpb->family_num += super->random;

		/* count arrays using the victim in the metadata */
		found = 0;
		for (a = st->arrays; a ; a = a->next) {
			dev = get_imsm_dev(super, a->info.container_member);
			map = get_imsm_map(dev, 0);

			if (get_imsm_disk_slot(map, victim) >= 0)
				found++;
		}

		/* delete the victim if it is no longer being
		 * utilized anywhere
		 */
		if (!found) {
			struct dl **dlp;

			/* We know that 'manager' isn't touching anything,
			 * so it is safe to delete
			 */
			for (dlp = &super->disks; *dlp; dlp = &(*dlp)->next)
				if ((*dlp)->index == victim)
					break;

			/* victim may be on the missing list */
			if (!*dlp)
				for (dlp = &super->missing; *dlp; dlp = &(*dlp)->next)
					if ((*dlp)->index == victim)
						break;
			imsm_delete(super, dlp, victim);
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
		struct intel_dev *dv;
		struct imsm_dev *dev;
		struct imsm_map *map, *new_map;
		unsigned long long start, end;
		unsigned long long new_start, new_end;
		int i;
		struct disk_info *inf;
		struct dl *dl;

		/* handle racing creates: first come first serve */
		if (u->dev_idx < mpb->num_raid_devs) {
			dprintf("%s: subarray %d already defined\n",
				__func__, u->dev_idx);
			goto create_error;
		}

		/* check update is next in sequence */
		if (u->dev_idx != mpb->num_raid_devs) {
			dprintf("%s: can not create array %d expected index %d\n",
				__func__, u->dev_idx, mpb->num_raid_devs);
			goto create_error;
		}

		new_map = get_imsm_map(&u->dev, 0);
		new_start = __le32_to_cpu(new_map->pba_of_lba0);
		new_end = new_start + __le32_to_cpu(new_map->blocks_per_member);
		inf = get_disk_info(u);

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
				/* overlap */;
			else
				continue;

			if (disks_overlap(super, i, u)) {
				dprintf("%s: arrays overlap\n", __func__);
				goto create_error;
			}
		}

		/* check that prepare update was successful */
		if (!update->space) {
			dprintf("%s: prepare update failed\n", __func__);
			goto create_error;
		}

		/* check that all disks are still active before committing
		 * changes.  FIXME: could we instead handle this by creating a
		 * degraded array?  That's probably not what the user expects,
		 * so better to drop this update on the floor.
		 */
		for (i = 0; i < new_map->num_members; i++) {
			dl = serial_to_dl(inf[i].serial, super);
			if (!dl) {
				dprintf("%s: disk disappeared\n", __func__);
				goto create_error;
			}
		}

		super->updates_pending++;

		/* convert spares to members and fixup ord_tbl */
		for (i = 0; i < new_map->num_members; i++) {
			dl = serial_to_dl(inf[i].serial, super);
			if (dl->index == -1) {
				dl->index = mpb->num_disks;
				mpb->num_disks++;
				dl->disk.status |= CONFIGURED_DISK;
				dl->disk.status &= ~SPARE_DISK;
			}
			set_imsm_ord_tbl_ent(new_map, i, dl->index);
		}

		dv = update->space;
		dev = dv->dev;
		update->space = NULL;
		imsm_copy_dev(dev, &u->dev);
		dv->index = u->dev_idx;
		dv->next = super->devlist;
		super->devlist = dv;
		mpb->num_raid_devs++;

		imsm_update_version_info(super);
		break;
 create_error:
		/* mdmon knows how to release update->space, but not
		 * ((struct intel_dev *) update->space)->dev
		 */
		if (update->space) {
			dv = update->space;
			free(dv->dev);
		}
		break;
	}
	case update_kill_array: {
		struct imsm_update_kill_array *u = (void *) update->buf;
		int victim = u->dev_idx;
		struct active_array *a;
		struct intel_dev **dp;
		struct imsm_dev *dev;

		/* sanity check that we are not affecting the uuid of
		 * active arrays, or deleting an active array
		 *
		 * FIXME when immutable ids are available, but note that
		 * we'll also need to fixup the invalidated/active
		 * subarray indexes in mdstat
		 */
		for (a = st->arrays; a; a = a->next)
			if (a->info.container_member >= victim)
				break;
		/* by definition if mdmon is running at least one array
		 * is active in the container, so checking
		 * mpb->num_raid_devs is just extra paranoia
		 */
		dev = get_imsm_dev(super, victim);
		if (a || !dev || mpb->num_raid_devs == 1) {
			dprintf("failed to delete subarray-%d\n", victim);
			break;
		}

		for (dp = &super->devlist; *dp;)
			if ((*dp)->index == (unsigned)super->current_vol) {
				*dp = (*dp)->next;
			} else {
				if ((*dp)->index > (unsigned)victim)
					(*dp)->index--;
				dp = &(*dp)->next;
			}
		mpb->num_raid_devs--;
		super->updates_pending++;
		break;
	}
	case update_rename_array: {
		struct imsm_update_rename_array *u = (void *) update->buf;
		char name[MAX_RAID_SERIAL_LEN+1];
		int target = u->dev_idx;
		struct active_array *a;
		struct imsm_dev *dev;

		/* sanity check that we are not affecting the uuid of
		 * an active array
		 */
		snprintf(name, MAX_RAID_SERIAL_LEN, "%s", (char *) u->name);
		name[MAX_RAID_SERIAL_LEN] = '\0';
		for (a = st->arrays; a; a = a->next)
			if (a->info.container_member == target)
				break;
		dev = get_imsm_dev(super, u->dev_idx);
		if (a || !dev || !check_name(super, name, 1)) {
			dprintf("failed to rename subarray-%d\n", target);
			break;
		}

		snprintf((char *) dev->volume, MAX_RAID_SERIAL_LEN, "%s", name);
		super->updates_pending++;
		break;
	}
	case update_add_disk:

		/* we may be able to repair some arrays if disks are
		 * being added */
		if (super->add) {
			struct active_array *a;

			super->updates_pending++;
 			for (a = st->arrays; a; a = a->next)
				a->check_degraded = 1;
		}
		/* add some spares to the metadata */
		while (super->add) {
			struct dl *al;

			al = super->add;
			super->add = al->next;
			al->next = super->disks;
			super->disks = al;
			dprintf("%s: added %x:%x\n",
				__func__, al->major, al->minor);
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
		struct intel_dev *dv;
		struct imsm_dev *dev = &u->dev;
		struct imsm_map *map = get_imsm_map(dev, 0);
		struct dl *dl;
		struct disk_info *inf;
		int i;
		int activate = 0;

		inf = get_disk_info(u);
		len = sizeof_imsm_dev(dev, 1);
		/* allocate a new super->devlist entry */
		dv = malloc(sizeof(*dv));
		if (dv) {
			dv->dev = malloc(len);
			if (dv->dev)
				update->space = dv;
			else {
				free(dv);
				update->space = NULL;
			}
		}

		/* count how many spares will be converted to members */
		for (i = 0; i < map->num_members; i++) {
			dl = serial_to_dl(inf[i].serial, super);
			if (!dl) {
				/* hmm maybe it failed?, nothing we can do about
				 * it here
				 */
				continue;
			}
			if (count_memberships(dl, super) == 0)
				activate++;
		}
		len += activate * sizeof(struct imsm_disk);
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
		if (posix_memalign(&super->next_buf, 512, buf_len) == 0)
			memset(super->next_buf, 0, buf_len);
		else
			super->next_buf = NULL;
	}
}

/* must be called while manager is quiesced */
static void imsm_delete(struct intel_super *super, struct dl **dlp, unsigned index)
{
	struct imsm_super *mpb = super->anchor;
	struct dl *iter;
	struct imsm_dev *dev;
	struct imsm_map *map;
	int i, j, num_members;
	__u32 ord;

	dprintf("%s: deleting device[%d] from imsm_super\n",
		__func__, index);

	/* shift all indexes down one */
	for (iter = super->disks; iter; iter = iter->next)
		if (iter->index > (int)index)
			iter->index--;
	for (iter = super->missing; iter; iter = iter->next)
		if (iter->index > (int)index)
			iter->index--;

	for (i = 0; i < mpb->num_raid_devs; i++) {
		dev = get_imsm_dev(super, i);
		map = get_imsm_map(dev, 0);
		num_members = map->num_members;
		for (j = 0; j < num_members; j++) {
			/* update ord entries being careful not to propagate
			 * ord-flags to the first map
			 */
			ord = get_imsm_ord_tbl_ent(dev, j);

			if (ord_to_idx(ord) <= index)
				continue;

			map = get_imsm_map(dev, 0);
			set_imsm_ord_tbl_ent(map, j, ord_to_idx(ord - 1));
			map = get_imsm_map(dev, 1);
			if (map)
				set_imsm_ord_tbl_ent(map, j, ord - 1);
		}
	}

	mpb->num_disks--;
	super->updates_pending++;
	if (*dlp) {
		struct dl *dl = *dlp;

		*dlp = (*dlp)->next;
		__free_imsm_disk(dl);
	}
}
#endif /* MDASSEMBLE */

struct superswitch super_imsm = {
#ifndef	MDASSEMBLE
	.examine_super	= examine_super_imsm,
	.brief_examine_super = brief_examine_super_imsm,
	.brief_examine_subarrays = brief_examine_subarrays_imsm,
	.export_examine_super = export_examine_super_imsm,
	.detail_super	= detail_super_imsm,
	.brief_detail_super = brief_detail_super_imsm,
	.write_init_super = write_init_super_imsm,
	.validate_geometry = validate_geometry_imsm,
	.default_chunk	= default_chunk_imsm,
	.add_to_super	= add_to_super_imsm,
	.detail_platform = detail_platform_imsm,
	.kill_subarray = kill_subarray_imsm,
	.update_subarray = update_subarray_imsm,
#endif
	.match_home	= match_home_imsm,
	.uuid_from_super= uuid_from_super_imsm,
	.getinfo_super  = getinfo_super_imsm,
	.update_super	= update_super_imsm,

	.avail_size	= avail_size_imsm,

	.compare_super	= compare_super_imsm,

	.load_super	= load_super_imsm,
	.init_super	= init_super_imsm,
	.store_super	= store_super_imsm,
	.free_super	= free_super_imsm,
	.match_metadata_desc = match_metadata_desc_imsm,
	.container_content = container_content_imsm,
	.default_layout = imsm_level_to_layout,

	.external	= 1,
	.name = "imsm",

#ifndef MDASSEMBLE
/* for mdmon */
	.open_new	= imsm_open_new,
	.set_array_state= imsm_set_array_state,
	.set_disk	= imsm_set_disk,
	.sync_metadata	= imsm_sync_metadata,
	.activate_spare = imsm_activate_spare,
	.process_update = imsm_process_update,
	.prepare_update = imsm_prepare_update,
#endif /* MDASSEMBLE */
};
