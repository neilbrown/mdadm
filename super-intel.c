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
#include "dlink.h"
#include "drive_encryption.h"
#include "sha1.h"
#include "platform-intel.h"
#include "xmalloc.h"

#include <ctype.h>
#include <dirent.h>
#include <scsi/scsi.h>
#include <scsi/sg.h>
#include <string.h>
#include <sys/ioctl.h>
#include <values.h>

/* MPB == Metadata Parameter Block */
#define MPB_SIGNATURE "Intel Raid ISM Cfg Sig. "
#define MPB_SIG_LEN (strlen(MPB_SIGNATURE))

/* Legacy IMSM versions:
 * MPB_VERSION_RAID0 1.0.00
 * MPB_VERSION_RAID1 1.1.00
 * MPB_VERSION_MANY_VOLUMES_PER_ARRAY 1.2.00
 * MPB_VERSION_3OR4_DISK_ARRAY 1.2.01
 * MPB_VERSION_RAID5 1.2.02
 * MPB_VERSION_5OR6_DISK_ARRAY 1.2.04
 * MPB_VERSION_CNG 1.2.06
 */

#define MPB_VERSION_ATTRIBS "1.3.00"
#define MPB_VERSION_ATTRIBS_JD "2.0.00"
#define MAX_SIGNATURE_LENGTH  32
#define MAX_RAID_SERIAL_LEN   16

/* supports RAID0 */
#define MPB_ATTRIB_RAID0		__cpu_to_le32(0x00000001)
/* supports RAID1 */
#define MPB_ATTRIB_RAID1		__cpu_to_le32(0x00000002)
/* supports RAID10 */
#define MPB_ATTRIB_RAID10		__cpu_to_le32(0x00000004)
/* supports RAID1E */
#define MPB_ATTRIB_RAID1E		__cpu_to_le32(0x00000008)
/* supports RAID5 */
#define MPB_ATTRIB_RAID5		__cpu_to_le32(0x00000010)
/* supports RAID CNG */
#define MPB_ATTRIB_RAIDCNG		__cpu_to_le32(0x00000020)
/* supports expanded stripe sizes of  256K, 512K and 1MB */
#define MPB_ATTRIB_EXP_STRIPE_SIZE	__cpu_to_le32(0x00000040)
/* supports RAID10 with more than 4 drives */
#define MPB_ATTRIB_RAID10_EXT		__cpu_to_le32(0x00000080)

/* The OROM Support RST Caching of Volumes */
#define MPB_ATTRIB_NVM			__cpu_to_le32(0x02000000)
/* The OROM supports creating disks greater than 2TB */
#define MPB_ATTRIB_2TB_DISK		__cpu_to_le32(0x04000000)
/* The OROM supports Bad Block Management */
#define MPB_ATTRIB_BBM			__cpu_to_le32(0x08000000)

/* THe OROM Supports NVM Caching of Volumes */
#define MPB_ATTRIB_NEVER_USE2           __cpu_to_le32(0x10000000)
/* The OROM supports creating volumes greater than 2TB */
#define MPB_ATTRIB_2TB			__cpu_to_le32(0x20000000)
/* originally for PMP, now it's wasted b/c. Never use this bit! */
#define MPB_ATTRIB_NEVER_USE		__cpu_to_le32(0x40000000)
/* Verify MPB contents against checksum after reading MPB */
#define MPB_ATTRIB_CHECKSUM_VERIFY	__cpu_to_le32(0x80000000)

/* Define all supported attributes that have to be accepted by mdadm
 */
#define MPB_ATTRIB_SUPPORTED	       (MPB_ATTRIB_CHECKSUM_VERIFY | \
					MPB_ATTRIB_2TB             | \
					MPB_ATTRIB_2TB_DISK        | \
					MPB_ATTRIB_RAID0           | \
					MPB_ATTRIB_RAID1           | \
					MPB_ATTRIB_RAID10          | \
					MPB_ATTRIB_RAID5           | \
					MPB_ATTRIB_EXP_STRIPE_SIZE | \
					MPB_ATTRIB_RAID10_EXT      | \
					MPB_ATTRIB_BBM)

/* Define attributes that are unused but not harmful */
#define MPB_ATTRIB_IGNORED		(MPB_ATTRIB_NEVER_USE)

#define MPB_SECTOR_CNT 2210
#define IMSM_RESERVED_SECTORS 8192
#define NUM_BLOCKS_DIRTY_STRIPE_REGION 2048
#define SECT_PER_MB_SHIFT 11
#define MAX_SECTOR_SIZE 4096
#define MULTIPLE_PPL_AREA_SIZE_IMSM (1024 * 1024) /* Size of the whole
						   * mutliple PPL area
						   */

/*
 * Internal Write-intent bitmap is stored in the same area where PPL.
 * Both features are mutually exclusive, so it is not an issue.
 * The first 8KiB of the area are reserved and shall not be used.
 */
#define IMSM_BITMAP_AREA_RESERVED_SIZE 8192

#define IMSM_BITMAP_HEADER_OFFSET (IMSM_BITMAP_AREA_RESERVED_SIZE)
#define IMSM_BITMAP_HEADER_SIZE MAX_SECTOR_SIZE

#define IMSM_BITMAP_START_OFFSET (IMSM_BITMAP_HEADER_OFFSET + IMSM_BITMAP_HEADER_SIZE)
#define IMSM_BITMAP_AREA_SIZE (MULTIPLE_PPL_AREA_SIZE_IMSM - IMSM_BITMAP_START_OFFSET)
#define IMSM_BITMAP_AND_HEADER_SIZE (IMSM_BITMAP_AREA_SIZE + IMSM_BITMAP_HEADER_SIZE)

#define IMSM_DEFAULT_BITMAP_CHUNKSIZE (64 * 1024 * 1024)
#define IMSM_DEFAULT_BITMAP_DAEMON_SLEEP 5

/*
 * This macro let's us ensure that no-one accidentally
 * changes the size of a struct
 */
#define ASSERT_SIZE(_struct, size) \
static inline void __assert_size_##_struct(void)	\
{							\
	switch (0) {					\
	case 0: break;					\
	case (sizeof(struct _struct) == size): break;	\
	}						\
}

/* Disk configuration info. */
#define IMSM_MAX_DEVICES 255
struct imsm_disk {
	__u8 serial[MAX_RAID_SERIAL_LEN];/* 0xD8 - 0xE7 ascii serial number */
	__u32 total_blocks_lo;		 /* 0xE8 - 0xEB total blocks lo */
	__u32 scsi_id;			 /* 0xEC - 0xEF scsi ID */
#define SPARE_DISK      __cpu_to_le32(0x01)  /* Spare */
#define CONFIGURED_DISK __cpu_to_le32(0x02)  /* Member of some RaidDev */
#define FAILED_DISK     __cpu_to_le32(0x04)  /* Permanent failure */
#define JOURNAL_DISK    __cpu_to_le32(0x2000000) /* Device marked as Journaling Drive */
	__u32 status;			 /* 0xF0 - 0xF3 */
	__u32 owner_cfg_num; /* which config 0,1,2... owns this disk */
	__u32 total_blocks_hi;		 /* 0xF4 - 0xF5 total blocks hi */
#define	IMSM_DISK_FILLERS	3
	__u32 filler[IMSM_DISK_FILLERS]; /* 0xF5 - 0x107 MPB_DISK_FILLERS for future expansion */
};
ASSERT_SIZE(imsm_disk, 48)

/* map selector for map managment
 */
#define MAP_0		0
#define MAP_1		1
#define MAP_X		-1

/* RAID map configuration infos. */
struct imsm_map {
	__u32 pba_of_lba0_lo;	/* start address of partition */
	__u32 blocks_per_member_lo;/* blocks per member */
	__u32 num_data_stripes_lo;	/* number of data stripes */
	__u16 blocks_per_strip;
	__u8  map_state;	/* Normal, Uninitialized, Degraded, Failed */
#define IMSM_T_STATE_NORMAL 0
#define IMSM_T_STATE_UNINITIALIZED 1
#define IMSM_T_STATE_DEGRADED 2
#define IMSM_T_STATE_FAILED 3
	__u8  raid_level;
#define IMSM_T_RAID0 0
#define IMSM_T_RAID1 1
#define IMSM_T_RAID5 5
#define IMSM_T_RAID10 10
	__u8  num_members;	/* number of member disks */
	__u8  num_domains;	/* number of parity domains */
	__u8  failed_disk_num;  /* valid only when state is degraded */
	__u8  ddf;
	__u32 pba_of_lba0_hi;
	__u32 blocks_per_member_hi;
	__u32 num_data_stripes_hi;
	__u32 filler[4];	/* expansion area */
#define IMSM_ORD_REBUILD (1 << 24)
	__u32 disk_ord_tbl[1];	/* disk_ord_tbl[num_members],
				 * top byte contains some flags
				 */
};
ASSERT_SIZE(imsm_map, 52)

struct imsm_vol {
	__u32 curr_migr_unit_lo;
	__u32 checkpoint_id;	/* id to access curr_migr_unit */
#define MIGR_STATE_NORMAL 0
#define MIGR_STATE_MIGRATING 1
	__u8  migr_state;	/* Normal or Migrating */
#define MIGR_INIT 0
#define MIGR_REBUILD 1
#define MIGR_VERIFY 2 /* analagous to echo check > sync_action */
#define MIGR_GEN_MIGR 3
#define MIGR_STATE_CHANGE 4
#define MIGR_REPAIR 5
	__u8  migr_type;	/* Initializing, Rebuilding, ... */
#define RAIDVOL_CLEAN          0
#define RAIDVOL_DIRTY          1
#define RAIDVOL_DSRECORD_VALID 2
	__u8  dirty;
	__u8  fs_state;		/* fast-sync state for CnG (0xff == disabled) */
	__u16 verify_errors;	/* number of mismatches */
	__u16 bad_blocks;	/* number of bad blocks during verify */
	__u32 curr_migr_unit_hi;
	__u32 filler[3];
	struct imsm_map map[1];
	/* here comes another one if migr_state */
};
ASSERT_SIZE(imsm_vol, 84)

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
	__u16 my_vol_raid_dev_num; /* Used in Unique volume Id for this RaidDev */

	/* NVM_EN */
	__u8 nv_cache_mode;
	__u8 nv_cache_flags;

	/* Unique Volume Id of the NvCache Volume associated with this volume */
	__u32 nvc_vol_orig_family_num;
	__u16 nvc_vol_raid_dev_num;

#define RWH_OFF 0
#define RWH_DISTRIBUTED 1
#define RWH_JOURNALING_DRIVE 2
#define RWH_MULTIPLE_DISTRIBUTED 3
#define RWH_MULTIPLE_PPLS_JOURNALING_DRIVE 4
#define RWH_MULTIPLE_OFF 5
#define RWH_BITMAP 6
	__u8  rwh_policy; /* Raid Write Hole Policy */
	__u8  jd_serial[MAX_RAID_SERIAL_LEN]; /* Journal Drive serial number */
	__u8  filler1;

#define IMSM_DEV_FILLERS 3
	__u32 filler[IMSM_DEV_FILLERS];
	struct imsm_vol vol;
};
ASSERT_SIZE(imsm_dev, 164)

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
	__u16 num_raid_devs_created;	/* 0x4C - 0x4D Used for generating unique
					 * volume IDs for raid_dev created in this array
					 * (starts at 1)
					 */
	__u16 filler1;			/* 0x4E - 0x4F */
	__u64 creation_time;		/* 0x50 - 0x57 Array creation time */
#define IMSM_FILLERS 32
	__u32 filler[IMSM_FILLERS];	/* 0x58 - 0xD7 RAID_MPB_FILLERS */
	struct imsm_disk disk[1];	/* 0xD8 diskTbl[numDisks] */
	/* here comes imsm_dev[num_raid_devs] */
	/* here comes BBM logs */
};
ASSERT_SIZE(imsm_super, 264)

#define BBM_LOG_MAX_ENTRIES 254
#define BBM_LOG_MAX_LBA_ENTRY_VAL 256		/* Represents 256 LBAs */
#define BBM_LOG_SIGNATURE 0xabadb10c

struct bbm_log_block_addr {
	__u16 w1;
	__u32 dw1;
} __attribute__ ((__packed__));

struct bbm_log_entry {
	__u8 marked_count;		/* Number of blocks marked - 1 */
	__u8 disk_ordinal;		/* Disk entry within the imsm_super */
	struct bbm_log_block_addr defective_block_start;
} __attribute__ ((__packed__));

struct bbm_log {
	__u32 signature; /* 0xABADB10C */
	__u32 entry_count;
	struct bbm_log_entry marked_block_entries[BBM_LOG_MAX_ENTRIES];
};
ASSERT_SIZE(bbm_log, 2040)

static char *map_state_str[] = { "normal", "uninitialized", "degraded", "failed" };

#define BLOCKS_PER_KB	(1024/512)

#define RAID_DISK_RESERVED_BLOCKS_IMSM_HI 2209

#define GEN_MIGR_AREA_SIZE 2048 /* General Migration Copy Area size in blocks */

#define MIGR_REC_BUF_SECTORS 1 /* size of migr_record i/o buffer in sectors */
#define MIGR_REC_SECTOR_POSITION 1 /* migr_record position offset on disk,
			       * MIGR_REC_BUF_SECTORS <= MIGR_REC_SECTOR_POS
			       */

#define UNIT_SRC_NORMAL     0   /* Source data for curr_migr_unit must
				 *  be recovered using srcMap */
#define UNIT_SRC_IN_CP_AREA 1   /* Source data for curr_migr_unit has
				 *  already been migrated and must
				 *  be recovered from checkpoint area */

#define PPL_ENTRY_SPACE (128 * 1024) /* Size of single PPL, without the header */

struct migr_record {
	__u32 rec_status;	    /* Status used to determine how to restart
				     * migration in case it aborts
				     * in some fashion */
	__u32 curr_migr_unit_lo;    /* 0..numMigrUnits-1 */
	__u32 family_num;	    /* Family number of MPB
				     * containing the RaidDev
				     * that is migrating */
	__u32 ascending_migr;	    /* True if migrating in increasing
				     * order of lbas */
	__u32 blocks_per_unit;      /* Num disk blocks per unit of operation */
	__u32 dest_depth_per_unit;  /* Num member blocks each destMap
				     * member disk
				     * advances per unit-of-operation */
	__u32 ckpt_area_pba_lo;	    /* Pba of first block of ckpt copy area */
	__u32 dest_1st_member_lba_lo;	/* First member lba on first
					 * stripe of destination */
	__u32 num_migr_units_lo;    /* Total num migration units-of-op */
	__u32 post_migr_vol_cap;    /* Size of volume after
				     * migration completes */
	__u32 post_migr_vol_cap_hi; /* Expansion space for LBA64 */
	__u32 ckpt_read_disk_num;   /* Which member disk in destSubMap[0] the
				     * migration ckpt record was read from
				     * (for recovered migrations) */
	__u32 curr_migr_unit_hi;    /* 0..numMigrUnits-1 high order 32 bits */
	__u32 ckpt_area_pba_hi;	    /* Pba of first block of ckpt copy area
				     * high order 32 bits */
	__u32 dest_1st_member_lba_hi; /* First member lba on first stripe of
				       * destination - high order 32 bits */
	__u32 num_migr_units_hi;      /* Total num migration units-of-op
				       * high order 32 bits */
	__u32 filler[16];
};
ASSERT_SIZE(migr_record, 128)

/**
 * enum imsm_status - internal IMSM return values representation.
 * @STATUS_OK: function succeeded.
 * @STATUS_ERROR: General error ocurred (not specified).
 *
 * Typedefed to imsm_status_t.
 */
typedef enum imsm_status {
	IMSM_STATUS_ERROR = -1,
	IMSM_STATUS_OK = 0,
} imsm_status_t;

struct md_list {
	/* usage marker:
	 *  1: load metadata
	 *  2: metadata does not match
	 *  4: already checked
	 */
	int   used;
	char  *devname;
	int   found;
	int   container;
	dev_t st_rdev;
	struct md_list *next;
};

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

static unsigned int sector_count(__u32 bytes, unsigned int sector_size)
{
	return ROUND_UP(bytes, sector_size) / sector_size;
}

static unsigned int mpb_sectors(struct imsm_super *mpb,
					unsigned int sector_size)
{
	return sector_count(__le32_to_cpu(mpb->mpb_size), sector_size);
}

struct intel_dev {
	struct imsm_dev *dev;
	struct intel_dev *next;
	unsigned index;
};

struct intel_hba {
	enum sys_dev_type type;
	char *path;
	char *pci_id;
	struct intel_hba *next;
};

enum action {
	DISK_REMOVE = 1,
	DISK_ADD
};
/* internal representation of IMSM metadata */
struct intel_super {
	union {
		void *buf; /* O_DIRECT buffer for reading/writing metadata */
		struct imsm_super *anchor; /* immovable parameters */
	};
	union {
		void *migr_rec_buf; /* buffer for I/O operations */
		struct migr_record *migr_rec; /* migration record */
	};
	int clean_migration_record_by_mdmon; /* when reshape is switched to next
		array, it indicates that mdmon is allowed to clean migration
		record */
	size_t len; /* size of the 'buf' allocation */
	size_t extra_space; /* extra space in 'buf' that is not used yet */
	void *next_buf; /* for realloc'ing buf from the manager */
	size_t next_len;
	int updates_pending; /* count of pending updates for mdmon */
	int current_vol; /* index of raid device undergoing creation */
	unsigned long long create_offset; /* common start for 'current_vol' */
	__u32 random; /* random data for seeding new family numbers */
	struct intel_dev *devlist;
	unsigned int sector_size; /* sector size of used member drives */
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
		enum action action;
	} *disks, *current_disk;
	struct dl *disk_mgmt_list; /* list of disks to add/remove while mdmon
				      active */
	struct dl *missing; /* disks removed while we weren't looking */
	struct bbm_log *bbm_log;
	struct intel_hba *hba; /* device path of the raid controller for this metadata */
	const struct imsm_orom *orom; /* platform firmware support */
	struct intel_super *next; /* (temp) list for disambiguating family_num */
	struct md_bb bb;	/* memory for get_bad_blocks call */
};

struct intel_disk {
	struct imsm_disk disk;
	#define IMSM_UNKNOWN_OWNER (-1)
	int owner;
	struct intel_disk *next;
};

/**
 * struct extent - reserved space details.
 * @start: start offset.
 * @size: size of reservation, set to 0 for metadata reservation.
 * @vol: index of the volume, meaningful if &size is set.
 */
struct extent {
	unsigned long long start, size;
	int vol;
};

/* definitions of reshape process types */
enum imsm_reshape_type {
	CH_TAKEOVER,
	CH_MIGRATION,
	CH_ARRAY_SIZE,
	CH_ABORT
};

/* definition of messages passed to imsm_process_update */
enum imsm_update_type {
	update_activate_spare,
	update_create_array,
	update_kill_array,
	update_rename_array,
	update_add_remove_disk,
	update_reshape_container_disks,
	update_reshape_migration,
	update_takeover,
	update_general_migration_checkpoint,
	update_size_change,
	update_prealloc_badblocks_mem,
	update_rwh_policy,
};

struct imsm_update_activate_spare {
	enum imsm_update_type type;
	struct dl *dl;
	int slot;
	int array;
	struct imsm_update_activate_spare *next;
};

struct geo_params {
	char devnm[32];
	char *dev_name;
	unsigned long long size;
	int level;
	int layout;
	int chunksize;
	int raid_disks;
};

enum takeover_direction {
	R10_TO_R0,
	R0_TO_R10
};
struct imsm_update_takeover {
	enum imsm_update_type type;
	int subarray;
	enum takeover_direction direction;
};

struct imsm_update_reshape {
	enum imsm_update_type type;
	int old_raid_disks;
	int new_raid_disks;

	int new_disks[1]; /* new_raid_disks - old_raid_disks makedev number */
};

struct imsm_update_reshape_migration {
	enum imsm_update_type type;
	int old_raid_disks;
	int new_raid_disks;
	/* fields for array migration changes
	 */
	int subdev;
	int new_level;
	int new_layout;
	int new_chunksize;

	int new_disks[1]; /* new_raid_disks - old_raid_disks makedev number */
};

struct imsm_update_size_change {
	enum imsm_update_type type;
	int subdev;
	long long new_size;
};

struct imsm_update_general_migration_checkpoint {
	enum imsm_update_type type;
	__u64 curr_migr_unit;
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

struct imsm_update_add_remove_disk {
	enum imsm_update_type type;
};

struct imsm_update_prealloc_bb_mem {
	enum imsm_update_type type;
};

struct imsm_update_rwh_policy {
	enum imsm_update_type type;
	int new_policy;
	int dev_idx;
};

enum imsm_sku {
	SKU_NO_KEY = 0,
	SKU_STANDARD_KEY = 1,
	SKU_PREMIUM_KEY = 2,
	SKU_INTEL_SSD_ONLY_KEY = 3,
	SKU_RAID1_ONLY_KEY = 4
};

static const char *_sys_dev_type[] = {
	[SYS_DEV_UNKNOWN] = "Unknown",
	[SYS_DEV_SAS] = "SAS",
	[SYS_DEV_SATA] = "SATA",
	[SYS_DEV_NVME] = "NVMe",
	[SYS_DEV_VMD] = "VMD",
	[SYS_DEV_SATA_VMD] = "SATA VMD"
};

struct imsm_chunk_ops {
	uint chunk;
	char *chunk_str;
};

static const struct imsm_chunk_ops imsm_chunk_ops[] = {
	{IMSM_OROM_SSS_2kB, "2k"},
	{IMSM_OROM_SSS_4kB, "4k"},
	{IMSM_OROM_SSS_8kB, "8k"},
	{IMSM_OROM_SSS_16kB, "16k"},
	{IMSM_OROM_SSS_32kB, "32k"},
	{IMSM_OROM_SSS_64kB, "64k"},
	{IMSM_OROM_SSS_128kB, "128k"},
	{IMSM_OROM_SSS_256kB, "256k"},
	{IMSM_OROM_SSS_512kB, "512k"},
	{IMSM_OROM_SSS_1MB, "1M"},
	{IMSM_OROM_SSS_2MB, "2M"},
	{IMSM_OROM_SSS_4MB, "4M"},
	{IMSM_OROM_SSS_8MB, "8M"},
	{IMSM_OROM_SSS_16MB, "16M"},
	{IMSM_OROM_SSS_32MB, "32M"},
	{IMSM_OROM_SSS_64MB, "64M"},
	{0, NULL}
};

static int no_platform = -1;

static int check_no_platform(void)
{
	static const char search[] = "mdadm.imsm.test=1";
	FILE *fp;

	if (no_platform >= 0)
		return no_platform;

	if (check_env("IMSM_NO_PLATFORM")) {
		no_platform = 1;
		return 1;
	}
	fp = fopen("/proc/cmdline", "r");
	if (fp) {
		char *l = conf_line(fp);
		char *w = l;

		if (l == NULL) {
			fclose(fp);
			return 0;
		}

		do {
			if (strcmp(w, search) == 0)
				no_platform = 1;
			w = dl_next(w);
		} while (w != l);
		free_line(l);
		fclose(fp);
		if (no_platform >= 0)
			return no_platform;
	}
	no_platform = 0;
	return 0;
}

void imsm_set_no_platform(int v)
{
	no_platform = v;
}

const char *get_sys_dev_type(enum sys_dev_type type)
{
	if (type >= SYS_DEV_MAX)
		type = SYS_DEV_UNKNOWN;

	return _sys_dev_type[type];
}

static struct intel_hba * alloc_intel_hba(struct sys_dev *device)
{
	struct intel_hba *result = xmalloc(sizeof(*result));

	result->type = device->type;
	result->path = xstrdup(device->path);
	result->next = NULL;
	if (result->path && (result->pci_id = strrchr(result->path, '/')) != NULL)
		result->pci_id++;

	return result;
}

static struct intel_hba * find_intel_hba(struct intel_hba *hba, struct sys_dev *device)
{
	struct intel_hba *result;

	for (result = hba; result; result = result->next) {
		if (result->type == device->type && strcmp(result->path, device->path) == 0)
			break;
	}
	return result;
}

static int attach_hba_to_super(struct intel_super *super, struct sys_dev *device)
{
	struct intel_hba *hba;

	/* check if disk attached to Intel HBA */
	hba = find_intel_hba(super->hba, device);
	if (hba != NULL)
		return 1;
	/* Check if HBA is already attached to super */
	if (super->hba == NULL) {
		super->hba = alloc_intel_hba(device);
		return 1;
	}

	hba = super->hba;
	/* Intel metadata allows for all disks attached to the same type HBA.
	 * Do not support HBA types mixing
	 */
	if (device->type != hba->type)
		return 2;

	/* Multiple same type HBAs can be used if they share the same OROM */
	const struct imsm_orom *device_orom = get_orom_by_device_id(device->dev_id);

	if (device_orom != super->orom)
		return 2;

	while (hba->next)
		hba = hba->next;

	hba->next = alloc_intel_hba(device);
	return 1;
}

static struct sys_dev* find_disk_attached_hba(int fd, const char *devname)
{
	struct sys_dev *list, *elem;
	char *disk_path;

	if ((list = find_intel_devices()) == NULL)
		return 0;

	if (!is_fd_valid(fd))
		disk_path  = (char *) devname;
	else
		disk_path = diskfd_to_devpath(fd, 1, NULL);

	if (!disk_path)
		return 0;

	for (elem = list; elem; elem = elem->next)
		if (is_path_attached_to_hba(disk_path, elem->path))
			break;

	if (disk_path != devname)
		free(disk_path);

	return elem;
}

static int find_intel_hba_capability(int fd, struct intel_super *super,
				     char *devname);

static struct supertype *match_metadata_desc_imsm(char *arg)
{
	struct supertype *st;

	if (strcmp(arg, "imsm") != 0 &&
	    strcmp(arg, "default") != 0
		)
		return NULL;

	st = xcalloc(1, sizeof(*st));
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

/* retrieve the disk description based on a index of the disk
 * in the sub-array
 */
static struct dl *get_imsm_dl_disk(struct intel_super *super, __u8 index)
{
	struct dl *d;

	for (d = super->disks; d; d = d->next)
		if (d->index == index)
			return d;

	return NULL;
}
/* retrieve a disk from the parsed metadata */
static struct imsm_disk *get_imsm_disk(struct intel_super *super, __u8 index)
{
	struct dl *dl;

	dl = get_imsm_dl_disk(super, index);
	if (dl)
		return &dl->disk;

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
	/* A device can have 2 maps if it is in the middle of a migration.
	 * If second_map is:
	 *    MAP_0 - we return the first map
	 *    MAP_1 - we return the second map if it exists, else NULL
	 *    MAP_X - we return the second map if it exists, else the first
	 */
	struct imsm_map *map = &dev->vol.map[0];
	struct imsm_map *map2 = NULL;

	if (dev->vol.migr_state)
		map2 = (void *)map + sizeof_imsm_map(map);

	switch (second_map) {
	case MAP_0:
		break;
	case MAP_1:
		map = map2;
		break;
	case MAP_X:
		if (map2)
			map = map2;
		break;
	default:
		map = NULL;
	}
	return map;

}

/* return the size of the device.
 * migr_state increases the returned size if map[0] were to be duplicated
 */
static size_t sizeof_imsm_dev(struct imsm_dev *dev, int migr_state)
{
	size_t size = sizeof(*dev) - sizeof(struct imsm_map) +
		      sizeof_imsm_map(get_imsm_map(dev, MAP_0));

	/* migrating means an additional map */
	if (dev->vol.migr_state)
		size += sizeof_imsm_map(get_imsm_map(dev, MAP_1));
	else if (migr_state)
		size += sizeof_imsm_map(get_imsm_map(dev, MAP_0));

	return size;
}

/* retrieve disk serial number list from a metadata update */
static struct disk_info *get_disk_info(struct imsm_update_create_array *update)
{
	void *u = update;
	struct disk_info *inf;

	inf = u + sizeof(*update) - sizeof(struct imsm_dev) +
	      sizeof_imsm_dev(&update->dev, 0);

	return inf;
}

/**
 * __get_imsm_dev() - Get device with index from imsm_super.
 * @mpb: &imsm_super pointer, not NULL.
 * @index: Device index.
 *
 * Function works as non-NULL, aborting in such a case,
 * when NULL would be returned.
 *
 * Device index should be in range 0 up to num_raid_devs.
 * Function assumes the index was already verified.
 * Index must be valid, otherwise abort() is called.
 *
 * Return: Pointer to corresponding imsm_dev.
 *
 */
static struct imsm_dev *__get_imsm_dev(struct imsm_super *mpb, __u8 index)
{
	int offset;
	int i;
	void *_mpb = mpb;

	if (index >= mpb->num_raid_devs)
		goto error;

	/* devices start after all disks */
	offset = ((void *) &mpb->disk[mpb->num_disks]) - _mpb;

	for (i = 0; i <= index; i++, offset += sizeof_imsm_dev(_mpb + offset, 0))
		if (i == index)
			return _mpb + offset;
error:
	pr_err("cannot find imsm_dev with index %u in imsm_super\n", index);
	abort();
}

/**
 * get_imsm_dev() - Get device with index from intel_super.
 * @super: &intel_super pointer, not NULL.
 * @index: Device index.
 *
 * Function works as non-NULL, aborting in such a case,
 * when NULL would be returned.
 *
 * Device index should be in range 0 up to num_raid_devs.
 * Function assumes the index was already verified.
 * Index must be valid, otherwise abort() is called.
 *
 * Return: Pointer to corresponding imsm_dev.
 *
 */
static struct imsm_dev *get_imsm_dev(struct intel_super *super, __u8 index)
{
	struct intel_dev *dv;

	if (index >= super->anchor->num_raid_devs)
		goto error;

	for (dv = super->devlist; dv; dv = dv->next)
		if (dv->index == index)
			return dv->dev;
error:
	pr_err("cannot find imsm_dev with index %u in intel_super\n", index);
	abort();
}

static inline unsigned long long __le48_to_cpu(const struct bbm_log_block_addr
					       *addr)
{
	return ((((__u64)__le32_to_cpu(addr->dw1)) << 16) |
		__le16_to_cpu(addr->w1));
}

static inline struct bbm_log_block_addr __cpu_to_le48(unsigned long long sec)
{
	struct bbm_log_block_addr addr;

	addr.w1 =  __cpu_to_le16((__u16)(sec & 0xffff));
	addr.dw1 = __cpu_to_le32((__u32)(sec >> 16) & 0xffffffff);
	return addr;
}

/* get size of the bbm log */
static __u32 get_imsm_bbm_log_size(struct bbm_log *log)
{
	if (!log || log->entry_count == 0)
		return 0;

	return sizeof(log->signature) +
		sizeof(log->entry_count) +
		log->entry_count * sizeof(struct bbm_log_entry);
}

/* check if bad block is not partially stored in bbm log */
static int is_stored_in_bbm(struct bbm_log *log, const __u8 idx, const unsigned
			    long long sector, const int length, __u32 *pos)
{
	__u32 i;

	for (i = *pos; i < log->entry_count; i++) {
		struct bbm_log_entry *entry = &log->marked_block_entries[i];
		unsigned long long bb_start;
		unsigned long long bb_end;

		bb_start = __le48_to_cpu(&entry->defective_block_start);
		bb_end = bb_start + (entry->marked_count + 1);

		if ((entry->disk_ordinal == idx) && (bb_start >= sector) &&
		    (bb_end <= sector + length)) {
			*pos = i;
			return 1;
		}
	}
	return 0;
}

/* record new bad block in bbm log */
static int record_new_badblock(struct bbm_log *log, const __u8 idx, unsigned
			       long long sector, int length)
{
	int new_bb = 0;
	__u32 pos = 0;
	struct bbm_log_entry *entry = NULL;

	while (is_stored_in_bbm(log, idx, sector, length, &pos)) {
		struct bbm_log_entry *e = &log->marked_block_entries[pos];

		if ((e->marked_count + 1 == BBM_LOG_MAX_LBA_ENTRY_VAL) &&
		    (__le48_to_cpu(&e->defective_block_start) == sector)) {
			sector += BBM_LOG_MAX_LBA_ENTRY_VAL;
			length -= BBM_LOG_MAX_LBA_ENTRY_VAL;
			pos = pos + 1;
			continue;
		}
		entry = e;
		break;
	}

	if (entry) {
		int cnt = (length <= BBM_LOG_MAX_LBA_ENTRY_VAL) ? length :
			BBM_LOG_MAX_LBA_ENTRY_VAL;
		entry->defective_block_start = __cpu_to_le48(sector);
		entry->marked_count = cnt - 1;
		if (cnt == length)
			return 1;
		sector += cnt;
		length -= cnt;
	}

	new_bb = ROUND_UP(length, BBM_LOG_MAX_LBA_ENTRY_VAL) /
		BBM_LOG_MAX_LBA_ENTRY_VAL;
	if (log->entry_count + new_bb > BBM_LOG_MAX_ENTRIES)
		return 0;

	while (length > 0) {
		int cnt = (length <= BBM_LOG_MAX_LBA_ENTRY_VAL) ? length :
			BBM_LOG_MAX_LBA_ENTRY_VAL;
		struct bbm_log_entry *entry =
			&log->marked_block_entries[log->entry_count];

		entry->defective_block_start = __cpu_to_le48(sector);
		entry->marked_count = cnt - 1;
		entry->disk_ordinal = idx;

		sector += cnt;
		length -= cnt;

		log->entry_count++;
	}

	return new_bb;
}

/* clear all bad blocks for given disk */
static void clear_disk_badblocks(struct bbm_log *log, const __u8 idx)
{
	__u32 i = 0;

	while (i < log->entry_count) {
		struct bbm_log_entry *entries = log->marked_block_entries;

		if (entries[i].disk_ordinal == idx) {
			if (i < log->entry_count - 1)
				entries[i] = entries[log->entry_count - 1];
			log->entry_count--;
		} else {
			i++;
		}
	}
}

/* clear given bad block */
static int clear_badblock(struct bbm_log *log, const __u8 idx, const unsigned
			  long long sector, const int length) {
	__u32 i = 0;

	while (i < log->entry_count) {
		struct bbm_log_entry *entries = log->marked_block_entries;

		if ((entries[i].disk_ordinal == idx) &&
		    (__le48_to_cpu(&entries[i].defective_block_start) ==
		     sector) && (entries[i].marked_count + 1 == length)) {
			if (i < log->entry_count - 1)
				entries[i] = entries[log->entry_count - 1];
			log->entry_count--;
			break;
		}
		i++;
	}

	return 1;
}

/* allocate and load BBM log from metadata */
static int load_bbm_log(struct intel_super *super)
{
	struct imsm_super *mpb = super->anchor;
	__u32 bbm_log_size =  __le32_to_cpu(mpb->bbm_log_size);

	super->bbm_log = xcalloc(1, sizeof(struct bbm_log));
	if (!super->bbm_log)
		return 1;

	if (bbm_log_size) {
		struct bbm_log *log = (void *)mpb +
			__le32_to_cpu(mpb->mpb_size) - bbm_log_size;

		__u32 entry_count;

		if (bbm_log_size < sizeof(log->signature) +
		    sizeof(log->entry_count))
			return 2;

		entry_count = __le32_to_cpu(log->entry_count);
		if ((__le32_to_cpu(log->signature) != BBM_LOG_SIGNATURE) ||
		    (entry_count > BBM_LOG_MAX_ENTRIES))
			return 3;

		if (bbm_log_size !=
		    sizeof(log->signature) + sizeof(log->entry_count) +
		    entry_count * sizeof(struct bbm_log_entry))
			return 4;

		memcpy(super->bbm_log, log, bbm_log_size);
	} else {
		super->bbm_log->signature = __cpu_to_le32(BBM_LOG_SIGNATURE);
		super->bbm_log->entry_count = 0;
	}

	return 0;
}

/* checks if bad block is within volume boundaries */
static int is_bad_block_in_volume(const struct bbm_log_entry *entry,
			const unsigned long long start_sector,
			const unsigned long long size)
{
	unsigned long long bb_start;
	unsigned long long bb_end;

	bb_start = __le48_to_cpu(&entry->defective_block_start);
	bb_end = bb_start + (entry->marked_count + 1);

	if (((bb_start >= start_sector) && (bb_start < start_sector + size)) ||
	    ((bb_end >= start_sector) && (bb_end <= start_sector + size)))
		return 1;

	return 0;
}

/* get list of bad blocks on a drive for a volume */
static void get_volume_badblocks(const struct bbm_log *log, const __u8 idx,
			const unsigned long long start_sector,
			const unsigned long long size,
			struct md_bb *bbs)
{
	__u32 count = 0;
	__u32 i;

	for (i = 0; i < log->entry_count; i++) {
		const struct bbm_log_entry *ent =
			&log->marked_block_entries[i];
		struct md_bb_entry *bb;

		if ((ent->disk_ordinal == idx) &&
		    is_bad_block_in_volume(ent, start_sector, size)) {

			if (!bbs->entries) {
				bbs->entries = xmalloc(BBM_LOG_MAX_ENTRIES *
						     sizeof(*bb));
				if (!bbs->entries)
					break;
			}

			bb = &bbs->entries[count++];
			bb->sector = __le48_to_cpu(&ent->defective_block_start);
			bb->length = ent->marked_count + 1;
		}
	}
	bbs->count = count;
}

/*
 * for second_map:
 *  == MAP_0 get first map
 *  == MAP_1 get second map
 *  == MAP_X than get map according to the current migr_state
 */
static __u32 get_imsm_ord_tbl_ent(struct imsm_dev *dev,
				  int slot,
				  int second_map)
{
	struct imsm_map *map;

	map = get_imsm_map(dev, second_map);

	/* top byte identifies disk under rebuild */
	return __le32_to_cpu(map->disk_ord_tbl[slot]);
}

#define ord_to_idx(ord) (((ord) << 8) >> 8)
static __u32 get_imsm_disk_idx(struct imsm_dev *dev, int slot, int second_map)
{
	__u32 ord = get_imsm_ord_tbl_ent(dev, slot, second_map);

	return ord_to_idx(ord);
}

static void set_imsm_ord_tbl_ent(struct imsm_map *map, int slot, __u32 ord)
{
	map->disk_ord_tbl[slot] = __cpu_to_le32(ord);
}

static int get_imsm_disk_slot(struct imsm_map *map, const unsigned int idx)
{
	int slot;
	__u32 ord;

	for (slot = 0; slot < map->num_members; slot++) {
		ord = __le32_to_cpu(map->disk_ord_tbl[slot]);
		if (ord_to_idx(ord) == idx)
			return slot;
	}

	return IMSM_STATUS_ERROR;
}
/**
 * update_imsm_raid_level() - update raid level appropriately in &imsm_map.
 * @map:	&imsm_map pointer.
 * @new_level:	MD style level.
 *
 * For backward compatibility reasons we need to differentiate RAID10.
 * In the past IMSM RAID10 was presented as RAID1.
 * Keep compatibility unless it is not explicitly updated by UEFI driver.
 *
 * Routine needs num_members to be set and (optionally) raid_level.
 */
static void update_imsm_raid_level(struct imsm_map *map, int new_level)
{
	if (new_level != IMSM_T_RAID10) {
		map->raid_level = new_level;
		return;
	}

	if (map->num_members == 4) {
		if (map->raid_level == IMSM_T_RAID10 || map->raid_level == IMSM_T_RAID1)
			return;

		map->raid_level = IMSM_T_RAID1;
		return;
	}

	map->raid_level = IMSM_T_RAID10;
}

static int get_imsm_raid_level(struct imsm_map *map)
{
	if (map->raid_level == IMSM_T_RAID1) {
		if (map->num_members == 2)
			return IMSM_T_RAID1;
		else
			return IMSM_T_RAID10;
	}

	return map->raid_level;
}

/**
 * get_disk_slot_in_dev() - retrieve disk slot from &imsm_dev.
 * @super: &intel_super pointer, not NULL.
 * @dev_idx: imsm device index.
 * @idx: disk index.
 *
 * Return: Slot on success, IMSM_STATUS_ERROR otherwise.
 */
static int get_disk_slot_in_dev(struct intel_super *super, const __u8 dev_idx,
				const unsigned int idx)
{
	struct imsm_dev *dev = get_imsm_dev(super, dev_idx);
	struct imsm_map *map = get_imsm_map(dev, MAP_0);

	return get_imsm_disk_slot(map, idx);
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

	for (i = 0; i < super->anchor->num_raid_devs; i++)
		if (get_disk_slot_in_dev(super, i, dl->index) >= 0)
			memberships++;

	return memberships;
}

static __u32 imsm_min_reserved_sectors(struct intel_super *super);

static int split_ull(unsigned long long n, void *lo, void *hi)
{
	if (lo == 0 || hi == 0)
		return 1;
	__put_unaligned32(__cpu_to_le32((__u32)n), lo);
	__put_unaligned32(__cpu_to_le32((n >> 32)), hi);
	return 0;
}

static unsigned long long join_u32(__u32 lo, __u32 hi)
{
	return (unsigned long long)__le32_to_cpu(lo) |
	       (((unsigned long long)__le32_to_cpu(hi)) << 32);
}

static unsigned long long total_blocks(struct imsm_disk *disk)
{
	if (disk == NULL)
		return 0;
	return join_u32(disk->total_blocks_lo, disk->total_blocks_hi);
}

/**
 * imsm_num_data_members() - get data drives count for an array.
 * @map: Map to analyze.
 *
 * num_data_members value represents minimal count of drives for level.
 * The name of the property could be misleading for RAID5 with asymmetric layout
 * because some data required to be calculated from parity.
 * The property is extracted from level and num_members value.
 *
 * Return: num_data_members value on success, zero otherwise.
 */
static __u8 imsm_num_data_members(struct imsm_map *map)
{
	switch (get_imsm_raid_level(map)) {
	case 0:
		return map->num_members;
	case 1:
	case 10:
		return map->num_members / 2;
	case 5:
		return map->num_members - 1;
	default:
		dprintf("unsupported raid level\n");
		return 0;
	}
}

static unsigned long long pba_of_lba0(struct imsm_map *map)
{
	if (map == NULL)
		return 0;
	return join_u32(map->pba_of_lba0_lo, map->pba_of_lba0_hi);
}

static unsigned long long blocks_per_member(struct imsm_map *map)
{
	if (map == NULL)
		return 0;
	return join_u32(map->blocks_per_member_lo, map->blocks_per_member_hi);
}

static unsigned long long num_data_stripes(struct imsm_map *map)
{
	if (map == NULL)
		return 0;
	return join_u32(map->num_data_stripes_lo, map->num_data_stripes_hi);
}

static unsigned long long vol_curr_migr_unit(struct imsm_dev *dev)
{
	if (dev == NULL)
		return 0;

	return join_u32(dev->vol.curr_migr_unit_lo, dev->vol.curr_migr_unit_hi);
}

static unsigned long long imsm_dev_size(struct imsm_dev *dev)
{
	if (dev == NULL)
		return 0;
	return join_u32(dev->size_low, dev->size_high);
}

static unsigned long long migr_chkp_area_pba(struct migr_record *migr_rec)
{
	if (migr_rec == NULL)
		return 0;
	return join_u32(migr_rec->ckpt_area_pba_lo,
			migr_rec->ckpt_area_pba_hi);
}

static unsigned long long current_migr_unit(struct migr_record *migr_rec)
{
	if (migr_rec == NULL)
		return 0;
	return join_u32(migr_rec->curr_migr_unit_lo,
			migr_rec->curr_migr_unit_hi);
}

static unsigned long long migr_dest_1st_member_lba(struct migr_record *migr_rec)
{
	if (migr_rec == NULL)
		return 0;
	return join_u32(migr_rec->dest_1st_member_lba_lo,
			migr_rec->dest_1st_member_lba_hi);
}

static unsigned long long get_num_migr_units(struct migr_record *migr_rec)
{
	if (migr_rec == NULL)
		return 0;
	return join_u32(migr_rec->num_migr_units_lo,
			migr_rec->num_migr_units_hi);
}

static void set_total_blocks(struct imsm_disk *disk, unsigned long long n)
{
	split_ull(n, &disk->total_blocks_lo, &disk->total_blocks_hi);
}

/**
 * set_num_domains() - Set number of domains for an array.
 * @map: Map to be updated.
 *
 * num_domains property represents copies count of each data drive, thus make
 * it meaningful only for RAID1 and RAID10. IMSM supports two domains for
 * raid1 and raid10.
 */
static void set_num_domains(struct imsm_map *map)
{
	int level = get_imsm_raid_level(map);

	if (level == 1 || level == 10)
		map->num_domains = 2;
	else
		map->num_domains = 1;
}

static void set_pba_of_lba0(struct imsm_map *map, unsigned long long n)
{
	split_ull(n, &map->pba_of_lba0_lo, &map->pba_of_lba0_hi);
}

static void set_blocks_per_member(struct imsm_map *map, unsigned long long n)
{
	split_ull(n, &map->blocks_per_member_lo, &map->blocks_per_member_hi);
}

static void set_num_data_stripes(struct imsm_map *map, unsigned long long n)
{
	split_ull(n, &map->num_data_stripes_lo, &map->num_data_stripes_hi);
}

/**
 * update_num_data_stripes() - Calculate and update num_data_stripes value.
 * @map: map to be updated.
 * @dev_size: size of volume.
 *
 * num_data_stripes value is addictionally divided by num_domains, therefore for
 * levels where num_domains is not 1, nds is a part of real value.
 */
static void update_num_data_stripes(struct imsm_map *map,
				     unsigned long long dev_size)
{
	unsigned long long nds = dev_size / imsm_num_data_members(map);

	nds /= map->num_domains;
	nds /= map->blocks_per_strip;
	set_num_data_stripes(map, nds);
}

static void set_vol_curr_migr_unit(struct imsm_dev *dev, unsigned long long n)
{
	if (dev == NULL)
		return;

	split_ull(n, &dev->vol.curr_migr_unit_lo, &dev->vol.curr_migr_unit_hi);
}

static void set_imsm_dev_size(struct imsm_dev *dev, unsigned long long n)
{
	split_ull(n, &dev->size_low, &dev->size_high);
}

static void set_migr_chkp_area_pba(struct migr_record *migr_rec,
				   unsigned long long n)
{
	split_ull(n, &migr_rec->ckpt_area_pba_lo, &migr_rec->ckpt_area_pba_hi);
}

static void set_current_migr_unit(struct migr_record *migr_rec,
				  unsigned long long n)
{
	split_ull(n, &migr_rec->curr_migr_unit_lo,
		  &migr_rec->curr_migr_unit_hi);
}

static void set_migr_dest_1st_member_lba(struct migr_record *migr_rec,
					 unsigned long long n)
{
	split_ull(n, &migr_rec->dest_1st_member_lba_lo,
		  &migr_rec->dest_1st_member_lba_hi);
}

static void set_num_migr_units(struct migr_record *migr_rec,
			       unsigned long long n)
{
	split_ull(n, &migr_rec->num_migr_units_lo,
		  &migr_rec->num_migr_units_hi);
}

static unsigned long long per_dev_array_size(struct imsm_map *map)
{
	unsigned long long array_size = 0;

	if (map == NULL)
		return array_size;

	array_size = num_data_stripes(map) * map->blocks_per_strip;
	if (get_imsm_raid_level(map) == 1 || get_imsm_raid_level(map) == 10)
		array_size *= 2;

	return array_size;
}

static struct extent *get_extents(struct intel_super *super, struct dl *dl,
				  int get_minimal_reservation)
{
	/* find a list of used extents on the given physical device */
	int memberships = count_memberships(dl, super);
	struct extent *rv = xcalloc(memberships + 1, sizeof(struct extent));
	struct extent *e = rv;
	int i;
	__u32 reservation;

	/* trim the reserved area for spares, so they can join any array
	 * regardless of whether the OROM has assigned sectors from the
	 * IMSM_RESERVED_SECTORS region
	 */
	if (dl->index == -1 || get_minimal_reservation)
		reservation = imsm_min_reserved_sectors(super);
	else
		reservation = MPB_SECTOR_CNT + IMSM_RESERVED_SECTORS;

	for (i = 0; i < super->anchor->num_raid_devs; i++) {
		struct imsm_dev *dev = get_imsm_dev(super, i);
		struct imsm_map *map = get_imsm_map(dev, MAP_0);

		if (get_imsm_disk_slot(map, dl->index) >= 0) {
			e->start = pba_of_lba0(map);
			e->size = per_dev_array_size(map);
			e->vol = i;
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
		unsigned long long remainder;

		remainder = total_blocks(&dl->disk) - (last->start + last->size);
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
	e->start = total_blocks(&dl->disk) - reservation;
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

	e = get_extents(super, dl, 0);
	if (!e)
		return MPB_SECTOR_CNT + IMSM_RESERVED_SECTORS;

	/* scroll to last entry */
	for (i = 0; e[i].size; i++)
		continue;

	rv = total_blocks(&dl->disk) - e[i].start;

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

static int is_journal(struct imsm_disk *disk)
{
	return (disk->status & JOURNAL_DISK) == JOURNAL_DISK;
}

/**
 * round_member_size_to_mb()- Round given size to closest MiB.
 * @size: size to round in sectors.
 */
static inline unsigned long long round_member_size_to_mb(unsigned long long size)
{
	return (size >> SECT_PER_MB_SHIFT) << SECT_PER_MB_SHIFT;
}

/**
 * round_size_to_mb()- Round given size.
 * @array_size: size to round in sectors.
 * @disk_count: count of data members.
 *
 * Get size per each data member and round it to closest MiB to ensure that data
 * splits evenly between members.
 *
 * Return: Array size, rounded down.
 */
static inline unsigned long long round_size_to_mb(unsigned long long array_size,
						  unsigned int disk_count)
{
	return round_member_size_to_mb(array_size / disk_count) * disk_count;
}

static int able_to_resync(int raid_level, int missing_disks)
{
	int max_missing_disks = 0;

	switch (raid_level) {
	case 10:
		max_missing_disks = 1;
		break;
	default:
		max_missing_disks = 0;
	}
	return missing_disks <= max_missing_disks;
}

/* try to determine how much space is reserved for metadata from
 * the last get_extents() entry on the smallest active disk,
 * otherwise fallback to the default
 */
static __u32 imsm_min_reserved_sectors(struct intel_super *super)
{
	struct extent *e;
	int i;
	unsigned long long min_active;
	__u32 remainder;
	__u32 rv = MPB_SECTOR_CNT + IMSM_RESERVED_SECTORS;
	struct dl *dl, *dl_min = NULL;

	if (!super)
		return rv;

	min_active = 0;
	for (dl = super->disks; dl; dl = dl->next) {
		if (dl->index < 0)
			continue;
		unsigned long long blocks = total_blocks(&dl->disk);
		if (blocks < min_active || min_active == 0) {
			dl_min = dl;
			min_active = blocks;
		}
	}
	if (!dl_min)
		return rv;

	/* find last lba used by subarrays on the smallest active disk */
	e = get_extents(super, dl_min, 0);
	if (!e)
		return rv;
	for (i = 0; e[i].size; i++)
		continue;

	remainder = min_active - e[i].start;
	free(e);

	/* to give priority to recovery we should not require full
	   IMSM_RESERVED_SECTORS from the spare */
	rv = MPB_SECTOR_CNT + NUM_BLOCKS_DIRTY_STRIPE_REGION;

	/* if real reservation is smaller use that value */
	return  (remainder < rv) ? remainder : rv;
}

static bool is_gen_migration(struct imsm_dev *dev);

#define IMSM_4K_DIV 8

static __u64 blocks_per_migr_unit(struct intel_super *super,
				  struct imsm_dev *dev);

static void print_imsm_dev(struct intel_super *super,
			   struct imsm_dev *dev,
			   char *uuid,
			   int disk_idx)
{
	__u64 sz;
	int slot, i;
	struct imsm_map *map = get_imsm_map(dev, MAP_0);
	struct imsm_map *map2 = get_imsm_map(dev, MAP_1);
	__u32 ord;

	printf("\n");
	printf("[%.16s]:\n", dev->volume);
	printf("       Subarray : %d\n", super->current_vol);
	printf("           UUID : %s\n", uuid);
	printf("     RAID Level : %d", get_imsm_raid_level(map));
	if (map2)
		printf(" <-- %d", get_imsm_raid_level(map2));
	printf("\n");
	printf("        Members : %d", map->num_members);
	if (map2)
		printf(" <-- %d", map2->num_members);
	printf("\n");
	printf("          Slots : [");
	for (i = 0; i < map->num_members; i++) {
		ord = get_imsm_ord_tbl_ent(dev, i, MAP_0);
		printf("%s", ord & IMSM_ORD_REBUILD ? "_" : "U");
	}
	printf("]");
	if (map2) {
		printf(" <-- [");
		for (i = 0; i < map2->num_members; i++) {
			ord = get_imsm_ord_tbl_ent(dev, i, MAP_1);
			printf("%s", ord & IMSM_ORD_REBUILD ? "_" : "U");
		}
		printf("]");
	}
	printf("\n");
	printf("    Failed disk : ");
	if (map->failed_disk_num == 0xff)
		printf(STR_COMMON_NONE);
	else
		printf("%i", map->failed_disk_num);
	printf("\n");
	slot = get_imsm_disk_slot(map, disk_idx);
	if (slot >= 0) {
		ord = get_imsm_ord_tbl_ent(dev, slot, MAP_X);
		printf("      This Slot : %d%s\n", slot,
		       ord & IMSM_ORD_REBUILD ? " (out-of-sync)" : "");
	} else
		printf("      This Slot : ?\n");
	printf("    Sector Size : %u\n", super->sector_size);
	sz = imsm_dev_size(dev);
	printf("     Array Size : %llu%s\n",
		   (unsigned long long)sz * 512 / super->sector_size,
	       human_size(sz * 512));
	sz = blocks_per_member(map);
	printf("   Per Dev Size : %llu%s\n",
		   (unsigned long long)sz * 512 / super->sector_size,
	       human_size(sz * 512));
	printf("  Sector Offset : %llu\n",
		pba_of_lba0(map) * 512 / super->sector_size);
	printf("    Num Stripes : %llu\n",
		num_data_stripes(map));
	printf("     Chunk Size : %u KiB",
		__le16_to_cpu(map->blocks_per_strip) / 2);
	if (map2)
		printf(" <-- %u KiB",
			__le16_to_cpu(map2->blocks_per_strip) / 2);
	printf("\n");
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
		struct imsm_map *map = get_imsm_map(dev, MAP_1);

		printf(" <-- %s", map_state_str[map->map_state]);
		printf("\n     Checkpoint : %llu ", vol_curr_migr_unit(dev));
		if (is_gen_migration(dev) && (slot > 1 || slot < 0))
			printf("(N/A)");
		else
			printf("(%llu)", (unsigned long long)
				   blocks_per_migr_unit(super, dev));
	}
	printf("\n");
	printf("    Dirty State : %s\n", (dev->vol.dirty & RAIDVOL_DIRTY) ?
					 "dirty" : "clean");
	printf("     RWH Policy : ");
	if (dev->rwh_policy == RWH_OFF || dev->rwh_policy == RWH_MULTIPLE_OFF)
		printf("off\n");
	else if (dev->rwh_policy == RWH_DISTRIBUTED)
		printf("PPL distributed\n");
	else if (dev->rwh_policy == RWH_JOURNALING_DRIVE)
		printf("PPL journaling drive\n");
	else if (dev->rwh_policy == RWH_MULTIPLE_DISTRIBUTED)
		printf("Multiple distributed PPLs\n");
	else if (dev->rwh_policy == RWH_MULTIPLE_PPLS_JOURNALING_DRIVE)
		printf("Multiple PPLs on journaling drive\n");
	else if (dev->rwh_policy == RWH_BITMAP)
		printf("Write-intent bitmap\n");
	else
		printf("<unknown:%d>\n", dev->rwh_policy);

	printf("      Volume ID : %u\n", dev->my_vol_raid_dev_num);
}

static void print_imsm_disk(struct imsm_disk *disk,
			    int index,
			    __u32 reserved,
			    unsigned int sector_size) {
	char str[MAX_RAID_SERIAL_LEN + 1];
	__u64 sz;

	if (index < -1 || !disk)
		return;

	printf("\n");
	snprintf(str, MAX_RAID_SERIAL_LEN + 1, "%s", disk->serial);
	if (index >= 0)
		printf("  Disk%02d Serial : %s\n", index, str);
	else
		printf("    Disk Serial : %s\n", str);
	printf("          State :%s%s%s%s\n", is_spare(disk) ? " spare" : "",
					      is_configured(disk) ? " active" : "",
					      is_failed(disk) ? " failed" : "",
					      is_journal(disk) ? " journal" : "");
	printf("             Id : %08x\n", __le32_to_cpu(disk->scsi_id));
	sz = total_blocks(disk) - reserved;
	printf("    Usable Size : %llu%s\n",
	       (unsigned long long)sz * 512 / sector_size,
	       human_size(sz * 512));
}

void convert_to_4k_imsm_migr_rec(struct intel_super *super)
{
	struct migr_record *migr_rec = super->migr_rec;

	migr_rec->blocks_per_unit /= IMSM_4K_DIV;
	migr_rec->dest_depth_per_unit /= IMSM_4K_DIV;
	split_ull((join_u32(migr_rec->post_migr_vol_cap,
		 migr_rec->post_migr_vol_cap_hi) / IMSM_4K_DIV),
		 &migr_rec->post_migr_vol_cap, &migr_rec->post_migr_vol_cap_hi);
	set_migr_chkp_area_pba(migr_rec,
		 migr_chkp_area_pba(migr_rec) / IMSM_4K_DIV);
	set_migr_dest_1st_member_lba(migr_rec,
		 migr_dest_1st_member_lba(migr_rec) / IMSM_4K_DIV);
}

void convert_to_4k_imsm_disk(struct imsm_disk *disk)
{
	set_total_blocks(disk, (total_blocks(disk)/IMSM_4K_DIV));
}

void convert_to_4k(struct intel_super *super)
{
	struct imsm_super *mpb = super->anchor;
	struct imsm_disk *disk;
	int i;
	__u32 bbm_log_size = __le32_to_cpu(mpb->bbm_log_size);

	for (i = 0; i < mpb->num_disks ; i++) {
		disk = __get_imsm_disk(mpb, i);
		/* disk */
		convert_to_4k_imsm_disk(disk);
	}
	for (i = 0; i < mpb->num_raid_devs; i++) {
		struct imsm_dev *dev = __get_imsm_dev(mpb, i);
		struct imsm_map *map = get_imsm_map(dev, MAP_0);
		/* dev */
		set_imsm_dev_size(dev, imsm_dev_size(dev)/IMSM_4K_DIV);
		set_vol_curr_migr_unit(dev,
				       vol_curr_migr_unit(dev) / IMSM_4K_DIV);

		/* map0 */
		set_blocks_per_member(map, blocks_per_member(map)/IMSM_4K_DIV);
		map->blocks_per_strip /= IMSM_4K_DIV;
		set_pba_of_lba0(map, pba_of_lba0(map)/IMSM_4K_DIV);

		if (dev->vol.migr_state) {
			/* map1 */
			map = get_imsm_map(dev, MAP_1);
			set_blocks_per_member(map,
			    blocks_per_member(map)/IMSM_4K_DIV);
			map->blocks_per_strip /= IMSM_4K_DIV;
			set_pba_of_lba0(map, pba_of_lba0(map)/IMSM_4K_DIV);
		}
	}
	if (bbm_log_size) {
		struct bbm_log *log = (void *)mpb +
			__le32_to_cpu(mpb->mpb_size) - bbm_log_size;
		__u32 i;

		for (i = 0; i < log->entry_count; i++) {
			struct bbm_log_entry *entry =
				&log->marked_block_entries[i];

			__u8 count = entry->marked_count + 1;
			unsigned long long sector =
				__le48_to_cpu(&entry->defective_block_start);

			entry->defective_block_start =
				__cpu_to_le48(sector/IMSM_4K_DIV);
			entry->marked_count = max(count/IMSM_4K_DIV, 1) - 1;
		}
	}

	mpb->check_sum = __gen_imsm_checksum(mpb);
}

void examine_migr_rec_imsm(struct intel_super *super)
{
	struct migr_record *migr_rec = super->migr_rec;
	struct imsm_super *mpb = super->anchor;
	int i;

	for (i = 0; i < mpb->num_raid_devs; i++) {
		struct imsm_dev *dev = __get_imsm_dev(mpb, i);
		struct imsm_map *map;
		int slot = -1;

		if (is_gen_migration(dev) == false)
				continue;

		printf("\nMigration Record Information:");

		/* first map under migration */
		map = get_imsm_map(dev, MAP_0);

		if (map)
			slot = get_imsm_disk_slot(map, super->disks->index);
		if (map == NULL || slot > 1 || slot < 0) {
			printf(" Empty\n                              ");
			printf("Examine one of first two disks in array\n");
			break;
		}
		printf("\n                     Status : ");
		if (__le32_to_cpu(migr_rec->rec_status) == UNIT_SRC_NORMAL)
			printf("Normal\n");
		else
			printf("Contains Data\n");
		printf("               Current Unit : %llu\n",
		       current_migr_unit(migr_rec));
		printf("                     Family : %u\n",
		       __le32_to_cpu(migr_rec->family_num));
		printf("                  Ascending : %u\n",
		       __le32_to_cpu(migr_rec->ascending_migr));
		printf("            Blocks Per Unit : %u\n",
		       __le32_to_cpu(migr_rec->blocks_per_unit));
		printf("       Dest. Depth Per Unit : %u\n",
		       __le32_to_cpu(migr_rec->dest_depth_per_unit));
		printf("        Checkpoint Area pba : %llu\n",
		       migr_chkp_area_pba(migr_rec));
		printf("           First member lba : %llu\n",
		       migr_dest_1st_member_lba(migr_rec));
		printf("      Total Number of Units : %llu\n",
		       get_num_migr_units(migr_rec));
		printf("             Size of volume : %llu\n",
		       join_u32(migr_rec->post_migr_vol_cap,
				migr_rec->post_migr_vol_cap_hi));
		printf("       Record was read from : %u\n",
		       __le32_to_cpu(migr_rec->ckpt_read_disk_num));

		break;
	}
}

void convert_from_4k_imsm_migr_rec(struct intel_super *super)
{
	struct migr_record *migr_rec = super->migr_rec;

	migr_rec->blocks_per_unit *= IMSM_4K_DIV;
	migr_rec->dest_depth_per_unit *= IMSM_4K_DIV;
	split_ull((join_u32(migr_rec->post_migr_vol_cap,
		 migr_rec->post_migr_vol_cap_hi) * IMSM_4K_DIV),
		 &migr_rec->post_migr_vol_cap,
		 &migr_rec->post_migr_vol_cap_hi);
	set_migr_chkp_area_pba(migr_rec,
		 migr_chkp_area_pba(migr_rec) * IMSM_4K_DIV);
	set_migr_dest_1st_member_lba(migr_rec,
		 migr_dest_1st_member_lba(migr_rec) * IMSM_4K_DIV);
}

void convert_from_4k(struct intel_super *super)
{
	struct imsm_super *mpb = super->anchor;
	struct imsm_disk *disk;
	int i;
	__u32 bbm_log_size = __le32_to_cpu(mpb->bbm_log_size);

	for (i = 0; i < mpb->num_disks ; i++) {
		disk = __get_imsm_disk(mpb, i);
		/* disk */
		set_total_blocks(disk, (total_blocks(disk)*IMSM_4K_DIV));
	}

	for (i = 0; i < mpb->num_raid_devs; i++) {
		struct imsm_dev *dev = __get_imsm_dev(mpb, i);
		struct imsm_map *map = get_imsm_map(dev, MAP_0);
		/* dev */
		set_imsm_dev_size(dev, imsm_dev_size(dev)*IMSM_4K_DIV);
		set_vol_curr_migr_unit(dev,
				       vol_curr_migr_unit(dev) * IMSM_4K_DIV);

		/* map0 */
		set_blocks_per_member(map, blocks_per_member(map)*IMSM_4K_DIV);
		map->blocks_per_strip *= IMSM_4K_DIV;
		set_pba_of_lba0(map, pba_of_lba0(map)*IMSM_4K_DIV);

		if (dev->vol.migr_state) {
			/* map1 */
			map = get_imsm_map(dev, MAP_1);
			set_blocks_per_member(map,
			    blocks_per_member(map)*IMSM_4K_DIV);
			map->blocks_per_strip *= IMSM_4K_DIV;
			set_pba_of_lba0(map, pba_of_lba0(map)*IMSM_4K_DIV);
		}
	}
	if (bbm_log_size) {
		struct bbm_log *log = (void *)mpb +
			__le32_to_cpu(mpb->mpb_size) - bbm_log_size;
		__u32 i;

		for (i = 0; i < log->entry_count; i++) {
			struct bbm_log_entry *entry =
				&log->marked_block_entries[i];

			__u8 count = entry->marked_count + 1;
			unsigned long long sector =
				__le48_to_cpu(&entry->defective_block_start);

			entry->defective_block_start =
				__cpu_to_le48(sector*IMSM_4K_DIV);
			entry->marked_count = count*IMSM_4K_DIV - 1;
		}
	}

	mpb->check_sum = __gen_imsm_checksum(mpb);
}

/**
 * imsm_check_attributes() - Check if features represented by attributes flags are supported.
 *
 * @attributes: attributes read from metadata.
 * Returns: true if all features are supported, false otherwise.
 */
static bool imsm_check_attributes(__u32 attributes)
{
	if ((attributes & (MPB_ATTRIB_SUPPORTED | MPB_ATTRIB_IGNORED)) == attributes)
		return true;

	return false;
}

static void getinfo_super_imsm(struct supertype *st, struct mdinfo *info, char *map);

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
	struct dl *dl;
	time_t creation_time;

	strncpy(str, (char *)mpb->sig, MPB_SIG_LEN);
	str[MPB_SIG_LEN-1] = '\0';
	printf("          Magic : %s\n", str);
	printf("        Version : %s\n", get_imsm_version(mpb));
	printf("    Orig Family : %08x\n", __le32_to_cpu(mpb->orig_family_num));
	printf("         Family : %08x\n", __le32_to_cpu(mpb->family_num));
	printf("     Generation : %08x\n", __le32_to_cpu(mpb->generation_num));
	creation_time = __le64_to_cpu(mpb->creation_time);
	printf("  Creation Time : %.24s\n",
		creation_time ? ctime(&creation_time) : "Unknown");

	printf("     Attributes : %08x (%s)\n", mpb->attributes,
	       imsm_check_attributes(mpb->attributes) ? "supported" : "not supported");

	getinfo_super_imsm(st, &info, NULL);
	fname_from_uuid(&info, nbuf);
	printf("           UUID : %s\n", nbuf + 5);
	sum = __le32_to_cpu(mpb->check_sum);
	printf("       Checksum : %08x %s\n", sum,
		__gen_imsm_checksum(mpb) == sum ? "correct" : "incorrect");
	printf("    MPB Sectors : %d\n", mpb_sectors(mpb, super->sector_size));
	printf("          Disks : %d\n", mpb->num_disks);
	printf("   RAID Devices : %d\n", mpb->num_raid_devs);
	print_imsm_disk(__get_imsm_disk(mpb, super->disks->index),
			super->disks->index, reserved, super->sector_size);
	if (get_imsm_bbm_log_size(super->bbm_log)) {
		struct bbm_log *log = super->bbm_log;

		printf("\n");
		printf("Bad Block Management Log:\n");
		printf("       Log Size : %d\n", __le32_to_cpu(mpb->bbm_log_size));
		printf("      Signature : %x\n", __le32_to_cpu(log->signature));
		printf("    Entry Count : %d\n", __le32_to_cpu(log->entry_count));
	}
	for (i = 0; i < mpb->num_raid_devs; i++) {
		struct mdinfo info;
		struct imsm_dev *dev = __get_imsm_dev(mpb, i);

		super->current_vol = i;
		getinfo_super_imsm(st, &info, NULL);
		fname_from_uuid(&info, nbuf);
		print_imsm_dev(super, dev, nbuf + 5, super->disks->index);
	}
	for (i = 0; i < mpb->num_disks; i++) {
		if (i == super->disks->index)
			continue;
		print_imsm_disk(__get_imsm_disk(mpb, i), i, reserved,
				super->sector_size);
	}

	for (dl = super->disks; dl; dl = dl->next)
		if (dl->index == -1)
			print_imsm_disk(&dl->disk, -1, reserved,
					super->sector_size);

	examine_migr_rec_imsm(super);
}

static void brief_examine_super_imsm(struct supertype *st, int verbose)
{
	/* We just write a generic IMSM ARRAY entry */
	struct mdinfo info;
	char nbuf[64];

	getinfo_super_imsm(st, &info, NULL);
	fname_from_uuid(&info, nbuf);
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

	getinfo_super_imsm(st, &info, NULL);
	fname_from_uuid(&info, nbuf);
	for (i = 0; i < super->anchor->num_raid_devs; i++) {
		struct imsm_dev *dev = get_imsm_dev(super, i);

		super->current_vol = i;
		getinfo_super_imsm(st, &info, NULL);
		fname_from_uuid(&info, nbuf1);
		printf("ARRAY " DEV_MD_DIR "%.16s container=%s member=%d UUID=%s\n",
		       dev->volume, nbuf + 5, i, nbuf1 + 5);
	}
}

static void export_examine_super_imsm(struct supertype *st)
{
	struct intel_super *super = st->sb;
	struct imsm_super *mpb = super->anchor;
	struct mdinfo info;
	char nbuf[64];

	getinfo_super_imsm(st, &info, NULL);
	fname_from_uuid(&info, nbuf);
	printf("MD_METADATA=imsm\n");
	printf("MD_LEVEL=container\n");
	printf("MD_UUID=%s\n", nbuf+5);
	printf("MD_DEVICES=%u\n", mpb->num_disks);
	printf("MD_CREATION_TIME=%llu\n", __le64_to_cpu(mpb->creation_time));
}

static void detail_super_imsm(struct supertype *st, char *homehost,
			      char *subarray)
{
	struct mdinfo info;
	char nbuf[64];
	struct intel_super *super = st->sb;
	int temp_vol = super->current_vol;

	if (subarray)
		super->current_vol = strtoul(subarray, NULL, 10);

	getinfo_super_imsm(st, &info, NULL);
	fname_from_uuid(&info, nbuf);
	printf("\n              UUID : %s\n", nbuf + 5);

	super->current_vol = temp_vol;
}

static void brief_detail_super_imsm(struct supertype *st, char *subarray)
{
	struct mdinfo info;
	char nbuf[64];
	struct intel_super *super = st->sb;
	int temp_vol = super->current_vol;

	if (subarray)
		super->current_vol = strtoul(subarray, NULL, 10);

	getinfo_super_imsm(st, &info, NULL);
	fname_from_uuid(&info, nbuf);
	printf(" UUID=%s", nbuf + 5);

	super->current_vol = temp_vol;
}

static int imsm_read_serial(int fd, char *devname, __u8 *serial,
			    size_t serial_buf_len);
static void fd2devname(int fd, char *name);

void print_encryption_information(int disk_fd, enum sys_dev_type hba_type)
{
	struct encryption_information information = {0};
	mdadm_status_t status = MDADM_STATUS_SUCCESS;
	const char *indent = "                    ";

	switch (hba_type) {
	case SYS_DEV_VMD:
	case SYS_DEV_NVME:
		status = get_nvme_opal_encryption_information(disk_fd, &information, 1);
		break;
	case SYS_DEV_SATA:
	case SYS_DEV_SATA_VMD:
		status = get_ata_encryption_information(disk_fd, &information, 1);
		break;
	default:
		return;
	}

	if (status) {
		pr_err("Failed to get drive encryption information.\n");
		return;
	}

	printf("%sEncryption(Ability|Status): %s|%s\n", indent,
	       get_encryption_ability_string(information.ability),
	       get_encryption_status_string(information.status));
}

static int ahci_enumerate_ports(struct sys_dev *hba, unsigned long port_count, int host_base,
				int verbose)
{
	/* dump an unsorted list of devices attached to AHCI Intel storage
	 * controller, as well as non-connected ports
	 */
	int hba_len = strlen(hba->path) + 1;
	struct dirent *ent;
	DIR *dir;
	char *path = NULL;
	int err = 0;
	unsigned long port_mask = (1 << port_count) - 1;

	if (port_count > (int)sizeof(port_mask) * 8) {
		if (verbose > 0)
			pr_err("port_count %ld out of range\n", port_count);
		return 2;
	}

	/* scroll through /sys/dev/block looking for devices attached to
	 * this hba
	 */
	dir = opendir("/sys/dev/block");
	if (!dir)
		return 1;

	for (ent = readdir(dir); ent; ent = readdir(dir)) {
		int fd;
		char model[64];
		char vendor[64];
		char buf[1024];
		int major, minor;
		char device[PATH_MAX];
		char *c;
		int port;
		int type;

		if (sscanf(ent->d_name, "%d:%d", &major, &minor) != 2)
			continue;
		path = devt_to_devpath(makedev(major, minor), 1, NULL);
		if (!path)
			continue;
		if (!is_path_attached_to_hba(path, hba->path)) {
			free(path);
			path = NULL;
			continue;
		}

		/* retrieve the scsi device */
		if (!devt_to_devpath(makedev(major, minor), 1, device)) {
			if (verbose > 0)
				pr_err("failed to get device\n");
			err = 2;
			break;
		}
		if (devpath_to_char(device, "type", buf, sizeof(buf), 0)) {
			err = 2;
			break;
		}
		type = strtoul(buf, NULL, 10);

		/* if it's not a disk print the vendor and model */
		if (!(type == 0 || type == 7 || type == 14)) {
			vendor[0] = '\0';
			model[0] = '\0';

			if (devpath_to_char(device, "vendor", buf,
					    sizeof(buf), 0) == 0) {
				strncpy(vendor, buf, sizeof(vendor));
				vendor[sizeof(vendor) - 1] = '\0';
				c = (char *) &vendor[sizeof(vendor) - 1];
				while (isspace(*c) || *c == '\0')
					*c-- = '\0';

			}

			if (devpath_to_char(device, "model", buf,
					    sizeof(buf), 0) == 0) {
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

		/* chop device path to 'host%d' and calculate the port number */
		c = strchr(&path[hba_len], '/');
		if (!c) {
			if (verbose > 0)
				pr_err("%s - invalid path name\n", path + hba_len);
			err = 2;
			break;
		}
		*c = '\0';
		if ((sscanf(&path[hba_len], "ata%d", &port) == 1) ||
		   ((sscanf(&path[hba_len], "host%d", &port) == 1)))
			port -= host_base;
		else {
			if (verbose > 0) {
				*c = '/'; /* repair the full string */
				pr_err("failed to determine port number for %s\n",
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
		if (!is_fd_valid(fd))
			printf("          Port%d : - disk info unavailable -\n", port);
		else {
			fd2devname(fd, buf);
			printf("          Port%d : %s", port, buf);
			if (imsm_read_serial(fd, NULL, (__u8 *)buf,
					     sizeof(buf)) == 0)
				printf(" (%s)\n", buf);
			else
				printf(" ()\n");

			print_encryption_information(fd, hba->type);
			close(fd);
		}
		free(path);
		path = NULL;
	}
	if (path)
		free(path);
	if (dir)
		closedir(dir);
	if (err == 0) {
		unsigned long i;

		for (i = 0; i < port_count; i++)
			if (port_mask & (1L << i))
				printf("          Port%ld : - no device attached -\n", i);
	}

	return err;
}

static int print_nvme_info(struct sys_dev *hba)
{
	struct dirent *ent;
	DIR *dir;

	dir = opendir("/sys/block/");
	if (!dir)
		return 1;

	for (ent = readdir(dir); ent; ent = readdir(dir)) {
		char ns_path[PATH_MAX];
		char cntrl_path[PATH_MAX];
		char buf[PATH_MAX];
		int fd = -1;

		if (!strstr(ent->d_name, "nvme"))
			goto skip;

		fd = open_dev(ent->d_name);
		if (!is_fd_valid(fd))
			goto skip;

		if (!diskfd_to_devpath(fd, 0, ns_path) ||
		    !diskfd_to_devpath(fd, 1, cntrl_path))
			goto skip;

		if (!is_path_attached_to_hba(cntrl_path, hba->path))
			goto skip;

		if (!imsm_is_nvme_namespace_supported(fd, 0))
			goto skip;

		fd2devname(fd, buf);
		if (hba->type == SYS_DEV_VMD)
			printf(" NVMe under VMD : %s", buf);
		else if (hba->type == SYS_DEV_NVME)
			printf("    NVMe Device : %s", buf);

		if (!imsm_read_serial(fd, NULL, (__u8 *)buf,
				      sizeof(buf)))
			printf(" (%s)\n", buf);
		else
			printf("()\n");

		print_encryption_information(fd, hba->type);

skip:
		close_fd(&fd);
	}

	closedir(dir);
	return 0;
}

static void print_found_intel_controllers(struct sys_dev *elem)
{
	for (; elem; elem = elem->next) {
		pr_err("found Intel(R) ");
		if (elem->type == SYS_DEV_SATA)
			fprintf(stderr, "SATA ");
		else if (elem->type == SYS_DEV_SAS)
			fprintf(stderr, "SAS ");
		else if (elem->type == SYS_DEV_NVME)
			fprintf(stderr, "NVMe ");

		if (elem->type == SYS_DEV_VMD)
			fprintf(stderr, "VMD domain");
		else if (elem->type == SYS_DEV_SATA_VMD)
			fprintf(stderr, "SATA VMD domain");
		else
			fprintf(stderr, "RAID controller");

		if (elem->pci_id)
			fprintf(stderr, " at %s", elem->pci_id);
		fprintf(stderr, ".\n");
	}
	fflush(stderr);
}

static int ahci_get_port_count(const char *hba_path, int *port_count)
{
	struct dirent *ent;
	DIR *dir;
	int host_base = -1;

	*port_count = 0;
	if ((dir = opendir(hba_path)) == NULL)
		return -1;

	for (ent = readdir(dir); ent; ent = readdir(dir)) {
		int host;

		if ((sscanf(ent->d_name, "ata%d", &host) != 1) &&
		   ((sscanf(ent->d_name, "host%d", &host) != 1)))
			continue;
		if (*port_count == 0)
			host_base = host;
		else if (host < host_base)
			host_base = host;

		if (host + 1 > *port_count + host_base)
			*port_count = host + 1 - host_base;
	}
	closedir(dir);
	return host_base;
}

static void print_imsm_level_capability(const struct imsm_orom *orom)
{
	int idx;

	for (idx = 0; imsm_level_ops[idx].name; idx++)
		if (imsm_level_ops[idx].is_level_supported(orom))
			printf("%s ", imsm_level_ops[idx].name);
}

static void print_imsm_sku_capability(const struct imsm_orom *orom)
{
	int key_val;

	key_val = (orom->driver_features & IMSM_OROM_CAPABILITIES_SKUMode_LOW) >>
		   IMSM_OROM_CAPABILITIES_SKUMode_LOW_SHIFT;
	key_val |= (orom->driver_features & IMSM_OROM_CAPABILITIES_SKUMode_HIGH) >>
		    IMSM_OROM_CAPABILITIES_SKUMode_HIGH_SHIFT;

	switch (key_val) {
	case SKU_NO_KEY:
		printf("Pass-through");
		break;
	case SKU_STANDARD_KEY:
		printf("Standard");
		break;
	case SKU_PREMIUM_KEY:
		printf("Premium");
		break;
	case SKU_INTEL_SSD_ONLY_KEY:
		printf("Intel-SSD-only");
		break;
	case SKU_RAID1_ONLY_KEY:
		printf("RAID1 Only");
		break;
	default:
		printf("Unknown");
	}

	if (orom->driver_features & IMSM_OROM_CAPABILITIES_SKUMode_NON_PRODUCTION)
		printf(" - for evaluation only");
}

static void print_imsm_chunk_size_capability(const struct imsm_orom *orom)
{
	int idx;

	for (idx = 0; imsm_chunk_ops[idx].chunk_str; idx++)
		if (imsm_chunk_ops[idx].chunk & orom->sss)
			printf("%s ", imsm_chunk_ops[idx].chunk_str);
}


static void print_imsm_capability(const struct orom_entry *entry)
{
	const struct imsm_orom *orom = &entry->orom;

	printf("       Platform : Intel(R) ");

	if (orom->capabilities == 0 && orom->driver_features == 0)
		printf("Matrix Storage Manager\n");
	else if (imsm_orom_is_enterprise(orom) && orom->major_ver >= 6)
		printf("Virtual RAID on CPU\n");
	else
		printf("Rapid Storage Technology%s\n",
			imsm_orom_is_enterprise(orom) ? " enterprise" : "");

	if (orom->major_ver || orom->minor_ver || orom->hotfix_ver || orom->build) {
		if (imsm_orom_is_vmd_without_efi(orom))
			printf("        Version : %d.%d\n", orom->major_ver, orom->minor_ver);
		else
			printf("        Version : %d.%d.%d.%d\n", orom->major_ver, orom->minor_ver,
			       orom->hotfix_ver, orom->build);
	}

	if (entry->type == SYS_DEV_VMD) {
		printf("        License : ");
		print_imsm_sku_capability(orom);
		printf("\n");
	}

	printf("    RAID Levels : ");
	print_imsm_level_capability(orom);
	printf("\n");

	printf("    Chunk Sizes : ");
	print_imsm_chunk_size_capability(orom);
	printf("\n");

	printf("    2TB volumes :%s supported\n", (orom->attr & IMSM_OROM_ATTR_2TB) ? "" : " not");

	printf("      2TB disks :%s supported\n",
	       (orom->attr & IMSM_OROM_ATTR_2TB_DISK) ? "" : " not");

	printf("      Max Disks : %d\n", orom->tds);

	printf("    Max Volumes : %d per array, %d per %s\n", orom->vpa, orom->vphba,
	       imsm_orom_is_nvme(orom) ? "platform" : "controller");

	if (entry->type == SYS_DEV_VMD || entry->type == SYS_DEV_NVME)
		/* This is only meaningful for controllers with nvme support */
		printf(" 3rd party NVMe :%s supported\n",
		       imsm_orom_has_tpv_support(&entry->orom) ? "" : " not");
	return;
}

static void print_imsm_capability_export(const struct imsm_orom *orom)
{
	printf("MD_FIRMWARE_TYPE=imsm\n");
	if (orom->major_ver || orom->minor_ver || orom->hotfix_ver || orom->build)
		printf("IMSM_VERSION=%d.%d.%d.%d\n", orom->major_ver, orom->minor_ver,
				orom->hotfix_ver, orom->build);

	printf("IMSM_SUPPORTED_RAID_LEVELS=");
	print_imsm_level_capability(orom);
	printf("\n");

	printf("IMSM_SUPPORTED_CHUNK_SIZES=");
	print_imsm_chunk_size_capability(orom);
	printf("\n");

	printf("IMSM_2TB_VOLUMES=%s\n",(orom->attr & IMSM_OROM_ATTR_2TB) ? "yes" : "no");
	printf("IMSM_2TB_DISKS=%s\n",(orom->attr & IMSM_OROM_ATTR_2TB_DISK) ? "yes" : "no");
	printf("IMSM_MAX_DISKS=%d\n",orom->tds);
	printf("IMSM_MAX_VOLUMES_PER_ARRAY=%d\n",orom->vpa);
	printf("IMSM_MAX_VOLUMES_PER_CONTROLLER=%d\n",orom->vphba);
}

static int detail_platform_imsm(int verbose, int enumerate_only, char *controller_path)
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
	const struct orom_entry *entry;
	struct sys_dev *list, *hba;
	struct devid_list *devid;
	int port_count = 0;
	int host_base = 0;
	int result = 1;

	if (enumerate_only) {
		if (check_no_platform())
			return 0;

		list = find_intel_devices();
		if (!list)
			return 2;

		for (hba = list; hba; hba = hba->next)
			if (find_imsm_capability(hba))
				return 0;
		return 2;
	}

	list = find_intel_devices();
	if (!list) {
		if (verbose > 0)
			pr_err("no active Intel(R) RAID controller found.\n");
		return 2;
	} else if (verbose > 0)
		print_found_intel_controllers(list);

	for (hba = list; hba; hba = hba->next) {
		if (controller_path && (compare_paths(hba->path, controller_path) != 0))
			continue;
		if (!find_imsm_capability(hba)) {
			char buf[PATH_MAX];

			pr_err("imsm capabilities not found for controller: %s (type %s)\n",
				  hba->type == SYS_DEV_VMD || hba->type == SYS_DEV_SATA_VMD ?
				  vmd_domain_to_controller(hba, buf) :
				  hba->path, get_sys_dev_type(hba->type));
			continue;
		}
		result = 0;
	}

	if (controller_path && result == 1) {
		pr_err("no active Intel(R) RAID controller found under %s\n",
				controller_path);
		return result;
	}

	for (entry = orom_entries; entry; entry = entry->next) {
		print_imsm_capability(entry);

		if (entry->type == SYS_DEV_VMD || entry->type == SYS_DEV_NVME) {
			for (hba = list; hba; hba = hba->next) {
				char buf[PATH_MAX];

				if (hba->type != entry->type)
					continue;

				if (hba->type == SYS_DEV_VMD)
					printf(" I/O Controller : %s (%s)\n",
					       vmd_domain_to_controller(hba, buf),
					       get_sys_dev_type(hba->type));

				print_nvme_info(hba);
			}
			printf("\n");
			continue;
		}

		for (devid = entry->devid_list; devid; devid = devid->next) {
			hba = device_by_id(devid->devid);
			if (!hba)
				continue;

			printf(" I/O Controller : %s (%s)\n",
				hba->path, get_sys_dev_type(hba->type));
			if (hba->type == SYS_DEV_SATA || hba->type == SYS_DEV_SATA_VMD) {
				host_base = ahci_get_port_count(hba->path, &port_count);
				if (ahci_enumerate_ports(hba, port_count, host_base, verbose)) {
					if (verbose > 0)
						pr_err("failed to enumerate ports on %s controller at %s.\n",
							get_sys_dev_type(hba->type), hba->pci_id);
					result |= 2;
				}
			}
		}
		printf("\n");
	}

	return result;
}

static int export_detail_platform_imsm(int verbose, char *controller_path)
{
	struct sys_dev *list, *hba;
	int result=1;

	list = find_intel_devices();
	if (!list) {
		if (verbose > 0)
			pr_err("IMSM_DETAIL_PLATFORM_ERROR=NO_INTEL_DEVICES\n");
		result = 2;
		return result;
	}

	for (hba = list; hba; hba = hba->next) {
		if (controller_path && (compare_paths(hba->path,controller_path) != 0))
			continue;
		if (!find_imsm_capability(hba) && verbose > 0) {
			char buf[PATH_MAX];
			pr_err("IMSM_DETAIL_PLATFORM_ERROR=NO_IMSM_CAPABLE_DEVICE_UNDER_%s\n",
				hba->type == SYS_DEV_VMD || hba->type == SYS_DEV_SATA_VMD ?
				vmd_domain_to_controller(hba, buf) : hba->path);
		}
		else
			result = 0;
	}

	const struct orom_entry *entry;

	for (entry = orom_entries; entry; entry = entry->next) {
		if (entry->type == SYS_DEV_VMD || entry->type == SYS_DEV_SATA_VMD) {
			for (hba = list; hba; hba = hba->next)
				print_imsm_capability_export(&entry->orom);
			continue;
		}
		print_imsm_capability_export(&entry->orom);
	}

	return result;
}

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

static __u32 migr_strip_blocks_resync(struct imsm_dev *dev)
{
	/* migr_strip_size when repairing or initializing parity */
	struct imsm_map *map = get_imsm_map(dev, MAP_0);
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
	struct imsm_map *map = get_imsm_map(dev, MAP_1);
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
	struct imsm_map *lo = get_imsm_map(dev, MAP_0);
	struct imsm_map *hi = get_imsm_map(dev, MAP_1);
	__u32 lo_chunk = __le32_to_cpu(lo->blocks_per_strip);
	__u32 hi_chunk = __le32_to_cpu(hi->blocks_per_strip);

	return max((__u32) 1, hi_chunk / lo_chunk);
}

static __u32 num_stripes_per_unit_rebuild(struct imsm_dev *dev)
{
	struct imsm_map *lo = get_imsm_map(dev, MAP_0);
	int level = get_imsm_raid_level(lo);

	if (level == 1 || level == 10) {
		struct imsm_map *hi = get_imsm_map(dev, MAP_1);

		return hi->num_domains;
	} else
		return num_stripes_per_unit_resync(dev);
}

static unsigned long long calc_component_size(struct imsm_map *map,
					      struct imsm_dev *dev)
{
	unsigned long long component_size;
	unsigned long long dev_size = imsm_dev_size(dev);
	long long calc_dev_size = 0;
	unsigned int member_disks = imsm_num_data_members(map);

	if (member_disks == 0)
		return 0;

	component_size = per_dev_array_size(map);
	calc_dev_size = component_size * member_disks;

	/* Component size is rounded to 1MB so difference between size from
	 * metadata and size calculated from num_data_stripes equals up to
	 * 2048 blocks per each device. If the difference is higher it means
	 * that array size was expanded and num_data_stripes was not updated.
	 */
	if (llabs(calc_dev_size - (long long)dev_size) >
	    (1 << SECT_PER_MB_SHIFT) * member_disks) {
		component_size = dev_size / member_disks;
		dprintf("Invalid num_data_stripes in metadata; expected=%llu, found=%llu\n",
			component_size / map->blocks_per_strip,
			num_data_stripes(map));
	}

	return component_size;
}

static __u32 parity_segment_depth(struct imsm_dev *dev)
{
	struct imsm_map *map = get_imsm_map(dev, MAP_0);
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
	struct imsm_map *map = get_imsm_map(dev, MAP_1);
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

static __u64 blocks_per_migr_unit(struct intel_super *super,
				  struct imsm_dev *dev)
{
	/* calculate the conversion factor between per member 'blocks'
	 * (md/{resync,rebuild}_start) and imsm migration units, return
	 * 0 for the 'not migrating' and 'unsupported migration' cases
	 */
	if (!dev->vol.migr_state)
		return 0;

	switch (migr_type(dev)) {
	case MIGR_GEN_MIGR: {
		struct migr_record *migr_rec = super->migr_rec;
		return __le32_to_cpu(migr_rec->blocks_per_unit);
	}
	case MIGR_VERIFY:
	case MIGR_REPAIR:
	case MIGR_INIT: {
		struct imsm_map *map = get_imsm_map(dev, MAP_0);
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
		disks = imsm_num_data_members(map);
		blocks_per_unit = stripes_per_unit * migr_chunk * disks;
		stripe = __le16_to_cpu(map->blocks_per_strip) * disks;
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

/*******************************************************************************
 * Function:	read_imsm_migr_rec
 * Description: Function reads imsm migration record from last sector of disk
 * Parameters:
 *	fd	: disk descriptor
 *	super	: metadata info
 * Returns:
 *	 0 : success,
 *	-1 : fail
 ******************************************************************************/
static int read_imsm_migr_rec(int fd, struct intel_super *super)
{
	int ret_val = -1;
	unsigned int sector_size = super->sector_size;
	unsigned long long dsize;

	get_dev_size(fd, NULL, &dsize);
	if (lseek64(fd, dsize - (sector_size*MIGR_REC_SECTOR_POSITION),
		   SEEK_SET) < 0) {
		pr_err("Cannot seek to anchor block: %s\n",
		       strerror(errno));
		goto out;
	}
	if ((unsigned int)read(fd, super->migr_rec_buf,
	    MIGR_REC_BUF_SECTORS*sector_size) !=
	    MIGR_REC_BUF_SECTORS*sector_size) {
		pr_err("Cannot read migr record block: %s\n",
		       strerror(errno));
		goto out;
	}
	ret_val = 0;
	if (sector_size == 4096)
		convert_from_4k_imsm_migr_rec(super);

out:
	return ret_val;
}

static struct imsm_dev *imsm_get_device_during_migration(
	struct intel_super *super)
{

	struct intel_dev *dv;

	for (dv = super->devlist; dv; dv = dv->next) {
		if (is_gen_migration(dv->dev))
			return dv->dev;
	}
	return NULL;
}

/*******************************************************************************
 * Function:	load_imsm_migr_rec
 * Description:	Function reads imsm migration record (it is stored at the last
 *		sector of disk)
 * Parameters:
 *	super	: imsm internal array info
 * Returns:
 *	 0 : success
 *	-1 : fail
 *	-2 : no migration in progress
 ******************************************************************************/
static int load_imsm_migr_rec(struct intel_super *super)
{
	struct dl *dl;
	char nm[30];
	int retval = -1;
	int fd = -1;
	struct imsm_dev *dev;
	struct imsm_map *map;
	int slot = -1;
	int keep_fd = 1;

	/* find map under migration */
	dev = imsm_get_device_during_migration(super);
	/* nothing to load,no migration in progress?
	*/
	if (dev == NULL)
		return -2;

	map = get_imsm_map(dev, MAP_0);
	if (!map)
		return -1;

	for (dl = super->disks; dl; dl = dl->next) {
		/* skip spare and failed disks
		 */
		if (dl->index < 0)
			continue;
		/* read only from one of the first two slots
		 */
		slot = get_imsm_disk_slot(map, dl->index);
		if (slot > 1 || slot < 0)
			continue;

		if (!is_fd_valid(dl->fd)) {
			sprintf(nm, "%d:%d", dl->major, dl->minor);
			fd = dev_open(nm, O_RDONLY);

			if (is_fd_valid(fd)) {
				keep_fd = 0;
				break;
			}
		} else {
			fd = dl->fd;
			break;
		}
	}

	if (!is_fd_valid(fd))
		return retval;
	retval = read_imsm_migr_rec(fd, super);
	if (!keep_fd)
		close(fd);

	return retval;
}

/*******************************************************************************
 * function: imsm_create_metadata_checkpoint_update
 * Description: It creates update for checkpoint change.
 * Parameters:
 *	super	: imsm internal array info
 *	u	: pointer to prepared update
 * Returns:
 *	Uptate length.
 *	If length is equal to 0, input pointer u contains no update
 ******************************************************************************/
static int imsm_create_metadata_checkpoint_update(
	struct intel_super *super,
	struct imsm_update_general_migration_checkpoint **u)
{

	int update_memory_size = 0;

	dprintf("(enter)\n");

	if (u == NULL)
		return 0;
	*u = NULL;

	/* size of all update data without anchor */
	update_memory_size =
		sizeof(struct imsm_update_general_migration_checkpoint);

	*u = xcalloc(1, update_memory_size);
	if (*u == NULL) {
		dprintf("error: cannot get memory\n");
		return 0;
	}
	(*u)->type = update_general_migration_checkpoint;
	(*u)->curr_migr_unit = current_migr_unit(super->migr_rec);
	dprintf("prepared for %llu\n", (unsigned long long)(*u)->curr_migr_unit);

	return update_memory_size;
}

static void imsm_update_metadata_locally(struct supertype *st,
					 void *buf, int len);

/*******************************************************************************
 * Function:	write_imsm_migr_rec
 * Description:	Function writes imsm migration record
 *		(at the last sector of disk)
 * Parameters:
 *	super	: imsm internal array info
 * Returns:
 *	 0 : success
 *	-1 : if fail
 ******************************************************************************/
static int write_imsm_migr_rec(struct supertype *st)
{
	struct intel_super *super = st->sb;
	unsigned int sector_size = super->sector_size;
	unsigned long long dsize;
	int retval = -1;
	struct dl *sd;
	int len;
	struct imsm_update_general_migration_checkpoint *u;
	struct imsm_dev *dev;
	struct imsm_map *map;

	/* find map under migration */
	dev = imsm_get_device_during_migration(super);
	/* if no migration, write buffer anyway to clear migr_record
	 * on disk based on first available device
	*/
	if (dev == NULL)
		dev = get_imsm_dev(super, super->current_vol < 0 ? 0 :
					  super->current_vol);

	map = get_imsm_map(dev, MAP_0);

	if (sector_size == 4096)
		convert_to_4k_imsm_migr_rec(super);
	for (sd = super->disks ; sd ; sd = sd->next) {
		int slot = -1;

		/* skip failed and spare devices */
		if (sd->index < 0)
			continue;
		/* write to 2 first slots only */
		if (map)
			slot = get_imsm_disk_slot(map, sd->index);
		if (map == NULL || slot > 1 || slot < 0)
			continue;

		get_dev_size(sd->fd, NULL, &dsize);
		if (lseek64(sd->fd, dsize - (MIGR_REC_SECTOR_POSITION *
		    sector_size),
		    SEEK_SET) < 0) {
			pr_err("Cannot seek to anchor block: %s\n",
			       strerror(errno));
			goto out;
		}
		if ((unsigned int)write(sd->fd, super->migr_rec_buf,
		    MIGR_REC_BUF_SECTORS*sector_size) !=
		    MIGR_REC_BUF_SECTORS*sector_size) {
			pr_err("Cannot write migr record block: %s\n",
			       strerror(errno));
			goto out;
		}
	}
	if (sector_size == 4096)
		convert_from_4k_imsm_migr_rec(super);
	/* update checkpoint information in metadata */
	len = imsm_create_metadata_checkpoint_update(super, &u);
	if (len <= 0) {
		dprintf("imsm: Cannot prepare update\n");
		goto out;
	}
	/* update metadata locally */
	imsm_update_metadata_locally(st, u, len);
	/* and possibly remotely */
	if (st->update_tail) {
		append_metadata_update(st, u, len);
		/* during reshape we do all work inside metadata handler
		 * manage_reshape(), so metadata update has to be triggered
		 * insida it
		 */
		flush_metadata_updates(st);
		st->update_tail = &st->updates;
	} else
		free(u);

	retval = 0;
 out:
	return retval;
}

/* spare/missing disks activations are not allowe when
 * array/container performs reshape operation, because
 * all arrays in container works on the same disks set
 */
int imsm_reshape_blocks_arrays_changes(struct intel_super *super)
{
	int rv = 0;
	struct intel_dev *i_dev;
	struct imsm_dev *dev;

	/* check whole container
	 */
	for (i_dev = super->devlist; i_dev; i_dev = i_dev->next) {
		dev = i_dev->dev;
		if (is_gen_migration(dev)) {
			/* No repair during any migration in container
			 */
			rv = 1;
			break;
		}
	}
	return rv;
}
static unsigned long long imsm_component_size_alignment_check(int level,
					      int chunk_size,
					      unsigned int sector_size,
					      unsigned long long component_size)
{
	unsigned int component_size_alignment;

	/* check component size alignment
	*/
	component_size_alignment = component_size % (chunk_size/sector_size);

	dprintf("(Level: %i, chunk_size = %i, component_size = %llu), component_size_alignment = %u\n",
		level, chunk_size, component_size,
		component_size_alignment);

	if (component_size_alignment && (level != 1) && (level != UnSet)) {
		dprintf("imsm: reported component size aligned from %llu ",
			component_size);
		component_size -= component_size_alignment;
		dprintf_cont("to %llu (%i).\n",
			component_size, component_size_alignment);
	}

	return component_size;
}

/*******************************************************************************
 * Function:	get_bitmap_header_sector
 * Description:	Returns the sector where the bitmap header is placed.
 * Parameters:
 *	st		: supertype information
 *	dev_idx		: index of the device with bitmap
 *
 * Returns:
 *	 The sector where the bitmap header is placed
 ******************************************************************************/
static unsigned long long get_bitmap_header_sector(struct intel_super *super,
						   int dev_idx)
{
	struct imsm_dev *dev = get_imsm_dev(super, dev_idx);
	struct imsm_map *map = get_imsm_map(dev, MAP_0);

	if (!super->sector_size) {
		dprintf("sector size is not set\n");
		return 0;
	}

	return pba_of_lba0(map) + calc_component_size(map, dev) +
	       (IMSM_BITMAP_HEADER_OFFSET / super->sector_size);
}

/*******************************************************************************
 * Function:	get_bitmap_sector
 * Description:	Returns the sector where the bitmap is placed.
 * Parameters:
 *	st		: supertype information
 *	dev_idx		: index of the device with bitmap
 *
 * Returns:
 *	 The sector where the bitmap is placed
 ******************************************************************************/
static unsigned long long get_bitmap_sector(struct intel_super *super,
					    int dev_idx)
{
	if (!super->sector_size) {
		dprintf("sector size is not set\n");
		return 0;
	}

	return get_bitmap_header_sector(super, dev_idx) +
	       (IMSM_BITMAP_HEADER_SIZE / super->sector_size);
}

static unsigned long long get_ppl_sector(struct intel_super *super, int dev_idx)
{
	struct imsm_dev *dev = get_imsm_dev(super, dev_idx);
	struct imsm_map *map = get_imsm_map(dev, MAP_0);

	return pba_of_lba0(map) +
	       (num_data_stripes(map) * map->blocks_per_strip);
}

static void getinfo_super_imsm_volume(struct supertype *st, struct mdinfo *info, char *dmap)
{
	struct intel_super *super = st->sb;
	struct migr_record *migr_rec = super->migr_rec;
	struct imsm_dev *dev = get_imsm_dev(super, super->current_vol);
	struct imsm_map *map = get_imsm_map(dev, MAP_0);
	struct imsm_map *prev_map = get_imsm_map(dev, MAP_1);
	struct imsm_map *map_to_analyse = map;
	struct dl *dl;
	int map_disks = info->array.raid_disks;

	memset(info, 0, sizeof(*info));
	if (prev_map)
		map_to_analyse = prev_map;

	dl = super->current_disk;

	info->container_member	  = super->current_vol;
	info->array.raid_disks    = map->num_members;
	info->array.level	  = get_imsm_raid_level(map_to_analyse);
	info->array.layout	  = imsm_level_to_layout(info->array.level);
	info->array.md_minor	  = -1;
	info->array.ctime	  = 0;
	info->array.utime	  = 0;
	info->array.chunk_size	  =
		__le16_to_cpu(map_to_analyse->blocks_per_strip) << 9;
	info->array.state	  = !(dev->vol.dirty & RAIDVOL_DIRTY);
	info->custom_array_size   = imsm_dev_size(dev);
	info->recovery_blocked = imsm_reshape_blocks_arrays_changes(st->sb);

	if (is_gen_migration(dev)) {
		/*
		 * device prev_map should be added if it is in the middle
		 * of migration
		 */
		assert(prev_map);

		info->reshape_active = 1;
		info->new_level = get_imsm_raid_level(map);
		info->new_layout = imsm_level_to_layout(info->new_level);
		info->new_chunk = __le16_to_cpu(map->blocks_per_strip) << 9;
		info->delta_disks = map->num_members - prev_map->num_members;
		if (info->delta_disks) {
			/* this needs to be applied to every array
			 * in the container.
			 */
			info->reshape_active = CONTAINER_RESHAPE;
		}
		/* We shape information that we give to md might have to be
		 * modify to cope with md's requirement for reshaping arrays.
		 * For example, when reshaping a RAID0, md requires it to be
		 * presented as a degraded RAID4.
		 * Also if a RAID0 is migrating to a RAID5 we need to specify
		 * the array as already being RAID5, but the 'before' layout
		 * is a RAID4-like layout.
		 */
		switch (info->array.level) {
		case 0:
			switch(info->new_level) {
			case 0:
				/* conversion is happening as RAID4 */
				info->array.level = 4;
				info->array.raid_disks += 1;
				break;
			case 5:
				/* conversion is happening as RAID5 */
				info->array.level = 5;
				info->array.layout = ALGORITHM_PARITY_N;
				info->delta_disks -= 1;
				break;
			default:
				/* FIXME error message */
				info->array.level = UnSet;
				break;
			}
			break;
		}
	} else {
		info->new_level = UnSet;
		info->new_layout = UnSet;
		info->new_chunk = info->array.chunk_size;
		info->delta_disks = 0;
	}

	if (dl) {
		info->disk.major = dl->major;
		info->disk.minor = dl->minor;
		info->disk.number = dl->index;
		info->disk.raid_disk = get_imsm_disk_slot(map_to_analyse,
							  dl->index);
	}

	info->data_offset	  = pba_of_lba0(map_to_analyse);
	info->component_size = calc_component_size(map, dev);
	info->component_size = imsm_component_size_alignment_check(
							info->array.level,
							info->array.chunk_size,
							super->sector_size,
							info->component_size);
	info->bb.supported = 1;

	memset(info->uuid, 0, sizeof(info->uuid));
	info->recovery_start = MaxSector;

	if (info->array.level == 5 &&
	    (dev->rwh_policy == RWH_DISTRIBUTED ||
	     dev->rwh_policy == RWH_MULTIPLE_DISTRIBUTED)) {
		info->consistency_policy = CONSISTENCY_POLICY_PPL;
		info->ppl_sector = get_ppl_sector(super, super->current_vol);
		if (dev->rwh_policy == RWH_MULTIPLE_DISTRIBUTED)
			info->ppl_size = MULTIPLE_PPL_AREA_SIZE_IMSM >> 9;
		else
			info->ppl_size = (PPL_HEADER_SIZE + PPL_ENTRY_SPACE)
					  >> 9;
	} else if (info->array.level <= 0) {
		info->consistency_policy = CONSISTENCY_POLICY_NONE;
	} else {
		if (dev->rwh_policy == RWH_BITMAP) {
			info->bitmap_offset = get_bitmap_sector(super, super->current_vol);
			info->consistency_policy = CONSISTENCY_POLICY_BITMAP;
		} else {
			info->consistency_policy = CONSISTENCY_POLICY_RESYNC;
		}
	}

	info->reshape_progress = 0;
	info->resync_start = MaxSector;
	if ((map_to_analyse->map_state == IMSM_T_STATE_UNINITIALIZED ||
	    !(info->array.state & 1)) &&
	    imsm_reshape_blocks_arrays_changes(super) == 0) {
		info->resync_start = 0;
	}
	if (dev->vol.migr_state) {
		switch (migr_type(dev)) {
		case MIGR_REPAIR:
		case MIGR_INIT: {
			__u64 blocks_per_unit = blocks_per_migr_unit(super,
								     dev);
			__u64 units = vol_curr_migr_unit(dev);

			info->resync_start = blocks_per_unit * units;
			break;
		}
		case MIGR_GEN_MIGR: {
			__u64 blocks_per_unit = blocks_per_migr_unit(super,
								     dev);
			__u64 units = current_migr_unit(migr_rec);
			int used_disks;

			if (__le32_to_cpu(migr_rec->ascending_migr) &&
			    (units <
				(get_num_migr_units(migr_rec)-1)) &&
			    (super->migr_rec->rec_status ==
					__cpu_to_le32(UNIT_SRC_IN_CP_AREA)))
				units++;

			info->reshape_progress = blocks_per_unit * units;

			dprintf("IMSM: General Migration checkpoint : %llu (%llu) -> read reshape progress : %llu\n",
				(unsigned long long)units,
				(unsigned long long)blocks_per_unit,
				info->reshape_progress);

			used_disks = imsm_num_data_members(prev_map);
			if (used_disks > 0) {
				info->custom_array_size = per_dev_array_size(map) *
					used_disks;
			}
		}
		case MIGR_VERIFY:
			/* we could emulate the checkpointing of
			 * 'sync_action=check' migrations, but for now
			 * we just immediately complete them
			 */
		case MIGR_REBUILD:
			/* this is handled by container_content_imsm() */
		case MIGR_STATE_CHANGE:
			/* FIXME handle other migrations */
		default:
			/* we are not dirty, so... */
			info->resync_start = MaxSector;
		}
	}

	strncpy(info->name, (char *) dev->volume, MAX_RAID_SERIAL_LEN);
	info->name[MAX_RAID_SERIAL_LEN] = 0;

	info->array.major_version = -1;
	info->array.minor_version = -2;
	sprintf(info->text_version, "/%s/%d", st->container_devnm, info->container_member);
	info->safe_mode_delay = 4000;  /* 4 secs like the Matrix driver */
	uuid_from_super_imsm(st, info->uuid);

	if (dmap) {
		int i, j;
		for (i=0; i<map_disks; i++) {
			dmap[i] = 0;
			if (i < info->array.raid_disks) {
				struct imsm_disk *dsk;
				j = get_imsm_disk_idx(dev, i, MAP_X);
				dsk = get_imsm_disk(super, j);
				if (dsk && (dsk->status & CONFIGURED_DISK))
					dmap[i] = 1;
			}
		}
	}
}

static __u8 imsm_check_degraded(struct intel_super *super, struct imsm_dev *dev,
				int failed, int look_in_map);

static int imsm_count_failed(struct intel_super *super, struct imsm_dev *dev,
			     int look_in_map);

static void manage_second_map(struct intel_super *super, struct imsm_dev *dev)
{
	if (is_gen_migration(dev)) {
		int failed;
		__u8 map_state;
		struct imsm_map *map2 = get_imsm_map(dev, MAP_1);

		failed = imsm_count_failed(super, dev, MAP_1);
		map_state = imsm_check_degraded(super, dev, failed, MAP_1);
		if (map2->map_state != map_state) {
			map2->map_state = map_state;
			super->updates_pending++;
		}
	}
}

static struct imsm_disk *get_imsm_missing(struct intel_super *super, __u8 index)
{
	struct dl *d;

	for (d = super->missing; d; d = d->next)
		if (d->index == index)
			return &d->disk;
	return NULL;
}

static void getinfo_super_imsm(struct supertype *st, struct mdinfo *info, char *map)
{
	struct intel_super *super = st->sb;
	struct imsm_disk *disk;
	int map_disks = info->array.raid_disks;
	int max_enough = -1;
	int i;
	struct imsm_super *mpb;

	if (super->current_vol >= 0) {
		getinfo_super_imsm_volume(st, info, map);
		return;
	}
	memset(info, 0, sizeof(*info));

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
	info->recovery_blocked = imsm_reshape_blocks_arrays_changes(st->sb);
	info->bb.supported = 1;

	/* do we have the all the insync disks that we expect? */
	mpb = super->anchor;
	info->events = __le32_to_cpu(mpb->generation_num);

	for (i = 0; i < mpb->num_raid_devs; i++) {
		struct imsm_dev *dev = get_imsm_dev(super, i);
		int failed, enough, j, missing = 0;
		struct imsm_map *map;
		__u8 state;

		failed = imsm_count_failed(super, dev, MAP_0);
		state = imsm_check_degraded(super, dev, failed, MAP_0);
		map = get_imsm_map(dev, MAP_0);

		/* any newly missing disks?
		 * (catches single-degraded vs double-degraded)
		 */
		for (j = 0; j < map->num_members; j++) {
			__u32 ord = get_imsm_ord_tbl_ent(dev, j, MAP_0);
			__u32 idx = ord_to_idx(ord);

			if (super->disks && super->disks->index == (int)idx)
				info->disk.raid_disk = j;

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
		if (is_gen_migration(dev) && missing) {
			/* during general migration we need all disks
			 * that process is running on.
			 * No new missing disk is allowed.
			 */
			max_enough = -1;
			enough = -1;
			/* no more checks necessary
			 */
			break;
		}
		/* in the missing/failed disk case check to see
		 * if at least one array is runnable
		 */
		max_enough = max(max_enough, enough);
	}

	info->container_enough = max_enough;

	if (super->disks) {
		__u32 reserved = imsm_reserved_sectors(super, super->disks);

		disk = &super->disks->disk;
		info->data_offset = total_blocks(&super->disks->disk) - reserved;
		info->component_size = reserved;
		info->disk.state  = is_configured(disk) ? (1 << MD_DISK_ACTIVE) : 0;
		/* we don't change info->disk.raid_disk here because
		 * this state will be finalized in mdmon after we have
		 * found the 'most fresh' version of the metadata
		 */
		info->disk.state |= is_failed(disk) ? (1 << MD_DISK_FAULTY) : 0;
		info->disk.state |= (is_spare(disk) || is_journal(disk)) ?
				    0 : (1 << MD_DISK_SYNC);
	}

	/* only call uuid_from_super_imsm when this disk is part of a populated container,
	 * ->compare_super may have updated the 'num_raid_devs' field for spares
	 */
	if (info->disk.state & (1 << MD_DISK_SYNC) || super->anchor->num_raid_devs)
		uuid_from_super_imsm(st, info->uuid);
	else
		memcpy(info->uuid, uuid_zero, sizeof(uuid_zero));

	/* I don't know how to compute 'map' on imsm, so use safe default */
	if (map) {
		int i;
		for (i = 0; i < map_disks; i++)
			map[i] = 1;
	}

}

/* allocates memory and fills disk in mdinfo structure
 * for each disk in array */
struct mdinfo *getinfo_super_disks_imsm(struct supertype *st)
{
	struct mdinfo *mddev;
	struct intel_super *super = st->sb;
	struct imsm_disk *disk;
	int count = 0;
	struct dl *dl;
	if (!super || !super->disks)
		return NULL;
	dl = super->disks;
	mddev = xcalloc(1, sizeof(*mddev));
	while (dl) {
		struct mdinfo *tmp;
		disk = &dl->disk;
		tmp = xcalloc(1, sizeof(*tmp));
		if (mddev->devs)
			tmp->next = mddev->devs;
		mddev->devs = tmp;
		tmp->disk.number = count++;
		tmp->disk.major = dl->major;
		tmp->disk.minor = dl->minor;
		tmp->disk.state = is_configured(disk) ?
				  (1 << MD_DISK_ACTIVE) : 0;
		tmp->disk.state |= is_failed(disk) ? (1 << MD_DISK_FAULTY) : 0;
		tmp->disk.state |= is_spare(disk) ? 0 : (1 << MD_DISK_SYNC);
		tmp->disk.raid_disk = -1;
		dl = dl->next;
	}
	return mddev;
}

static int update_super_imsm(struct supertype *st, struct mdinfo *info,
			     enum update_opt update, char *devname,
			     int verbose, int uuid_set, char *homehost)
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

	switch (update) {
	case UOPT_UUID:
		/* We take this to mean that the family_num should be updated.
		 * However that is much smaller than the uuid so we cannot really
		 * allow an explicit uuid to be given.  And it is hard to reliably
		 * know if one was.
		 * So if !uuid_set we know the current uuid is random and just used
		 * the first 'int' and copy it to the other 3 positions.
		 * Otherwise we require the 4 'int's to be the same as would be the
		 * case if we are using a random uuid.  So an explicit uuid will be
		 * accepted as long as all for ints are the same... which shouldn't hurt
		 */
		if (!uuid_set) {
			info->uuid[1] = info->uuid[2] = info->uuid[3] = info->uuid[0];
			rv = 0;
		} else {
			if (info->uuid[0] != info->uuid[1] ||
			    info->uuid[1] != info->uuid[2] ||
			    info->uuid[2] != info->uuid[3])
				rv = -1;
			else
				rv = 0;
		}
		if (rv == 0)
			mpb->orig_family_num = info->uuid[0];
		break;
	case UOPT_SPEC_ASSEMBLE:
		rv = 0;
		break;
	default:
		rv = -1;
		break;
	}

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
	/* maximum bbm log */
	size += sizeof(struct bbm_log);

	return size;
}

static __u64 avail_size_imsm(struct supertype *st, __u64 devsize,
			     unsigned long long data_offset)
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

static int compare_super_imsm(struct supertype *st, struct supertype *tst,
			      int verbose)
{
	/*  return:
	 *  0 same, or first was empty, and second was copied
	 *  1 sb are different
	 */
	struct intel_super *first = st->sb;
	struct intel_super *sec = tst->sb;

	if (!first) {
		st->sb = tst->sb;
		tst->sb = NULL;
		return 0;
	}

	/* in platform dependent environment test if the disks
	 * use the same Intel hba
	 * if not on Intel hba at all, allow anything.
	 * doesn't check HBAs if num_raid_devs is not set, as it means
	 * it is a free floating spare, and all spares regardless of HBA type
	 * will fall into separate container during the assembly
	 */
	if (first->hba && sec->hba && first->anchor->num_raid_devs != 0) {
		if (first->hba->type != sec->hba->type) {
			if (verbose)
				pr_err("HBAs of devices do not match %s != %s\n",
				       get_sys_dev_type(first->hba->type),
				       get_sys_dev_type(sec->hba->type));
			return 1;
		}
		if (first->orom != sec->orom) {
			if (verbose)
				pr_err("HBAs of devices do not match %s != %s\n",
				       first->hba->pci_id, sec->hba->pci_id);
			return 1;
		}
	}

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
			return 1;

		if (first_family == 0)
			first_family = first->anchor->family_num;
		if (sec_family == 0)
			sec_family = sec->anchor->family_num;

		if (first_family != sec_family)
			return 1;

	}

	/* if an anchor does not have num_raid_devs set then it is a free
	* floating spare. don't assosiate spare with any array, as during assembly
	* spares shall fall into separate container, from which they can be moved
	* when necessary
	*/
	if (first->anchor->num_raid_devs ^ sec->anchor->num_raid_devs)
		return 1;

	return 0;
}

static void fd2devname(int fd, char *name)
{
	char *nm;

	nm = fd2kname(fd);
	if (!nm)
		return;

	snprintf(name, MAX_RAID_SERIAL_LEN, "/dev/%s", nm);
}

static int nvme_get_serial(int fd, void *buf, size_t buf_len)
{
	char path[PATH_MAX];
	char *name = fd2kname(fd);

	if (!name)
		return 1;

	if (strncmp(name, "nvme", 4) != 0)
		return 1;

	if (!diskfd_to_devpath(fd, 1, path))
		return 1;

	return devpath_to_char(path, "serial", buf, buf_len, 0);
}

mdadm_status_t scsi_get_serial(int fd, void *buf, size_t buf_len)
{
	struct sg_io_hdr io_hdr = {0};
	unsigned char rsp_buf[255];
	unsigned char inq_cmd[] = {INQUIRY, 1, 0x80, 0, sizeof(rsp_buf), 0};
	unsigned char sense[32];
	unsigned int rsp_len;
	int rv;

	io_hdr.interface_id = 'S';
	io_hdr.cmdp = inq_cmd;
	io_hdr.cmd_len = sizeof(inq_cmd);
	io_hdr.dxferp = rsp_buf;
	io_hdr.dxfer_len = sizeof(rsp_buf);
	io_hdr.dxfer_direction = SG_DXFER_FROM_DEV;
	io_hdr.sbp = sense;
	io_hdr.mx_sb_len = sizeof(sense);
	io_hdr.timeout = 5000;

	rv = ioctl(fd, SG_IO, &io_hdr);

	if (rv)
		return MDADM_STATUS_ERROR;

	if ((io_hdr.info & SG_INFO_OK_MASK) != SG_INFO_OK)
		return MDADM_STATUS_ERROR;

	rsp_len = rsp_buf[3];

	if (!rsp_len || buf_len < rsp_len)
		return MDADM_STATUS_ERROR;

	memcpy(buf, &rsp_buf[4], rsp_len);

	return MDADM_STATUS_SUCCESS;
}


static int imsm_read_serial(int fd, char *devname,
			    __u8 *serial, size_t serial_buf_len)
{
	char buf[50];
	int rv;
	size_t len;
	char *dest;
	char *src;
	unsigned int i;

	memset(buf, 0, sizeof(buf));

	if (check_env("IMSM_DEVNAME_AS_SERIAL")) {
		memset(serial, 0, serial_buf_len);
		fd2devname(fd, (char *) serial);
		return 0;
	}

	rv = nvme_get_serial(fd, buf, sizeof(buf));

	if (rv)
		rv = scsi_get_serial(fd, buf, sizeof(buf));

	if (rv != 0) {
		if (devname)
			pr_err("Failed to retrieve serial for %s\n",
			       devname);
		return rv;
	}

	/* trim all whitespace and non-printable characters and convert
	 * ':' to ';'
	 */
	for (i = 0, dest = buf; i < sizeof(buf) && buf[i]; i++) {
		src = &buf[i];
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
	len = dest - buf;
	dest = buf;

	if (len > serial_buf_len) {
		/* truncate leading characters */
		dest += len - serial_buf_len;
		len = serial_buf_len;
	}

	memset(serial, 0, serial_buf_len);
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

static struct dl *serial_to_dl(__u8 *serial, struct intel_super *super)
{
	struct dl *dl;

	for (dl = super->disks; dl; dl = dl->next)
		if (serialcmp(dl->serial, serial) == 0)
			break;

	return dl;
}

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

	rv = imsm_read_serial(fd, devname, serial, MAX_RAID_SERIAL_LEN);

	if (rv != 0)
		return 2;

	dl = xcalloc(1, sizeof(*dl));

	if (fstat(fd, &stb) != 0) {
		free(dl);
		return 1;
	}
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
		dl->devname = xstrdup(devname);
	else
		dl->devname = xstrdup(name);

	/* look up this disk's index in the current anchor */
	disk = __serial_to_disk(dl->serial, super->anchor, &dl->index);
	if (disk) {
		dl->disk = *disk;
		/* only set index on disks that are a member of a
		 * populated contianer, i.e. one with raid_devs
		 */
		if (is_failed(&dl->disk))
			dl->index = -2;
		else if (is_spare(&dl->disk) || is_journal(&dl->disk))
			dl->index = -1;
	}

	return 0;
}

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
 * 5/ Migration (mig_state=1 migr_type=MIGR_GEN_MIGR map0state=normal
 *    map1state=normal)
 */
static void migrate(struct imsm_dev *dev, struct intel_super *super,
		    __u8 to_state, int migr_type)
{
	struct imsm_map *dest;
	struct imsm_map *src = get_imsm_map(dev, MAP_0);

	dev->vol.migr_state = MIGR_STATE_MIGRATING;
	set_migr_type(dev, migr_type);
	set_vol_curr_migr_unit(dev, 0);
	dest = get_imsm_map(dev, MAP_1);

	/* duplicate and then set the target end state in map[0] */
	memcpy(dest, src, sizeof_imsm_map(src));
	if (migr_type == MIGR_GEN_MIGR) {
		__u32 ord;
		int i;

		for (i = 0; i < src->num_members; i++) {
			ord = __le32_to_cpu(src->disk_ord_tbl[i]);
			set_imsm_ord_tbl_ent(src, i, ord_to_idx(ord));
		}
	}

	if (migr_type == MIGR_GEN_MIGR)
		/* Clear migration record */
		memset(super->migr_rec, 0, sizeof(struct migr_record));

	src->map_state = to_state;
}

static void end_migration(struct imsm_dev *dev, struct intel_super *super,
			  __u8 map_state)
{
	/* To avoid compilation error, saying dev can't be NULL when
	 * migr_state is assigned.
	 */
	if (dev == NULL)
		return;

	struct imsm_map *map = get_imsm_map(dev, MAP_0);
	struct imsm_map *prev = get_imsm_map(dev, dev->vol.migr_state == MIGR_STATE_NORMAL ?
						    MAP_0 : MAP_1);
	int i, j;

	/* merge any IMSM_ORD_REBUILD bits that were not successfully
	 * completed in the last migration.
	 *
	 * FIXME add support for raid-level-migration
	 */
	if (map_state != map->map_state && (is_gen_migration(dev) == false) &&
	    prev->map_state != IMSM_T_STATE_UNINITIALIZED) {
		/* when final map state is other than expected
		 * merge maps (not for migration)
		 */
		int failed;

		for (i = 0; i < prev->num_members; i++)
			for (j = 0; j < map->num_members; j++)
				/* during online capacity expansion
				 * disks position can be changed
				 * if takeover is used
				 */
				if (ord_to_idx(map->disk_ord_tbl[j]) ==
				    ord_to_idx(prev->disk_ord_tbl[i])) {
					map->disk_ord_tbl[j] |=
						prev->disk_ord_tbl[i];
					break;
				}
		failed = imsm_count_failed(super, dev, MAP_0);
		map_state = imsm_check_degraded(super, dev, failed, MAP_0);
	}

	dev->vol.migr_state = MIGR_STATE_NORMAL;
	set_migr_type(dev, 0);
	set_vol_curr_migr_unit(dev, 0);
	map->map_state = map_state;
}

static int parse_raid_devices(struct intel_super *super)
{
	int i;
	struct imsm_dev *dev_new;
	size_t len, len_migr;
	size_t max_len = 0;
	size_t space_needed = 0;
	struct imsm_super *mpb = super->anchor;

	for (i = 0; i < super->anchor->num_raid_devs; i++) {
		struct imsm_dev *dev_iter = __get_imsm_dev(super->anchor, i);
		struct intel_dev *dv;

		len = sizeof_imsm_dev(dev_iter, 0);
		len_migr = sizeof_imsm_dev(dev_iter, 1);
		if (len_migr > len)
			space_needed += len_migr - len;

		dv = xmalloc(sizeof(*dv));
		if (max_len < len_migr)
			max_len = len_migr;
		if (max_len > len_migr)
			space_needed += max_len - len_migr;
		dev_new = xmalloc(max_len);
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

		len = ROUND_UP(__le32_to_cpu(mpb->mpb_size) + space_needed,
			      super->sector_size);
		if (posix_memalign(&buf, MAX_SECTOR_SIZE, len) != 0)
			return 1;

		memcpy(buf, super->buf, super->len);
		memset(buf + super->len, 0, len - super->len);
		free(super->buf);
		super->buf = buf;
		super->len = len;
	}

	super->extra_space += space_needed;

	return 0;
}

/*******************************************************************************
 * Function:	check_mpb_migr_compatibility
 * Description:	Function checks for unsupported migration features:
 *		- migration optimization area (pba_of_lba0)
 *		- descending reshape (ascending_migr)
 * Parameters:
 *	super	: imsm metadata information
 * Returns:
 *	 0 : migration is compatible
 *	-1 : migration is not compatible
 ******************************************************************************/
int check_mpb_migr_compatibility(struct intel_super *super)
{
	struct imsm_map *map0, *map1;
	struct migr_record *migr_rec = super->migr_rec;
	int i;

	for (i = 0; i < super->anchor->num_raid_devs; i++) {
		struct imsm_dev *dev_iter = __get_imsm_dev(super->anchor, i);

		if (dev_iter->vol.migr_state == MIGR_STATE_MIGRATING &&
		    dev_iter->vol.migr_type == MIGR_GEN_MIGR) {
			/* This device is migrating */
			map0 = get_imsm_map(dev_iter, MAP_0);
			map1 = get_imsm_map(dev_iter, MAP_1);
			if (pba_of_lba0(map0) != pba_of_lba0(map1))
				/* migration optimization area was used */
				return -1;
			if (migr_rec->ascending_migr == 0 &&
			    migr_rec->dest_depth_per_unit > 0)
				/* descending reshape not supported yet */
				return -1;
		}
	}
	return 0;
}

static void __free_imsm(struct intel_super *super, int free_disks);

/* load_imsm_mpb - read matrix metadata
 * allocates super->mpb to be freed by free_imsm
 */
static int load_imsm_mpb(int fd, struct intel_super *super, char *devname)
{
	unsigned long long dsize;
	unsigned long long sectors;
	unsigned int sector_size = super->sector_size;
	struct stat;
	struct imsm_super *anchor;
	__u32 check_sum;

	get_dev_size(fd, NULL, &dsize);
	if (dsize < 2*sector_size) {
		if (devname)
			pr_err("%s: device to small for imsm\n",
			       devname);
		return 1;
	}

	if (lseek64(fd, dsize - (sector_size * 2), SEEK_SET) < 0) {
		if (devname)
			pr_err("Cannot seek to anchor block on %s: %s\n",
			       devname, strerror(errno));
		return 1;
	}

	if (posix_memalign((void **)&anchor, sector_size, sector_size) != 0) {
		if (devname)
			pr_err("Failed to allocate imsm anchor buffer on %s\n", devname);
		return 1;
	}
	if ((unsigned int)read(fd, anchor, sector_size) != sector_size) {
		if (devname)
			pr_err("Cannot read anchor block on %s: %s\n",
			       devname, strerror(errno));
		free(anchor);
		return 1;
	}

	if (strncmp((char *) anchor->sig, MPB_SIGNATURE, MPB_SIG_LEN) != 0) {
		if (devname)
			pr_err("no IMSM anchor on %s\n", devname);
		free(anchor);
		return 2;
	}

	__free_imsm(super, 0);
	/*  reload capability and hba */

	/* capability and hba must be updated with new super allocation */
	find_intel_hba_capability(fd, super, devname);
	super->len = ROUND_UP(anchor->mpb_size, sector_size);
	if (posix_memalign(&super->buf, MAX_SECTOR_SIZE, super->len) != 0) {
		if (devname)
			pr_err("unable to allocate %zu byte mpb buffer\n",
			       super->len);
		free(anchor);
		return 2;
	}
	memcpy(super->buf, anchor, sector_size);

	sectors = mpb_sectors(anchor, sector_size) - 1;
	free(anchor);

	if (posix_memalign(&super->migr_rec_buf, MAX_SECTOR_SIZE,
	    MIGR_REC_BUF_SECTORS*MAX_SECTOR_SIZE) != 0) {
		pr_err("could not allocate migr_rec buffer\n");
		free(super->buf);
		super->buf = NULL;
		return 2;
	}
	super->clean_migration_record_by_mdmon = 0;

	if (!sectors) {
		check_sum = __gen_imsm_checksum(super->anchor);
		if (check_sum != __le32_to_cpu(super->anchor->check_sum)) {
			if (devname)
				pr_err("IMSM checksum %x != %x on %s\n",
				       check_sum,
				       __le32_to_cpu(super->anchor->check_sum),
				       devname);
			return 2;
		}

		return 0;
	}

	/* read the extended mpb */
	if (lseek64(fd, dsize - (sector_size * (2 + sectors)), SEEK_SET) < 0) {
		if (devname)
			pr_err("Cannot seek to extended mpb on %s: %s\n",
			       devname, strerror(errno));
		return 1;
	}

	if ((unsigned int)read(fd, super->buf + sector_size,
		    super->len - sector_size) != super->len - sector_size) {
		if (devname)
			pr_err("Cannot read extended mpb on %s: %s\n",
			       devname, strerror(errno));
		return 2;
	}

	check_sum = __gen_imsm_checksum(super->anchor);
	if (check_sum != __le32_to_cpu(super->anchor->check_sum)) {
		if (devname)
			pr_err("IMSM checksum %x != %x on %s\n",
			       check_sum, __le32_to_cpu(super->anchor->check_sum),
			       devname);
		return 3;
	}

	return 0;
}

static int read_imsm_migr_rec(int fd, struct intel_super *super);

/* clears hi bits in metadata if MPB_ATTRIB_2TB_DISK not set */
static void clear_hi(struct intel_super *super)
{
	struct imsm_super *mpb = super->anchor;
	int i, n;
	if (mpb->attributes & MPB_ATTRIB_2TB_DISK)
		return;
	for (i = 0; i < mpb->num_disks; ++i) {
		struct imsm_disk *disk = &mpb->disk[i];
		disk->total_blocks_hi = 0;
	}
	for (i = 0; i < mpb->num_raid_devs; ++i) {
		struct imsm_dev *dev = get_imsm_dev(super, i);
		for (n = 0; n < 2; ++n) {
			struct imsm_map *map = get_imsm_map(dev, n);
			if (!map)
				continue;
			map->pba_of_lba0_hi = 0;
			map->blocks_per_member_hi = 0;
			map->num_data_stripes_hi = 0;
		}
	}
}

static int
load_and_parse_mpb(int fd, struct intel_super *super, char *devname, int keep_fd)
{
	int err;

	err = load_imsm_mpb(fd, super, devname);
	if (err)
		return err;
	if (super->sector_size == 4096)
		convert_from_4k(super);
	err = load_imsm_disk(fd, super, devname, keep_fd);
	if (err)
		return err;
	err = parse_raid_devices(super);
	if (err)
		return err;
	err = load_bbm_log(super);
	clear_hi(super);
	return err;
}

static void __free_imsm_disk(struct dl *d, int do_close)
{
	if (do_close)
		close_fd(&d->fd);
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
		__free_imsm_disk(d, 1);
	}
	while (super->disk_mgmt_list) {
		d = super->disk_mgmt_list;
		super->disk_mgmt_list = d->next;
		__free_imsm_disk(d, 1);
	}
	while (super->missing) {
		d = super->missing;
		super->missing = d->next;
		__free_imsm_disk(d, 1);
	}

}

/* free all the pieces hanging off of a super pointer */
static void __free_imsm(struct intel_super *super, int free_disks)
{
	struct intel_hba *elem, *next;

	if (super->buf) {
		free(super->buf);
		super->buf = NULL;
	}
	/* unlink capability description */
	super->orom = NULL;
	if (super->migr_rec_buf) {
		free(super->migr_rec_buf);
		super->migr_rec_buf = NULL;
	}
	if (free_disks)
		free_imsm_disks(super);
	free_devlist(super);
	elem = super->hba;
	while (elem) {
		if (elem->path)
			free((void *)elem->path);
		next = elem->next;
		free(elem);
		elem = next;
	}
	if (super->bbm_log)
		free(super->bbm_log);
	super->hba = NULL;
}

static void free_imsm(struct intel_super *super)
{
	__free_imsm(super, 1);
	free(super->bb.entries);
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
	struct intel_super *super = xcalloc(1, sizeof(*super));

	super->current_vol = -1;
	super->create_offset = ~((unsigned long long) 0);

	super->bb.entries = xmalloc(BBM_LOG_MAX_ENTRIES *
				   sizeof(struct md_bb_entry));
	if (!super->bb.entries) {
		free(super);
		return NULL;
	}

	return super;
}

/*
 * find and allocate hba and OROM/EFI based on valid fd of RAID component device
 */
static int find_intel_hba_capability(int fd, struct intel_super *super, char *devname)
{
	struct sys_dev *hba_name;
	int rv = 0;

	if (is_fd_valid(fd) && test_partition(fd)) {
		pr_err("imsm: %s is a partition, cannot be used in IMSM\n",
		       devname);
		return 1;
	}
	if (!is_fd_valid(fd) || check_no_platform()) {
		super->orom = NULL;
		super->hba = NULL;
		return 0;
	}
	hba_name = find_disk_attached_hba(fd, NULL);
	if (!hba_name) {
		if (devname)
			pr_err("%s is not attached to Intel(R) RAID controller.\n",
			       devname);
		return 1;
	}
	rv = attach_hba_to_super(super, hba_name);
	if (rv == 2) {
		if (devname) {
			struct intel_hba *hba = super->hba;

			pr_err("%s is attached to Intel(R) %s %s (%s),\n"
				"    but the container is assigned to Intel(R) %s %s (",
				devname,
				get_sys_dev_type(hba_name->type),
				hba_name->type == SYS_DEV_VMD || hba_name->type == SYS_DEV_SATA_VMD ?
					"domain" : "RAID controller",
				hba_name->pci_id ? : "Err!",
				get_sys_dev_type(super->hba->type),
				hba->type == SYS_DEV_VMD || hba_name->type == SYS_DEV_SATA_VMD ?
					"domain" : "RAID controller");

			while (hba) {
				fprintf(stderr, "%s", hba->pci_id ? : "Err!");
				if (hba->next)
					fprintf(stderr, ", ");
				hba = hba->next;
			}
			fprintf(stderr, ").\n"
				"    Mixing devices attached to different controllers is not allowed.\n");
		}
		return 2;
	}
	super->orom = find_imsm_capability(hba_name);
	if (!super->orom)
		return 3;

	return 0;
}

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

		dl = xmalloc(sizeof(*dl));
		dl->major = 0;
		dl->minor = 0;
		dl->fd = -1;
		dl->devname = xstrdup("missing");
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
				dprintf("mpb from %d:%d matches %d:%d\n",
					super->disks->major,
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

				dprintf("mpb from %d:%d replaces %d:%d\n",
					super->disks->major,
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

				dprintf("mpb from %d:%d prefer %d:%d\n",
					super->disks->major,
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
			idisk = xcalloc(1, sizeof(*idisk));
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
				dprintf("'%.16s' owner %d != %d\n",
					disk->serial, idisk->owner,
					owner);
		} else {
			dprintf("unknown disk %x [%d]: %.16s\n",
				__le32_to_cpu(mpb->family_num), i,
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
		pr_err("Conflict, offlining family %#x on '%s'\n",
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
			dprintf("marking family: %#x from %d:%d offline\n",
				super_table[i]->anchor->family_num,
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
		pr_err("Chose family %#x on '%s', assemble conflicts to new container with '--update=uuid'\n",
			__le32_to_cpu(s->anchor->family_num), s->disks->devname);

	/* collect all dl's onto 'champion', and update them to
	 * champion's version of the status
	 */
	for (s = *super_list; s; s = s->next) {
		struct imsm_super *mpb = champion->anchor;
		struct dl *dl = s->disks;

		if (s == champion)
			continue;

		mpb->attributes |= s->anchor->attributes & MPB_ATTRIB_2TB_DISK;

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

static int
get_sra_super_block(int fd, struct intel_super **super_list, char *devname, int *max, int keep_fd);
static int get_super_block(struct intel_super **super_list, char *devnm, char *devname,
			   int major, int minor, int keep_fd);
static int
get_devlist_super_block(struct md_list *devlist, struct intel_super **super_list,
			int *max, int keep_fd);

static int load_super_imsm_all(struct supertype *st, int fd, void **sbp,
			       char *devname, struct md_list *devlist,
			       int keep_fd)
{
	struct intel_super *super_list = NULL;
	struct intel_super *super = NULL;
	int err = 0;
	int i = 0;

	if (is_fd_valid(fd))
		/* 'fd' is an opened container */
		err = get_sra_super_block(fd, &super_list, devname, &i, keep_fd);
	else
		/* get super block from devlist devices */
		err = get_devlist_super_block(devlist, &super_list, &i, keep_fd);
	if (err)
		goto error;
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

	/* load migration record */
	err = load_imsm_migr_rec(super);
	if (err == -1) {
		/* migration is in progress,
		 * but migr_rec cannot be loaded,
		 */
		err = 4;
		goto error;
	}

	/* Check migration compatibility */
	if (err == 0 && check_mpb_migr_compatibility(super) != 0) {
		pr_err("Unsupported migration detected");
		if (devname)
			fprintf(stderr, " on %s\n", devname);
		else
			fprintf(stderr, " (IMSM).\n");

		err = 5;
		goto error;
	}

	err = 0;

 error:
	while (super_list) {
		struct intel_super *s = super_list;

		super_list = super_list->next;
		free_imsm(s);
	}

	if (err)
		return err;

	*sbp = super;
	if (is_fd_valid(fd))
		strcpy(st->container_devnm, fd2devnm(fd));
	else
		st->container_devnm[0] = 0;
	if (err == 0 && st->ss == NULL) {
		st->ss = &super_imsm;
		st->minor_version = 0;
		st->max_devs = IMSM_MAX_DEVICES;
	}
	return 0;
}

static int
get_devlist_super_block(struct md_list *devlist, struct intel_super **super_list,
			int *max, int keep_fd)
{
	struct md_list *tmpdev;
	int err = 0;
	int i = 0;

	for (i = 0, tmpdev = devlist; tmpdev; tmpdev = tmpdev->next) {
		if (tmpdev->used != 1)
			continue;
		if (tmpdev->container == 1) {
			int lmax = 0;
			int fd = dev_open(tmpdev->devname, O_RDONLY|O_EXCL);
			if (!is_fd_valid(fd)) {
				pr_err("cannot open device %s: %s\n",
					tmpdev->devname, strerror(errno));
				err = 8;
				goto error;
			}
			err = get_sra_super_block(fd, super_list,
						  tmpdev->devname, &lmax,
						  keep_fd);
			i += lmax;
			close(fd);
			if (err) {
				err = 7;
				goto error;
			}
		} else {
			int major = major(tmpdev->st_rdev);
			int minor = minor(tmpdev->st_rdev);
			err = get_super_block(super_list,
					      NULL,
					      tmpdev->devname,
					      major, minor,
					      keep_fd);
			i++;
			if (err) {
				err = 6;
				goto error;
			}
		}
	}
 error:
	*max = i;
	return err;
}

static int get_super_block(struct intel_super **super_list, char *devnm, char *devname,
			   int major, int minor, int keep_fd)
{
	struct intel_super *s;
	char nm[32];
	int dfd = -1;
	int err = 0;
	int retry;

	s = alloc_super();
	if (!s) {
		err = 1;
		goto error;
	}

	sprintf(nm, "%d:%d", major, minor);
	dfd = dev_open(nm, O_RDWR);
	if (!is_fd_valid(dfd)) {
		err = 2;
		goto error;
	}

	if (!get_dev_sector_size(dfd, NULL, &s->sector_size)) {
		err = 2;
		goto error;
	}
	find_intel_hba_capability(dfd, s, devname);
	err = load_and_parse_mpb(dfd, s, NULL, keep_fd);

	/* retry the load if we might have raced against mdmon */
	if (err == 3 && devnm && mdmon_running(devnm))
		for (retry = 0; retry < 3; retry++) {
			sleep_for(0, MSEC_TO_NSEC(3), true);
			err = load_and_parse_mpb(dfd, s, NULL, keep_fd);
			if (err != 3)
				break;
		}
 error:
	if (!err) {
		s->next = *super_list;
		*super_list = s;
	} else {
		if (s)
			free_imsm(s);
		close_fd(&dfd);
	}
	if (!keep_fd)
		close_fd(&dfd);
	return err;

}

static int
get_sra_super_block(int fd, struct intel_super **super_list, char *devname, int *max, int keep_fd)
{
	struct mdinfo *sra;
	char *devnm;
	struct mdinfo *sd;
	int err = 0;
	int i = 0;
	sra = sysfs_read(fd, NULL, GET_LEVEL|GET_VERSION|GET_DEVS|GET_STATE);
	if (!sra)
		return 1;

	if (sra->array.major_version != -1 ||
	    sra->array.minor_version != -2 ||
	    strcmp(sra->text_version, "imsm") != 0) {
		err = 1;
		goto error;
	}
	/* load all mpbs */
	devnm = fd2devnm(fd);
	for (sd = sra->devs, i = 0; sd; sd = sd->next, i++) {
		if (get_super_block(super_list, devnm, devname,
				    sd->disk.major, sd->disk.minor, keep_fd) != 0) {
			err = 7;
			goto error;
		}
	}
 error:
	sysfs_free(sra);
	*max = i;
	return err;
}

static int load_container_imsm(struct supertype *st, int fd, char *devname)
{
	return load_super_imsm_all(st, fd, &st->sb, devname, NULL, 1);
}

static int load_super_imsm(struct supertype *st, int fd, char *devname)
{
	struct intel_super *super;
	int rv;
	int retry;

	if (test_partition(fd))
		/* IMSM not allowed on partitions */
		return 1;

	free_super_imsm(st);

	super = alloc_super();
	if (!super)
		return 1;

	if (!get_dev_sector_size(fd, NULL, &super->sector_size)) {
		free_imsm(super);
		return 1;
	}
	/* Load hba and capabilities if they exist.
	 * But do not preclude loading metadata in case capabilities or hba are
	 * non-compliant and ignore_hw_compat is set.
	 */
	rv = find_intel_hba_capability(fd, super, devname);
	/* no orom/efi or non-intel hba of the disk */
	if (rv != 0 && st->ignore_hw_compat == 0) {
		if (devname)
			pr_err("No OROM/EFI properties for %s\n", devname);
		free_imsm(super);
		return 2;
	}
	rv = load_and_parse_mpb(fd, super, devname, 0);

	/* retry the load if we might have raced against mdmon */
	if (rv == 3) {
		struct mdstat_ent *mdstat = NULL;
		char *name = fd2kname(fd);

		if (name)
			mdstat = mdstat_by_component(name);

		if (mdstat && mdmon_running(mdstat->devnm) && getpid() != mdmon_pid(mdstat->devnm)) {
			for (retry = 0; retry < 3; retry++) {
				sleep_for(0, MSEC_TO_NSEC(3), true);
				rv = load_and_parse_mpb(fd, super, devname, 0);
				if (rv != 3)
					break;
			}
		}

		free_mdstat(mdstat);
	}

	if (rv) {
		if (devname)
			pr_err("Failed to load all information sections on %s\n", devname);
		free_imsm(super);
		return rv;
	}

	st->sb = super;
	if (st->ss == NULL) {
		st->ss = &super_imsm;
		st->minor_version = 0;
		st->max_devs = IMSM_MAX_DEVICES;
	}

	/* load migration record */
	if (load_imsm_migr_rec(super) == 0) {
		/* Check for unsupported migration features */
		if (check_mpb_migr_compatibility(super) != 0) {
			pr_err("Unsupported migration detected");
			if (devname)
				fprintf(stderr, " on %s\n", devname);
			else
				fprintf(stderr, " (IMSM).\n");
			return 3;
		}
	}

	return 0;
}

static __u16 info_to_blocks_per_strip(mdu_array_info_t *info)
{
	if (info->level == 1)
		return 128;
	return info->chunk_size >> 9;
}

static unsigned long long info_to_blocks_per_member(mdu_array_info_t *info,
						    unsigned long long size)
{
	if (info->level == 1)
		return size * 2;
	else
		return (size * 2) & ~(info_to_blocks_per_strip(info) - 1);
}

static void imsm_write_signature(struct imsm_super *mpb)
{
	/* It is safer to eventually truncate version rather than left it not NULL ended */
	snprintf((char *) mpb->sig, MAX_SIGNATURE_LENGTH, MPB_SIGNATURE MPB_VERSION_ATTRIBS);
}

static void imsm_update_version_info(struct intel_super *super)
{
	/* update the version and attributes */
	struct imsm_super *mpb = super->anchor;
	struct imsm_dev *dev;
	struct imsm_map *map;
	int i;

	mpb->attributes |= MPB_ATTRIB_CHECKSUM_VERIFY;

	for (i = 0; i < mpb->num_raid_devs; i++) {
		dev = get_imsm_dev(super, i);
		map = get_imsm_map(dev, MAP_0);

		if (__le32_to_cpu(dev->size_high) > 0)
			mpb->attributes |= MPB_ATTRIB_2TB;

		switch (get_imsm_raid_level(map)) {
		case IMSM_T_RAID0:
			mpb->attributes |= MPB_ATTRIB_RAID0;
			break;
		case IMSM_T_RAID1:
			mpb->attributes |= MPB_ATTRIB_RAID1;
			break;
		case IMSM_T_RAID5:
			mpb->attributes |= MPB_ATTRIB_RAID5;
			break;
		case IMSM_T_RAID10:
			mpb->attributes |= MPB_ATTRIB_RAID10;
			if (map->num_members > 4)
				mpb->attributes |= MPB_ATTRIB_RAID10_EXT;
			break;
		}
	}

	imsm_write_signature(mpb);
}

/**
 * imsm_check_name() - check imsm naming criteria.
 * @super: &intel_super pointer, not NULL.
 * @name: name to check.
 * @verbose: verbose level.
 *
 * Name must be no longer than &MAX_RAID_SERIAL_LEN and must be unique across volumes.
 *
 * Returns: &true if @name matches, &false otherwise.
 */
static bool imsm_is_name_allowed(struct intel_super *super, const char * const name,
				 const int verbose)
{
	struct imsm_super *mpb = super->anchor;
	int i;

	if (is_string_lq(name, MAX_RAID_SERIAL_LEN + 1) == false) {
		pr_vrb("imsm: Name \"%s\" is too long\n", name);
		return false;
	}

	for (i = 0; i < mpb->num_raid_devs; i++) {
		struct imsm_dev *dev = get_imsm_dev(super, i);

		if (strncmp((char *) dev->volume, name, MAX_RAID_SERIAL_LEN) == 0) {
			pr_vrb("imsm: Name \"%s\" already exists\n", name);
			return false;
		}
	}

	return true;
}

static int init_super_imsm_volume(struct supertype *st, mdu_array_info_t *info,
				  struct shape *s, char *name,
				  char *homehost, int *uuid,
				  long long data_offset)
{
	/* We are creating a volume inside a pre-existing container.
	 * so st->sb is already set.
	 */
	struct intel_super *super = st->sb;
	unsigned int sector_size = super->sector_size;
	struct imsm_super *mpb = super->anchor;
	struct intel_dev *dv;
	struct imsm_dev *dev;
	struct imsm_vol *vol;
	struct imsm_map *map;
	int idx = mpb->num_raid_devs;
	int i;
	int namelen;
	unsigned long long array_blocks;
	size_t size_old, size_new;
	unsigned int data_disks;
	unsigned long long size_per_member;

	if (super->orom && mpb->num_raid_devs >= super->orom->vpa) {
		pr_err("This imsm-container already has the maximum of %d volumes\n", super->orom->vpa);
		return 0;
	}

	/* ensure the mpb is large enough for the new data */
	size_old = __le32_to_cpu(mpb->mpb_size);
	size_new = disks_to_mpb_size(info->nr_disks);
	if (size_new > size_old) {
		void *mpb_new;
		size_t size_round = ROUND_UP(size_new, sector_size);

		if (posix_memalign(&mpb_new, sector_size, size_round) != 0) {
			pr_err("could not allocate new mpb\n");
			return 0;
		}
		if (posix_memalign(&super->migr_rec_buf, MAX_SECTOR_SIZE,
				   MIGR_REC_BUF_SECTORS*
				   MAX_SECTOR_SIZE) != 0) {
			pr_err("could not allocate migr_rec buffer\n");
			free(super->buf);
			free(super);
			free(mpb_new);
			return 0;
		}
		memcpy(mpb_new, mpb, size_old);
		free(mpb);
		mpb = mpb_new;
		super->anchor = mpb_new;
		mpb->mpb_size = __cpu_to_le32(size_new);
		memset(mpb_new + size_old, 0, size_round - size_old);
		super->len = size_round;
	}
	super->current_vol = idx;

	/* handle 'failed_disks' by either:
	 * a) create dummy disk entries in the table if this the first
	 *    volume in the array.  We add them here as this is the only
	 *    opportunity to add them. add_to_super_imsm_volume()
	 *    handles the non-failed disks and continues incrementing
	 *    mpb->num_disks.
	 * b) validate that 'failed_disks' matches the current number
	 *    of missing disks if the container is populated
	 */
	if (super->current_vol == 0) {
		mpb->num_disks = 0;
		for (i = 0; i < info->failed_disks; i++) {
			struct imsm_disk *disk;

			mpb->num_disks++;
			disk = __get_imsm_disk(mpb, i);
			disk->status = CONFIGURED_DISK | FAILED_DISK;
			disk->scsi_id = __cpu_to_le32(~(__u32)0);
			snprintf((char *) disk->serial, MAX_RAID_SERIAL_LEN,
				 "missing:%d", (__u8)i);
		}
		find_missing(super);
	} else {
		int missing = 0;
		struct dl *d;

		for (d = super->missing; d; d = d->next)
			missing++;
		if (info->failed_disks > missing) {
			pr_err("unable to add 'missing' disk to container\n");
			return 0;
		}
	}

	if (imsm_is_name_allowed(super, name, 1) == false)
		return 0;

	dv = xmalloc(sizeof(*dv));
	dev = xcalloc(1, sizeof(*dev) + sizeof(__u32) * (info->raid_disks - 1));
	/*
	 * Explicitly allow truncating to not confuse gcc's
	 * -Werror=stringop-truncation
	 */
	namelen = min((int) strlen(name), MAX_RAID_SERIAL_LEN);
	memcpy(dev->volume, name, namelen);
	array_blocks = calc_array_size(info->level, info->raid_disks,
					       info->layout, info->chunk_size,
					       s->size * BLOCKS_PER_KB);
	data_disks = get_data_disks(info->level, info->layout,
				    info->raid_disks);
	array_blocks = round_size_to_mb(array_blocks, data_disks);
	size_per_member = array_blocks / data_disks;

	set_imsm_dev_size(dev, array_blocks);
	dev->status = (DEV_READ_COALESCING | DEV_WRITE_COALESCING);
	vol = &dev->vol;
	vol->migr_state = MIGR_STATE_NORMAL;
	set_migr_type(dev, MIGR_INIT);
	vol->dirty = !info->state;
	set_vol_curr_migr_unit(dev, 0);
	map = get_imsm_map(dev, MAP_0);
	set_pba_of_lba0(map, super->create_offset);
	map->blocks_per_strip = __cpu_to_le16(info_to_blocks_per_strip(info));
	map->failed_disk_num = ~0;
	if (info->level > IMSM_T_RAID0)
		map->map_state = (info->state ? IMSM_T_STATE_NORMAL
				  : IMSM_T_STATE_UNINITIALIZED);
	else
		map->map_state = info->failed_disks ? IMSM_T_STATE_FAILED :
						      IMSM_T_STATE_NORMAL;
	map->ddf = 1;

	if (info->level == IMSM_T_RAID1 && info->raid_disks > 2) {
		free(dev);
		free(dv);
		pr_err("imsm does not support more than 2 disks in a raid1 volume\n");
		return 0;
	}
	map->num_members = info->raid_disks;

	update_imsm_raid_level(map, info->level);
	set_num_domains(map);

	size_per_member += NUM_BLOCKS_DIRTY_STRIPE_REGION;
	set_blocks_per_member(map, info_to_blocks_per_member(info,
							     size_per_member /
							     BLOCKS_PER_KB));

	update_num_data_stripes(map, array_blocks);
	for (i = 0; i < map->num_members; i++) {
		/* initialized in add_to_super */
		set_imsm_ord_tbl_ent(map, i, IMSM_ORD_REBUILD);
	}
	mpb->num_raid_devs++;
	mpb->num_raid_devs_created++;
	dev->my_vol_raid_dev_num = mpb->num_raid_devs_created;

	if (s->consistency_policy <= CONSISTENCY_POLICY_RESYNC) {
		dev->rwh_policy = RWH_MULTIPLE_OFF;
	} else if (s->consistency_policy == CONSISTENCY_POLICY_PPL) {
		dev->rwh_policy = RWH_MULTIPLE_DISTRIBUTED;
	} else {
		free(dev);
		free(dv);
		pr_err("imsm does not support consistency policy %s\n",
		       map_num_s(consistency_policies, s->consistency_policy));
		return 0;
	}

	dv->dev = dev;
	dv->index = super->current_vol;
	dv->next = super->devlist;
	super->devlist = dv;

	imsm_update_version_info(super);

	return 1;
}

static int init_super_imsm(struct supertype *st, mdu_array_info_t *info,
		           struct shape *s, char *name,
			   char *homehost, int *uuid,
			   unsigned long long data_offset)
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

	if (data_offset != INVALID_SECTORS) {
		pr_err("data-offset not supported by imsm\n");
		return 0;
	}

	if (st->sb)
		return init_super_imsm_volume(st, info, s, name, homehost, uuid,
					      data_offset);

	if (info)
		mpb_size = disks_to_mpb_size(info->nr_disks);
	else
		mpb_size = MAX_SECTOR_SIZE;

	super = alloc_super();
	if (super &&
	    posix_memalign(&super->buf, MAX_SECTOR_SIZE, mpb_size) != 0) {
		free_imsm(super);
		super = NULL;
	}
	if (!super) {
		pr_err("could not allocate superblock\n");
		return 0;
	}
	if (posix_memalign(&super->migr_rec_buf, MAX_SECTOR_SIZE,
	    MIGR_REC_BUF_SECTORS*MAX_SECTOR_SIZE) != 0) {
		pr_err("could not allocate migr_rec buffer\n");
		free(super->buf);
		free_imsm(super);
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

	imsm_update_version_info(super);
	return 1;
}

static int drive_validate_sector_size(struct intel_super *super, struct dl *dl)
{
	unsigned int member_sector_size;

	if (!is_fd_valid(dl->fd)) {
		pr_err("Invalid file descriptor for %s\n", dl->devname);
		return 0;
	}

	if (!get_dev_sector_size(dl->fd, dl->devname, &member_sector_size))
		return 0;
	if (member_sector_size != super->sector_size)
		return 0;
	return 1;
}

static int add_to_super_imsm_volume(struct supertype *st, mdu_disk_info_t *dk,
				     int fd, char *devname)
{
	struct intel_super *super = st->sb;
	struct imsm_super *mpb = super->anchor;
	struct imsm_disk *_disk;
	struct imsm_dev *dev;
	struct imsm_map *map;
	struct dl *dl, *df;
	int slot;
	int autolayout = 0;

	if (!is_fd_valid(fd))
		autolayout = 1;

	dev = get_imsm_dev(super, super->current_vol);
	map = get_imsm_map(dev, MAP_0);

	if (! (dk->state & (1<<MD_DISK_SYNC))) {
		pr_err("%s: Cannot add spare devices to IMSM volume\n",
			devname);
		return 1;
	}

	for (dl = super->disks; dl ; dl = dl->next) {
		if (autolayout) {
			if (dl->raiddisk == dk->raid_disk)
				break;
		} else if (dl->major == dk->major && dl->minor == dk->minor)
			break;
	}

	if (!dl) {
		if (!autolayout)
			pr_err("%s is not a member of the same container.\n",
			       devname);
		return 1;
	}

	if (!autolayout && super->current_vol > 0) {
		int _slot = get_disk_slot_in_dev(super, 0, dl->index);

		if (_slot != dk->raid_disk) {
			pr_err("Member %s is in %d slot for the first volume, but is in %d slot for a new volume.\n",
			       dl->devname, _slot, dk->raid_disk);
			pr_err("Raid members are in different order than for the first volume, aborting.\n");
			return 1;
		}
	}

	if (mpb->num_disks == 0)
		if (!get_dev_sector_size(dl->fd, dl->devname,
					 &super->sector_size))
			return 1;

	if (!drive_validate_sector_size(super, dl)) {
		pr_err("Combining drives of different sector size in one volume is not allowed\n");
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
	    (get_imsm_ord_tbl_ent(dev, slot, MAP_X) & IMSM_ORD_REBUILD) == 0) {
		pr_err("%s has been included in this array twice\n",
			devname);
		return 1;
	}
	set_imsm_ord_tbl_ent(map, dk->raid_disk, dl->index);
	dl->disk.status = CONFIGURED_DISK;

	/* update size of 'missing' disks to be at least as large as the
	 * largest acitve member (we only have dummy missing disks when
	 * creating the first volume)
	 */
	if (super->current_vol == 0) {
		for (df = super->missing; df; df = df->next) {
			if (total_blocks(&dl->disk) > total_blocks(&df->disk))
				set_total_blocks(&df->disk, total_blocks(&dl->disk));
			_disk = __get_imsm_disk(mpb, df->index);
			*_disk = df->disk;
		}
	}

	/* refresh unset/failed slots to point to valid 'missing' entries */
	for (df = super->missing; df; df = df->next)
		for (slot = 0; slot < mpb->num_disks; slot++) {
			__u32 ord = get_imsm_ord_tbl_ent(dev, slot, MAP_X);

			if ((ord & IMSM_ORD_REBUILD) == 0)
				continue;
			set_imsm_ord_tbl_ent(map, slot, df->index | IMSM_ORD_REBUILD);
			if (is_gen_migration(dev)) {
				struct imsm_map *map2 = get_imsm_map(dev,
								     MAP_1);
				int slot2 = get_imsm_disk_slot(map2, df->index);
				if (slot2 < map2->num_members && slot2 >= 0) {
					__u32 ord2 = get_imsm_ord_tbl_ent(dev,
									 slot2,
									 MAP_1);
					if ((unsigned)df->index ==
							       ord_to_idx(ord2))
						set_imsm_ord_tbl_ent(map2,
							slot2,
							df->index |
							IMSM_ORD_REBUILD);
				}
			}
			dprintf("set slot:%d to missing disk:%d\n", slot, df->index);
			break;
		}

	/* if we are creating the first raid device update the family number */
	if (super->current_vol == 0) {
		__u32 sum;
		struct imsm_dev *_dev = __get_imsm_dev(mpb, 0);

		_disk = __get_imsm_disk(mpb, dl->index);
		if (!_disk) {
			pr_err("BUG mpb setup error\n");
			return 1;
		}
		*_dev = *dev;
		*_disk = dl->disk;
		sum = random32();
		sum += __gen_imsm_checksum(mpb);
		mpb->family_num = __cpu_to_le32(sum);
		mpb->orig_family_num = mpb->family_num;
		mpb->creation_time = __cpu_to_le64((__u64)time(NULL));
	}
	super->current_disk = dl;
	return 0;
}

/* mark_spare()
 *   Function marks disk as spare and restores disk serial
 *   in case it was previously marked as failed by takeover operation
 * reruns:
 *   -1 : critical error
 *    0 : disk is marked as spare but serial is not set
 *    1 : success
 */
int mark_spare(struct dl *disk)
{
	__u8 serial[MAX_RAID_SERIAL_LEN];
	int ret_val = -1;

	if (!disk)
		return ret_val;

	ret_val = 0;
	if (!imsm_read_serial(disk->fd, NULL, serial, MAX_RAID_SERIAL_LEN)) {
		/* Restore disk serial number, because takeover marks disk
		 * as failed and adds to serial ':0' before it becomes
		 * a spare disk.
		 */
		serialcpy(disk->serial, serial);
		serialcpy(disk->disk.serial, serial);
		ret_val = 1;
	}
	disk->disk.status = SPARE_DISK;
	disk->index = -1;

	return ret_val;
}


static int write_super_imsm_spare(struct intel_super *super, struct dl *d);

static int add_to_super_imsm(struct supertype *st, mdu_disk_info_t *dk,
			     int fd, char *devname,
			     unsigned long long data_offset)
{
	struct intel_super *super = st->sb;
	unsigned int member_sector_size;
	unsigned long long size;
	struct stat stb;
	struct dl *dd;
	__u32 id;
	int rv;

	/* If we are on an RAID enabled platform check that the disk is
	 * attached to the raid controller.
	 * We do not need to test disks attachment for container based additions,
	 * they shall be already tested when container was created/assembled.
	 */
	rv = find_intel_hba_capability(fd, super, devname);
	/* no orom/efi or non-intel hba of the disk */
	if (rv != 0) {
		dprintf("capability: %p fd: %d ret: %d\n", super->orom, fd, rv);
		return MDADM_STATUS_ERROR;
	}

	if (super->current_vol >= 0)
		return add_to_super_imsm_volume(st, dk, fd, devname);

	if (fstat(fd, &stb) != 0)
		return MDADM_STATUS_ERROR;

	dd = xcalloc(sizeof(*dd), 1);

	if (devname)
		dd->devname = xstrdup(devname);

	if (sysfs_disk_to_scsi_id(fd, &id) == 0)
		dd->disk.scsi_id = __cpu_to_le32(id);

	dd->major = major(stb.st_rdev);
	dd->minor = minor(stb.st_rdev);
	dd->action = DISK_ADD;
	dd->fd = fd;

	rv = imsm_read_serial(fd, devname, dd->serial, MAX_RAID_SERIAL_LEN);
	if (rv) {
		pr_err("failed to retrieve scsi serial, aborting\n");
		goto error;
	}

	if (super->hba && ((super->hba->type == SYS_DEV_NVME) ||
	   (super->hba->type == SYS_DEV_VMD))) {
		char pci_dev_path[PATH_MAX];
		char cntrl_path[PATH_MAX];

		if (!diskfd_to_devpath(fd, 2, pci_dev_path) ||
		    !diskfd_to_devpath(fd, 1, cntrl_path)) {
			pr_err("failed to get dev paths, aborting\n");
			goto error;
		}

		if (is_multipath_nvme(fd))
			pr_err("%s controller supports Multi-Path I/O, Intel (R) VROC does not support multipathing\n",
			       basename(cntrl_path));

		if (super->orom && devpath_to_vendor(pci_dev_path) != 0x8086 &&
		    !imsm_orom_has_tpv_support(super->orom)) {
			pr_err("\tPlatform configuration does not support non-Intel NVMe drives.\n"
			       "\tPlease refer to Intel(R) RSTe/VROC user guide.\n");
			goto error;
		}
	}

	if (!get_dev_size(fd, NULL, &size) || !get_dev_sector_size(fd, NULL, &member_sector_size))
		goto error;

	if (super->sector_size == 0)
		/* this a first device, so sector_size is not set yet */
		super->sector_size = member_sector_size;

	/* clear migr_rec when adding disk to container */
	memset(super->migr_rec_buf, 0, MIGR_REC_BUF_SECTORS * MAX_SECTOR_SIZE);

	if (lseek64(fd, (size - MIGR_REC_SECTOR_POSITION * member_sector_size), SEEK_SET) >= 0) {
		unsigned int nbytes = MIGR_REC_BUF_SECTORS * member_sector_size;

		if ((unsigned int)write(fd, super->migr_rec_buf, nbytes) != nbytes)
			perror("Write migr_rec failed");
	}

	size /= 512;
	serialcpy(dd->disk.serial, dd->serial);
	set_total_blocks(&dd->disk, size);

	if (__le32_to_cpu(dd->disk.total_blocks_hi) > 0) {
		struct imsm_super *mpb = super->anchor;

		mpb->attributes |= MPB_ATTRIB_2TB_DISK;
	}

	mark_spare(dd);

	if (st->update_tail) {
		dd->next = super->disk_mgmt_list;
		super->disk_mgmt_list = dd;
	} else {
		/* this is called outside of mdmon
		 * write initial spare metadata
		 * mdmon will overwrite it.
		 */
		dd->next = super->disks;
		super->disks = dd;
		write_super_imsm_spare(super, dd);
	}

	return MDADM_STATUS_SUCCESS;

error:
	__free_imsm_disk(dd, 0);
	return MDADM_STATUS_ERROR;
}

static int remove_from_super_imsm(struct supertype *st, mdu_disk_info_t *dk)
{
	struct intel_super *super = st->sb;
	struct dl *dd;

	/* remove from super works only in mdmon - for communication
	 * manager - monitor. Check if communication memory buffer
	 * is prepared.
	 */
	if (!st->update_tail) {
		pr_err("shall be used in mdmon context only\n");
		return 1;
	}
	dd = xcalloc(1, sizeof(*dd));
	dd->major = dk->major;
	dd->minor = dk->minor;
	dd->fd = -1;
	mark_spare(dd);
	dd->action = DISK_REMOVE;

	dd->next = super->disk_mgmt_list;
	super->disk_mgmt_list = dd;

	return 0;
}

static int store_imsm_mpb(int fd, struct imsm_super *mpb);

static union {
	char buf[MAX_SECTOR_SIZE];
	struct imsm_super anchor;
} spare_record __attribute__ ((aligned(MAX_SECTOR_SIZE)));


static int write_super_imsm_spare(struct intel_super *super, struct dl *d)
{
	struct imsm_super *spare = &spare_record.anchor;
	__u32 sum;

	if (d->index != -1)
		return 1;

	spare->mpb_size = __cpu_to_le32(sizeof(struct imsm_super));
	spare->generation_num = __cpu_to_le32(1UL);
	spare->num_disks = 1;
	spare->num_raid_devs = 0;
	spare->pwr_cycle_count = __cpu_to_le32(1);

	imsm_write_signature(spare);

	spare->disk[0] = d->disk;
	if (__le32_to_cpu(d->disk.total_blocks_hi) > 0)
		spare->attributes |= MPB_ATTRIB_2TB_DISK;

	if (super->sector_size == 4096)
		convert_to_4k_imsm_disk(&spare->disk[0]);

	sum = __gen_imsm_checksum(spare);
	spare->family_num = __cpu_to_le32(sum);
	spare->orig_family_num = 0;
	sum = __gen_imsm_checksum(spare);
	spare->check_sum = __cpu_to_le32(sum);

	if (store_imsm_mpb(d->fd, spare)) {
		pr_err("failed for device %d:%d %s\n",
			d->major, d->minor, strerror(errno));
		return 1;
	}

	return 0;
}
/* spare records have their own family number and do not have any defined raid
 * devices
 */
static int write_super_imsm_spares(struct intel_super *super, int doclose)
{
	struct dl *d;

	for (d = super->disks; d; d = d->next) {
		if (d->index != -1)
			continue;

		if (write_super_imsm_spare(super, d))
			return 1;

		if (doclose)
			close_fd(&d->fd);
	}

	return 0;
}

static int write_super_imsm(struct supertype *st, int doclose)
{
	struct intel_super *super = st->sb;
	unsigned int sector_size = super->sector_size;
	struct imsm_super *mpb = super->anchor;
	struct dl *d;
	__u32 generation;
	__u32 sum;
	int spares = 0;
	int i;
	__u32 mpb_size = sizeof(struct imsm_super) - sizeof(struct imsm_disk);
	int num_disks = 0;
	int clear_migration_record = 1;
	__u32 bbm_log_size;

	/* 'generation' is incremented everytime the metadata is written */
	generation = __le32_to_cpu(mpb->generation_num);
	generation++;
	mpb->generation_num = __cpu_to_le32(generation);

	/* fix up cases where previous mdadm releases failed to set
	 * orig_family_num
	 */
	if (mpb->orig_family_num == 0)
		mpb->orig_family_num = mpb->family_num;

	for (d = super->disks; d; d = d->next) {
		if (d->index == -1)
			spares++;
		else {
			mpb->disk[d->index] = d->disk;
			num_disks++;
		}
	}
	for (d = super->missing; d; d = d->next) {
		mpb->disk[d->index] = d->disk;
		num_disks++;
	}
	mpb->num_disks = num_disks;
	mpb_size += sizeof(struct imsm_disk) * mpb->num_disks;

	for (i = 0; i < mpb->num_raid_devs; i++) {
		struct imsm_dev *dev = __get_imsm_dev(mpb, i);
		struct imsm_dev *dev2 = get_imsm_dev(super, i);

		imsm_copy_dev(dev, dev2);
		mpb_size += sizeof_imsm_dev(dev, 0);

		if (is_gen_migration(dev2))
			clear_migration_record = 0;
	}

	bbm_log_size = get_imsm_bbm_log_size(super->bbm_log);

	if (bbm_log_size) {
		memcpy((void *)mpb + mpb_size, super->bbm_log, bbm_log_size);
		mpb->attributes |= MPB_ATTRIB_BBM;
	} else
		mpb->attributes &= ~MPB_ATTRIB_BBM;

	super->anchor->bbm_log_size = __cpu_to_le32(bbm_log_size);
	mpb_size += bbm_log_size;
	mpb->mpb_size = __cpu_to_le32(mpb_size);

#ifdef DEBUG
	assert(super->len == 0 || mpb_size <= super->len);
#endif

	/* recalculate checksum */
	sum = __gen_imsm_checksum(mpb);
	mpb->check_sum = __cpu_to_le32(sum);

	if (super->clean_migration_record_by_mdmon) {
		clear_migration_record = 1;
		super->clean_migration_record_by_mdmon = 0;
	}
	if (clear_migration_record)
		memset(super->migr_rec_buf, 0,
		    MIGR_REC_BUF_SECTORS*MAX_SECTOR_SIZE);

	if (sector_size == 4096)
		convert_to_4k(super);

	/* write the mpb for disks that compose raid devices */
	for (d = super->disks; d ; d = d->next) {
		if (d->index < 0 || is_failed(&d->disk))
			continue;

		if (clear_migration_record) {
			unsigned long long dsize;

			get_dev_size(d->fd, NULL, &dsize);
			if (lseek64(d->fd, dsize - sector_size,
			    SEEK_SET) >= 0) {
				if ((unsigned int)write(d->fd,
				    super->migr_rec_buf,
				    MIGR_REC_BUF_SECTORS*sector_size) !=
				    MIGR_REC_BUF_SECTORS*sector_size)
					perror("Write migr_rec failed");
			}
		}

		if (store_imsm_mpb(d->fd, mpb))
			fprintf(stderr,
				"failed for device %d:%d (fd: %d)%s\n",
				d->major, d->minor,
				d->fd, strerror(errno));

		if (doclose)
			close_fd(&d->fd);
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
	struct imsm_map *map = get_imsm_map(dev, MAP_0);
	struct disk_info *inf;
	struct imsm_disk *disk;
	int i;

	len = sizeof(*u) - sizeof(*dev) + sizeof_imsm_dev(dev, 0) +
	      sizeof(*inf) * map->num_members;
	u = xmalloc(len);
	u->type = update_create_array;
	u->dev_idx = dev_idx;
	imsm_copy_dev(&u->dev, dev);
	inf = get_disk_info(u);
	for (i = 0; i < map->num_members; i++) {
		int idx = get_imsm_disk_idx(dev, i, MAP_X);

		disk = get_imsm_disk(super, idx);
		if (!disk)
			disk = get_imsm_missing(super, idx);
		serialcpy(inf[i].serial, disk->serial);
	}
	append_metadata_update(st, u, len);

	return 0;
}

static int mgmt_disk(struct supertype *st)
{
	struct intel_super *super = st->sb;
	size_t len;
	struct imsm_update_add_remove_disk *u;

	if (!super->disk_mgmt_list)
		return 0;

	len = sizeof(*u);
	u = xmalloc(len);
	u->type = update_add_remove_disk;
	append_metadata_update(st, u, len);

	return 0;
}

__u32 crc32c_le(__u32 crc, unsigned char const *p, size_t len);

static int write_ppl_header(unsigned long long ppl_sector, int fd, void *buf)
{
	struct ppl_header *ppl_hdr = buf;
	int ret;

	ppl_hdr->checksum = __cpu_to_le32(~crc32c_le(~0, buf, PPL_HEADER_SIZE));

	if (lseek64(fd, ppl_sector * 512, SEEK_SET) < 0) {
		ret = -errno;
		perror("Failed to seek to PPL header location");
		return ret;
	}

	if (write(fd, buf, PPL_HEADER_SIZE) != PPL_HEADER_SIZE) {
		ret = -errno;
		perror("Write PPL header failed");
		return ret;
	}

	fsync(fd);

	return 0;
}

static int write_init_ppl_imsm(struct supertype *st, struct mdinfo *info, int fd)
{
	struct intel_super *super = st->sb;
	void *buf;
	struct ppl_header *ppl_hdr;
	int ret;

	/* first clear entire ppl space */
	ret = zero_disk_range(fd, info->ppl_sector, info->ppl_size);
	if (ret)
		return ret;

	ret = posix_memalign(&buf, MAX_SECTOR_SIZE, PPL_HEADER_SIZE);
	if (ret) {
		pr_err("Failed to allocate PPL header buffer\n");
		return -ret;
	}

	memset(buf, 0, PPL_HEADER_SIZE);
	ppl_hdr = buf;
	memset(ppl_hdr->reserved, 0xff, PPL_HDR_RESERVED);
	ppl_hdr->signature = __cpu_to_le32(super->anchor->orig_family_num);

	if (info->mismatch_cnt) {
		/*
		 * We are overwriting an invalid ppl. Make one entry with wrong
		 * checksum to prevent the kernel from skipping resync.
		 */
		ppl_hdr->entries_count = __cpu_to_le32(1);
		ppl_hdr->entries[0].checksum = ~0;
	}

	ret = write_ppl_header(info->ppl_sector, fd, buf);

	free(buf);
	return ret;
}

static int is_rebuilding(struct imsm_dev *dev);

static int validate_ppl_imsm(struct supertype *st, struct mdinfo *info,
			     struct mdinfo *disk)
{
	struct intel_super *super = st->sb;
	struct dl *d;
	void *buf_orig, *buf, *buf_prev = NULL;
	int ret = 0;
	struct ppl_header *ppl_hdr = NULL;
	__u32 crc;
	struct imsm_dev *dev;
	__u32 idx;
	unsigned int i;
	unsigned long long ppl_offset = 0;
	unsigned long long prev_gen_num = 0;

	if (disk->disk.raid_disk < 0)
		return 0;

	dev = get_imsm_dev(super, info->container_member);
	idx = get_imsm_disk_idx(dev, disk->disk.raid_disk, MAP_0);
	d = get_imsm_dl_disk(super, idx);

	if (!d || d->index < 0 || is_failed(&d->disk))
		return 0;

	if (posix_memalign(&buf_orig, MAX_SECTOR_SIZE, PPL_HEADER_SIZE * 2)) {
		pr_err("Failed to allocate PPL header buffer\n");
		return -1;
	}
	buf = buf_orig;

	ret = 1;
	while (ppl_offset < MULTIPLE_PPL_AREA_SIZE_IMSM) {
		void *tmp;

		dprintf("Checking potential PPL at offset: %llu\n", ppl_offset);

		if (lseek64(d->fd, info->ppl_sector * 512 + ppl_offset,
			    SEEK_SET) < 0) {
			perror("Failed to seek to PPL header location");
			ret = -1;
			break;
		}

		if (read(d->fd, buf, PPL_HEADER_SIZE) != PPL_HEADER_SIZE) {
			perror("Read PPL header failed");
			ret = -1;
			break;
		}

		ppl_hdr = buf;

		crc = __le32_to_cpu(ppl_hdr->checksum);
		ppl_hdr->checksum = 0;

		if (crc != ~crc32c_le(~0, buf, PPL_HEADER_SIZE)) {
			dprintf("Wrong PPL header checksum on %s\n",
				d->devname);
			break;
		}

		if (prev_gen_num > __le64_to_cpu(ppl_hdr->generation)) {
			/* previous was newest, it was already checked */
			break;
		}

		if ((__le32_to_cpu(ppl_hdr->signature) !=
			      super->anchor->orig_family_num)) {
			dprintf("Wrong PPL header signature on %s\n",
				d->devname);
			ret = 1;
			break;
		}

		ret = 0;
		prev_gen_num = __le64_to_cpu(ppl_hdr->generation);

		ppl_offset += PPL_HEADER_SIZE;
		for (i = 0; i < __le32_to_cpu(ppl_hdr->entries_count); i++)
			ppl_offset +=
				   __le32_to_cpu(ppl_hdr->entries[i].pp_size);

		if (!buf_prev)
			buf_prev = buf + PPL_HEADER_SIZE;
		tmp = buf_prev;
		buf_prev = buf;
		buf = tmp;
	}

	if (buf_prev) {
		buf = buf_prev;
		ppl_hdr = buf_prev;
	}

	/*
	 * Update metadata to use mutliple PPLs area (1MB).
	 * This is done once for all RAID members
	 */
	if (info->consistency_policy == CONSISTENCY_POLICY_PPL &&
	    info->ppl_size != (MULTIPLE_PPL_AREA_SIZE_IMSM >> 9)) {
		char subarray[20];
		struct mdinfo *member_dev;

		sprintf(subarray, "%d", info->container_member);

		if (mdmon_running(st->container_devnm))
			st->update_tail = &st->updates;

		if (st->ss->update_subarray(st, subarray, UOPT_PPL, NULL)) {
			pr_err("Failed to update subarray %s\n",
			      subarray);
		} else {
			if (st->update_tail)
				flush_metadata_updates(st);
			else
				st->ss->sync_metadata(st);
			info->ppl_size = (MULTIPLE_PPL_AREA_SIZE_IMSM >> 9);
			for (member_dev = info->devs; member_dev;
			     member_dev = member_dev->next)
				member_dev->ppl_size =
				    (MULTIPLE_PPL_AREA_SIZE_IMSM >> 9);
		}
	}

	if (ret == 1) {
		struct imsm_map *map = get_imsm_map(dev, MAP_X);

		if (map->map_state == IMSM_T_STATE_UNINITIALIZED ||
		   (map->map_state == IMSM_T_STATE_NORMAL &&
		   !(dev->vol.dirty & RAIDVOL_DIRTY)) ||
		   (is_rebuilding(dev) &&
		    vol_curr_migr_unit(dev) == 0 &&
		    get_imsm_disk_idx(dev, disk->disk.raid_disk, MAP_1) != idx))
			ret = st->ss->write_init_ppl(st, info, d->fd);
		else
			info->mismatch_cnt++;
	} else if (ret == 0 &&
		   ppl_hdr->entries_count == 0 &&
		   is_rebuilding(dev) &&
		   info->resync_start == 0) {
		/*
		 * The header has no entries - add a single empty entry and
		 * rewrite the header to prevent the kernel from going into
		 * resync after an interrupted rebuild.
		 */
		ppl_hdr->entries_count = __cpu_to_le32(1);
		ret = write_ppl_header(info->ppl_sector, d->fd, buf);
	}

	free(buf_orig);

	return ret;
}

static int write_init_ppl_imsm_all(struct supertype *st, struct mdinfo *info)
{
	struct intel_super *super = st->sb;
	struct dl *d;
	int ret = 0;

	if (info->consistency_policy != CONSISTENCY_POLICY_PPL ||
	    info->array.level != 5)
		return 0;

	for (d = super->disks; d ; d = d->next) {
		if (d->index < 0 || is_failed(&d->disk))
			continue;

		ret = st->ss->write_init_ppl(st, info, d->fd);
		if (ret)
			break;
	}

	return ret;
}

/*******************************************************************************
 * Function:	write_init_bitmap_imsm_vol
 * Description:	Write a bitmap header and prepares the area for the bitmap.
 * Parameters:
 *	st	: supertype information
 *	vol_idx	: the volume index to use
 *
 * Returns:
 *	 0 : success
 *	-1 : fail
 ******************************************************************************/
static int write_init_bitmap_imsm_vol(struct supertype *st, int vol_idx)
{
	struct intel_super *super = st->sb;
	int prev_current_vol = super->current_vol;
	struct dl *d;
	int ret = 0;

	super->current_vol = vol_idx;
	for (d = super->disks; d; d = d->next) {
		if (d->index < 0 || is_failed(&d->disk))
			continue;
		ret = st->ss->write_bitmap(st, d->fd, NoUpdate);
		if (ret)
			break;
	}
	super->current_vol = prev_current_vol;
	return ret;
}

/*******************************************************************************
 * Function:	write_init_bitmap_imsm_all
 * Description:	Write a bitmap header and prepares the area for the bitmap.
 *		Operation is executed for volumes with CONSISTENCY_POLICY_BITMAP.
 * Parameters:
 *	st	: supertype information
 *	info	: info about the volume where the bitmap should be written
 *	vol_idx	: the volume index to use
 *
 * Returns:
 *	 0 : success
 *	-1 : fail
 ******************************************************************************/
static int write_init_bitmap_imsm_all(struct supertype *st, struct mdinfo *info,
				      int vol_idx)
{
	int ret = 0;

	if (info && (info->consistency_policy == CONSISTENCY_POLICY_BITMAP))
		ret = write_init_bitmap_imsm_vol(st, vol_idx);

	return ret;
}

static int write_init_super_imsm(struct supertype *st)
{
	struct intel_super *super = st->sb;
	int current_vol = super->current_vol;
	int rv = 0;
	struct mdinfo info;

	getinfo_super_imsm(st, &info, NULL);

	/* we are done with current_vol reset it to point st at the container */
	super->current_vol = -1;

	if (st->update_tail) {
		/* queue the recently created array / added disk
		 * as a metadata update */

		/* determine if we are creating a volume or adding a disk */
		if (current_vol < 0) {
			/* in the mgmt (add/remove) disk case we are running
			 * in mdmon context, so don't close fd's
			 */
			rv = mgmt_disk(st);
		} else {
			/* adding the second volume to the array */
			rv = write_init_ppl_imsm_all(st, &info);
			if (!rv)
				rv = write_init_bitmap_imsm_all(st, &info, current_vol);
			if (!rv)
				rv = create_array(st, current_vol);
		}
	} else {
		struct dl *d;
		for (d = super->disks; d; d = d->next)
			Kill(d->devname, NULL, 0, -1, 1);
		if (current_vol >= 0) {
			rv = write_init_ppl_imsm_all(st, &info);
			if (!rv)
				rv = write_init_bitmap_imsm_all(st, &info, current_vol);
		}

		if (!rv)
			rv = write_super_imsm(st, 1);
	}

	return rv;
}

static int store_super_imsm(struct supertype *st, int fd)
{
	struct intel_super *super = st->sb;
	struct imsm_super *mpb = super ? super->anchor : NULL;

	if (!mpb)
		return 1;

	if (super->sector_size == 4096)
		convert_to_4k(super);
	return store_imsm_mpb(fd, mpb);
}

static int validate_geometry_imsm_container(struct supertype *st, int level,
					    int raiddisks,
					    unsigned long long data_offset,
					    char *dev,
					    unsigned long long *freesize,
					    int verbose)
{
	int fd;
	unsigned long long ldsize;
	struct intel_super *super = NULL;
	int rv = 0;

	if (!is_container(level))
		return 0;
	if (!dev)
		return 1;

	fd = dev_open(dev, O_RDONLY|O_EXCL);
	if (!is_fd_valid(fd)) {
		pr_vrb("imsm: Cannot open %s: %s\n", dev, strerror(errno));
		return 0;
	}
	if (!get_dev_size(fd, dev, &ldsize))
		goto exit;

	/* capabilities retrieve could be possible
	 * note that there is no fd for the disks in array.
	 */
	super = alloc_super();
	if (!super)
		goto exit;

	if (!get_dev_sector_size(fd, NULL, &super->sector_size))
		goto exit;

	rv = find_intel_hba_capability(fd, super, verbose > 0 ? dev : NULL);
	if (rv != 0) {
#if DEBUG
		char str[256];
		fd2devname(fd, str);
		dprintf("fd: %d %s orom: %p rv: %d raiddisk: %d\n",
			fd, str, super->orom, rv, raiddisks);
#endif
		/* no orom/efi or non-intel hba of the disk */
		rv = 0;
		goto exit;
	}
	if (super->orom) {
		if (raiddisks > super->orom->tds) {
			if (verbose)
				pr_err("%d exceeds maximum number of platform supported disks: %d\n",
					raiddisks, super->orom->tds);
			goto exit;
		}
		if ((super->orom->attr & IMSM_OROM_ATTR_2TB_DISK) == 0 &&
		    (ldsize >> 9) >> 32 > 0) {
			if (verbose)
				pr_err("%s exceeds maximum platform supported size\n", dev);
			goto exit;
		}

		if (super->hba->type == SYS_DEV_VMD ||
		    super->hba->type == SYS_DEV_NVME) {
			if (!imsm_is_nvme_namespace_supported(fd, 1)) {
				if (verbose)
					pr_err("NVMe namespace %s is not supported by IMSM\n",
						basename(dev));
				goto exit;
			}
		}
	}
	if (freesize)
		*freesize = avail_size_imsm(st, ldsize >> 9, data_offset);
	rv = 1;
exit:
	if (super)
		free_imsm(super);
	close(fd);

	return rv;
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

/** merge_extents() - analyze extents and get free size.
 * @super: Intel metadata, not NULL.
 * @expanding: if set, we are expanding &super->current_vol.
 *
 * Build a composite disk with all known extents and generate a size given the
 * "all disks in an array must share a common start offset" constraint.
 * If a volume is expanded, then return free space after the volume.
 *
 * Return: Free space or 0 on failure.
 */
static unsigned long long merge_extents(struct intel_super *super, const bool expanding)
{
	struct extent *e;
	struct dl *dl;
	int i, j, pos_vol_idx = -1;
	int extent_idx = 0;
	int sum_extents = 0;
	unsigned long long pos = 0;
	unsigned long long start = 0;
	unsigned long long free_size = 0;

	unsigned long pre_reservation = 0;
	unsigned long post_reservation = IMSM_RESERVED_SECTORS;
	unsigned long reservation_size;

	for (dl = super->disks; dl; dl = dl->next)
		if (dl->e)
			sum_extents += dl->extent_cnt;
	e = xcalloc(sum_extents, sizeof(struct extent));

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
		e[j].vol = e[i].vol;
		e[j].size = find_size(e, &i, sum_extents);
		j++;
		if (e[j-1].size == 0)
			break;
	}

	i = 0;
	do {
		unsigned long long esize = e[i].start - pos;

		if (expanding ? pos_vol_idx == super->current_vol : esize >= free_size) {
			free_size = esize;
			start = pos;
			extent_idx = i;
		}

		pos = e[i].start + e[i].size;
		pos_vol_idx = e[i].vol;

		i++;
	} while (e[i-1].size);

	if (free_size == 0) {
		dprintf("imsm: Cannot find free size.\n");
		free(e);
		return 0;
	}

	if (!expanding && extent_idx != 0)
		/*
		 * Not a real first volume in a container is created, pre_reservation is needed.
		 */
		pre_reservation = IMSM_RESERVED_SECTORS;

	if (e[extent_idx].size == 0)
		/*
		 * extent_idx points to the metadata, post_reservation is allready done.
		 */
		post_reservation = 0;
	free(e);

	reservation_size = pre_reservation + post_reservation;

	if (free_size < reservation_size) {
		dprintf("imsm: Reservation size is greater than free space.\n");
		return 0;
	}

	super->create_offset = start + pre_reservation;
	return free_size - reservation_size;
}

/**
 * is_raid_level_supported() - check if this count of drives and level is supported by platform.
 * @orom: hardware properties, could be NULL.
 * @level: requested raid level.
 * @raiddisks: requested disk count.
 *
 * IMSM UEFI/OROM does not provide information about supported count of raid disks
 * for particular level. That is why it is hardcoded.
 * It is recommended to not allow of usage other levels than supported,
 * IMSM code is not tested against different level implementations.
 *
 * Return: true if supported, false otherwise.
 */
static bool is_raid_level_supported(const struct imsm_orom *orom, int level, int raiddisks)
{
	int idx;

	for (idx = 0; imsm_level_ops[idx].name; idx++) {
		if (imsm_level_ops[idx].level == level)
			break;
	}

	if (!imsm_level_ops[idx].name)
		return false;

	if (!imsm_level_ops[idx].is_raiddisks_count_supported(raiddisks))
		return false;

	if (!orom)
		return true;

	if (imsm_level_ops[idx].is_level_supported(orom))
		return true;

	return false;
}

static int
active_arrays_by_format(char *name, char* hba, struct md_list **devlist,
			int dpa, int verbose)
{
	struct mdstat_ent *mdstat = mdstat_read(0, 0);
	struct mdstat_ent *memb;
	int count = 0;
	int num = 0;
	struct md_list *dv;
	int found;

	for (memb = mdstat ; memb ; memb = memb->next) {
		if (is_mdstat_ent_external(memb) && !is_subarray(memb->metadata_version + 9) &&
		    strcmp(&memb->metadata_version[9], name) == 0 && memb->members) {
			struct dev_member *dev = memb->members;
			int fd = -1;

			while (dev && !is_fd_valid(fd)) {
				char path[PATH_MAX];

				num = snprintf(path, PATH_MAX, "%s%s", "/dev/", dev->name);
				if (num > 0)
					fd = open(path, O_RDONLY, 0);
				if (num <= 0 || !is_fd_valid(fd)) {
					pr_vrb("Cannot open %s: %s\n",
					       dev->name, strerror(errno));
				}
				dev = dev->next;
			}
			found = 0;
			if (is_fd_valid(fd) && disk_attached_to_hba(fd, hba)) {
				struct mdstat_ent *vol;
				for (vol = mdstat ; vol ; vol = vol->next) {
					if (vol->active > 0 &&
					    is_container_member(vol, memb->devnm)) {
						found++;
						count++;
					}
				}
				if (*devlist && (found < dpa)) {
					dv = xcalloc(1, sizeof(*dv));
					dv->devname = xmalloc(strlen(memb->devnm) + strlen("/dev/") + 1);
					sprintf(dv->devname, "%s%s", "/dev/", memb->devnm);
					dv->found = found;
					dv->used = 0;
					dv->next = *devlist;
					*devlist = dv;
				}
			}
			close_fd(&fd);
		}
	}
	free_mdstat(mdstat);
	return count;
}

#ifdef DEBUG_LOOP
static struct md_list*
get_loop_devices(void)
{
	int i;
	struct md_list *devlist = NULL;
	struct md_list *dv;

	for(i = 0; i < 12; i++) {
		dv = xcalloc(1, sizeof(*dv));
		dv->devname = xmalloc(40);
		sprintf(dv->devname, "/dev/loop%d", i);
		dv->next = devlist;
		devlist = dv;
	}
	return devlist;
}
#endif

static struct md_list*
get_devices(const char *hba_path)
{
	struct md_list *devlist = NULL;
	struct md_list *dv;
	struct dirent *ent;
	DIR *dir;

#if DEBUG_LOOP
	devlist = get_loop_devices();
	return devlist;
#endif
	/* scroll through /sys/dev/block looking for devices attached to
	 * this hba
	 */
	dir = opendir("/sys/dev/block");
	for (ent = dir ? readdir(dir) : NULL; ent; ent = readdir(dir)) {
		int fd;
		char buf[1024];
		int major, minor;
		char *path = NULL;
		if (sscanf(ent->d_name, "%d:%d", &major, &minor) != 2)
			continue;
		path = devt_to_devpath(makedev(major, minor), 1, NULL);
		if (!path)
			continue;
		if (!is_path_attached_to_hba(path, hba_path)) {
			free(path);
			path = NULL;
			continue;
		}
		free(path);
		path = NULL;
		fd = dev_open(ent->d_name, O_RDONLY);
		if (is_fd_valid(fd)) {
			fd2devname(fd, buf);
			close(fd);
		} else {
			pr_err("cannot open device: %s\n",
				ent->d_name);
			continue;
		}

		dv = xcalloc(1, sizeof(*dv));
		dv->devname = xstrdup(buf);
		dv->next = devlist;
		devlist = dv;
	}
	closedir(dir);
	return devlist;
}

static int
count_volumes_list(struct md_list *devlist, char *homehost,
		   int verbose, int *found)
{
	struct md_list *tmpdev;
	int count = 0;
	struct supertype *st;

	/* first walk the list of devices to find a consistent set
	 * that match the criterea, if that is possible.
	 * We flag the ones we like with 'used'.
	 */
	*found = 0;
	st = match_metadata_desc_imsm("imsm");
	if (st == NULL) {
		pr_vrb("cannot allocate memory for imsm supertype\n");
		return 0;
	}

	for (tmpdev = devlist; tmpdev; tmpdev = tmpdev->next) {
		char *devname = tmpdev->devname;
		dev_t rdev;
		struct supertype *tst;
		int dfd;
		if (tmpdev->used > 1)
			continue;
		tst = dup_super(st);
		if (tst == NULL) {
			pr_vrb("cannot allocate memory for imsm supertype\n");
			goto err_1;
		}
		tmpdev->container = 0;
		dfd = dev_open(devname, O_RDONLY|O_EXCL);
		if (!is_fd_valid(dfd)) {
			dprintf("cannot open device %s: %s\n",
				devname, strerror(errno));
			tmpdev->used = 2;
		} else if (!fstat_is_blkdev(dfd, devname, &rdev)) {
			tmpdev->used = 2;
		} else if (must_be_container(dfd)) {
			struct supertype *cst;
			cst = super_by_fd(dfd, NULL);
			if (cst == NULL) {
				dprintf("cannot recognize container type %s\n",
					devname);
				tmpdev->used = 2;
			} else if (tst->ss != st->ss) {
				dprintf("non-imsm container - ignore it: %s\n",
					devname);
				tmpdev->used = 2;
			} else if (!tst->ss->load_container ||
				   tst->ss->load_container(tst, dfd, NULL))
				tmpdev->used = 2;
			else {
				tmpdev->container = 1;
			}
			if (cst)
				cst->ss->free_super(cst);
		} else {
			tmpdev->st_rdev = rdev;
			if (tst->ss->load_super(tst,dfd, NULL)) {
				dprintf("no RAID superblock on %s\n",
					devname);
				tmpdev->used = 2;
			} else if (tst->ss->compare_super == NULL) {
				dprintf("Cannot assemble %s metadata on %s\n",
					tst->ss->name, devname);
				tmpdev->used = 2;
			}
		}
		close_fd(&dfd);

		if (tmpdev->used == 2 || tmpdev->used == 4) {
			/* Ignore unrecognised devices during auto-assembly */
			goto loop;
		}
		else {
			struct mdinfo info;
			tst->ss->getinfo_super(tst, &info, NULL);

			if (st->minor_version == -1)
				st->minor_version = tst->minor_version;

			if (memcmp(info.uuid, uuid_zero,
				   sizeof(int[4])) == 0) {
				/* this is a floating spare.  It cannot define
				 * an array unless there are no more arrays of
				 * this type to be found.  It can be included
				 * in an array of this type though.
				 */
				tmpdev->used = 3;
				goto loop;
			}

			if (st->ss != tst->ss ||
			    st->minor_version != tst->minor_version ||
			    st->ss->compare_super(st, tst, 1) != 0) {
				/* Some mismatch. If exactly one array matches this host,
				 * we can resolve on that one.
				 * Or, if we are auto assembling, we just ignore the second
				 * for now.
				 */
				dprintf("superblock on %s doesn't match others - assembly aborted\n",
					devname);
				goto loop;
			}
			tmpdev->used = 1;
			*found = 1;
			dprintf("found: devname: %s\n", devname);
		}
	loop:
		if (tst)
			tst->ss->free_super(tst);
	}
	if (*found != 0) {
		int err;
		if ((err = load_super_imsm_all(st, -1, &st->sb, NULL, devlist, 0)) == 0) {
			struct mdinfo *iter, *head = st->ss->container_content(st, NULL);
			for (iter = head; iter; iter = iter->next) {
				dprintf("content->text_version: %s vol\n",
					iter->text_version);
				if (iter->array.state & (1<<MD_SB_BLOCK_VOLUME)) {
					/* do not assemble arrays with unsupported
					   configurations */
					dprintf("Cannot activate member %s.\n",
						iter->text_version);
				} else
					count++;
			}
			sysfs_free(head);

		} else {
			dprintf("No valid super block on device list: err: %d %p\n",
				err, st->sb);
		}
	} else {
		dprintf("no more devices to examine\n");
	}

	for (tmpdev = devlist; tmpdev; tmpdev = tmpdev->next) {
		if (tmpdev->used == 1 && tmpdev->found) {
			if (count) {
				if (count < tmpdev->found)
					count = 0;
				else
					count -= tmpdev->found;
			}
		}
		if (tmpdev->used == 1)
			tmpdev->used = 4;
	}
	err_1:
	if (st)
		st->ss->free_super(st);
	return count;
}

static int __count_volumes(char *hba_path, int dpa, int verbose,
			   int cmp_hba_path)
{
	struct sys_dev *idev, *intel_devices = find_intel_devices();
	int count = 0;
	const struct orom_entry *entry;
	struct devid_list *dv, *devid_list;

	if (!hba_path)
		return 0;

	for (idev = intel_devices; idev; idev = idev->next) {
		if (strstr(idev->path, hba_path))
			break;
	}

	if (!idev || !idev->dev_id)
		return 0;

	entry = get_orom_entry_by_device_id(idev->dev_id);

	if (!entry || !entry->devid_list)
		return 0;

	devid_list = entry->devid_list;
	for (dv = devid_list; dv; dv = dv->next) {
		struct md_list *devlist;
		struct sys_dev *device = NULL;
		char *hpath;
		int found = 0;

		if (cmp_hba_path)
			device = device_by_id_and_path(dv->devid, hba_path);
		else
			device = device_by_id(dv->devid);

		if (device)
			hpath = device->path;
		else
			return 0;

		devlist = get_devices(hpath);
		/* if no intel devices return zero volumes */
		if (devlist == NULL)
			return 0;

		count += active_arrays_by_format("imsm", hpath, &devlist, dpa,
						 verbose);
		dprintf("path: %s active arrays: %d\n", hpath, count);
		if (devlist == NULL)
			return 0;
		do  {
			found = 0;
			count += count_volumes_list(devlist,
							NULL,
							verbose,
							&found);
			dprintf("found %d count: %d\n", found, count);
		} while (found);

		dprintf("path: %s total number of volumes: %d\n", hpath, count);

		while (devlist) {
			struct md_list *dv = devlist;
			devlist = devlist->next;
			free(dv->devname);
			free(dv);
		}
	}
	return count;
}

static int count_volumes(struct intel_hba *hba, int dpa, int verbose)
{
	if (!hba)
		return 0;
	if (hba->type == SYS_DEV_VMD) {
		struct sys_dev *dev;
		int count = 0;

		for (dev = find_intel_devices(); dev; dev = dev->next) {
			if (dev->type == SYS_DEV_VMD)
				count += __count_volumes(dev->path, dpa,
							 verbose, 1);
		}
		return count;
	}
	return __count_volumes(hba->path, dpa, verbose, 0);
}

static int imsm_default_chunk(const struct imsm_orom *orom)
{
	/* up to 512 if the plaform supports it, otherwise the platform max.
	 * 128 if no platform detected
	 */
	int fs = max(7, orom ? fls(orom->sss) : 0);

	return min(512, (1 << fs));
}

static int
validate_geometry_imsm_orom(struct intel_super *super, int level, int layout,
			    int raiddisks, int *chunk, unsigned long long size, int verbose)
{
	/* check/set platform and metadata limits/defaults */
	if (super->orom && raiddisks > super->orom->dpa) {
		pr_vrb("platform supports a maximum of %d disks per array\n",
		       super->orom->dpa);
		return 0;
	}

	/* capabilities of OROM tested - copied from validate_geometry_imsm_volume */
	if (!is_raid_level_supported(super->orom, level, raiddisks)) {
		pr_vrb("platform does not support raid%d with %d disk%s\n",
			level, raiddisks, raiddisks > 1 ? "s" : "");
		return 0;
	}

	if (*chunk == 0 || *chunk == UnSet)
		*chunk = imsm_default_chunk(super->orom);

	if (super->orom && !imsm_orom_has_chunk(super->orom, *chunk)) {
		pr_vrb("platform does not support a chunk size of: %d\n", *chunk);
		return 0;
	}

	if (layout != imsm_level_to_layout(level)) {
		if (level == 5)
			pr_vrb("imsm raid 5 only supports the left-asymmetric layout\n");
		else if (level == 10)
			pr_vrb("imsm raid 10 only supports the n2 layout\n");
		else
			pr_vrb("imsm unknown layout %#x for this raid level %d\n",
				layout, level);
		return 0;
	}

	if (super->orom && (super->orom->attr & IMSM_OROM_ATTR_2TB) == 0 &&
			(calc_array_size(level, raiddisks, layout, *chunk, size) >> 32) > 0) {
		pr_vrb("platform does not support a volume size over 2TB\n");
		return 0;
	}

	return 1;
}

/* validate_geometry_imsm_volume - lifted from validate_geometry_ddf_bvd
 * FIX ME add ahci details
 */
static int validate_geometry_imsm_volume(struct supertype *st, int level,
					 int layout, int raiddisks, int *chunk,
					 unsigned long long size,
					 unsigned long long data_offset,
					 char *dev,
					 unsigned long long *freesize,
					 int verbose)
{
	dev_t rdev;
	struct intel_super *super = st->sb;
	struct imsm_super *mpb;
	struct dl *dl;
	unsigned long long pos = 0;
	unsigned long long maxsize;
	struct extent *e;
	int i;

	/* We must have the container info already read in. */
	if (!super)
		return 0;

	mpb = super->anchor;

	if (!validate_geometry_imsm_orom(super, level, layout, raiddisks, chunk, size, verbose)) {
		pr_err("RAID geometry validation failed. Cannot proceed with the action(s).\n");
		return 0;
	}
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
			e = get_extents(super, dl, 0);
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
				pr_err("imsm: Not enough devices with space for this array (%d < %d)\n",
					dcnt, raiddisks);
			return 0;
		}
		return 1;
	}

	/* This device must be a member of the set */
	if (!stat_is_blkdev(dev, &rdev))
		return 0;
	for (dl = super->disks ; dl ; dl = dl->next) {
		if (dl->major == (int)major(rdev) &&
		    dl->minor == (int)minor(rdev))
			break;
	}
	if (!dl) {
		if (verbose)
			pr_err("%s is not in the same imsm set\n", dev);
		return 0;
	} else if (super->orom && dl->index < 0 && mpb->num_raid_devs) {
		/* If a volume is present then the current creation attempt
		 * cannot incorporate new spares because the orom may not
		 * understand this configuration (all member disks must be
		 * members of each array in the container).
		 */
		pr_err("%s is a spare and a volume is already defined for this container\n", dev);
		pr_err("The option-rom requires all member disks to be a member of all volumes\n");
		return 0;
	} else if (super->orom && mpb->num_raid_devs > 0 &&
		   mpb->num_disks != raiddisks) {
		pr_err("The option-rom requires all member disks to be a member of all volumes\n");
		return 0;
	}

	/* retrieve the largest free space block */
	e = get_extents(super, dl, 0);
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
			pr_err("unable to determine free space for: %s\n",
				dev);
		return 0;
	}
	if (maxsize < size) {
		if (verbose)
			pr_err("%s not enough space (%llu < %llu)\n",
				dev, maxsize, size);
		return 0;
	}

	maxsize = merge_extents(super, false);

	if (mpb->num_raid_devs > 0 && size && size != maxsize)
		pr_err("attempting to create a second volume with size less then remaining space.\n");

	if (maxsize < size || maxsize == 0) {
		if (verbose) {
			if (maxsize == 0)
				pr_err("no free space left on device. Aborting...\n");
			else
				pr_err("not enough space to create volume of given size (%llu < %llu). Aborting...\n",
						maxsize, size);
		}
		return 0;
	}

	*freesize = maxsize;

	if (super->orom) {
		int count = count_volumes(super->hba,
				      super->orom->dpa, verbose);
		if (super->orom->vphba <= count) {
			pr_vrb("platform does not support more than %d raid volumes.\n",
			       super->orom->vphba);
			return 0;
		}
	}
	return 1;
}

/**
 * imsm_get_free_size() - get the biggest, common free space from members.
 * @super: &intel_super pointer, not NULL.
 * @raiddisks: number of raid disks.
 * @size: requested size, could be 0 (means max size).
 * @chunk: requested chunk size in KiB.
 * @freesize: pointer for returned size value.
 *
 * Return: &IMSM_STATUS_OK or &IMSM_STATUS_ERROR.
 *
 * @freesize is set to meaningful value, this can be @size, or calculated
 * max free size.
 * super->create_offset value is modified and set appropriately in
 * merge_extends() for further creation.
 */
static imsm_status_t imsm_get_free_size(struct intel_super *super,
					const int raiddisks,
					unsigned long long size,
					const int chunk,
					unsigned long long *freesize,
					bool expanding)
{
	struct imsm_super *mpb = super->anchor;
	struct dl *dl;
	int i;
	struct extent *e;
	int cnt = 0;
	int used = 0;
	unsigned long long maxsize;
	unsigned long long minsize = size;

	if (minsize == 0)
		minsize = chunk * 2;

	/* find the largest common start free region of the possible disks */
	for (dl = super->disks; dl; dl = dl->next) {
		dl->raiddisk = -1;

		if (dl->index >= 0)
			used++;

		/* don't activate new spares if we are orom constrained
		 * and there is already a volume active in the container
		 */
		if (super->orom && dl->index < 0 && mpb->num_raid_devs)
			continue;

		e = get_extents(super, dl, 0);
		if (!e)
			continue;
		for (i = 1; e[i-1].size; i++)
			;
		dl->e = e;
		dl->extent_cnt = i;
		cnt++;
	}

	maxsize = merge_extents(super, expanding);
	if (maxsize < minsize)  {
		pr_err("imsm: Free space is %llu but must be equal or larger than %llu.\n",
		       maxsize, minsize);
		return IMSM_STATUS_ERROR;
	}

	if (cnt < raiddisks || (super->orom && used && used != raiddisks)) {
		pr_err("imsm: Not enough devices with space to create array.\n");
		return IMSM_STATUS_ERROR;
	}

	if (size == 0) {
		size = maxsize;
		if (chunk) {
			size /= 2 * chunk;
			size *= 2 * chunk;
		}
		maxsize = size;
	}
	if (mpb->num_raid_devs > 0 && size && size != maxsize)
		pr_err("attempting to create a second volume with size less then remaining space.\n");
	*freesize = size;

	dprintf("imsm: imsm_get_free_size() returns : %llu\n", size);

	return IMSM_STATUS_OK;
}

/**
 * autolayout_imsm() - automatically layout a new volume.
 * @super: &intel_super pointer, not NULL.
 * @raiddisks: number of raid disks.
 * @size: requested size, could be 0 (means max size).
 * @chunk: requested chunk.
 * @freesize: pointer for returned size value.
 *
 * We are being asked to automatically layout a new volume based on the current
 * contents of the container. If the parameters can be satisfied autolayout_imsm
 * will record the disks, start offset, and will return size of the volume to
 * be created. See imsm_get_free_size() for details.
 * add_to_super() and getinfo_super() detect when autolayout is in progress.
 * If first volume exists, slots are set consistently to it.
 *
 * Return: &IMSM_STATUS_OK on success, &IMSM_STATUS_ERROR otherwise.
 *
 * Disks are marked for creation via dl->raiddisk.
 */
static imsm_status_t autolayout_imsm(struct intel_super *super,
				     const int raiddisks,
				     unsigned long long size, const int chunk,
				     unsigned long long *freesize)
{
	int curr_slot = 0;
	struct dl *disk;
	int vol_cnt = super->anchor->num_raid_devs;
	imsm_status_t rv;

	rv = imsm_get_free_size(super, raiddisks, size, chunk, freesize, false);
	if (rv != IMSM_STATUS_OK)
		return IMSM_STATUS_ERROR;

	for (disk = super->disks; disk; disk = disk->next) {
		if (!disk->e)
			continue;

		if (curr_slot == raiddisks)
			break;

		if (vol_cnt == 0) {
			disk->raiddisk = curr_slot;
		} else {
			int _slot = get_disk_slot_in_dev(super, 0, disk->index);

			if (_slot == -1) {
				pr_err("Disk %s is not used in first volume, aborting\n",
				       disk->devname);
				return IMSM_STATUS_ERROR;
			}
			disk->raiddisk = _slot;
		}
		curr_slot++;
	}

	return IMSM_STATUS_OK;
}

static int validate_geometry_imsm(struct supertype *st, int level, int layout,
				  int raiddisks, int *chunk, unsigned long long size,
				  unsigned long long data_offset,
				  char *dev, unsigned long long *freesize,
				  int consistency_policy, int verbose)
{
	struct intel_super *super = st->sb;
	struct mdinfo *sra;
	int is_member = 0;
	imsm_status_t rv;
	int fd, cfd;

	/* load capability
	 * if given unused devices create a container
	 * if given given devices in a container create a member volume
	 */
	if (is_container(level))
		/* Must be a fresh device to add to a container */
		return validate_geometry_imsm_container(st, level, raiddisks,
							data_offset, dev,
							freesize, verbose);

	/*
	 * Size is given in sectors.
	 */
	if (size && (size < 2048)) {
		pr_err("Given size must be greater than 1M.\n");
		/* Depends on algorithm in Create.c :
		 * if container was given (dev == NULL) return -1,
		 * if block device was given ( dev != NULL) return 0.
		 */
		return dev ? -1 : 0;
	}

	if (!dev) {
		/*
		 * Autolayout mode, st->sb must be set.
		 */

		if (!super) {
			pr_vrb("superblock must be set for autolayout, aborting\n");
			return 0;
		}

		if (!validate_geometry_imsm_orom(st->sb, level, layout,
						 raiddisks, chunk, size,
						 verbose))
			return 0;

		if (super->orom) {
			int count = count_volumes(super->hba, super->orom->dpa, verbose);

			if (super->orom->vphba <= count) {
				pr_vrb("platform does not support more than %d raid volumes.\n",
				       super->orom->vphba);
				return 0;
			}
		}

		if (freesize) {
			rv = autolayout_imsm(super, raiddisks, size, *chunk, freesize);
				if (rv != IMSM_STATUS_OK)
					return 0;
		}

		return 1;
	}
	if (st->sb) {
		/* creating in a given container */
		return validate_geometry_imsm_volume(st, level, layout,
						     raiddisks, chunk, size,
						     data_offset,
						     dev, freesize, verbose);
	}

	/* This device needs to be a device in an 'imsm' container */
	fd = open(dev, O_RDONLY|O_EXCL, 0);

	if (is_fd_valid(fd)) {
		pr_vrb("Cannot create this array on device %s\n", dev);
		close(fd);
		return 0;
	}
	if (errno == EBUSY)
		fd = open(dev, O_RDONLY, 0);

	if (!is_fd_valid(fd)) {
		pr_vrb("Cannot open %s: %s\n", dev, strerror(errno));
		return 0;
	}

	/* Well, it is in use by someone, maybe an 'imsm' container. */
	cfd = open_container(fd);
	close_fd(&fd);

	if (!is_fd_valid(cfd)) {
		pr_vrb("Cannot use %s: It is busy\n", dev);
		return 0;
	}
	sra = sysfs_read(cfd, NULL, GET_VERSION);
	if (sra && sra->array.major_version == -1 &&
	    strcmp(sra->text_version, "imsm") == 0)
		is_member = 1;
	sysfs_free(sra);
	if (is_member) {
		/* This is a member of a imsm container.  Load the container
		 * and try to create a volume
		 */
		struct intel_super *super;

		if (load_super_imsm_all(st, cfd, (void **) &super, NULL, NULL, 1) == 0) {
			st->sb = super;
			strcpy(st->container_devnm, fd2devnm(cfd));
			close(cfd);
			return validate_geometry_imsm_volume(st, level, layout,
							     raiddisks, chunk,
							     size, data_offset, dev,
							     freesize, 1)
				? 1 : -1;
		}
	}

	if (verbose)
		pr_err("failed container membership check\n");

	close(cfd);
	return 0;
}

static void default_geometry_imsm(struct supertype *st, int *level, int *layout, int *chunk)
{
	struct intel_super *super = st->sb;

	if (level && *level == UnSet)
		*level = LEVEL_CONTAINER;

	if (level && layout && *layout == UnSet)
		*layout = imsm_level_to_layout(*level);

	if (chunk && (*chunk == UnSet || *chunk == 0))
		*chunk = imsm_default_chunk(super->orom);
}

static void handle_missing(struct intel_super *super, struct imsm_dev *dev);

static int kill_subarray_imsm(struct supertype *st, char *subarray_id)
{
	/* remove the subarray currently referenced by subarray_id */
	__u8 i;
	struct intel_dev **dp;
	struct intel_super *super = st->sb;
	__u8 current_vol = strtoul(subarray_id, NULL, 10);
	struct imsm_super *mpb = super->anchor;

	if (mpb->num_raid_devs == 0)
		return 2;

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
		snprintf(subarray, sizeof(subarray), "%u", i);
		if (is_subarray_active(subarray, st->devnm)) {
			pr_err("deleting subarray-%d would change the UUID of active subarray-%d, aborting\n",
			       current_vol, i);

			return 2;
		}
	}

	if (st->update_tail) {
		struct imsm_update_kill_array *u = xmalloc(sizeof(*u));

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
			if (d->index > -2)
				mark_spare(d);
	}

	super->updates_pending++;

	return 0;
}

/**
 * get_rwh_policy_from_update() - Get the rwh policy for update option.
 * @update: Update option.
 */
static int get_rwh_policy_from_update(enum update_opt update)
{
	switch (update) {
	case UOPT_PPL:
		return RWH_MULTIPLE_DISTRIBUTED;
	case UOPT_NO_PPL:
		return RWH_MULTIPLE_OFF;
	case UOPT_BITMAP:
		return RWH_BITMAP;
	case UOPT_NO_BITMAP:
		return RWH_OFF;
	default:
		break;
	}
	return UOPT_UNDEFINED;
}

static int update_subarray_imsm(struct supertype *st, char *subarray,
				enum update_opt update, struct mddev_ident *ident)
{
	/* update the subarray currently referenced by ->current_vol */
	struct intel_super *super = st->sb;
	struct imsm_super *mpb = super->anchor;

	if (update == UOPT_NAME) {
		char *name = ident->name;
		char *ep;
		int vol;

		if (imsm_is_name_allowed(super, name, 1) == false)
			return 2;

		vol = strtoul(subarray, &ep, 10);
		if (*ep != '\0' || vol >= super->anchor->num_raid_devs)
			return 2;

		if (st->update_tail) {
			struct imsm_update_rename_array *u = xmalloc(sizeof(*u));

			u->type = update_rename_array;
			u->dev_idx = vol;
			strncpy((char *) u->name, name, MAX_RAID_SERIAL_LEN);
			u->name[MAX_RAID_SERIAL_LEN-1] = '\0';
			append_metadata_update(st, u, sizeof(*u));
		} else {
			struct imsm_dev *dev;
			int i, namelen;

			dev = get_imsm_dev(super, vol);
			memset(dev->volume, '\0', MAX_RAID_SERIAL_LEN);
			namelen = min((int)strlen(name), MAX_RAID_SERIAL_LEN);
			memcpy(dev->volume, name, namelen);
			for (i = 0; i < mpb->num_raid_devs; i++) {
				dev = get_imsm_dev(super, i);
				handle_missing(super, dev);
			}
			super->updates_pending++;
		}
	} else if (get_rwh_policy_from_update(update) != UOPT_UNDEFINED) {
		int new_policy;
		char *ep;
		int vol = strtoul(subarray, &ep, 10);

		if (*ep != '\0' || vol >= super->anchor->num_raid_devs)
			return 2;

		new_policy = get_rwh_policy_from_update(update);

		if (st->update_tail) {
			struct imsm_update_rwh_policy *u = xmalloc(sizeof(*u));

			u->type = update_rwh_policy;
			u->dev_idx = vol;
			u->new_policy = new_policy;
			append_metadata_update(st, u, sizeof(*u));
		} else {
			struct imsm_dev *dev;

			dev = get_imsm_dev(super, vol);
			dev->rwh_policy = new_policy;
			super->updates_pending++;
		}
		if (new_policy == RWH_BITMAP)
			return write_init_bitmap_imsm_vol(st, vol);
	} else
		return 2;

	return 0;
}

static bool is_gen_migration(struct imsm_dev *dev)
{
	if (dev && dev->vol.migr_state &&
	    migr_type(dev) == MIGR_GEN_MIGR)
		return true;

	return false;
}

static int is_rebuilding(struct imsm_dev *dev)
{
	struct imsm_map *migr_map;

	if (!dev->vol.migr_state)
		return 0;

	if (migr_type(dev) != MIGR_REBUILD)
		return 0;

	migr_map = get_imsm_map(dev, MAP_1);

	if (migr_map->map_state == IMSM_T_STATE_DEGRADED)
		return 1;
	else
		return 0;
}

static int is_initializing(struct imsm_dev *dev)
{
	struct imsm_map *migr_map;

	if (!dev->vol.migr_state)
		return 0;

	if (migr_type(dev) != MIGR_INIT)
		return 0;

	migr_map = get_imsm_map(dev, MAP_1);

	if (migr_map->map_state == IMSM_T_STATE_UNINITIALIZED)
		return 1;

	return 0;
}

static void update_recovery_start(struct intel_super *super,
					struct imsm_dev *dev,
					struct mdinfo *array)
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
		dprintf("failed to locate out-of-sync disk\n");
		return;
	}

	units = vol_curr_migr_unit(dev);
	rebuild->recovery_start = units * blocks_per_migr_unit(super, dev);
}

static int recover_backup_imsm(struct supertype *st, struct mdinfo *info);

static struct mdinfo *container_content_imsm(struct supertype *st, char *subarray)
{
	/* Given a container loaded by load_super_imsm_all,
	 * extract information about all the arrays into
	 * an mdinfo tree.
	 * If 'subarray' is given, just extract info about that array.
	 *
	 * For each imsm_dev create an mdinfo, fill it in,
	 *  then look for matching devices in super->disks
	 *  and create appropriate device mdinfo.
	 */
	struct intel_super *super = st->sb;
	struct imsm_super *mpb = super->anchor;
	struct mdinfo *rest = NULL;
	unsigned int i;
	int sb_errors = 0;
	struct dl *d;
	int spare_disks = 0;
	int current_vol = super->current_vol;

	/* do not assemble arrays when not all attributes are supported */
	if (imsm_check_attributes(mpb->attributes) == false) {
		sb_errors = 1;
		pr_err("Unsupported attributes in IMSM metadata. Arrays activation is blocked.\n");
	}

	/* count spare devices, not used in maps
	 */
	for (d = super->disks; d; d = d->next)
		if (d->index == -1)
			spare_disks++;

	for (i = 0; i < mpb->num_raid_devs; i++) {
		struct imsm_dev *dev;
		struct imsm_map *map;
		struct imsm_map *map2;
		struct mdinfo *this;
		int slot;
		int chunk;
		char *ep;
		int level;

		if (subarray &&
		    (i != strtoul(subarray, &ep, 10) || *ep != '\0'))
			continue;

		dev = get_imsm_dev(super, i);
		map = get_imsm_map(dev, MAP_0);
		map2 = get_imsm_map(dev, MAP_1);
		level = get_imsm_raid_level(map);

		/* do not publish arrays that are in the middle of an
		 * unsupported migration
		 */
		if (dev->vol.migr_state &&
		    (migr_type(dev) == MIGR_STATE_CHANGE)) {
			pr_err("cannot assemble volume '%.16s': unsupported migration in progress\n",
				dev->volume);
			continue;
		}
		/* do not publish arrays that are not support by controller's
		 * OROM/EFI
		 */

		this = xmalloc(sizeof(*this));

		super->current_vol = i;
		getinfo_super_imsm_volume(st, this, NULL);
		this->next = rest;
		chunk = __le16_to_cpu(map->blocks_per_strip) >> 1;
		/* mdadm does not support all metadata features- set the bit in all arrays state */
		if (!validate_geometry_imsm_orom(super,
						 level, /* RAID level */
						 imsm_level_to_layout(level),
						 map->num_members, /* raid disks */
						 &chunk, imsm_dev_size(dev),
						 1 /* verbose */)) {
			pr_err("IMSM RAID geometry validation failed.  Array %s activation is blocked.\n",
				dev->volume);
			this->array.state |=
			  (1<<MD_SB_BLOCK_CONTAINER_RESHAPE) |
			  (1<<MD_SB_BLOCK_VOLUME);
		}

		/* if array has bad blocks, set suitable bit in all arrays state */
		if (sb_errors)
			this->array.state |=
			  (1<<MD_SB_BLOCK_CONTAINER_RESHAPE) |
			  (1<<MD_SB_BLOCK_VOLUME);

		for (slot = 0 ; slot <  map->num_members; slot++) {
			unsigned long long recovery_start;
			struct mdinfo *info_d;
			struct dl *d;
			int idx;
			int skip;
			__u32 ord;
			int missing = 0;

			skip = 0;
			idx = get_imsm_disk_idx(dev, slot, MAP_0);
			ord = get_imsm_ord_tbl_ent(dev, slot, MAP_X);
			for (d = super->disks; d ; d = d->next)
				if (d->index == idx)
					break;

			recovery_start = MaxSector;
			if (d == NULL)
				skip = 1;
			if (d && is_failed(&d->disk))
				skip = 1;
			if (!skip && (ord & IMSM_ORD_REBUILD))
				recovery_start = 0;
			if (!(ord & IMSM_ORD_REBUILD))
				this->array.working_disks++;
			/*
			 * if we skip some disks the array will be assmebled degraded;
			 * reset resync start to avoid a dirty-degraded
			 * situation when performing the intial sync
			 */
			if (skip)
				missing++;

			if (!(dev->vol.dirty & RAIDVOL_DIRTY)) {
				if ((!able_to_resync(level, missing) ||
				     recovery_start == 0))
					this->resync_start = MaxSector;
			}

			if (skip)
				continue;

			info_d = xcalloc(1, sizeof(*info_d));
			info_d->next = this->devs;
			this->devs = info_d;

			info_d->disk.number = d->index;
			info_d->disk.major = d->major;
			info_d->disk.minor = d->minor;
			info_d->disk.raid_disk = slot;
			info_d->recovery_start = recovery_start;
			if (map2) {
				if (slot < map2->num_members)
					info_d->disk.state = (1 << MD_DISK_ACTIVE);
				else
					this->array.spare_disks++;
			} else {
				if (slot < map->num_members)
					info_d->disk.state = (1 << MD_DISK_ACTIVE);
				else
					this->array.spare_disks++;
			}

			info_d->events = __le32_to_cpu(mpb->generation_num);
			info_d->data_offset = pba_of_lba0(map);
			info_d->component_size = calc_component_size(map, dev);

			if (map->raid_level == IMSM_T_RAID5) {
				info_d->ppl_sector = this->ppl_sector;
				info_d->ppl_size = this->ppl_size;
				if (this->consistency_policy == CONSISTENCY_POLICY_PPL &&
				    recovery_start == 0)
					this->resync_start = 0;
			}

			info_d->bb.supported = 1;
			get_volume_badblocks(super->bbm_log, ord_to_idx(ord),
					     info_d->data_offset,
					     info_d->component_size,
					     &info_d->bb);
		}
		/* now that the disk list is up-to-date fixup recovery_start */
		update_recovery_start(super, dev, this);
		this->array.spare_disks += spare_disks;

		/* check for reshape */
		if (this->reshape_active == 1)
			recover_backup_imsm(st, this);
		rest = this;
	}

	super->current_vol = current_vol;
	return rest;
}

static __u8 imsm_check_degraded(struct intel_super *super, struct imsm_dev *dev,
				int failed, int look_in_map)
{
	struct imsm_map *map;

	map = get_imsm_map(dev, look_in_map);

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
			__u32 ord = get_imsm_ord_tbl_ent(dev, i, MAP_X);
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

static int imsm_count_failed(struct intel_super *super, struct imsm_dev *dev,
			     int look_in_map)
{
	int i;
	int failed = 0;
	struct imsm_disk *disk;
	struct imsm_map *map = get_imsm_map(dev, MAP_0);
	struct imsm_map *prev = get_imsm_map(dev, MAP_1);
	struct imsm_map *map_for_loop;
	__u32 ord;
	int idx;
	int idx_1;

	/* at the beginning of migration we set IMSM_ORD_REBUILD on
	 * disks that are being rebuilt.  New failures are recorded to
	 * map[0].  So we look through all the disks we started with and
	 * see if any failures are still present, or if any new ones
	 * have arrived
	 */
	map_for_loop = map;
	if (prev && (map->num_members < prev->num_members))
		map_for_loop = prev;

	for (i = 0; i < map_for_loop->num_members; i++) {
		idx_1 = -255;
		/* when MAP_X is passed both maps failures are counted
		 */
		if (prev &&
		    (look_in_map == MAP_1 || look_in_map == MAP_X) &&
		    i < prev->num_members) {
			ord = __le32_to_cpu(prev->disk_ord_tbl[i]);
			idx_1 = ord_to_idx(ord);

			disk = get_imsm_disk(super, idx_1);
			if (!disk || is_failed(disk) || ord & IMSM_ORD_REBUILD)
				failed++;
		}
		if ((look_in_map == MAP_0 || look_in_map == MAP_X) &&
		    i < map->num_members) {
			ord = __le32_to_cpu(map->disk_ord_tbl[i]);
			idx = ord_to_idx(ord);

			if (idx != idx_1) {
				disk = get_imsm_disk(super, idx);
				if (!disk || is_failed(disk) ||
				    ord & IMSM_ORD_REBUILD)
					failed++;
			}
		}
	}

	return failed;
}

static int imsm_prepare_update(struct supertype *st,
			       struct metadata_update *update);
static int imsm_open_new(struct supertype *c, struct active_array *a,
			 int inst)
{
	struct intel_super *super = c->sb;
	struct imsm_super *mpb = super->anchor;
	struct imsm_update_prealloc_bb_mem *u;
	struct metadata_update mu;

	if (inst >= mpb->num_raid_devs) {
		pr_err("subarry index %d, out of range\n", inst);
		return -ENODEV;
	}

	dprintf("imsm: open_new %d\n", inst);
	a->info.container_member = inst;

	u = xmalloc(sizeof(*u));
	u->type = update_prealloc_badblocks_mem;
	mu.len = sizeof(*u);
	mu.buf = (char *)u;
	imsm_prepare_update(c, &mu);
	if (c->update_tail)
		append_metadata_update(c, u, sizeof(*u));

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

	if (migr_type(dev) == MIGR_GEN_MIGR)
		return 0;

	migr_map = get_imsm_map(dev, MAP_1);

	if (migr_map->map_state == IMSM_T_STATE_NORMAL &&
	    dev->vol.migr_type != MIGR_GEN_MIGR)
		return 1;
	else
		return 0;
}

/* return true if we recorded new information */
static int mark_failure(struct intel_super *super,
			struct imsm_dev *dev, struct imsm_disk *disk, int idx)
{
	__u32 ord;
	int slot;
	struct imsm_map *map;
	char buf[MAX_RAID_SERIAL_LEN+3];
	unsigned int len, shift = 0;

	/* new failures are always set in map[0] */
	map = get_imsm_map(dev, MAP_0);

	slot = get_imsm_disk_slot(map, idx);
	if (slot < 0)
		return 0;

	ord = __le32_to_cpu(map->disk_ord_tbl[slot]);
	if (is_failed(disk) && (ord & IMSM_ORD_REBUILD))
		return 0;

	memcpy(buf, disk->serial, MAX_RAID_SERIAL_LEN);
	buf[MAX_RAID_SERIAL_LEN] = '\000';
	strcat(buf, ":0");
	if ((len = strlen(buf)) >= MAX_RAID_SERIAL_LEN)
		shift = len - MAX_RAID_SERIAL_LEN + 1;
	memcpy(disk->serial, &buf[shift], len + 1 - shift);

	disk->status |= FAILED_DISK;
	set_imsm_ord_tbl_ent(map, slot, idx | IMSM_ORD_REBUILD);
	/* mark failures in second map if second map exists and this disk
	 * in this slot.
	 * This is valid for migration, initialization and rebuild
	 */
	if (dev->vol.migr_state) {
		struct imsm_map *map2 = get_imsm_map(dev, MAP_1);
		int slot2 = get_imsm_disk_slot(map2, idx);

		if (slot2 < map2->num_members && slot2 >= 0)
			set_imsm_ord_tbl_ent(map2, slot2,
					     idx | IMSM_ORD_REBUILD);
	}
	if (map->failed_disk_num == 0xff ||
		(!is_rebuilding(dev) && map->failed_disk_num > slot))
		map->failed_disk_num = slot;

	clear_disk_badblocks(super->bbm_log, ord_to_idx(ord));

	return 1;
}

static void mark_missing(struct intel_super *super,
			 struct imsm_dev *dev, struct imsm_disk *disk, int idx)
{
	mark_failure(super, dev, disk, idx);

	if (disk->scsi_id == __cpu_to_le32(~(__u32)0))
		return;

	disk->scsi_id = __cpu_to_le32(~(__u32)0);
	memmove(&disk->serial[0], &disk->serial[1], MAX_RAID_SERIAL_LEN - 1);
}

static void handle_missing(struct intel_super *super, struct imsm_dev *dev)
{
	struct dl *dl;

	if (!super->missing)
		return;

	/* When orom adds replacement for missing disk it does
	 * not remove entry of missing disk, but just updates map with
	 * new added disk. So it is not enough just to test if there is
	 * any missing disk, we have to look if there are any failed disks
	 * in map to stop migration */

	dprintf("imsm: mark missing\n");
	/* end process for initialization and rebuild only
	 */
	if (is_gen_migration(dev) == false) {
		int failed = imsm_count_failed(super, dev, MAP_0);

		if (failed) {
			__u8 map_state;
			struct imsm_map *map = get_imsm_map(dev, MAP_0);
			struct imsm_map *map1;
			int i, ord, ord_map1;
			int rebuilt = 1;

			for (i = 0; i < map->num_members; i++) {
				ord = get_imsm_ord_tbl_ent(dev, i, MAP_0);
				if (!(ord & IMSM_ORD_REBUILD))
					continue;

				map1 = get_imsm_map(dev, MAP_1);
				if (!map1)
					continue;

				ord_map1 = __le32_to_cpu(map1->disk_ord_tbl[i]);
				if (ord_map1 & IMSM_ORD_REBUILD)
					rebuilt = 0;
			}

			if (rebuilt) {
				map_state = imsm_check_degraded(super, dev,
								failed, MAP_0);
				end_migration(dev, super, map_state);
			}
		}
	}
	for (dl = super->missing; dl; dl = dl->next)
		mark_missing(super, dev, &dl->disk, dl->index);
	super->updates_pending++;
}

static unsigned long long imsm_set_array_size(struct imsm_dev *dev,
					      long long new_size)
{
	unsigned long long array_blocks;
	struct imsm_map *map = get_imsm_map(dev, MAP_0);
	int used_disks = imsm_num_data_members(map);

	if (used_disks == 0) {
		/* when problems occures
		 * return current array_blocks value
		 */
		array_blocks = imsm_dev_size(dev);

		return array_blocks;
	}

	/* set array size in metadata
	 */
	if (new_size <= 0)
		/* OLCE size change is caused by added disks
		 */
		array_blocks = per_dev_array_size(map) * used_disks;
	else
		/* Online Volume Size Change
		 * Using  available free space
		 */
		array_blocks = new_size;

	array_blocks = round_size_to_mb(array_blocks, used_disks);
	set_imsm_dev_size(dev, array_blocks);

	return array_blocks;
}

static void imsm_set_disk(struct active_array *a, int n, int state);

static void imsm_progress_container_reshape(struct intel_super *super)
{
	/* if no device has a migr_state, but some device has a
	 * different number of members than the previous device, start
	 * changing the number of devices in this device to match
	 * previous.
	 */
	struct imsm_super *mpb = super->anchor;
	int prev_disks = -1;
	int i;
	int copy_map_size;

	for (i = 0; i < mpb->num_raid_devs; i++) {
		struct imsm_dev *dev = get_imsm_dev(super, i);
		struct imsm_map *map = get_imsm_map(dev, MAP_0);
		struct imsm_map *map2;
		int prev_num_members;

		if (dev->vol.migr_state)
			return;

		if (prev_disks == -1)
			prev_disks = map->num_members;
		if (prev_disks == map->num_members)
			continue;

		/* OK, this array needs to enter reshape mode.
		 * i.e it needs a migr_state
		 */

		copy_map_size = sizeof_imsm_map(map);
		prev_num_members = map->num_members;
		map->num_members = prev_disks;
		dev->vol.migr_state = MIGR_STATE_MIGRATING;
		set_vol_curr_migr_unit(dev, 0);
		set_migr_type(dev, MIGR_GEN_MIGR);
		for (i = prev_num_members;
		     i < map->num_members; i++)
			set_imsm_ord_tbl_ent(map, i, i);
		map2 = get_imsm_map(dev, MAP_1);
		/* Copy the current map */
		memcpy(map2, map, copy_map_size);
		map2->num_members = prev_num_members;

		imsm_set_array_size(dev, -1);
		super->clean_migration_record_by_mdmon = 1;
		super->updates_pending++;
	}
}

/* Handle dirty -> clean transititions, resync and reshape.  Degraded and rebuild
 * states are handled in imsm_set_disk() with one exception, when a
 * resync is stopped due to a new failure this routine will set the
 * 'degraded' state for the array.
 */
static int imsm_set_array_state(struct active_array *a, int consistent)
{
	int inst = a->info.container_member;
	struct intel_super *super = a->container->sb;
	struct imsm_dev *dev = get_imsm_dev(super, inst);
	struct imsm_map *map = get_imsm_map(dev, MAP_0);
	int failed = imsm_count_failed(super, dev, MAP_0);
	__u8 map_state = imsm_check_degraded(super, dev, failed, MAP_0);
	__u32 blocks_per_unit;

	if (dev->vol.migr_state &&
	    dev->vol.migr_type  == MIGR_GEN_MIGR) {
		/* array state change is blocked due to reshape action
		 * We might need to
		 * - abort the reshape (if last_checkpoint is 0 and action!= reshape)
		 * - finish the reshape (if last_checkpoint is big and action != reshape)
		 * - update vol_curr_migr_unit
		 */
		if (a->curr_action == reshape) {
			/* still reshaping, maybe update vol_curr_migr_unit */
			goto mark_checkpoint;
		} else {
			if (a->last_checkpoint >= a->info.component_size) {
				unsigned long long array_blocks;
				int used_disks;
				struct mdinfo *mdi;

				used_disks = imsm_num_data_members(map);
				if (used_disks > 0) {
					array_blocks =
						per_dev_array_size(map) *
						used_disks;
					array_blocks =
						round_size_to_mb(array_blocks,
								 used_disks);
					a->info.custom_array_size = array_blocks;
					/* encourage manager to update array
					 * size
					 */

					a->check_reshape = 1;
				}
				/* finalize online capacity expansion/reshape */
				for (mdi = a->info.devs; mdi; mdi = mdi->next)
					imsm_set_disk(a,
						      mdi->disk.raid_disk,
						      mdi->curr_state);

				imsm_progress_container_reshape(super);
			}
		}
	}

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
			end_migration(dev, super, map_state);
			super->updates_pending++;
			a->last_checkpoint = 0;
		}
	} else if ((!is_resyncing(dev) && !failed) &&
		   (imsm_reshape_blocks_arrays_changes(super) == 0)) {
		/* mark the start of the init process if nothing is failed */
		dprintf("imsm: mark resync start\n");
		if (map->map_state == IMSM_T_STATE_UNINITIALIZED)
			migrate(dev, super, IMSM_T_STATE_NORMAL, MIGR_INIT);
		else
			migrate(dev, super, IMSM_T_STATE_NORMAL, MIGR_REPAIR);
		super->updates_pending++;
	}

	if (a->prev_action == idle)
		goto skip_mark_checkpoint;

mark_checkpoint:
	/* skip checkpointing for general migration,
	 * it is controlled in mdadm
	 */
	if (is_gen_migration(dev))
		goto skip_mark_checkpoint;

	/* check if we can update vol_curr_migr_unit from resync_start,
	 * recovery_start
	 */
	blocks_per_unit = blocks_per_migr_unit(super, dev);
	if (blocks_per_unit) {
		set_vol_curr_migr_unit(dev,
				       a->last_checkpoint / blocks_per_unit);
		dprintf("imsm: mark checkpoint (%llu)\n",
			vol_curr_migr_unit(dev));
		super->updates_pending++;
	}

skip_mark_checkpoint:
	/* mark dirty / clean */
	if (((dev->vol.dirty & RAIDVOL_DIRTY) && consistent) ||
	    (!(dev->vol.dirty & RAIDVOL_DIRTY) && !consistent)) {
		dprintf("imsm: mark '%s'\n", consistent ? "clean" : "dirty");
		if (consistent) {
			dev->vol.dirty = RAIDVOL_CLEAN;
		} else {
			dev->vol.dirty = RAIDVOL_DIRTY;
			if (dev->rwh_policy == RWH_DISTRIBUTED ||
			    dev->rwh_policy == RWH_MULTIPLE_DISTRIBUTED)
				dev->vol.dirty |= RAIDVOL_DSRECORD_VALID;
		}
		super->updates_pending++;
	}

	return consistent;
}

static int imsm_disk_slot_to_ord(struct active_array *a, int slot)
{
	int inst = a->info.container_member;
	struct intel_super *super = a->container->sb;
	struct imsm_dev *dev = get_imsm_dev(super, inst);
	struct imsm_map *map = get_imsm_map(dev, MAP_0);

	if (slot > map->num_members) {
		pr_err("imsm: imsm_disk_slot_to_ord %d out of range 0..%d\n",
		       slot, map->num_members - 1);
		return -1;
	}

	if (slot < 0)
		return -1;

	return get_imsm_ord_tbl_ent(dev, slot, MAP_0);
}

static void imsm_set_disk(struct active_array *a, int n, int state)
{
	int inst = a->info.container_member;
	struct intel_super *super = a->container->sb;
	struct imsm_dev *dev = get_imsm_dev(super, inst);
	struct imsm_map *map = get_imsm_map(dev, MAP_0);
	struct imsm_disk *disk;
	struct mdinfo *mdi;
	int recovery_not_finished = 0;
	int failed;
	int ord;
	__u8 map_state;
	int rebuild_done = 0;
	int i;

	ord = get_imsm_ord_tbl_ent(dev, n, MAP_X);
	if (ord < 0)
		return;

	dprintf("imsm: set_disk %d:%x\n", n, state);
	disk = get_imsm_disk(super, ord_to_idx(ord));

	/* check for new failures */
	if (disk && (state & DS_FAULTY)) {
		if (mark_failure(super, dev, disk, ord_to_idx(ord)))
			super->updates_pending++;
	}

	/* check if in_sync */
	if (state & DS_INSYNC && ord & IMSM_ORD_REBUILD && is_rebuilding(dev)) {
		struct imsm_map *migr_map = get_imsm_map(dev, MAP_1);

		set_imsm_ord_tbl_ent(migr_map, n, ord_to_idx(ord));
		rebuild_done = 1;
		super->updates_pending++;
	}

	failed = imsm_count_failed(super, dev, MAP_0);
	map_state = imsm_check_degraded(super, dev, failed, MAP_0);

	/* check if recovery complete, newly degraded, or failed */
	dprintf("imsm: Detected transition to state ");
	switch (map_state) {
	case IMSM_T_STATE_NORMAL: /* transition to normal state */
		dprintf("normal: ");
		if (is_rebuilding(dev)) {
			dprintf_cont("while rebuilding");
			/* check if recovery is really finished */
			for (mdi = a->info.devs; mdi ; mdi = mdi->next)
				if (mdi->recovery_start != MaxSector) {
					recovery_not_finished = 1;
					break;
				}
			if (recovery_not_finished) {
				dprintf_cont("\n");
				dprintf("Rebuild has not finished yet, state not changed");
				if (a->last_checkpoint < mdi->recovery_start) {
					a->last_checkpoint = mdi->recovery_start;
					super->updates_pending++;
				}
				break;
			}
			end_migration(dev, super, map_state);
			map->failed_disk_num = ~0;
			super->updates_pending++;
			a->last_checkpoint = 0;
			break;
		}
		if (is_gen_migration(dev)) {
			dprintf_cont("while general migration");
			if (a->last_checkpoint >= a->info.component_size)
				end_migration(dev, super, map_state);
			else
				map->map_state = map_state;
			map->failed_disk_num = ~0;
			super->updates_pending++;
			break;
		}
	break;
	case IMSM_T_STATE_DEGRADED: /* transition to degraded state */
		dprintf_cont("degraded: ");
		if (map->map_state != map_state && !dev->vol.migr_state) {
			dprintf_cont("mark degraded");
			map->map_state = map_state;
			super->updates_pending++;
			a->last_checkpoint = 0;
			break;
		}
		if (is_rebuilding(dev)) {
			dprintf_cont("while rebuilding ");
			if (state & DS_FAULTY)  {
				dprintf_cont("removing failed drive ");
				if (n == map->failed_disk_num) {
					dprintf_cont("end migration");
					end_migration(dev, super, map_state);
					a->last_checkpoint = 0;
				} else {
					dprintf_cont("fail detected during rebuild, changing map state");
					map->map_state = map_state;
				}
				super->updates_pending++;
			}

			if (!rebuild_done)
				break;

			/* check if recovery is really finished */
			for (mdi = a->info.devs; mdi ; mdi = mdi->next)
				if (mdi->recovery_start != MaxSector) {
					recovery_not_finished = 1;
					break;
				}
			if (recovery_not_finished) {
				dprintf_cont("\n");
				dprintf_cont("Rebuild has not finished yet");
				if (a->last_checkpoint < mdi->recovery_start) {
					a->last_checkpoint =
						mdi->recovery_start;
					super->updates_pending++;
				}
				break;
			}

			dprintf_cont(" Rebuild done, still degraded");
			end_migration(dev, super, map_state);
			a->last_checkpoint = 0;
			super->updates_pending++;

			for (i = 0; i < map->num_members; i++) {
				int idx = get_imsm_ord_tbl_ent(dev, i, MAP_0);

				if (idx & IMSM_ORD_REBUILD)
					map->failed_disk_num = i;
			}
			super->updates_pending++;
			break;
		}
		if (is_gen_migration(dev)) {
			dprintf_cont("while general migration");
			if (a->last_checkpoint >= a->info.component_size)
				end_migration(dev, super, map_state);
			else {
				map->map_state = map_state;
				manage_second_map(super, dev);
			}
			super->updates_pending++;
			break;
		}
		if (is_initializing(dev)) {
			dprintf_cont("while initialization.");
			map->map_state = map_state;
			super->updates_pending++;
			break;
		}
	break;
	case IMSM_T_STATE_FAILED: /* transition to failed state */
		dprintf_cont("failed: ");
		if (is_gen_migration(dev)) {
			dprintf_cont("while general migration");
			map->map_state = map_state;
			super->updates_pending++;
			break;
		}
		if (map->map_state != map_state) {
			dprintf_cont("mark failed");
			end_migration(dev, super, map_state);
			super->updates_pending++;
			a->last_checkpoint = 0;
			break;
		}
	break;
	default:
		dprintf_cont("state %i\n", map_state);
	}
	dprintf_cont("\n");
}

static int store_imsm_mpb(int fd, struct imsm_super *mpb)
{
	void *buf = mpb;
	__u32 mpb_size = __le32_to_cpu(mpb->mpb_size);
	unsigned long long dsize;
	unsigned long long sectors;
	unsigned int sector_size;

	if (!get_dev_sector_size(fd, NULL, &sector_size))
		return 1;
	get_dev_size(fd, NULL, &dsize);

	if (mpb_size > sector_size) {
		/* -1 to account for anchor */
		sectors = mpb_sectors(mpb, sector_size) - 1;

		/* write the extended mpb to the sectors preceeding the anchor */
		if (lseek64(fd, dsize - (sector_size * (2 + sectors)),
		   SEEK_SET) < 0)
			return 1;

		if ((unsigned long long)write(fd, buf + sector_size,
		   sector_size * sectors) != sector_size * sectors)
			return 1;
	}

	/* first block is stored on second to last sector of the disk */
	if (lseek64(fd, dsize - (sector_size * 2), SEEK_SET) < 0)
		return 1;

	if ((unsigned int)write(fd, buf, sector_size) != sector_size)
		return 1;

	return 0;
}

static void imsm_sync_metadata(struct supertype *container)
{
	struct intel_super *super = container->sb;

	dprintf("sync metadata: %d\n", super->updates_pending);
	if (!super->updates_pending)
		return;

	write_super_imsm(container, 0);

	super->updates_pending = 0;
}

static struct dl *imsm_readd(struct intel_super *super, int idx, struct active_array *a)
{
	struct imsm_dev *dev = get_imsm_dev(super, a->info.container_member);
	int i = get_imsm_disk_idx(dev, idx, MAP_X);
	struct dl *dl;

	for (dl = super->disks; dl; dl = dl->next)
		if (dl->index == i)
			break;

	if (dl && is_failed(&dl->disk))
		dl = NULL;

	if (dl)
		dprintf("found %x:%x\n", dl->major, dl->minor);

	return dl;
}

static struct dl *imsm_add_spare(struct intel_super *super, int slot,
				 struct active_array *a, int activate_new,
				 struct mdinfo *additional_test_list)
{
	struct imsm_dev *dev = get_imsm_dev(super, a->info.container_member);
	int idx = get_imsm_disk_idx(dev, slot, MAP_X);
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
	struct mdinfo *test_list;

	for (dl = super->disks; dl; dl = dl->next) {
		/* If in this array, skip */
		for (d = a->info.devs ; d ; d = d->next)
			if (is_fd_valid(d->state_fd) &&
			    d->disk.major == dl->major &&
			    d->disk.minor == dl->minor) {
				dprintf("%x:%x already in array\n",
					dl->major, dl->minor);
				break;
			}
		if (d)
			continue;
		test_list = additional_test_list;
		while (test_list) {
			if (test_list->disk.major == dl->major &&
			    test_list->disk.minor == dl->minor) {
				dprintf("%x:%x already in additional test list\n",
					dl->major, dl->minor);
				break;
			}
			test_list = test_list->next;
		}
		if (test_list)
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

		if (!drive_validate_sector_size(super, dl))
			continue;

		/* Does this unused device have the requisite free space?
		 * It needs to be able to cover all member volumes
		 */
		ex = get_extents(super, dl, 1);
		if (!ex) {
			dprintf("cannot get extents\n");
			continue;
		}
		for (i = 0; i < mpb->num_raid_devs; i++) {
			dev = get_imsm_dev(super, i);
			map = get_imsm_map(dev, MAP_0);

			/* check if this disk is already a member of
			 * this array
			 */
			if (get_imsm_disk_slot(map, dl->index) >= 0)
				continue;

			found = 0;
			j = 0;
			pos = 0;
			array_start = pba_of_lba0(map);
			array_end = array_start +
				    per_dev_array_size(map) - 1;

			do {
				/* check that we can start at pba_of_lba0 with
				 * num_data_stripes*blocks_per_stripe of space
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

static int imsm_rebuild_allowed(struct supertype *cont, int dev_idx, int failed)
{
	struct imsm_dev *dev2;
	struct imsm_map *map;
	struct dl *idisk;
	int slot;
	int idx;
	__u8 state;

	dev2 = get_imsm_dev(cont->sb, dev_idx);

	state = imsm_check_degraded(cont->sb, dev2, failed, MAP_0);
	if (state == IMSM_T_STATE_FAILED) {
		map = get_imsm_map(dev2, MAP_0);
		for (slot = 0; slot < map->num_members; slot++) {
			/*
			 * Check if failed disks are deleted from intel
			 * disk list or are marked to be deleted
			 */
			idx = get_imsm_disk_idx(dev2, slot, MAP_X);
			idisk = get_imsm_dl_disk(cont->sb, idx);
			/*
			 * Do not rebuild the array if failed disks
			 * from failed sub-array are not removed from
			 * container.
			 */
			if (idisk &&
			    is_failed(&idisk->disk) &&
			    (idisk->action != DISK_REMOVE))
				return 0;
		}
	}
	return 1;
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
	struct imsm_map *map = get_imsm_map(dev, MAP_0);
	int failed = a->info.array.raid_disks;
	struct mdinfo *rv = NULL;
	struct mdinfo *d;
	struct mdinfo *di;
	struct metadata_update *mu;
	struct dl *dl;
	struct imsm_update_activate_spare *u;
	int num_spares = 0;
	int i;
	int allowed;

	for (d = a->info.devs ; d; d = d->next) {
		if (!is_fd_valid(d->state_fd))
			continue;

		if (d->curr_state & DS_FAULTY)
			/* wait for Removal to happen */
			return NULL;

		failed--;
	}

	dprintf("imsm: activate spare: inst=%d failed=%d (%d) level=%d\n",
		inst, failed, a->info.array.raid_disks, a->info.array.level);

	if (imsm_reshape_blocks_arrays_changes(super))
			return NULL;

	/* Cannot activate another spare if rebuild is in progress already
	 */
	if (is_rebuilding(dev)) {
		dprintf("imsm: No spare activation allowed. Rebuild in progress already.\n");
		return NULL;
	}

	if (a->info.array.level == 4)
		/* No repair for takeovered array
		 * imsm doesn't support raid4
		 */
		return NULL;

	if (imsm_check_degraded(super, dev, failed, MAP_0) !=
			IMSM_T_STATE_DEGRADED)
		return NULL;

	if (get_imsm_map(dev, MAP_0)->map_state == IMSM_T_STATE_UNINITIALIZED) {
		dprintf("imsm: No spare activation allowed. Volume is not initialized.\n");
		return NULL;
	}

	/*
	 * If there are any failed disks check state of the other volume.
	 * Block rebuild if the another one is failed until failed disks
	 * are removed from container.
	 */
	if (failed) {
		dprintf("found failed disks in %.*s, check if there anotherfailed sub-array.\n",
			MAX_RAID_SERIAL_LEN, dev->volume);
		/* check if states of the other volumes allow for rebuild */
		for (i = 0; i <  super->anchor->num_raid_devs; i++) {
			if (i != inst) {
				allowed = imsm_rebuild_allowed(a->container,
							       i, failed);
				if (!allowed)
					return NULL;
			}
		}
	}

	/* For each slot, if it is not working, find a spare */
	for (i = 0; i < a->info.array.raid_disks; i++) {
		for (d = a->info.devs ; d ; d = d->next)
			if (d->disk.raid_disk == i)
				break;
		dprintf("found %d: %p %x\n", i, d, d?d->curr_state:0);
		if (d && is_fd_valid(d->state_fd))
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
			dl = imsm_add_spare(super, i, a, 0, rv);
		if (!dl)
			dl = imsm_add_spare(super, i, a, 1, rv);
		if (!dl)
			continue;

		/* found a usable disk with enough space */
		di = xcalloc(1, sizeof(*di));

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
		di->data_offset = pba_of_lba0(map);
		di->component_size = a->info.component_size;
		di->container_member = inst;
		di->bb.supported = 1;
		if (a->info.consistency_policy == CONSISTENCY_POLICY_PPL) {
			di->ppl_sector = get_ppl_sector(super, inst);
			di->ppl_size = MULTIPLE_PPL_AREA_SIZE_IMSM >> 9;
		}
		super->random = random32();
		di->next = rv;
		rv = di;
		num_spares++;
		dprintf("%x:%x to be %d at %llu\n", dl->major, dl->minor,
			i, di->data_offset);
	}

	if (!rv)
		/* No spares found */
		return rv;
	/* Now 'rv' has a list of devices to return.
	 * Create a metadata_update record to update the
	 * disk_ord_tbl for the array
	 */
	mu = xmalloc(sizeof(*mu));
	mu->buf = xcalloc(num_spares,
			  sizeof(struct imsm_update_activate_spare));
	mu->space = NULL;
	mu->space_list = NULL;
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
	struct imsm_map *map = get_imsm_map(dev, MAP_0);
	struct imsm_map *new_map = get_imsm_map(&u->dev, MAP_0);
	struct disk_info *inf = get_disk_info(u);
	struct imsm_disk *disk;
	int i;
	int j;

	for (i = 0; i < map->num_members; i++) {
		disk = get_imsm_disk(super, get_imsm_disk_idx(dev, i, MAP_X));
		for (j = 0; j < new_map->num_members; j++)
			if (serialcmp(disk->serial, inf[j].serial) == 0)
				return 1;
	}

	return 0;
}

static struct dl *get_disk_super(struct intel_super *super, int major, int minor)
{
	struct dl *dl;

	for (dl = super->disks; dl; dl = dl->next)
		if (dl->major == major &&  dl->minor == minor)
			return dl;
	return NULL;
}

static int remove_disk_super(struct intel_super *super, int major, int minor)
{
	struct dl *prev;
	struct dl *dl;

	prev = NULL;
	for (dl = super->disks; dl; dl = dl->next) {
		if (dl->major == major && dl->minor == minor) {
			/* remove */
			if (prev)
				prev->next = dl->next;
			else
				super->disks = dl->next;
			dl->next = NULL;
			__free_imsm_disk(dl, 1);
			dprintf("removed %x:%x\n", major, minor);
			break;
		}
		prev = dl;
	}
	return 0;
}

static void imsm_delete(struct intel_super *super, struct dl **dlp, unsigned index);

static int add_remove_disk_update(struct intel_super *super)
{
	int check_degraded = 0;
	struct dl *disk;

	/* add/remove some spares to/from the metadata/contrainer */
	while (super->disk_mgmt_list) {
		struct dl *disk_cfg;

		disk_cfg = super->disk_mgmt_list;
		super->disk_mgmt_list = disk_cfg->next;
		disk_cfg->next = NULL;

		if (disk_cfg->action == DISK_ADD) {
			disk_cfg->next = super->disks;
			super->disks = disk_cfg;
			check_degraded = 1;
			dprintf("added %x:%x\n",
				disk_cfg->major, disk_cfg->minor);
		} else if (disk_cfg->action == DISK_REMOVE) {
			dprintf("Disk remove action processed: %x.%x\n",
				disk_cfg->major, disk_cfg->minor);
			disk = get_disk_super(super,
					      disk_cfg->major,
					      disk_cfg->minor);
			if (disk) {
				/* store action status */
				disk->action = DISK_REMOVE;
				/* remove spare disks only */
				if (disk->index == -1) {
					remove_disk_super(super,
							  disk_cfg->major,
							  disk_cfg->minor);
				} else {
					disk_cfg->fd = disk->fd;
					disk->fd = -1;
				}
			}
			/* release allocate disk structure */
			__free_imsm_disk(disk_cfg, 1);
		}
	}
	return check_degraded;
}

static int apply_reshape_migration_update(struct imsm_update_reshape_migration *u,
						struct intel_super *super,
						void ***space_list)
{
	struct intel_dev *id;
	void **tofree = NULL;
	int ret_val = 0;

	dprintf("(enter)\n");
	if (u->subdev < 0 || u->subdev > 1) {
		dprintf("imsm: Error: Wrong subdev: %i\n", u->subdev);
		return ret_val;
	}
	if (space_list == NULL || *space_list == NULL) {
		dprintf("imsm: Error: Memory is not allocated\n");
		return ret_val;
	}

	for (id = super->devlist ; id; id = id->next) {
		if (id->index == (unsigned)u->subdev) {
			struct imsm_dev *dev = get_imsm_dev(super, u->subdev);
			struct imsm_map *map;
			struct imsm_dev *new_dev =
				(struct imsm_dev *)*space_list;
			struct imsm_map *migr_map = get_imsm_map(dev, MAP_1);
			int to_state;
			struct dl *new_disk;

			if (new_dev == NULL)
				return ret_val;
			*space_list = **space_list;
			memcpy(new_dev, dev, sizeof_imsm_dev(dev, 0));
			map = get_imsm_map(new_dev, MAP_0);
			if (migr_map) {
				dprintf("imsm: Error: migration in progress");
				return ret_val;
			}

			to_state = map->map_state;
			if ((u->new_level == IMSM_T_RAID5) && (map->raid_level == IMSM_T_RAID0)) {
				map->num_members++;
				/* this should not happen */
				if (u->new_disks[0] < 0) {
					map->failed_disk_num =
						map->num_members - 1;
					to_state = IMSM_T_STATE_DEGRADED;
				} else
					to_state = IMSM_T_STATE_NORMAL;
			}
			migrate(new_dev, super, to_state, MIGR_GEN_MIGR);

			if (u->new_level > -1)
				update_imsm_raid_level(map, u->new_level);

			migr_map = get_imsm_map(new_dev, MAP_1);
			if ((u->new_level == IMSM_T_RAID5) &&
			    (migr_map->raid_level == IMSM_T_RAID0)) {
				int ord = map->num_members - 1;
				migr_map->num_members--;
				if (u->new_disks[0] < 0)
					ord |= IMSM_ORD_REBUILD;
				set_imsm_ord_tbl_ent(map,
						     map->num_members - 1,
						     ord);
			}
			id->dev = new_dev;
			tofree = (void **)dev;

			/* update chunk size
			 */
			if (u->new_chunksize > 0) {
				struct imsm_map *dest_map =
					get_imsm_map(dev, MAP_0);
				int used_disks =
					imsm_num_data_members(dest_map);

				if (used_disks == 0)
					return ret_val;

				map->blocks_per_strip =
					__cpu_to_le16(u->new_chunksize * 2);
				update_num_data_stripes(map, imsm_dev_size(dev));
			}

			/* ensure blocks_per_member has valid value
			 */
			set_blocks_per_member(map,
					      per_dev_array_size(map) +
					      NUM_BLOCKS_DIRTY_STRIPE_REGION);

			/* add disk
			 */
			if (u->new_level != IMSM_T_RAID5 || migr_map->raid_level != IMSM_T_RAID0 ||
			    migr_map->raid_level == map->raid_level)
				goto skip_disk_add;

			if (u->new_disks[0] >= 0) {
				/* use passes spare
				 */
				new_disk = get_disk_super(super,
							major(u->new_disks[0]),
							minor(u->new_disks[0]));
				dprintf("imsm: new disk for reshape is: %i:%i (%p, index = %i)\n",
					major(u->new_disks[0]),
					minor(u->new_disks[0]),
					new_disk, new_disk->index);
				if (new_disk == NULL)
					goto error_disk_add;

				new_disk->index = map->num_members - 1;
				/* slot to fill in autolayout
				 */
				new_disk->raiddisk = new_disk->index;
				new_disk->disk.status |= CONFIGURED_DISK;
				new_disk->disk.status &= ~SPARE_DISK;
			} else
				goto error_disk_add;

skip_disk_add:
			*tofree = *space_list;
			/* calculate new size
			 */
			imsm_set_array_size(new_dev, -1);

			ret_val = 1;
		}
	}

	if (tofree)
		*space_list = tofree;
	return ret_val;

error_disk_add:
	dprintf("Error: imsm: Cannot find disk.\n");
	return ret_val;
}

static int apply_size_change_update(struct imsm_update_size_change *u,
		struct intel_super *super)
{
	struct intel_dev *id;
	int ret_val = 0;

	dprintf("(enter)\n");
	if (u->subdev < 0 || u->subdev > 1) {
		dprintf("imsm: Error: Wrong subdev: %i\n", u->subdev);
		return ret_val;
	}

	for (id = super->devlist ; id; id = id->next) {
		if (id->index == (unsigned)u->subdev) {
			struct imsm_dev *dev = get_imsm_dev(super, u->subdev);
			struct imsm_map *map = get_imsm_map(dev, MAP_0);
			int used_disks = imsm_num_data_members(map);
			unsigned long long blocks_per_member;
			unsigned long long new_size_per_disk;

			if (used_disks == 0)
				return 0;

			/* calculate new size
			 */
			new_size_per_disk = u->new_size / used_disks;
			blocks_per_member = new_size_per_disk +
					    NUM_BLOCKS_DIRTY_STRIPE_REGION;

			imsm_set_array_size(dev, u->new_size);
			set_blocks_per_member(map, blocks_per_member);
			update_num_data_stripes(map, u->new_size);
			ret_val = 1;
			break;
		}
	}

	return ret_val;
}

static int prepare_spare_to_activate(struct supertype *st,
				     struct imsm_update_activate_spare *u)
{
	struct intel_super *super = st->sb;
	int prev_current_vol = super->current_vol;
	struct active_array *a;
	int ret = 1;

	for (a = st->arrays; a; a = a->next)
		/*
		 * Additional initialization (adding bitmap header, filling
		 * the bitmap area with '1's to force initial rebuild for a whole
		 * data-area) is required when adding the spare to the volume
		 * with write-intent bitmap.
		 */
		if (a->info.container_member == u->array &&
		    a->info.consistency_policy == CONSISTENCY_POLICY_BITMAP) {
			struct dl *dl;

			for (dl = super->disks; dl; dl = dl->next)
				if (dl == u->dl)
					break;
			if (!dl)
				break;

			super->current_vol = u->array;
			if (st->ss->write_bitmap(st, dl->fd, NoUpdate))
				ret = 0;
			super->current_vol = prev_current_vol;
		}
	return ret;
}

static int apply_update_activate_spare(struct imsm_update_activate_spare *u,
				       struct intel_super *super,
				       struct active_array *active_array)
{
	struct imsm_super *mpb = super->anchor;
	struct imsm_dev *dev = get_imsm_dev(super, u->array);
	struct imsm_map *map = get_imsm_map(dev, MAP_0);
	struct imsm_map *migr_map;
	struct active_array *a;
	struct imsm_disk *disk;
	__u8 to_state;
	struct dl *dl;
	unsigned int found;
	int failed;
	int victim;
	int i;
	int second_map_created = 0;

	for (; u; u = u->next) {
		victim = get_imsm_disk_idx(dev, u->slot, MAP_X);

		if (victim < 0)
			return 0;

		for (dl = super->disks; dl; dl = dl->next)
			if (dl == u->dl)
				break;

		if (!dl) {
			pr_err("error: imsm_activate_spare passed an unknown disk (index: %d)\n",
				u->dl->index);
			return 0;
		}

		/* count failures (excluding rebuilds and the victim)
		 * to determine map[0] state
		 */
		failed = 0;
		for (i = 0; i < map->num_members; i++) {
			if (i == u->slot)
				continue;
			disk = get_imsm_disk(super,
					     get_imsm_disk_idx(dev, i, MAP_X));
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
		to_state = imsm_check_degraded(super, dev, failed, MAP_0);
		if (!second_map_created) {
			second_map_created = 1;
			map->map_state = IMSM_T_STATE_DEGRADED;
			migrate(dev, super, to_state, MIGR_REBUILD);
		} else
			map->map_state = to_state;
		migr_map = get_imsm_map(dev, MAP_1);
		set_imsm_ord_tbl_ent(map, u->slot, dl->index);
		set_imsm_ord_tbl_ent(migr_map, u->slot,
				     dl->index | IMSM_ORD_REBUILD);

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
		for (a = active_array; a ; a = a->next) {
			int dev_idx = a->info.container_member;

			if (get_disk_slot_in_dev(super, dev_idx, victim) >= 0)
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
				for (dlp = &super->missing; *dlp;
				     dlp = &(*dlp)->next)
					if ((*dlp)->index == victim)
						break;
			imsm_delete(super, dlp, victim);
		}
	}

	return 1;
}

static int apply_reshape_container_disks_update(struct imsm_update_reshape *u,
						struct intel_super *super,
						void ***space_list)
{
	struct dl *new_disk;
	struct intel_dev *id;
	int i;
	int delta_disks = u->new_raid_disks - u->old_raid_disks;
	int disk_count = u->old_raid_disks;
	void **tofree = NULL;
	int devices_to_reshape = 1;
	struct imsm_super *mpb = super->anchor;
	int ret_val = 0;
	unsigned int dev_id;

	dprintf("(enter)\n");

	/* enable spares to use in array */
	for (i = 0; i < delta_disks; i++) {
		new_disk = get_disk_super(super,
					  major(u->new_disks[i]),
					  minor(u->new_disks[i]));
		dprintf("imsm: new disk for reshape is: %i:%i (%p, index = %i)\n",
			major(u->new_disks[i]), minor(u->new_disks[i]),
			new_disk, new_disk->index);
		if (new_disk == NULL ||
		    (new_disk->index >= 0 &&
		     new_disk->index < u->old_raid_disks))
			goto update_reshape_exit;
		new_disk->index = disk_count++;
		/* slot to fill in autolayout
		 */
		new_disk->raiddisk = new_disk->index;
		new_disk->disk.status |=
			CONFIGURED_DISK;
		new_disk->disk.status &= ~SPARE_DISK;
	}

	dprintf("imsm: volume set mpb->num_raid_devs = %i\n",
		mpb->num_raid_devs);
	/* manage changes in volume
	 */
	for (dev_id = 0; dev_id < mpb->num_raid_devs; dev_id++) {
		void **sp = *space_list;
		struct imsm_dev *newdev;
		struct imsm_map *newmap, *oldmap;

		for (id = super->devlist ; id; id = id->next) {
			if (id->index == dev_id)
				break;
		}
		if (id == NULL)
			break;
		if (!sp)
			continue;
		*space_list = *sp;
		newdev = (void*)sp;
		/* Copy the dev, but not (all of) the map */
		memcpy(newdev, id->dev, sizeof(*newdev));
		oldmap = get_imsm_map(id->dev, MAP_0);
		newmap = get_imsm_map(newdev, MAP_0);
		/* Copy the current map */
		memcpy(newmap, oldmap, sizeof_imsm_map(oldmap));
		/* update one device only
		 */
		if (devices_to_reshape) {
			dprintf("imsm: modifying subdev: %i\n",
				id->index);
			devices_to_reshape--;
			newdev->vol.migr_state = MIGR_STATE_MIGRATING;
			set_vol_curr_migr_unit(newdev, 0);
			set_migr_type(newdev, MIGR_GEN_MIGR);
			newmap->num_members = u->new_raid_disks;
			for (i = 0; i < delta_disks; i++) {
				set_imsm_ord_tbl_ent(newmap,
						     u->old_raid_disks + i,
						     u->old_raid_disks + i);
			}
			/* New map is correct, now need to save old map
			 */
			newmap = get_imsm_map(newdev, MAP_1);
			memcpy(newmap, oldmap, sizeof_imsm_map(oldmap));

			imsm_set_array_size(newdev, -1);
		}

		sp = (void **)id->dev;
		id->dev = newdev;
		*sp = tofree;
		tofree = sp;

		/* Clear migration record */
		memset(super->migr_rec, 0, sizeof(struct migr_record));
	}
	if (tofree)
		*space_list = tofree;
	ret_val = 1;

update_reshape_exit:

	return ret_val;
}

static int apply_takeover_update(struct imsm_update_takeover *u,
				 struct intel_super *super,
				 void ***space_list)
{
	struct imsm_dev *dev = NULL;
	struct intel_dev *dv;
	struct imsm_dev *dev_new;
	struct imsm_map *map;
	struct dl *dm, *du;
	int i;

	for (dv = super->devlist; dv; dv = dv->next)
		if (dv->index == (unsigned int)u->subarray) {
			dev = dv->dev;
			break;
		}

	if (dev == NULL)
		return 0;

	map = get_imsm_map(dev, MAP_0);

	if (u->direction == R10_TO_R0) {
		/* Number of failed disks must be half of initial disk number */
		if (imsm_count_failed(super, dev, MAP_0) !=
				(map->num_members / 2))
			return 0;

		/* iterate through devices to mark removed disks as spare */
		for (dm = super->disks; dm; dm = dm->next) {
			if (dm->disk.status & FAILED_DISK) {
				int idx = dm->index;
				/* update indexes on the disk list */
/* FIXME this loop-with-the-loop looks wrong,  I'm not convinced
   the index values will end up being correct.... NB */
				for (du = super->disks; du; du = du->next)
					if (du->index > idx)
						du->index--;
				/* mark as spare disk */
				mark_spare(dm);
			}
		}
		/* update map */
		map->num_members /= map->num_domains;
		map->map_state = IMSM_T_STATE_NORMAL;
		update_imsm_raid_level(map, IMSM_T_RAID0);
		set_num_domains(map);
		update_num_data_stripes(map, imsm_dev_size(dev));
		map->failed_disk_num = -1;
	}

	if (u->direction == R0_TO_R10) {
		void **space;

		/* update slots in current disk list */
		for (dm = super->disks; dm; dm = dm->next) {
			if (dm->index >= 0)
				dm->index *= 2;
		}
		/* create new *missing* disks */
		for (i = 0; i < map->num_members; i++) {
			space = *space_list;
			if (!space)
				continue;
			*space_list = *space;
			du = (void *)space;
			memcpy(du, super->disks, sizeof(*du));
			du->fd = -1;
			du->minor = 0;
			du->major = 0;
			du->index = (i * 2) + 1;
			sprintf((char *)du->disk.serial,
				" MISSING_%d", du->index);
			sprintf((char *)du->serial,
				"MISSING_%d", du->index);
			du->next = super->missing;
			super->missing = du;
		}
		/* create new dev and map */
		space = *space_list;
		if (!space)
			return 0;
		*space_list = *space;
		dev_new = (void *)space;
		memcpy(dev_new, dev, sizeof(*dev));
		/* update new map */
		map = get_imsm_map(dev_new, MAP_0);

		map->map_state = IMSM_T_STATE_DEGRADED;
		update_imsm_raid_level(map, IMSM_T_RAID10);
		set_num_domains(map);
		map->num_members = map->num_members * map->num_domains;
		update_num_data_stripes(map, imsm_dev_size(dev));

		/* replace dev<->dev_new */
		dv->dev = dev_new;
	}
	/* update disk order table */
	for (du = super->disks; du; du = du->next)
		if (du->index >= 0)
			set_imsm_ord_tbl_ent(map, du->index, du->index);
	for (du = super->missing; du; du = du->next)
		if (du->index >= 0) {
			set_imsm_ord_tbl_ent(map, du->index, du->index);
			mark_missing(super, dv->dev, &du->disk, du->index);
		}

	return 1;
}

static void imsm_process_update(struct supertype *st,
			        struct metadata_update *update)
{
	/**
	 * crack open the metadata_update envelope to find the update record
	 * update can be one of:
	 *    update_reshape_container_disks - all the arrays in the container
	 *      are being reshaped to have more devices.  We need to mark
	 *      the arrays for general migration and convert selected spares
	 *      into active devices.
	 *    update_activate_spare - a spare device has replaced a failed
	 *      device in an array, update the disk_ord_tbl.  If this disk is
	 *      present in all member arrays then also clear the SPARE_DISK
	 *      flag
	 *    update_create_array
	 *    update_kill_array
	 *    update_rename_array
	 *    update_add_remove_disk
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
	case update_general_migration_checkpoint: {
		struct intel_dev *id;
		struct imsm_update_general_migration_checkpoint *u =
							(void *)update->buf;

		dprintf("called for update_general_migration_checkpoint\n");

		/* find device under general migration */
		for (id = super->devlist ; id; id = id->next) {
			if (is_gen_migration(id->dev)) {
				set_vol_curr_migr_unit(id->dev,
						   u->curr_migr_unit);
				super->updates_pending++;
			}
		}
		break;
	}
	case update_takeover: {
		struct imsm_update_takeover *u = (void *)update->buf;
		if (apply_takeover_update(u, super, &update->space_list)) {
			imsm_update_version_info(super);
			super->updates_pending++;
		}
		break;
	}

	case update_reshape_container_disks: {
		struct imsm_update_reshape *u = (void *)update->buf;
		if (apply_reshape_container_disks_update(
			    u, super, &update->space_list))
			super->updates_pending++;
		break;
	}
	case update_reshape_migration: {
		struct imsm_update_reshape_migration *u = (void *)update->buf;
		if (apply_reshape_migration_update(
			    u, super, &update->space_list))
			super->updates_pending++;
		break;
	}
	case update_size_change: {
		struct imsm_update_size_change *u = (void *)update->buf;
		if (apply_size_change_update(u, super))
			super->updates_pending++;
		break;
	}
	case update_activate_spare: {
		struct imsm_update_activate_spare *u = (void *) update->buf;

		if (prepare_spare_to_activate(st, u) &&
		    apply_update_activate_spare(u, super, st->arrays))
			super->updates_pending++;
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
			dprintf("subarray %d already defined\n", u->dev_idx);
			goto create_error;
		}

		/* check update is next in sequence */
		if (u->dev_idx != mpb->num_raid_devs) {
			dprintf("can not create array %d expected index %d\n",
				u->dev_idx, mpb->num_raid_devs);
			goto create_error;
		}

		new_map = get_imsm_map(&u->dev, MAP_0);
		new_start = pba_of_lba0(new_map);
		new_end = new_start + per_dev_array_size(new_map);
		inf = get_disk_info(u);

		/* handle activate_spare versus create race:
		 * check to make sure that overlapping arrays do not include
		 * overalpping disks
		 */
		for (i = 0; i < mpb->num_raid_devs; i++) {
			dev = get_imsm_dev(super, i);
			map = get_imsm_map(dev, MAP_0);
			start = pba_of_lba0(map);
			end = start + per_dev_array_size(map);
			if ((new_start >= start && new_start <= end) ||
			    (start >= new_start && start <= new_end))
				/* overlap */;
			else
				continue;

			if (disks_overlap(super, i, u)) {
				dprintf("arrays overlap\n");
				goto create_error;
			}
		}

		/* check that prepare update was successful */
		if (!update->space) {
			dprintf("prepare update failed\n");
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
				dprintf("disk disappeared\n");
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
		if (a || mpb->num_raid_devs == 1 || victim >= super->anchor->num_raid_devs) {
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
		memset(name, 0, sizeof(name));
		snprintf(name, MAX_RAID_SERIAL_LEN, "%s", (char *) u->name);
		name[MAX_RAID_SERIAL_LEN] = '\0';
		for (a = st->arrays; a; a = a->next)
			if (a->info.container_member == target)
				break;
		dev = get_imsm_dev(super, u->dev_idx);

		if (a || !dev || imsm_is_name_allowed(super, name, 0) == false) {
			dprintf("failed to rename subarray-%d\n", target);
			break;
		}

		memcpy(dev->volume, name, MAX_RAID_SERIAL_LEN);
		super->updates_pending++;
		break;
	}
	case update_add_remove_disk: {
		/* we may be able to repair some arrays if disks are
		 * being added, check the status of add_remove_disk
		 * if discs has been added.
		 */
		if (add_remove_disk_update(super)) {
			struct active_array *a;

			super->updates_pending++;
			for (a = st->arrays; a; a = a->next)
				a->check_degraded = 1;
		}
		break;
	}
	case update_prealloc_badblocks_mem:
		break;
	case update_rwh_policy: {
		struct imsm_update_rwh_policy *u = (void *)update->buf;
		int target = u->dev_idx;
		struct imsm_dev *dev = get_imsm_dev(super, target);

		if (dev->rwh_policy != u->new_policy) {
			dev->rwh_policy = u->new_policy;
			super->updates_pending++;
		}
		break;
	}
	default:
		pr_err("error: unsupported process update type:(type: %d)\n",	type);
	}
}

static struct mdinfo *get_spares_for_grow(struct supertype *st);

static int imsm_prepare_update(struct supertype *st,
			       struct metadata_update *update)
{
	/**
	 * Allocate space to hold new disk entries, raid-device entries or a new
	 * mpb if necessary.  The manager synchronously waits for updates to
	 * complete in the monitor, so new mpb buffers allocated here can be
	 * integrated by the monitor thread without worrying about live pointers
	 * in the manager thread.
	 */
	enum imsm_update_type type;
	struct intel_super *super = st->sb;
	unsigned int sector_size = super->sector_size;
	struct imsm_super *mpb = super->anchor;
	size_t buf_len;
	size_t len = 0;

	if (update->len < (int)sizeof(type))
		return 0;

	type = *(enum imsm_update_type *) update->buf;

	switch (type) {
	case update_general_migration_checkpoint:
		if (update->len < (int)sizeof(struct imsm_update_general_migration_checkpoint))
			return 0;
		dprintf("called for update_general_migration_checkpoint\n");
		break;
	case update_takeover: {
		struct imsm_update_takeover *u = (void *)update->buf;
		if (update->len < (int)sizeof(*u))
			return 0;
		if (u->direction == R0_TO_R10) {
			void **tail = (void **)&update->space_list;
			struct imsm_dev *dev = get_imsm_dev(super, u->subarray);
			struct imsm_map *map = get_imsm_map(dev, MAP_0);
			int num_members = map->num_members;
			void *space;
			int size, i;
			/* allocate memory for added disks */
			for (i = 0; i < num_members; i++) {
				size = sizeof(struct dl);
				space = xmalloc(size);
				*tail = space;
				tail = space;
				*tail = NULL;
			}
			/* allocate memory for new device */
			size = sizeof_imsm_dev(super->devlist->dev, 0) +
				(num_members * sizeof(__u32));
			space = xmalloc(size);
			*tail = space;
			tail = space;
			*tail = NULL;
			len = disks_to_mpb_size(num_members * 2);
		}

		break;
	}
	case update_reshape_container_disks: {
		/* Every raid device in the container is about to
		 * gain some more devices, and we will enter a
		 * reconfiguration.
		 * So each 'imsm_map' will be bigger, and the imsm_vol
		 * will now hold 2 of them.
		 * Thus we need new 'struct imsm_dev' allocations sized
		 * as sizeof_imsm_dev but with more devices in both maps.
		 */
		struct imsm_update_reshape *u = (void *)update->buf;
		struct intel_dev *dl;
		void **space_tail = (void**)&update->space_list;

		if (update->len < (int)sizeof(*u))
			return 0;

		dprintf("for update_reshape\n");

		for (dl = super->devlist; dl; dl = dl->next) {
			int size = sizeof_imsm_dev(dl->dev, 1);
			void *s;
			if (u->new_raid_disks > u->old_raid_disks)
				size += sizeof(__u32)*2*
					(u->new_raid_disks - u->old_raid_disks);
			s = xmalloc(size);
			*space_tail = s;
			space_tail = s;
			*space_tail = NULL;
		}

		len = disks_to_mpb_size(u->new_raid_disks);
		dprintf("New anchor length is %llu\n", (unsigned long long)len);
		break;
	}
	case update_reshape_migration: {
		/* for migration level 0->5 we need to add disks
		 * so the same as for container operation we will copy
		 * device to the bigger location.
		 * in memory prepared device and new disk area are prepared
		 * for usage in process update
		 */
		struct imsm_update_reshape_migration *u = (void *)update->buf;
		struct intel_dev *id;
		void **space_tail = (void **)&update->space_list;
		int size;
		void *s;
		int current_level = -1;

		if (update->len < (int)sizeof(*u))
			return 0;

		dprintf("for update_reshape\n");

		/* add space for bigger array in update
		 */
		for (id = super->devlist; id; id = id->next) {
			if (id->index == (unsigned)u->subdev) {
				size = sizeof_imsm_dev(id->dev, 1);
				if (u->new_raid_disks > u->old_raid_disks)
					size += sizeof(__u32)*2*
					(u->new_raid_disks - u->old_raid_disks);
				s = xmalloc(size);
				*space_tail = s;
				space_tail = s;
				*space_tail = NULL;
				break;
			}
		}
		if (update->space_list == NULL)
			break;

		/* add space for disk in update
		 */
		size = sizeof(struct dl);
		s = xmalloc(size);
		*space_tail = s;
		space_tail = s;
		*space_tail = NULL;

		/* add spare device to update
		 */
		for (id = super->devlist ; id; id = id->next)
			if (id->index == (unsigned)u->subdev) {
				struct imsm_dev *dev;
				struct imsm_map *map;

				dev = get_imsm_dev(super, u->subdev);
				map = get_imsm_map(dev, MAP_0);
				current_level = map->raid_level;
				break;
			}
		if (u->new_level == 5 && u->new_level != current_level) {
			struct mdinfo *spares;

			spares = get_spares_for_grow(st);
			if (spares) {
				struct dl *dl;
				struct mdinfo *dev;

				dev = spares->devs;
				if (dev) {
					u->new_disks[0] =
						makedev(dev->disk.major,
							dev->disk.minor);
					dl = get_disk_super(super,
							    dev->disk.major,
							    dev->disk.minor);
					dl->index = u->old_raid_disks;
					dev = dev->next;
				}
				sysfs_free(spares);
			}
		}
		len = disks_to_mpb_size(u->new_raid_disks);
		dprintf("New anchor length is %llu\n", (unsigned long long)len);
		break;
	}
	case update_size_change: {
		if (update->len < (int)sizeof(struct imsm_update_size_change))
			return 0;
		break;
	}
	case update_activate_spare: {
		if (update->len < (int)sizeof(struct imsm_update_activate_spare))
			return 0;
		break;
	}
	case update_create_array: {
		struct imsm_update_create_array *u = (void *) update->buf;
		struct intel_dev *dv;
		struct imsm_dev *dev = &u->dev;
		struct imsm_map *map = get_imsm_map(dev, MAP_0);
		struct dl *dl;
		struct disk_info *inf;
		int i;
		int activate = 0;

		if (update->len < (int)sizeof(*u))
			return 0;

		inf = get_disk_info(u);
		len = sizeof_imsm_dev(dev, 1);
		/* allocate a new super->devlist entry */
		dv = xmalloc(sizeof(*dv));
		dv->dev = xmalloc(len);
		update->space = dv;

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
	}
	case update_kill_array: {
		if (update->len < (int)sizeof(struct imsm_update_kill_array))
			return 0;
		break;
	}
	case update_rename_array: {
		if (update->len < (int)sizeof(struct imsm_update_rename_array))
			return 0;
		break;
	}
	case update_add_remove_disk:
		/* no update->len needed */
		break;
	case update_prealloc_badblocks_mem:
		super->extra_space += sizeof(struct bbm_log) -
			get_imsm_bbm_log_size(super->bbm_log);
		break;
	case update_rwh_policy: {
		if (update->len < (int)sizeof(struct imsm_update_rwh_policy))
			return 0;
		break;
	}
	default:
		return 0;
	}

	/* check if we need a larger metadata buffer */
	if (super->next_buf)
		buf_len = super->next_len;
	else
		buf_len = super->len;

	if (__le32_to_cpu(mpb->mpb_size) + super->extra_space + len > buf_len) {
		/* ok we need a larger buf than what is currently allocated
		 * if this allocation fails process_update will notice that
		 * ->next_len is set and ->next_buf is NULL
		 */
		buf_len = ROUND_UP(__le32_to_cpu(mpb->mpb_size) +
				   super->extra_space + len, sector_size);
		if (super->next_buf)
			free(super->next_buf);

		super->next_len = buf_len;
		if (posix_memalign(&super->next_buf, sector_size, buf_len) == 0)
			memset(super->next_buf, 0, buf_len);
		else
			super->next_buf = NULL;
	}
	return 1;
}

/* must be called while manager is quiesced */
static void imsm_delete(struct intel_super *super, struct dl **dlp, unsigned index)
{
	struct imsm_super *mpb = super->anchor;
	struct dl *iter;
	struct imsm_dev *dev;
	struct imsm_map *map;
	unsigned int i, j, num_members;
	__u32 ord, ord_map0;
	struct bbm_log *log = super->bbm_log;

	dprintf("deleting device[%d] from imsm_super\n", index);

	/* shift all indexes down one */
	for (iter = super->disks; iter; iter = iter->next)
		if (iter->index > (int)index)
			iter->index--;
	for (iter = super->missing; iter; iter = iter->next)
		if (iter->index > (int)index)
			iter->index--;

	for (i = 0; i < mpb->num_raid_devs; i++) {
		dev = get_imsm_dev(super, i);
		map = get_imsm_map(dev, MAP_0);
		num_members = map->num_members;
		for (j = 0; j < num_members; j++) {
			/* update ord entries being careful not to propagate
			 * ord-flags to the first map
			 */
			ord = get_imsm_ord_tbl_ent(dev, j, MAP_X);
			ord_map0 = get_imsm_ord_tbl_ent(dev, j, MAP_0);

			if (ord_to_idx(ord) <= index)
				continue;

			map = get_imsm_map(dev, MAP_0);
			set_imsm_ord_tbl_ent(map, j, ord_map0 - 1);
			map = get_imsm_map(dev, MAP_1);
			if (map)
				set_imsm_ord_tbl_ent(map, j, ord - 1);
		}
	}

	for (i = 0; i < log->entry_count; i++) {
		struct bbm_log_entry *entry = &log->marked_block_entries[i];

		if (entry->disk_ordinal <= index)
			continue;
		entry->disk_ordinal--;
	}

	mpb->num_disks--;
	super->updates_pending++;
	if (*dlp) {
		struct dl *dl = *dlp;

		*dlp = (*dlp)->next;
		__free_imsm_disk(dl, 1);
	}
}

static int imsm_get_allowed_degradation(int level, int raid_disks,
					struct intel_super *super,
					struct imsm_dev *dev)
{
	switch (level) {
	case 1:
	case 10:{
		int ret_val = 0;
		struct imsm_map *map;
		int i;

		ret_val = raid_disks/2;
		/* check map if all disks pairs not failed
		 * in both maps
		 */
		map = get_imsm_map(dev, MAP_0);
		for (i = 0; i < ret_val; i++) {
			int degradation = 0;
			if (get_imsm_disk(super, i) == NULL)
				degradation++;
			if (get_imsm_disk(super, i + 1) == NULL)
				degradation++;
			if (degradation == 2)
				return 0;
		}
		map = get_imsm_map(dev, MAP_1);
		/* if there is no second map
		 * result can be returned
		 */
		if (map == NULL)
			return ret_val;
		/* check degradation in second map
		 */
		for (i = 0; i < ret_val; i++) {
			int degradation = 0;
		if (get_imsm_disk(super, i) == NULL)
				degradation++;
			if (get_imsm_disk(super, i + 1) == NULL)
				degradation++;
			if (degradation == 2)
				return 0;
		}
		return ret_val;
	}
	case 5:
		return 1;
	case 6:
		return 2;
	default:
		return 0;
	}
}

/*******************************************************************************
 * Function:	validate_container_imsm
 * Description: This routine validates container after assemble,
 *		eg. if devices in container are under the same controller.
 *
 * Parameters:
 *	info	: linked list with info about devices used in array
 * Returns:
 *	1 : HBA mismatch
 *	0 : Success
 ******************************************************************************/
int validate_container_imsm(struct mdinfo *info)
{
	if (check_no_platform())
		return 0;

	struct sys_dev *idev;
	struct sys_dev *hba = NULL;
	struct sys_dev *intel_devices = find_intel_devices();
	char *dev_path = devt_to_devpath(makedev(info->disk.major,
						 info->disk.minor), 1, NULL);

	for (idev = intel_devices; idev; idev = idev->next) {
		if (dev_path && strstr(dev_path, idev->path)) {
			hba = idev;
			break;
		}
	}
	if (dev_path)
		free(dev_path);

	if (!hba) {
		pr_err("WARNING - Cannot detect HBA for device %s!\n",
				devid2kname(makedev(info->disk.major, info->disk.minor)));
		return 1;
	}

	const struct imsm_orom *orom = get_orom_by_device_id(hba->dev_id);
	struct mdinfo *dev;

	for (dev = info->next; dev; dev = dev->next) {
		dev_path = devt_to_devpath(makedev(dev->disk.major,
						   dev->disk.minor), 1, NULL);

		struct sys_dev *hba2 = NULL;
		for (idev = intel_devices; idev; idev = idev->next) {
			if (dev_path && strstr(dev_path, idev->path)) {
				hba2 = idev;
				break;
			}
		}
		if (dev_path)
			free(dev_path);

		const struct imsm_orom *orom2 = hba2 == NULL ? NULL :
				get_orom_by_device_id(hba2->dev_id);

		if (hba2 && hba->type != hba2->type) {
			pr_err("WARNING - HBAs of devices do not match %s != %s\n",
				get_sys_dev_type(hba->type), get_sys_dev_type(hba2->type));
			return 1;
		}

		if (orom != orom2) {
			pr_err("WARNING - IMSM container assembled with disks under different HBAs!\n"
				"       This operation is not supported and can lead to data loss.\n");
			return 1;
		}

		if (!orom) {
			pr_err("WARNING - IMSM container assembled with disks under HBAs without IMSM platform support!\n"
				"       This operation is not supported and can lead to data loss.\n");
			return 1;
		}
	}

	return 0;
}

/*******************************************************************************
* Function:   imsm_record_badblock
* Description: This routine stores new bad block record in BBM log
*
* Parameters:
*     a		: array containing a bad block
*     slot	: disk number containing a bad block
*     sector	: bad block sector
*     length	: bad block sectors range
* Returns:
*     1 : Success
*     0 : Error
******************************************************************************/
static int imsm_record_badblock(struct active_array *a, int slot,
			  unsigned long long sector, int length)
{
	struct intel_super *super = a->container->sb;
	int ord;
	int ret;

	ord = imsm_disk_slot_to_ord(a, slot);
	if (ord < 0)
		return 0;

	ret = record_new_badblock(super->bbm_log, ord_to_idx(ord), sector,
				   length);
	if (ret)
		super->updates_pending++;

	return ret;
}
/*******************************************************************************
* Function:   imsm_clear_badblock
* Description: This routine clears bad block record from BBM log
*
* Parameters:
*     a		: array containing a bad block
*     slot	: disk number containing a bad block
*     sector	: bad block sector
*     length	: bad block sectors range
* Returns:
*     1 : Success
*     0 : Error
******************************************************************************/
static int imsm_clear_badblock(struct active_array *a, int slot,
			unsigned long long sector, int length)
{
	struct intel_super *super = a->container->sb;
	int ord;
	int ret;

	ord = imsm_disk_slot_to_ord(a, slot);
	if (ord < 0)
		return 0;

	ret = clear_badblock(super->bbm_log, ord_to_idx(ord), sector, length);
	if (ret)
		super->updates_pending++;

	return ret;
}
/*******************************************************************************
* Function:   imsm_get_badblocks
* Description: This routine get list of bad blocks for an array
*
* Parameters:
*     a		: array
*     slot	: disk number
* Returns:
*     bb	: structure containing bad blocks
*     NULL	: error
******************************************************************************/
static struct md_bb *imsm_get_badblocks(struct active_array *a, int slot)
{
	int inst = a->info.container_member;
	struct intel_super *super = a->container->sb;
	struct imsm_dev *dev = get_imsm_dev(super, inst);
	struct imsm_map *map = get_imsm_map(dev, MAP_0);
	int ord;

	ord = imsm_disk_slot_to_ord(a, slot);
	if (ord < 0)
		return NULL;

	get_volume_badblocks(super->bbm_log, ord_to_idx(ord), pba_of_lba0(map),
			     per_dev_array_size(map), &super->bb);

	return &super->bb;
}
/*******************************************************************************
* Function:   examine_badblocks_imsm
* Description: Prints list of bad blocks on a disk to the standard output
*
* Parameters:
*     st	: metadata handler
*     fd	: open file descriptor for device
*     devname	: device name
* Returns:
*     0 : Success
*     1 : Error
******************************************************************************/
static int examine_badblocks_imsm(struct supertype *st, int fd, char *devname)
{
	struct intel_super *super = st->sb;
	struct bbm_log *log = super->bbm_log;
	struct dl *d = NULL;
	int any = 0;

	for (d = super->disks; d ; d = d->next) {
		if (strcmp(d->devname, devname) == 0)
			break;
	}

	if ((d == NULL) || (d->index < 0)) { /* serial mismatch probably */
		pr_err("%s doesn't appear to be part of a raid array\n",
		       devname);
		return 1;
	}

	if (log != NULL) {
		unsigned int i;
		struct bbm_log_entry *entry = &log->marked_block_entries[0];

		for (i = 0; i < log->entry_count; i++) {
			if (entry[i].disk_ordinal == d->index) {
				unsigned long long sector = __le48_to_cpu(
					&entry[i].defective_block_start);
				int cnt = entry[i].marked_count + 1;

				if (!any) {
					printf("Bad-blocks on %s:\n", devname);
					any = 1;
				}

				printf("%20llu for %d sectors\n", sector, cnt);
			}
		}
	}

	if (!any)
		printf("No bad-blocks list configured on %s\n", devname);

	return 0;
}
/*******************************************************************************
 * Function:	init_migr_record_imsm
 * Description:	Function inits imsm migration record
 * Parameters:
 *	super	: imsm internal array info
 *	dev	: device under migration
 *	info	: general array info to find the smallest device
 * Returns:
 *	none
 ******************************************************************************/
void init_migr_record_imsm(struct supertype *st, struct imsm_dev *dev,
			   struct mdinfo *info)
{
	struct intel_super *super = st->sb;
	struct migr_record *migr_rec = super->migr_rec;
	int new_data_disks;
	unsigned long long dsize, dev_sectors;
	long long unsigned min_dev_sectors = -1LLU;
	struct imsm_map *map_dest = get_imsm_map(dev, MAP_0);
	struct imsm_map *map_src = get_imsm_map(dev, MAP_1);
	unsigned long long num_migr_units;
	unsigned long long array_blocks;
	struct dl *dl_disk = NULL;

	memset(migr_rec, 0, sizeof(struct migr_record));
	migr_rec->family_num = __cpu_to_le32(super->anchor->family_num);

	/* only ascending reshape supported now */
	migr_rec->ascending_migr = __cpu_to_le32(1);

	migr_rec->dest_depth_per_unit = GEN_MIGR_AREA_SIZE /
		max(map_dest->blocks_per_strip, map_src->blocks_per_strip);
	migr_rec->dest_depth_per_unit *=
		max(map_dest->blocks_per_strip, map_src->blocks_per_strip);
	new_data_disks = imsm_num_data_members(map_dest);
	migr_rec->blocks_per_unit =
		__cpu_to_le32(migr_rec->dest_depth_per_unit * new_data_disks);
	migr_rec->dest_depth_per_unit =
		__cpu_to_le32(migr_rec->dest_depth_per_unit);
	array_blocks = info->component_size * new_data_disks;
	num_migr_units =
		array_blocks / __le32_to_cpu(migr_rec->blocks_per_unit);

	if (array_blocks % __le32_to_cpu(migr_rec->blocks_per_unit))
		num_migr_units++;
	set_num_migr_units(migr_rec, num_migr_units);

	migr_rec->post_migr_vol_cap =  dev->size_low;
	migr_rec->post_migr_vol_cap_hi = dev->size_high;

	/* Find the smallest dev */
	for (dl_disk =  super->disks; dl_disk ; dl_disk = dl_disk->next) {
		/* ignore spares in container */
		if (dl_disk->index < 0)
			continue;
		get_dev_size(dl_disk->fd, NULL, &dsize);
		dev_sectors = dsize / 512;
		if (dev_sectors < min_dev_sectors)
			min_dev_sectors = dev_sectors;
	}
	set_migr_chkp_area_pba(migr_rec, min_dev_sectors -
					RAID_DISK_RESERVED_BLOCKS_IMSM_HI);

	write_imsm_migr_rec(st);

	return;
}

/*******************************************************************************
 * Function:	save_backup_imsm
 * Description:	Function saves critical data stripes to Migration Copy Area
 *		and updates the current migration unit status.
 *		Use restore_stripes() to form a destination stripe,
 *		and to write it to the Copy Area.
 * Parameters:
 *	st		: supertype information
 *	dev		: imsm device that backup is saved for
 *	info		: general array info
 *	buf		: input buffer
 *	length		: length of data to backup (blocks_per_unit)
 * Returns:
 *	 0 : success
 *,	-1 : fail
 ******************************************************************************/
int save_backup_imsm(struct supertype *st,
		     struct imsm_dev *dev,
		     struct mdinfo *info,
		     void *buf,
		     int length)
{
	int rv = -1;
	struct intel_super *super = st->sb;
	int i;
	struct imsm_map *map_dest = get_imsm_map(dev, MAP_0);
	int new_disks = map_dest->num_members;
	int dest_layout = 0;
	int dest_chunk, targets[new_disks];
	unsigned long long start, target_offsets[new_disks];
	int data_disks = imsm_num_data_members(map_dest);

	for (i = 0; i < new_disks; i++) {
		struct dl *dl_disk = get_imsm_dl_disk(super, i);
		if (dl_disk && is_fd_valid(dl_disk->fd))
			targets[i] = dl_disk->fd;
		else
			goto abort;
	}

	start = info->reshape_progress * 512;
	for (i = 0; i < new_disks; i++) {
		target_offsets[i] = migr_chkp_area_pba(super->migr_rec) * 512;
		/* move back copy area adderss, it will be moved forward
		 * in restore_stripes() using start input variable
		 */
		target_offsets[i] -= start/data_disks;
	}

	dest_layout = imsm_level_to_layout(map_dest->raid_level);
	dest_chunk = __le16_to_cpu(map_dest->blocks_per_strip) * 512;

	if (restore_stripes(targets, /* list of dest devices */
			    target_offsets, /* migration record offsets */
			    new_disks,
			    dest_chunk,
			    map_dest->raid_level,
			    dest_layout,
			    -1,    /* source backup file descriptor */
			    0,     /* input buf offset
				    * always 0 buf is already offseted */
			    start,
			    length,
			    buf) != 0) {
		pr_err("Error restoring stripes\n");
		goto abort;
	}

	rv = 0;

abort:
	return rv;
}

/*******************************************************************************
 * Function:	save_checkpoint_imsm
 * Description:	Function called for current unit status update
 *		in the migration record. It writes it to disk.
 * Parameters:
 *	super	: imsm internal array info
 *	info	: general array info
 * Returns:
 *	0: success
 *	1: failure
 *	2: failure, means no valid migration record
 *		   / no general migration in progress /
 ******************************************************************************/
int save_checkpoint_imsm(struct supertype *st, struct mdinfo *info, int state)
{
	struct intel_super *super = st->sb;
	unsigned long long blocks_per_unit;
	unsigned long long curr_migr_unit;

	if (load_imsm_migr_rec(super) != 0) {
		dprintf("imsm: ERROR: Cannot read migration record for checkpoint save.\n");
		return 1;
	}

	blocks_per_unit = __le32_to_cpu(super->migr_rec->blocks_per_unit);
	if (blocks_per_unit == 0) {
		dprintf("imsm: no migration in progress.\n");
		return 2;
	}
	curr_migr_unit = info->reshape_progress / blocks_per_unit;
	/* check if array is alligned to copy area
	 * if it is not alligned, add one to current migration unit value
	 * this can happend on array reshape finish only
	 */
	if (info->reshape_progress % blocks_per_unit)
		curr_migr_unit++;

	set_current_migr_unit(super->migr_rec, curr_migr_unit);
	super->migr_rec->rec_status = __cpu_to_le32(state);
	set_migr_dest_1st_member_lba(super->migr_rec,
			super->migr_rec->dest_depth_per_unit * curr_migr_unit);

	if (write_imsm_migr_rec(st) < 0) {
		dprintf("imsm: Cannot write migration record outside backup area\n");
		return 1;
	}

	return 0;
}

/*******************************************************************************
 * Function:	recover_backup_imsm
 * Description:	Function recovers critical data from the Migration Copy Area
 *		while assembling an array.
 * Parameters:
 *	super	: imsm internal array info
 *	info	: general array info
 * Returns:
 *	0 : success (or there is no data to recover)
 *	1 : fail
 ******************************************************************************/
int recover_backup_imsm(struct supertype *st, struct mdinfo *info)
{
	struct intel_super *super = st->sb;
	struct migr_record *migr_rec = super->migr_rec;
	struct imsm_map *map_dest;
	struct intel_dev *id = NULL;
	unsigned long long read_offset;
	unsigned long long write_offset;
	unsigned unit_len;
	int new_disks, err;
	char *buf = NULL;
	int retval = 1;
	unsigned int sector_size = super->sector_size;
	unsigned long long curr_migr_unit = current_migr_unit(migr_rec);
	unsigned long long num_migr_units = get_num_migr_units(migr_rec);
	char buffer[SYSFS_MAX_BUF_SIZE];
	int skipped_disks = 0;
	struct dl *dl_disk;

	err = sysfs_get_str(info, NULL, "array_state", (char *)buffer, sizeof(buffer));
	if (err < 1)
		return 1;

	/* recover data only during assemblation */
	if (strncmp(buffer, "inactive", 8) != 0)
		return 0;
	/* no data to recover */
	if (__le32_to_cpu(migr_rec->rec_status) == UNIT_SRC_NORMAL)
		return 0;
	if (curr_migr_unit >= num_migr_units)
		return 1;

	/* find device during reshape */
	for (id = super->devlist; id; id = id->next)
		if (is_gen_migration(id->dev))
			break;
	if (id == NULL)
		return 1;

	map_dest = get_imsm_map(id->dev, MAP_0);
	new_disks = map_dest->num_members;

	read_offset = migr_chkp_area_pba(migr_rec) * 512;

	write_offset = (migr_dest_1st_member_lba(migr_rec) +
			pba_of_lba0(map_dest)) * 512;

	unit_len = __le32_to_cpu(migr_rec->dest_depth_per_unit) * 512;
	if (posix_memalign((void **)&buf, sector_size, unit_len) != 0)
		goto abort;

	for (dl_disk = super->disks; dl_disk; dl_disk = dl_disk->next) {
		if (dl_disk->index < 0)
			continue;

		if (!is_fd_valid(dl_disk->fd)) {
			skipped_disks++;
			continue;
		}
		if (lseek64(dl_disk->fd, read_offset, SEEK_SET) < 0) {
			pr_err("Cannot seek to block: %s\n",
			       strerror(errno));
			skipped_disks++;
			continue;
		}
		if (read(dl_disk->fd, buf, unit_len) != (ssize_t)unit_len) {
			pr_err("Cannot read copy area block: %s\n",
			       strerror(errno));
			skipped_disks++;
			continue;
		}
		if (lseek64(dl_disk->fd, write_offset, SEEK_SET) < 0) {
			pr_err("Cannot seek to block: %s\n",
			       strerror(errno));
			skipped_disks++;
			continue;
		}
		if (write(dl_disk->fd, buf, unit_len) != (ssize_t)unit_len) {
			pr_err("Cannot restore block: %s\n",
			       strerror(errno));
			skipped_disks++;
			continue;
		}
	}

	if (skipped_disks > imsm_get_allowed_degradation(info->new_level,
							 new_disks,
							 super,
							 id->dev)) {
		pr_err("Cannot restore data from backup. Too many failed disks\n");
		goto abort;
	}

	if (save_checkpoint_imsm(st, info, UNIT_SRC_NORMAL)) {
		/* ignore error == 2, this can mean end of reshape here
		 */
		dprintf("imsm: Cannot write checkpoint to migration record (UNIT_SRC_NORMAL) during restart\n");
	} else
		retval = 0;

abort:
	free(buf);
	return retval;
}

/**
 * test_and_add_drive_controller_policy_imsm() - add disk controller to policies list.
 * @type: Policy type to search on list.
 * @pols: List of currently recorded policies.
 * @disk_fd: File descriptor of the device to check.
 * @hba: The hba disk is attached, could be NULL if verification is disabled.
 * @verbose: verbose flag.
 *
 * IMSM cares about drive physical placement. If @hba is not set, it adds unknown policy.
 * If there is no controller policy on pols we are free to add first one. If there is a policy then,
 * new must be the same - no controller mixing allowed.
 */
static mdadm_status_t
test_and_add_drive_controller_policy_imsm(const char * const type, dev_policy_t **pols, int disk_fd,
					  struct sys_dev *hba, const int verbose)
{
	const char *controller_policy = get_sys_dev_type(SYS_DEV_UNKNOWN);
	struct dev_policy *pol = pol_find(*pols, (char *)type);
	char devname[MAX_RAID_SERIAL_LEN];

	if (hba)
		controller_policy = get_sys_dev_type(hba->type);

	if (!pol) {
		pol_add(pols, (char *)type, (char *)controller_policy, "imsm");
		return MDADM_STATUS_SUCCESS;
	}

	if (strcmp(pol->value, controller_policy) == 0)
		return MDADM_STATUS_SUCCESS;

	fd2devname(disk_fd, devname);
	pr_vrb("Intel(R) raid controller \"%s\" found for %s, but \"%s\" was detected earlier\n",
	       controller_policy, devname, pol->value);
	pr_vrb("Disks under different controllers cannot be used, aborting\n");

	return MDADM_STATUS_ERROR;
}

/**
 * test_and_add_drive_encryption_policy_imsm() - add disk encryption to policies list.
 * @type: policy type to search in the list.
 * @pols: list of currently recorded policies.
 * @disk_fd: file descriptor of the device to check.
 * @hba: The hba to which the drive is attached, could be NULL if verification is disabled.
 * @verbose: verbose flag.
 *
 * IMSM cares about drive encryption state. It is not allowed to mix disks with different
 * encryption state within one md device.
 * If there is no encryption policy on pols we are free to add first one.
 * If there is a policy then, new must be the same.
 */
static mdadm_status_t
test_and_add_drive_encryption_policy_imsm(const char * const type, dev_policy_t **pols, int disk_fd,
					  struct sys_dev *hba, const int verbose)
{
	struct dev_policy *expected_policy = pol_find(*pols, (char *)type);
	struct encryption_information information = {0};
	char *encryption_state = "Unknown";
	int status = MDADM_STATUS_SUCCESS;
	bool encryption_checked = true;
	char devname[PATH_MAX];

	if (!hba)
		goto check_policy;

	switch (hba->type) {
	case SYS_DEV_NVME:
	case SYS_DEV_VMD:
		status = get_nvme_opal_encryption_information(disk_fd, &information, verbose);
		break;
	case SYS_DEV_SATA:
	case SYS_DEV_SATA_VMD:
		status = get_ata_encryption_information(disk_fd, &information, verbose);
		break;
	default:
		encryption_checked = false;
	}

	if (status) {
		fd2devname(disk_fd, devname);
		pr_vrb("Failed to read encryption information of device %s\n", devname);
		return MDADM_STATUS_ERROR;
	}

	if (encryption_checked) {
		if (information.status == ENC_STATUS_LOCKED) {
			fd2devname(disk_fd, devname);
			pr_vrb("Device %s is in Locked state, cannot use. Aborting.\n", devname);
			return MDADM_STATUS_ERROR;
		}
		encryption_state = (char *)get_encryption_status_string(information.status);
	}

check_policy:
	if (expected_policy) {
		if (strcmp(expected_policy->value, encryption_state) == 0)
			return MDADM_STATUS_SUCCESS;

		fd2devname(disk_fd, devname);
		pr_vrb("Encryption status \"%s\" detected for disk %s, but \"%s\" status was detected earlier.\n",
		       encryption_state, devname, expected_policy->value);
		pr_vrb("Disks with different encryption status cannot be used.\n");
		return MDADM_STATUS_ERROR;
	}

	pol_add(pols, (char *)type, encryption_state, "imsm");

	return MDADM_STATUS_SUCCESS;
}

struct imsm_drive_policy {
	char *type;
	mdadm_status_t (*test_and_add_drive_policy)(const char * const type,
						    struct dev_policy **pols, int disk_fd,
						    struct sys_dev *hba, const int verbose);
};

struct imsm_drive_policy imsm_policies[] = {
	{"controller", test_and_add_drive_controller_policy_imsm},
	{"encryption", test_and_add_drive_encryption_policy_imsm}
};

mdadm_status_t test_and_add_drive_policies_imsm(struct dev_policy **pols, int disk_fd,
						const int verbose)
{
	struct imsm_drive_policy *imsm_pol;
	struct sys_dev *hba = NULL;
	char path[PATH_MAX];
	mdadm_status_t ret;
	unsigned int i;

	/* If imsm platform verification is disabled, do not search for hba. */
	if (check_no_platform() != 1) {
		if (!diskfd_to_devpath(disk_fd, 1, path)) {
			pr_vrb("IMSM: Failed to retrieve device path by file descriptor.\n");
			return MDADM_STATUS_ERROR;
		}

		hba = find_disk_attached_hba(disk_fd, path);
		if (!hba) {
			pr_vrb("IMSM: Failed to find hba for %s\n", path);
			return MDADM_STATUS_ERROR;
		}
	}

	for (i = 0; i < ARRAY_SIZE(imsm_policies); i++) {
		imsm_pol = &imsm_policies[i];

		ret = imsm_pol->test_and_add_drive_policy(imsm_pol->type, pols, disk_fd, hba,
							  verbose);
		if (ret != MDADM_STATUS_SUCCESS)
			/* Inherit error code */
			return ret;
	}

	return MDADM_STATUS_SUCCESS;
}

/**
 * get_spare_criteria_imsm() - set spare criteria.
 * @st: supertype.
 * @mddev_path: path to md device devnode, it must be container.
 * @c: spare_criteria struct to fill, not NULL.
 *
 * If superblock is not loaded, use mddev_path to load_container. It must be given in this case.
 * Filles size and sector size accordingly to superblock.
 */
mdadm_status_t get_spare_criteria_imsm(struct supertype *st, char *mddev_path,
				       struct spare_criteria *c)
{
	mdadm_status_t ret = MDADM_STATUS_ERROR;
	bool free_superblock = false;
	unsigned long long size = 0;
	struct intel_super *super;
	struct extent *e;
	struct dl *dl;
	int i;

	/* If no superblock and no mddev_path, we cannot load superblock. */
	assert(st->sb || mddev_path);

	if (mddev_path) {
		int fd = open(mddev_path, O_RDONLY);
		mdadm_status_t rv;

		if (!is_fd_valid(fd))
			return MDADM_STATUS_ERROR;

		if (!st->sb) {
			if (load_container_imsm(st, fd, st->devnm)) {
				close(fd);
				return MDADM_STATUS_ERROR;
			}
			free_superblock = true;
		}

		rv = mddev_test_and_add_drive_policies(st, &c->pols, fd, 0);
		close(fd);

		if (rv != MDADM_STATUS_SUCCESS)
			goto out;
	}

	super = st->sb;

	/* find first active disk in array */
	dl = super->disks;
	while (dl && (is_failed(&dl->disk) || dl->index == -1))
		dl = dl->next;

	if (!dl)
		goto out;

	/* find last lba used by subarrays */
	e = get_extents(super, dl, 0);
	if (!e)
		goto out;

	for (i = 0; e[i].size; i++)
		continue;
	if (i > 0)
		size = e[i - 1].start + e[i - 1].size;
	free(e);

	/* add the amount of space needed for metadata */
	size += imsm_min_reserved_sectors(super);

	c->min_size = size * 512;
	c->sector_size = super->sector_size;
	c->criteria_set = true;
	ret = MDADM_STATUS_SUCCESS;

out:
	if (free_superblock)
		free_super_imsm(st);

	if (ret != MDADM_STATUS_SUCCESS)
		c->criteria_set = false;

	return ret;
}

static char *imsm_find_array_devnm_by_subdev(int subdev, char *container)
{
	static char devnm[32];
	char subdev_name[20];
	struct mdstat_ent *mdstat;

	sprintf(subdev_name, "%d", subdev);
	mdstat = mdstat_by_subdev(subdev_name, container);
	if (!mdstat)
		return NULL;

	strcpy(devnm, mdstat->devnm);
	free_mdstat(mdstat);
	return devnm;
}

static int imsm_reshape_is_allowed_on_container(struct supertype *st,
						struct geo_params *geo,
						int *old_raid_disks,
						int direction)
{
	/* currently we only support increasing the number of devices
	 * for a container.  This increases the number of device for each
	 * member array.  They must all be RAID0 or RAID5.
	 */
	int ret_val = 0;
	struct mdinfo *info, *member;
	int devices_that_can_grow = 0;

	dprintf("imsm: imsm_reshape_is_allowed_on_container(ENTER): st->devnm = (%s)\n", st->devnm);

	if (geo->size > 0 ||
	    geo->level != UnSet ||
	    geo->layout != UnSet ||
	    geo->chunksize != 0 ||
	    geo->raid_disks == UnSet) {
		dprintf("imsm: Container operation is allowed for raid disks number change only.\n");
		return ret_val;
	}

	if (direction == ROLLBACK_METADATA_CHANGES) {
		dprintf("imsm: Metadata changes rollback is not supported for container operation.\n");
		return ret_val;
	}

	info = container_content_imsm(st, NULL);
	for (member = info; member; member = member->next) {
		char *result;

		dprintf("imsm: checking device_num: %i\n",
			member->container_member);

		if (geo->raid_disks <= member->array.raid_disks) {
			/* we work on container for Online Capacity Expansion
			 * only so raid_disks has to grow
			 */
			dprintf("imsm: for container operation raid disks increase is required\n");
			break;
		}

		if (info->array.level != 0 && info->array.level != 5) {
			/* we cannot use this container with other raid level
			 */
			dprintf("imsm: for container operation wrong raid level (%i) detected\n",
				info->array.level);
			break;
		} else {
			/* check for platform support
			 * for this raid level configuration
			 */
			struct intel_super *super = st->sb;
			if (!is_raid_level_supported(super->orom,
						     member->array.level,
						     geo->raid_disks)) {
				dprintf("platform does not support raid%d with %d disk%s\n",
					 info->array.level,
					 geo->raid_disks,
					 geo->raid_disks > 1 ? "s" : "");
				break;
			}
			/* check if component size is aligned to chunk size
			 */
			if (info->component_size %
			    (info->array.chunk_size/512)) {
				dprintf("Component size is not aligned to chunk size\n");
				break;
			}
		}

		if (*old_raid_disks &&
		    info->array.raid_disks != *old_raid_disks)
			break;
		*old_raid_disks = info->array.raid_disks;

		/* All raid5 and raid0 volumes in container
		 * have to be ready for Online Capacity Expansion
		 * so they need to be assembled.  We have already
		 * checked that no recovery etc is happening.
		 */
		result = imsm_find_array_devnm_by_subdev(member->container_member,
							 st->container_devnm);
		if (result == NULL) {
			dprintf("imsm: cannot find array\n");
			break;
		}
		devices_that_can_grow++;
	}
	sysfs_free(info);
	if (!member && devices_that_can_grow)
		ret_val = 1;

	if (ret_val)
		dprintf("Container operation allowed\n");
	else
		dprintf("Error: %i\n", ret_val);

	return ret_val;
}

/* Function: get_spares_for_grow
 * Description: Allocates memory and creates list of spare devices
 *		avaliable in container. Checks if spare drive size is acceptable.
 * Parameters: Pointer to the supertype structure
 * Returns: Pointer to the list of spare devices (mdinfo structure) on success,
 *		NULL if fail
 */
static struct mdinfo *get_spares_for_grow(struct supertype *st)
{
	struct spare_criteria sc = {0};
	struct mdinfo *spares;

	get_spare_criteria_imsm(st, NULL, &sc);
	spares = container_choose_spares(st, &sc, NULL, NULL, NULL, 0);

	dev_policy_free(sc.pols);

	return spares;
}

/******************************************************************************
 * function: imsm_create_metadata_update_for_reshape
 * Function creates update for whole IMSM container.
 *
 ******************************************************************************/
static int imsm_create_metadata_update_for_reshape(
	struct supertype *st,
	struct geo_params *geo,
	int old_raid_disks,
	struct imsm_update_reshape **updatep)
{
	struct intel_super *super = st->sb;
	struct imsm_super *mpb = super->anchor;
	int update_memory_size;
	struct imsm_update_reshape *u;
	struct mdinfo *spares;
	int i;
	int delta_disks;
	struct mdinfo *dev;

	dprintf("(enter) raid_disks = %i\n", geo->raid_disks);

	delta_disks = geo->raid_disks - old_raid_disks;

	/* size of all update data without anchor */
	update_memory_size = sizeof(struct imsm_update_reshape);

	/* now add space for spare disks that we need to add. */
	update_memory_size += sizeof(u->new_disks[0]) * (delta_disks - 1);

	u = xcalloc(1, update_memory_size);
	u->type = update_reshape_container_disks;
	u->old_raid_disks = old_raid_disks;
	u->new_raid_disks = geo->raid_disks;

	/* now get spare disks list
	 */
	spares = get_spares_for_grow(st);

	if (spares == NULL || delta_disks > spares->array.spare_disks) {
		pr_err("imsm: ERROR: Cannot get spare devices for %s.\n", geo->dev_name);
		i = -1;
		goto abort;
	}

	/* we have got spares
	 * update disk list in imsm_disk list table in anchor
	 */
	dprintf("imsm: %i spares are available.\n\n",
		spares->array.spare_disks);

	dev = spares->devs;
	for (i = 0; i < delta_disks; i++) {
		struct dl *dl;

		if (dev == NULL)
			break;
		u->new_disks[i] = makedev(dev->disk.major,
					  dev->disk.minor);
		dl = get_disk_super(super, dev->disk.major, dev->disk.minor);
		dl->index = mpb->num_disks;
		mpb->num_disks++;
		dev = dev->next;
	}

abort:
	/* free spares
	 */
	sysfs_free(spares);

	dprintf("imsm: reshape update preparation :");
	if (i == delta_disks) {
		dprintf_cont(" OK\n");
		*updatep = u;
		return update_memory_size;
	}
	free(u);
	dprintf_cont(" Error\n");

	return 0;
}

/******************************************************************************
 * function: imsm_create_metadata_update_for_size_change()
 *           Creates update for IMSM array for array size change.
 *
 ******************************************************************************/
static int imsm_create_metadata_update_for_size_change(
				struct supertype *st,
				struct geo_params *geo,
				struct imsm_update_size_change **updatep)
{
	struct intel_super *super = st->sb;
	int update_memory_size;
	struct imsm_update_size_change *u;

	dprintf("(enter) New size = %llu\n", geo->size);

	/* size of all update data without anchor */
	update_memory_size = sizeof(struct imsm_update_size_change);

	u = xcalloc(1, update_memory_size);
	u->type = update_size_change;
	u->subdev = super->current_vol;
	u->new_size = geo->size;

	dprintf("imsm: reshape update preparation : OK\n");
	*updatep = u;

	return update_memory_size;
}

/******************************************************************************
 * function: imsm_create_metadata_update_for_migration()
 *           Creates update for IMSM array.
 *
 ******************************************************************************/
static int imsm_create_metadata_update_for_migration(
					struct supertype *st,
					struct geo_params *geo,
					struct imsm_update_reshape_migration **updatep)
{
	struct intel_super *super = st->sb;
	int update_memory_size;
	int current_chunk_size;
	struct imsm_update_reshape_migration *u;
	struct imsm_dev *dev = get_imsm_dev(super, super->current_vol);
	struct imsm_map *map = get_imsm_map(dev, MAP_0);
	int previous_level = -1;

	dprintf("(enter) New Level = %i\n", geo->level);

	/* size of all update data without anchor */
	update_memory_size = sizeof(struct imsm_update_reshape_migration);

	u = xcalloc(1, update_memory_size);
	u->type = update_reshape_migration;
	u->subdev = super->current_vol;
	u->new_level = geo->level;
	u->new_layout = geo->layout;
	u->new_raid_disks = u->old_raid_disks = geo->raid_disks;
	u->new_disks[0] = -1;
	u->new_chunksize = -1;

	current_chunk_size = __le16_to_cpu(map->blocks_per_strip) / 2;

	if (geo->chunksize != current_chunk_size) {
		u->new_chunksize = geo->chunksize / 1024;
		dprintf("imsm: chunk size change from %i to %i\n",
			current_chunk_size, u->new_chunksize);
	}
	previous_level = map->raid_level;

	if (geo->level == 5 && previous_level == 0) {
		struct mdinfo *spares = NULL;

		u->new_raid_disks++;
		spares = get_spares_for_grow(st);
		if (spares == NULL || spares->array.spare_disks < 1) {
			free(u);
			sysfs_free(spares);
			update_memory_size = 0;
			pr_err("cannot get spare device for requested migration\n");
			return 0;
		}
		sysfs_free(spares);
	}
	dprintf("imsm: reshape update preparation : OK\n");
	*updatep = u;

	return update_memory_size;
}

static void imsm_update_metadata_locally(struct supertype *st,
					 void *buf, int len)
{
	struct metadata_update mu;

	mu.buf = buf;
	mu.len = len;
	mu.space = NULL;
	mu.space_list = NULL;
	mu.next = NULL;
	if (imsm_prepare_update(st, &mu))
		imsm_process_update(st, &mu);

	while (mu.space_list) {
		void **space = mu.space_list;
		mu.space_list = *space;
		free(space);
	}
}

/**
 * imsm_analyze_expand() - check expand properties and calculate new size.
 * @st: imsm supertype.
 * @geo: new geometry params.
 * @array: array info.
 * @direction: reshape direction.
 *
 * Obtain free space after the &array and verify if expand to requested size is
 * possible. If geo->size is set to %MAX_SIZE, assume that max free size is
 * requested.
 *
 * Return:
 * On success %IMSM_STATUS_OK is returned, geo->size and geo->raid_disks are
 * updated.
 * On error, %IMSM_STATUS_ERROR is returned.
 */
static imsm_status_t imsm_analyze_expand(struct supertype *st,
					 struct geo_params *geo,
					 struct mdinfo *array,
					 int direction)
{
	struct intel_super *super = st->sb;
	struct imsm_dev *dev = get_imsm_dev(super, super->current_vol);
	struct imsm_map *map = get_imsm_map(dev, MAP_0);
	int data_disks = imsm_num_data_members(map);

	unsigned long long current_size;
	unsigned long long free_size;
	unsigned long long new_size;
	unsigned long long max_size;

	const int chunk_kib = geo->chunksize / 1024;
	imsm_status_t rv;

	if (direction == ROLLBACK_METADATA_CHANGES) {
		/**
		 * Accept size for rollback only.
		 */
		new_size = geo->size * 2;
		goto success;
	}

	if (data_disks == 0) {
		pr_err("imsm: Cannot retrieve data disks.\n");
		return IMSM_STATUS_ERROR;
	}
	current_size = array->custom_array_size / data_disks;

	rv = imsm_get_free_size(super, dev->vol.map->num_members, 0, chunk_kib, &free_size, true);
	if (rv != IMSM_STATUS_OK) {
		pr_err("imsm: Cannot find free space for expand.\n");
		return IMSM_STATUS_ERROR;
	}
	max_size = round_member_size_to_mb(free_size + current_size);

	if (geo->size == MAX_SIZE)
		new_size = max_size;
	else
		new_size = round_member_size_to_mb(geo->size * 2);

	if (new_size == 0) {
		pr_err("imsm: Rounded requested size is 0.\n");
		return IMSM_STATUS_ERROR;
	}

	if (new_size > max_size) {
		pr_err("imsm: Rounded requested size (%llu) is larger than free space available (%llu).\n",
		       new_size, max_size);
		return IMSM_STATUS_ERROR;
	}

	if (new_size == current_size) {
		pr_err("imsm: Rounded requested size (%llu) is same as current size (%llu).\n",
		       new_size, current_size);
		return IMSM_STATUS_ERROR;
	}

	if (new_size < current_size) {
		pr_err("imsm: Size reduction is not supported, rounded requested size (%llu) is smaller than current (%llu).\n",
		       new_size, current_size);
		return IMSM_STATUS_ERROR;
	}

success:
	dprintf("imsm: New size per member is %llu.\n", new_size);
	geo->size = data_disks * new_size;
	geo->raid_disks = dev->vol.map->num_members;
	return IMSM_STATUS_OK;
}

/***************************************************************************
* Function:	imsm_analyze_change
* Description:	Function analyze change for single volume
*		and validate if transition is supported
* Parameters:	Geometry parameters, supertype structure,
*		metadata change direction (apply/rollback)
* Returns:	Operation type code on success, -1 if fail
****************************************************************************/
enum imsm_reshape_type imsm_analyze_change(struct supertype *st,
					   struct geo_params *geo,
					   int direction, struct context *c)
{
	struct mdinfo info;
	int change = -1;
	int check_devs = 0;
	int chunk;
	/* imsm compatible layout value for array geometry verification */
	int imsm_layout = -1;
	int raid_disks = geo->raid_disks;
	imsm_status_t rv;

	getinfo_super_imsm_volume(st, &info, NULL);
	if (geo->level != info.array.level && geo->level >= IMSM_T_RAID0 &&
	    geo->level != UnSet) {
		switch (info.array.level) {
		case IMSM_T_RAID0:
			if (geo->level == IMSM_T_RAID5) {
				change = CH_MIGRATION;
				if (geo->layout != ALGORITHM_LEFT_ASYMMETRIC) {
					pr_err("Error. Requested Layout not supported (left-asymmetric layout is supported only)!\n");
					change = -1;
					goto analyse_change_exit;
				}
				imsm_layout =  geo->layout;
				check_devs = 1;
				raid_disks += 1; /* parity disk added */
			} else if (geo->level == IMSM_T_RAID10) {
				if (geo->level == IMSM_T_RAID10 && geo->raid_disks > 2 &&
				    !c->force) {
					pr_err("Warning! VROC UEFI driver does not support RAID10 in requested layout.\n");
					pr_err("Array won't be suitable as boot device.\n");
					pr_err("Note: You can omit this check with \"--force\"\n");
					if (ask("Do you want to continue") < 1)
						return CH_ABORT;
				}
				change = CH_TAKEOVER;
				check_devs = 1;
				raid_disks *= 2; /* mirrors added */
				imsm_layout = 0x102; /* imsm supported layout */
			}
			break;
		case IMSM_T_RAID1:
		case IMSM_T_RAID10:
			if (geo->level == 0) {
				change = CH_TAKEOVER;
				check_devs = 1;
				raid_disks /= 2;
				imsm_layout = 0; /* imsm raid0 layout */
			}
			break;
		}
		if (change == -1) {
			pr_err("Error. Level Migration from %d to %d not supported!\n",
			       info.array.level, geo->level);
			goto analyse_change_exit;
		}
	} else
		geo->level = info.array.level;

	if (geo->layout != info.array.layout &&
	    (geo->layout != UnSet && geo->layout != -1)) {
		change = CH_MIGRATION;
		if (info.array.layout == 0 && info.array.level == IMSM_T_RAID5 &&
		    geo->layout == 5) {
			/* reshape 5 -> 4 */
		} else if (info.array.layout == 5 && info.array.level == IMSM_T_RAID5 &&
			   geo->layout == 0) {
			/* reshape 4 -> 5 */
			geo->layout = 0;
			geo->level = 5;
		} else {
			pr_err("Error. Layout Migration from %d to %d not supported!\n",
			       info.array.layout, geo->layout);
			change = -1;
			goto analyse_change_exit;
		}
	} else {
		geo->layout = info.array.layout;
		if (imsm_layout == -1)
			imsm_layout = info.array.layout;
	}

	if (geo->chunksize > 0 && geo->chunksize != UnSet &&
	    geo->chunksize != info.array.chunk_size) {
		if (info.array.level == IMSM_T_RAID10) {
			pr_err("Error. Chunk size change for RAID 10 is not supported.\n");
			change = -1;
			goto analyse_change_exit;
		} else if (info.component_size % (geo->chunksize/512)) {
			pr_err("New chunk size (%dK) does not evenly divide device size (%lluk). Aborting...\n",
			       geo->chunksize/1024, info.component_size/2);
			change = -1;
			goto analyse_change_exit;
		}
		change = CH_MIGRATION;
	} else {
		geo->chunksize = info.array.chunk_size;
	}

	if (geo->size > 0) {
		if (change != -1) {
			pr_err("Error. Size change should be the only one at a time.\n");
			change = -1;
			goto analyse_change_exit;
		}

		rv = imsm_analyze_expand(st, geo, &info, direction);
		if (rv != IMSM_STATUS_OK)
			goto analyse_change_exit;
		raid_disks = geo->raid_disks;
		change = CH_ARRAY_SIZE;
	}

	chunk = geo->chunksize / 1024;

	if (!validate_geometry_imsm(st,
				    geo->level,
				    imsm_layout,
				    raid_disks,
				    &chunk,
				    geo->size, INVALID_SECTORS,
				    0, 0, info.consistency_policy, 1))
		change = -1;

	if (check_devs) {
		struct intel_super *super = st->sb;
		struct imsm_super *mpb = super->anchor;

		if (mpb->num_raid_devs > 1) {
			pr_err("Error. Cannot perform operation on %s- for this operation "
			       "it MUST be single array in container\n", geo->dev_name);
			change = -1;
		}
	}

analyse_change_exit:
	if (direction == ROLLBACK_METADATA_CHANGES &&
	    (change == CH_MIGRATION || change == CH_TAKEOVER)) {
		dprintf("imsm: Metadata changes rollback is not supported for migration and takeover operations.\n");
		change = -1;
	}
	return change;
}

int imsm_takeover(struct supertype *st, struct geo_params *geo)
{
	struct intel_super *super = st->sb;
	struct imsm_update_takeover *u;

	u = xmalloc(sizeof(struct imsm_update_takeover));

	u->type = update_takeover;
	u->subarray = super->current_vol;

	/* 10->0 transition */
	if (geo->level == 0)
		u->direction = R10_TO_R0;

	/* 0->10 transition */
	if (geo->level == 10)
		u->direction = R0_TO_R10;

	/* update metadata locally */
	imsm_update_metadata_locally(st, u,
					sizeof(struct imsm_update_takeover));
	/* and possibly remotely */
	if (st->update_tail)
		append_metadata_update(st, u,
					sizeof(struct imsm_update_takeover));
	else
		free(u);

	return 0;
}

/* Flush size update if size calculated by num_data_stripes is higher than
 * imsm_dev_size to eliminate differences during reshape.
 * Mdmon will recalculate them correctly.
 * If subarray index is not set then check whole container.
 * Returns:
 *	0 - no error occurred
 *	1 - error detected
 */
static int imsm_fix_size_mismatch(struct supertype *st, int subarray_index)
{
	struct intel_super *super = st->sb;
	int tmp = super->current_vol;
	int ret_val = 1;
	int i;

	for (i = 0; i < super->anchor->num_raid_devs; i++) {
		if (subarray_index >= 0 && i != subarray_index)
			continue;
		super->current_vol = i;
		struct imsm_dev *dev = get_imsm_dev(super, super->current_vol);
		struct imsm_map *map = get_imsm_map(dev, MAP_0);
		unsigned int disc_count = imsm_num_data_members(map);
		struct geo_params geo;
		struct imsm_update_size_change *update;
		unsigned long long calc_size = per_dev_array_size(map) * disc_count;
		unsigned long long d_size = imsm_dev_size(dev);
		int u_size;

		if (calc_size == d_size)
			continue;

		/* There is a difference, confirm that imsm_dev_size is
		 * smaller and push update.
		 */
		if (d_size > calc_size) {
			pr_err("imsm: dev size of subarray %d is incorrect\n",
				i);
			goto exit;
		}
		memset(&geo, 0, sizeof(struct geo_params));
		geo.size = d_size;
		u_size = imsm_create_metadata_update_for_size_change(st, &geo,
								     &update);
		imsm_update_metadata_locally(st, update, u_size);
		if (st->update_tail) {
			append_metadata_update(st, update, u_size);
			flush_metadata_updates(st);
			st->update_tail = &st->updates;
		} else {
			imsm_sync_metadata(st);
			free(update);
		}
	}
	ret_val = 0;
exit:
	super->current_vol = tmp;
	return ret_val;
}

/**
 * shape_to_geo() - fill geo_params from shape.
 *
 * @shape: array details.
 * @geo: new geometry params.
 * Returns: 0 on success, 1 otherwise.
 */
static void shape_to_geo(struct shape *shape, struct geo_params *geo)
{
	assert(shape);
	assert(geo);

	geo->dev_name = shape->dev;
	geo->size = shape->size;
	geo->level = shape->level;
	geo->layout = shape->layout;
	geo->chunksize = shape->chunk;
	geo->raid_disks = shape->raiddisks;
}

static int imsm_reshape_super(struct supertype *st, struct shape *shape, struct context *c)
{
	int ret_val = 1;
	struct geo_params geo = {0};

	dprintf("(enter)\n");

	shape_to_geo(shape, &geo);
	strcpy(geo.devnm, st->devnm);
	if (shape->delta_disks != UnSet)
		geo.raid_disks += shape->delta_disks;

	dprintf("for level      : %i\n", geo.level);
	dprintf("for raid_disks : %i\n", geo.raid_disks);

	if (strcmp(st->container_devnm, st->devnm) == 0) {
		/* On container level we can only increase number of devices. */
		dprintf("imsm: info: Container operation\n");
		int old_raid_disks = 0;

		if (imsm_reshape_is_allowed_on_container(
			    st, &geo, &old_raid_disks, shape->direction)) {
			struct imsm_update_reshape *u = NULL;
			int len;

			if (imsm_fix_size_mismatch(st, -1)) {
				dprintf("imsm: Cannot fix size mismatch\n");
				goto exit_imsm_reshape_super;
			}

			len = imsm_create_metadata_update_for_reshape(
				st, &geo, old_raid_disks, &u);

			if (len <= 0) {
				dprintf("imsm: Cannot prepare update\n");
				goto exit_imsm_reshape_super;
			}

			ret_val = 0;
			/* update metadata locally */
			imsm_update_metadata_locally(st, u, len);
			/* and possibly remotely */
			if (st->update_tail)
				append_metadata_update(st, u, len);
			else
				free(u);

		} else {
			pr_err("(imsm) Operation is not allowed on this container\n");
		}
	} else {
		/* On volume level we support following operations
		 * - takeover: raid10 -> raid0; raid0 -> raid10
		 * - chunk size migration
		 * - migration: raid5 -> raid0; raid0 -> raid5
		 */
		struct intel_super *super = st->sb;
		struct intel_dev *dev = super->devlist;
		int change;
		dprintf("imsm: info: Volume operation\n");
		/* find requested device */
		while (dev) {
			char *devnm =
				imsm_find_array_devnm_by_subdev(
					dev->index, st->container_devnm);
			if (devnm && strcmp(devnm, geo.devnm) == 0)
				break;
			dev = dev->next;
		}
		if (dev == NULL) {
			pr_err("Cannot find %s (%s) subarray\n",
				geo.dev_name, geo.devnm);
			goto exit_imsm_reshape_super;
		}
		super->current_vol = dev->index;
		change = imsm_analyze_change(st, &geo, shape->direction, c);
		switch (change) {
		case CH_TAKEOVER:
			ret_val = imsm_takeover(st, &geo);
			break;
		case CH_MIGRATION: {
			struct imsm_update_reshape_migration *u = NULL;
			int len =
				imsm_create_metadata_update_for_migration(
					st, &geo, &u);
			if (len < 1) {
				dprintf("imsm: Cannot prepare update\n");
				break;
			}
			ret_val = 0;
			/* update metadata locally */
			imsm_update_metadata_locally(st, u, len);
			/* and possibly remotely */
			if (st->update_tail)
				append_metadata_update(st, u, len);
			else
				free(u);
		}
		break;
		case CH_ARRAY_SIZE: {
			struct imsm_update_size_change *u = NULL;
			int len =
				imsm_create_metadata_update_for_size_change(
					st, &geo, &u);
			if (len < 1) {
				dprintf("imsm: Cannot prepare update\n");
				break;
			}
			ret_val = 0;
			/* update metadata locally */
			imsm_update_metadata_locally(st, u, len);
			/* and possibly remotely */
			if (st->update_tail)
				append_metadata_update(st, u, len);
			else
				free(u);
		}
		break;
		case CH_ABORT:
		default:
			ret_val = 1;
		}
	}

exit_imsm_reshape_super:
	dprintf("imsm: reshape_super Exit code = %i\n", ret_val);
	return ret_val;
}

#define COMPLETED_OK		0
#define COMPLETED_NONE		1
#define COMPLETED_DELAYED	2

static int read_completed(int fd, unsigned long long *val)
{
	int ret;
	char buf[SYSFS_MAX_BUF_SIZE];

	ret = sysfs_fd_get_str(fd, buf, sizeof(buf));
	if (ret < 0)
		return ret;

	ret = COMPLETED_OK;
	if (str_is_none(buf) == true) {
		ret = COMPLETED_NONE;
	} else if (strncmp(buf, "delayed", 7) == 0) {
		ret = COMPLETED_DELAYED;
	} else {
		char *ep;
		*val = strtoull(buf, &ep, 0);
		if (ep == buf || (*ep != 0 && *ep != '\n' && *ep != ' '))
			ret = -1;
	}
	return ret;
}

/*******************************************************************************
 * Function:	wait_for_reshape_imsm
 * Description:	Function writes new sync_max value and waits until
 *		reshape process reach new position
 * Parameters:
 *	sra		: general array info
 *	ndata		: number of disks in new array's layout
 * Returns:
 *	 0 : success,
 *	 1 : there is no reshape in progress,
 *	-1 : fail
 ******************************************************************************/
int wait_for_reshape_imsm(struct mdinfo *sra, int ndata)
{
	int fd = sysfs_get_fd(sra, NULL, "sync_completed");
	int retry = 3;
	unsigned long long completed;
	/* to_complete : new sync_max position */
	unsigned long long to_complete = sra->reshape_progress;
	unsigned long long position_to_set = to_complete / ndata;

	if (!is_fd_valid(fd)) {
		dprintf("cannot open reshape_position\n");
		return 1;
	}

	do {
		if (sysfs_fd_get_ll(fd, &completed) < 0) {
			if (!retry) {
				dprintf("cannot read reshape_position (no reshape in progres)\n");
				close(fd);
				return 1;
			}
			sleep_for(0, MSEC_TO_NSEC(30), true);
		} else
			break;
	} while (retry--);

	if (completed > position_to_set) {
		dprintf("wrong next position to set %llu (%llu)\n",
			to_complete, position_to_set);
		close(fd);
		return -1;
	}
	dprintf("Position set: %llu\n", position_to_set);
	if (sysfs_set_num(sra, NULL, "sync_max",
			  position_to_set) != 0) {
		dprintf("cannot set reshape position to %llu\n",
			position_to_set);
		close(fd);
		return -1;
	}

	do {
		int rc;
		char action[SYSFS_MAX_BUF_SIZE];
		int timeout = 3000;

		sysfs_wait(fd, &timeout);
		if (sysfs_get_str(sra, NULL, "sync_action",
				  action, sizeof(action)) > 0 &&
				strncmp(action, "reshape", 7) != 0) {
			if (strncmp(action, "idle", 4) == 0)
				break;
			close(fd);
			return -1;
		}

		rc = read_completed(fd, &completed);
		if (rc < 0) {
			dprintf("cannot read reshape_position (in loop)\n");
			close(fd);
			return 1;
		} else if (rc == COMPLETED_NONE)
			break;
	} while (completed < position_to_set);

	close(fd);
	return 0;
}

/*******************************************************************************
 * Function:	check_degradation_change
 * Description:	Check that array hasn't become failed.
 * Parameters:
 *	info	: for sysfs access
 *	sources	: source disks descriptors
 *	degraded: previous degradation level
 * Returns:
 *	degradation level
 ******************************************************************************/
int check_degradation_change(struct mdinfo *info,
			     int *sources,
			     int degraded)
{
	unsigned long long new_degraded;
	int rv;

	rv = sysfs_get_ll(info, NULL, "degraded", &new_degraded);
	if (rv == -1 || (new_degraded != (unsigned long long)degraded)) {
		/* check each device to ensure it is still working */
		struct mdinfo *sd;
		new_degraded = 0;
		for (sd = info->devs ; sd ; sd = sd->next) {
			if (sd->disk.state & (1<<MD_DISK_FAULTY))
				continue;
			if (sd->disk.state & (1<<MD_DISK_SYNC)) {
				char sbuf[SYSFS_MAX_BUF_SIZE];
				int raid_disk = sd->disk.raid_disk;

				if (sysfs_get_str(info,
					sd, "state", sbuf, sizeof(sbuf)) < 0 ||
					strstr(sbuf, "faulty") ||
					strstr(sbuf, "in_sync") == NULL) {
					/* this device is dead */
					sd->disk.state = (1<<MD_DISK_FAULTY);
					if (raid_disk >= 0)
						close_fd(&sources[raid_disk]);
					new_degraded++;
				}
			}
		}
	}

	return new_degraded;
}

/*******************************************************************************
 * Function:	imsm_manage_reshape
 * Description:	Function finds array under reshape and it manages reshape
 *		process. It creates stripes backups (if required) and sets
 *		checkpoints.
 * Parameters:
 *	afd		: Backup handle (nattive) - not used
 *	sra		: general array info
 *	reshape		: reshape parameters - not used
 *	st		: supertype structure
 *	blocks		: size of critical section [blocks]
 *	fds		: table of source device descriptor
 *	offsets		: start of array (offest per devices)
 *	dests		: not used
 *	destfd		: table of destination device descriptor
 *	destoffsets	: table of destination offsets (per device)
 * Returns:
 *	1 : success, reshape is done
 *	0 : fail
 ******************************************************************************/
static int imsm_manage_reshape(
	int afd, struct mdinfo *sra, struct reshape *reshape,
	struct supertype *st, unsigned long backup_blocks,
	int *fds, unsigned long long *offsets,
	int dests, int *destfd, unsigned long long *destoffsets)
{
	int ret_val = 0;
	struct intel_super *super = st->sb;
	struct intel_dev *dv;
	unsigned int sector_size = super->sector_size;
	struct imsm_dev *dev = NULL;
	struct imsm_map *map_src, *map_dest;
	int migr_vol_qan = 0;
	int ndata, odata; /* [bytes] */
	int chunk; /* [bytes] */
	struct migr_record *migr_rec;
	char *buf = NULL;
	unsigned int buf_size; /* [bytes] */
	unsigned long long max_position; /* array size [bytes] */
	unsigned long long next_step; /* [blocks]/[bytes] */
	unsigned long long old_data_stripe_length;
	unsigned long long start_src; /* [bytes] */
	unsigned long long start; /* [bytes] */
	unsigned long long start_buf_shift; /* [bytes] */
	int degraded = 0;
	int source_layout = 0;
	int subarray_index = -1;

	if (!sra)
		return ret_val;

	if (!fds || !offsets)
		goto abort;

	/* Find volume during the reshape */
	for (dv = super->devlist; dv; dv = dv->next) {
		if (dv->dev->vol.migr_type == MIGR_GEN_MIGR &&
		    dv->dev->vol.migr_state == 1) {
			dev = dv->dev;
			migr_vol_qan++;
			subarray_index = dv->index;
		}
	}
	/* Only one volume can migrate at the same time */
	if (migr_vol_qan != 1) {
		pr_err("%s", migr_vol_qan ?
			"Number of migrating volumes greater than 1\n" :
			"There is no volume during migrationg\n");
		goto abort;
	}

	map_dest = get_imsm_map(dev, MAP_0);
	map_src = get_imsm_map(dev, MAP_1);
	if (map_src == NULL)
		goto abort;

	ndata = imsm_num_data_members(map_dest);
	odata = imsm_num_data_members(map_src);

	chunk = __le16_to_cpu(map_src->blocks_per_strip) * 512;
	old_data_stripe_length = odata * chunk;

	migr_rec = super->migr_rec;

	/* initialize migration record for start condition */
	if (sra->reshape_progress == 0)
		init_migr_record_imsm(st, dev, sra);
	else {
		if (__le32_to_cpu(migr_rec->rec_status) != UNIT_SRC_NORMAL) {
			pr_err("imsm: Cannot restart migration when data are present in copy area.\n"
			       "      Reassemble array to try to restore critical sector.\n");
			goto abort;
		}
		/* Save checkpoint to update migration record for current
		 * reshape position (in md). It can be farther than current
		 * reshape position in metadata.
		 */
		if (save_checkpoint_imsm(st, sra, UNIT_SRC_NORMAL) == 1) {
			/* ignore error == 2, this can mean end of reshape here
			 */
			dprintf("imsm: Cannot write checkpoint to migration record (UNIT_SRC_NORMAL, initial save)\n");
			goto abort;
		}
	}

	/* size for data */
	buf_size = __le32_to_cpu(migr_rec->blocks_per_unit) * 512;
	/* extend  buffer size for parity disk */
	buf_size += __le32_to_cpu(migr_rec->dest_depth_per_unit) * 512;
	/* add space for stripe alignment */
	buf_size += old_data_stripe_length;
	if (posix_memalign((void **)&buf, MAX_SECTOR_SIZE, buf_size)) {
		dprintf("imsm: Cannot allocate checkpoint buffer\n");
		goto abort;
	}

	max_position = sra->component_size * ndata;
	source_layout = imsm_level_to_layout(map_src->raid_level);

	while (current_migr_unit(migr_rec) <
	       get_num_migr_units(migr_rec)) {
		/* current reshape position [blocks] */
		unsigned long long current_position =
			__le32_to_cpu(migr_rec->blocks_per_unit)
			* current_migr_unit(migr_rec);
		unsigned long long border;

		/* Check that array hasn't become failed.
		 */
		degraded = check_degradation_change(sra, fds, degraded);
		if (degraded > 1) {
			dprintf("imsm: Abort reshape due to degradation level (%i)\n", degraded);
			goto abort;
		}

		next_step = __le32_to_cpu(migr_rec->blocks_per_unit);

		if ((current_position + next_step) > max_position)
			next_step = max_position - current_position;

		start = current_position * 512;

		/* align reading start to old geometry */
		start_buf_shift = start % old_data_stripe_length;
		start_src = start - start_buf_shift;

		border = (start_src / odata) - (start / ndata);
		border /= 512;
		if (border <= __le32_to_cpu(migr_rec->dest_depth_per_unit)) {
			/* save critical stripes to buf
			 * start     - start address of current unit
			 *             to backup [bytes]
			 * start_src - start address of current unit
			 *             to backup alligned to source array
			 *             [bytes]
			 */
			unsigned long long next_step_filler;
			unsigned long long copy_length = next_step * 512;

			/* allign copy area length to stripe in old geometry */
			next_step_filler = ((copy_length + start_buf_shift)
					    % old_data_stripe_length);
			if (next_step_filler)
				next_step_filler = (old_data_stripe_length
						    - next_step_filler);
			dprintf("save_stripes() parameters: start = %llu,\tstart_src = %llu,\tnext_step*512 = %llu,\tstart_in_buf_shift = %llu,\tnext_step_filler = %llu\n",
				start, start_src, copy_length,
				start_buf_shift, next_step_filler);

			if (save_stripes(fds, offsets, map_src->num_members,
					 chunk, map_src->raid_level,
					 source_layout, 0, NULL, start_src,
					 copy_length +
					 next_step_filler + start_buf_shift,
					 buf)) {
				dprintf("imsm: Cannot save stripes to buffer\n");
				goto abort;
			}
			/* Convert data to destination format and store it
			 * in backup general migration area
			 */
			if (save_backup_imsm(st, dev, sra,
				buf + start_buf_shift, copy_length)) {
				dprintf("imsm: Cannot save stripes to target devices\n");
				goto abort;
			}
			if (save_checkpoint_imsm(st, sra,
						 UNIT_SRC_IN_CP_AREA)) {
				dprintf("imsm: Cannot write checkpoint to migration record (UNIT_SRC_IN_CP_AREA)\n");
				goto abort;
			}
		} else {
			/* set next step to use whole border area */
			border /= next_step;
			if (border > 1)
				next_step *= border;
		}
		/* When data backed up, checkpoint stored,
		 * kick the kernel to reshape unit of data
		 */
		next_step = next_step + sra->reshape_progress;
		/* limit next step to array max position */
		if (next_step > max_position)
			next_step = max_position;
		sysfs_set_num(sra, NULL, "suspend_lo", sra->reshape_progress);
		sysfs_set_num(sra, NULL, "suspend_hi", next_step);
		sra->reshape_progress = next_step;

		/* wait until reshape finish */
		if (wait_for_reshape_imsm(sra, ndata)) {
			dprintf("wait_for_reshape_imsm returned error!\n");
			goto abort;
		}

		if (save_checkpoint_imsm(st, sra, UNIT_SRC_NORMAL) == 1) {
			/* ignore error == 2, this can mean end of reshape here
			 */
			dprintf("imsm: Cannot write checkpoint to migration record (UNIT_SRC_NORMAL)\n");
			goto abort;
		}

		if (sigterm)
			goto abort;

	}

	/* clear migr_rec on disks after successful migration */
	struct dl *d;

	memset(super->migr_rec_buf, 0, MIGR_REC_BUF_SECTORS*MAX_SECTOR_SIZE);
	for (d = super->disks; d; d = d->next) {
		if (d->index < 0 || is_failed(&d->disk))
			continue;
		unsigned long long dsize;

		get_dev_size(d->fd, NULL, &dsize);
		if (lseek64(d->fd, dsize - MIGR_REC_SECTOR_POSITION*sector_size,
			    SEEK_SET) >= 0) {
			if ((unsigned int)write(d->fd, super->migr_rec_buf,
			    MIGR_REC_BUF_SECTORS*sector_size) !=
			    MIGR_REC_BUF_SECTORS*sector_size)
				perror("Write migr_rec failed");
		}
	}

	/* return '1' if done */
	ret_val = 1;

	/* After the reshape eliminate size mismatch in metadata.
	 * Don't update md/component_size here, volume hasn't
	 * to take whole space. It is allowed by kernel.
	 * md/component_size will be set propoperly after next assembly.
	 */
	imsm_fix_size_mismatch(st, subarray_index);

abort:
	free(buf);
	/* See Grow.c: abort_reshape() for further explanation */
	sysfs_set_num(sra, NULL, "suspend_lo", 0x7FFFFFFFFFFFFFFFULL);
	sysfs_set_num(sra, NULL, "suspend_hi", 0);
	sysfs_set_num(sra, NULL, "suspend_lo", 0);

	return ret_val;
}

/*******************************************************************************
 * Function:	calculate_bitmap_min_chunksize
 * Description:	Calculates the minimal valid bitmap chunk size
 * Parameters:
 *	max_bits	: indicate how many bits can be used for the bitmap
 *	data_area_size	: the size of the data area covered by the bitmap
 *
 * Returns:
 *	 The bitmap chunk size
 ******************************************************************************/
static unsigned long long
calculate_bitmap_min_chunksize(unsigned long long max_bits,
			       unsigned long long data_area_size)
{
	unsigned long long min_chunk =
		4096; /* sub-page chunks don't work yet.. */
	unsigned long long bits = data_area_size / min_chunk + 1;

	while (bits > max_bits) {
		min_chunk *= 2;
		bits = (bits + 1) / 2;
	}
	return min_chunk;
}

/*******************************************************************************
 * Function:	calculate_bitmap_chunksize
 * Description:	Calculates the bitmap chunk size for the given device
 * Parameters:
 *	st	: supertype information
 *	dev	: device for the bitmap
 *
 * Returns:
 *	 The bitmap chunk size
 ******************************************************************************/
static unsigned long long calculate_bitmap_chunksize(struct supertype *st,
						     struct imsm_dev *dev)
{
	struct intel_super *super = st->sb;
	unsigned long long min_chunksize;
	unsigned long long result = IMSM_DEFAULT_BITMAP_CHUNKSIZE;
	size_t dev_size = imsm_dev_size(dev);

	min_chunksize = calculate_bitmap_min_chunksize(
		IMSM_BITMAP_AREA_SIZE * super->sector_size, dev_size);

	if (result < min_chunksize)
		result = min_chunksize;

	return result;
}

/*******************************************************************************
 * Function:	init_bitmap_header
 * Description:	Initialize the bitmap header structure
 * Parameters:
 *	st	: supertype information
 *	bms	: bitmap header struct to initialize
 *	dev	: device for the bitmap
 *
 * Returns:
 *	 0 : success
 *	-1 : fail
 ******************************************************************************/
static int init_bitmap_header(struct supertype *st, struct bitmap_super_s *bms,
			      struct imsm_dev *dev)
{
	int vol_uuid[4];

	if (!bms || !dev)
		return -1;

	bms->magic = __cpu_to_le32(BITMAP_MAGIC);
	bms->version = __cpu_to_le32(BITMAP_MAJOR_HI);
	bms->daemon_sleep = __cpu_to_le32(IMSM_DEFAULT_BITMAP_DAEMON_SLEEP);
	bms->sync_size = __cpu_to_le64(IMSM_BITMAP_AREA_SIZE);
	bms->write_behind = __cpu_to_le32(0);

	uuid_from_super_imsm(st, vol_uuid);
	memcpy(bms->uuid, vol_uuid, 16);

	bms->chunksize = calculate_bitmap_chunksize(st, dev);

	return 0;
}

/*******************************************************************************
 * Function:	validate_internal_bitmap_for_drive
 * Description:	Verify if the bitmap header for a given drive.
 * Parameters:
 *	st	: supertype information
 *	offset	: The offset from the beginning of the drive where to look for
 *		  the bitmap header.
 *	d	: the drive info
 *
 * Returns:
 *	 0 : success
 *	-1 : fail
 ******************************************************************************/
static int validate_internal_bitmap_for_drive(struct supertype *st,
					      unsigned long long offset,
					      struct dl *d)
{
	struct intel_super *super = st->sb;
	int ret = -1;
	int vol_uuid[4];
	bitmap_super_t *bms;
	int fd;

	if (!d)
		return -1;

	void *read_buf;

	if (posix_memalign(&read_buf, MAX_SECTOR_SIZE, IMSM_BITMAP_HEADER_SIZE))
		return -1;

	fd = d->fd;
	if (!is_fd_valid(fd)) {
		fd = open(d->devname, O_RDONLY, 0);

		if (!is_fd_valid(fd)) {
			dprintf("cannot open the device %s\n", d->devname);
			goto abort;
		}
	}

	if (lseek64(fd, offset * super->sector_size, SEEK_SET) < 0)
		goto abort;
	if (read(fd, read_buf, IMSM_BITMAP_HEADER_SIZE) !=
	    IMSM_BITMAP_HEADER_SIZE)
		goto abort;

	uuid_from_super_imsm(st, vol_uuid);

	bms = read_buf;
	if ((bms->magic != __cpu_to_le32(BITMAP_MAGIC)) ||
	    (bms->version != __cpu_to_le32(BITMAP_MAJOR_HI)) ||
	    (!same_uuid((int *)bms->uuid, vol_uuid, st->ss->swapuuid))) {
		dprintf("wrong bitmap header detected\n");
		goto abort;
	}

	ret = 0;
abort:
	if (!is_fd_valid(d->fd))
		close_fd(&fd);

	if (read_buf)
		free(read_buf);

	return ret;
}

/*******************************************************************************
 * Function:	validate_internal_bitmap_imsm
 * Description:	Verify if the bitmap header is in place and with proper data.
 * Parameters:
 *	st	: supertype information
 *
 * Returns:
 *	 0 : success or device w/o RWH_BITMAP
 *	-1 : fail
 ******************************************************************************/
static int validate_internal_bitmap_imsm(struct supertype *st)
{
	struct intel_super *super = st->sb;
	struct imsm_dev *dev = get_imsm_dev(super, super->current_vol);
	unsigned long long offset;
	struct dl *d;

	if (dev->rwh_policy != RWH_BITMAP)
		return 0;

	offset = get_bitmap_header_sector(super, super->current_vol);
	for (d = super->disks; d; d = d->next) {
		if (d->index < 0 || is_failed(&d->disk))
			continue;

		if (validate_internal_bitmap_for_drive(st, offset, d)) {
			pr_err("imsm: bitmap validation failed\n");
			return -1;
		}
	}
	return 0;
}

/*******************************************************************************
 * Function:	add_internal_bitmap_imsm
 * Description:	Mark the volume to use the bitmap and updates the chunk size value.
 * Parameters:
 *	st		: supertype information
 *	chunkp		: bitmap chunk size
 *	delay		: not used for imsm
 *	write_behind	: not used for imsm
 *	size		: not used for imsm
 *	may_change	: not used for imsm
 *	amajor		: not used for imsm
 *
 * Returns:
 *	 0 : success
 *	-1 : fail
 ******************************************************************************/
static int add_internal_bitmap_imsm(struct supertype *st, int *chunkp,
				    int delay, int write_behind,
				    unsigned long long size, int may_change,
				    int amajor)
{
	struct intel_super *super = st->sb;
	int vol_idx = super->current_vol;
	struct imsm_dev *dev;

	if (!super->devlist || vol_idx == -1 || !chunkp)
		return -1;

	dev = get_imsm_dev(super, vol_idx);
	dev->rwh_policy = RWH_BITMAP;
	*chunkp = calculate_bitmap_chunksize(st, dev);
	return 0;
}

/*******************************************************************************
 * Function:	locate_bitmap_imsm
 * Description:	Seek 'fd' to start of write-intent-bitmap.
 * Parameters:
 *	st		: supertype information
 *	fd		: file descriptor for the device
 *	node_num	: not used for imsm
 *
 * Returns:
 *	 0 : success
 *	-1 : fail
 ******************************************************************************/
static int locate_bitmap_imsm(struct supertype *st, int fd, int node_num)
{
	struct intel_super *super = st->sb;
	unsigned long long offset;
	int vol_idx = super->current_vol;

	if (!super->devlist || vol_idx == -1)
		return -1;

	offset = get_bitmap_header_sector(super, super->current_vol);
	dprintf("bitmap header offset is %llu\n", offset);

	lseek64(fd, offset << 9, 0);

	return 0;
}

/*******************************************************************************
 * Function:	write_init_bitmap_imsm
 * Description:	Write a bitmap header and prepares the area for the bitmap.
 * Parameters:
 *	st	: supertype information
 *	fd	: file descriptor for the device
 *	update	: not used for imsm
 *
 * Returns:
 *	 0 : success
 *	-1 : fail
 ******************************************************************************/
static int write_init_bitmap_imsm(struct supertype *st, int fd,
				  enum bitmap_update update)
{
	struct intel_super *super = st->sb;
	int vol_idx = super->current_vol;
	int ret = 0;
	unsigned long long offset;
	bitmap_super_t bms = { 0 };
	size_t written = 0;
	size_t to_write;
	ssize_t rv_num;
	void *buf;

	if (!super->devlist || !super->sector_size || vol_idx == -1)
		return -1;

	struct imsm_dev *dev = get_imsm_dev(super, vol_idx);

	/* first clear the space for bitmap header */
	unsigned long long bitmap_area_start =
		get_bitmap_header_sector(super, vol_idx);

	dprintf("zeroing area start (%llu) and size (%u)\n", bitmap_area_start,
		IMSM_BITMAP_AND_HEADER_SIZE / super->sector_size);
	if (zero_disk_range(fd, bitmap_area_start,
			    IMSM_BITMAP_HEADER_SIZE / super->sector_size)) {
		pr_err("imsm: cannot zeroing the space for the bitmap\n");
		return -1;
	}

	/* The bitmap area should be filled with "1"s to perform initial
	 * synchronization.
	 */
	if (posix_memalign(&buf, MAX_SECTOR_SIZE, MAX_SECTOR_SIZE))
		return -1;
	memset(buf, 0xFF, MAX_SECTOR_SIZE);
	offset = get_bitmap_sector(super, vol_idx);
	lseek64(fd, offset << 9, 0);
	while (written < IMSM_BITMAP_AREA_SIZE) {
		to_write = IMSM_BITMAP_AREA_SIZE - written;
		if (to_write > MAX_SECTOR_SIZE)
			to_write = MAX_SECTOR_SIZE;
		rv_num = write(fd, buf, MAX_SECTOR_SIZE);
		if (rv_num != MAX_SECTOR_SIZE) {
			ret = -1;
			dprintf("cannot initialize bitmap area\n");
			goto abort;
		}
		written += rv_num;
	}

	/* write a bitmap header */
	init_bitmap_header(st, &bms, dev);
	memset(buf, 0, MAX_SECTOR_SIZE);
	memcpy(buf, &bms, sizeof(bitmap_super_t));
	if (locate_bitmap_imsm(st, fd, 0)) {
		ret = -1;
		dprintf("cannot locate the bitmap\n");
		goto abort;
	}
	if (write(fd, buf, MAX_SECTOR_SIZE) != MAX_SECTOR_SIZE) {
		ret = -1;
		dprintf("cannot write the bitmap header\n");
		goto abort;
	}
	fsync(fd);

abort:
	free(buf);

	return ret;
}

/*******************************************************************************
 * Function:	is_vol_to_setup_bitmap
 * Description:	Checks if a bitmap should be activated on the dev.
 * Parameters:
 *	info	: info about the volume to setup the bitmap
 *	dev	: the device to check against bitmap creation
 *
 * Returns:
 *	 0 : bitmap should be set up on the device
 *	-1 : otherwise
 ******************************************************************************/
static int is_vol_to_setup_bitmap(struct mdinfo *info, struct imsm_dev *dev)
{
	if (!dev || !info)
		return -1;

	if ((strcmp((char *)dev->volume, info->name) == 0) &&
	    (dev->rwh_policy == RWH_BITMAP))
		return -1;

	return 0;
}

/*******************************************************************************
 * Function:	set_bitmap_sysfs
 * Description:	Set the sysfs atributes of a given volume to activate the bitmap.
 * Parameters:
 *	info		: info about the volume where the bitmap should be setup
 *	chunksize	: bitmap chunk size
 *	location	: location of the bitmap
 *
 * Returns:
 *	 0 : success
 *	-1 : fail
 ******************************************************************************/
static int set_bitmap_sysfs(struct mdinfo *info, unsigned long long chunksize,
			    char *location)
{
	/* The bitmap/metadata is set to external to allow changing of value for
	 * bitmap/location. When external is used, the kernel will treat an offset
	 * related to the device's first lba (in opposition to the "internal" case
	 * when this value is related to the beginning of the superblock).
	 */
	if (sysfs_set_str(info, NULL, "bitmap/metadata", "external")) {
		dprintf("failed to set bitmap/metadata\n");
		return -1;
	}

	/* It can only be changed when no bitmap is active.
	 * Should be bigger than 512 and must be power of 2.
	 * It is expecting the value in bytes.
	 */
	if (sysfs_set_num(info, NULL, "bitmap/chunksize",
					  __cpu_to_le32(chunksize))) {
		dprintf("failed to set bitmap/chunksize\n");
		return -1;
	}

	/* It is expecting the value in sectors. */
	if (sysfs_set_num(info, NULL, "bitmap/space",
					  __cpu_to_le64(IMSM_BITMAP_AREA_SIZE))) {
		dprintf("failed to set bitmap/space\n");
		return -1;
	}

	/* Determines the delay between the bitmap updates.
	 * It is expecting the value in seconds.
	 */
	if (sysfs_set_num(info, NULL, "bitmap/time_base",
					  __cpu_to_le64(IMSM_DEFAULT_BITMAP_DAEMON_SLEEP))) {
		dprintf("failed to set bitmap/time_base\n");
		return -1;
	}

	/* It is expecting the value in sectors with a sign at the beginning. */
	if (sysfs_set_str(info, NULL, "bitmap/location", location)) {
		dprintf("failed to set bitmap/location\n");
		return -1;
	}

	return 0;
}

/*******************************************************************************
 * Function:	set_bitmap_imsm
 * Description:	Setup the bitmap for the given volume
 * Parameters:
 *	st	: supertype information
 *	info	: info about the volume where the bitmap should be setup
 *
 * Returns:
 *	 0 : success
 *	-1 : fail
 ******************************************************************************/
static int set_bitmap_imsm(struct supertype *st, struct mdinfo *info)
{
	struct intel_super *super = st->sb;
	int prev_current_vol = super->current_vol;
	struct imsm_dev *dev;
	int ret = -1;
	char location[16] = "";
	unsigned long long chunksize;
	struct intel_dev *dev_it;

	for (dev_it = super->devlist; dev_it; dev_it = dev_it->next) {
		super->current_vol = dev_it->index;
		dev = get_imsm_dev(super, super->current_vol);

		if (is_vol_to_setup_bitmap(info, dev)) {
			if (validate_internal_bitmap_imsm(st)) {
				dprintf("bitmap header validation failed\n");
				goto abort;
			}

			chunksize = calculate_bitmap_chunksize(st, dev);
			dprintf("chunk size is %llu\n", chunksize);

			snprintf(location, sizeof(location), "+%llu",
				 get_bitmap_sector(super, super->current_vol));
			dprintf("bitmap offset is %s\n", location);

			if (set_bitmap_sysfs(info, chunksize, location)) {
				dprintf("cannot setup the bitmap\n");
				goto abort;
			}
		}
	}
	ret = 0;
abort:
	super->current_vol = prev_current_vol;
	return ret;
}

struct superswitch super_imsm = {
	.examine_super	= examine_super_imsm,
	.brief_examine_super = brief_examine_super_imsm,
	.brief_examine_subarrays = brief_examine_subarrays_imsm,
	.export_examine_super = export_examine_super_imsm,
	.detail_super	= detail_super_imsm,
	.brief_detail_super = brief_detail_super_imsm,
	.write_init_super = write_init_super_imsm,
	.validate_geometry = validate_geometry_imsm,
	.add_to_super	= add_to_super_imsm,
	.remove_from_super = remove_from_super_imsm,
	.detail_platform = detail_platform_imsm,
	.export_detail_platform = export_detail_platform_imsm,
	.kill_subarray = kill_subarray_imsm,
	.update_subarray = update_subarray_imsm,
	.load_container	= load_container_imsm,
	.default_geometry = default_geometry_imsm,
	.test_and_add_drive_policies = test_and_add_drive_policies_imsm,
	.reshape_super  = imsm_reshape_super,
	.manage_reshape = imsm_manage_reshape,
	.recover_backup = recover_backup_imsm,
	.examine_badblocks = examine_badblocks_imsm,
	.match_home	= match_home_imsm,
	.uuid_from_super= uuid_from_super_imsm,
	.getinfo_super  = getinfo_super_imsm,
	.getinfo_super_disks = getinfo_super_disks_imsm,
	.update_super	= update_super_imsm,

	.avail_size	= avail_size_imsm,
	.get_spare_criteria = get_spare_criteria_imsm,

	.compare_super	= compare_super_imsm,

	.load_super	= load_super_imsm,
	.init_super	= init_super_imsm,
	.store_super	= store_super_imsm,
	.free_super	= free_super_imsm,
	.match_metadata_desc = match_metadata_desc_imsm,
	.container_content = container_content_imsm,
	.validate_container = validate_container_imsm,

	.add_internal_bitmap = add_internal_bitmap_imsm,
	.locate_bitmap = locate_bitmap_imsm,
	.write_bitmap = write_init_bitmap_imsm,
	.set_bitmap = set_bitmap_imsm,

	.write_init_ppl = write_init_ppl_imsm,
	.validate_ppl	= validate_ppl_imsm,

	.external	= 1,
	.swapuuid	= 0,
	.name = "imsm",

/* for mdmon */
	.open_new	= imsm_open_new,
	.set_array_state= imsm_set_array_state,
	.set_disk	= imsm_set_disk,
	.sync_metadata	= imsm_sync_metadata,
	.activate_spare = imsm_activate_spare,
	.process_update = imsm_process_update,
	.prepare_update = imsm_prepare_update,
	.record_bad_block = imsm_record_badblock,
	.clear_bad_block  = imsm_clear_badblock,
	.get_bad_blocks   = imsm_get_badblocks,
};
