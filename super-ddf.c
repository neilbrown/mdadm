/*
 * mdadm - manage Linux "md" devices aka RAID arrays.
 *
 * Copyright (C) 2006-2009 Neil Brown <neilb@suse.de>
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
 *    Email: <neil@brown.name>
 *
 * Specifications for DDF takes from Common RAID DDF Specification Revision 1.2
 * (July 28 2006).  Reused by permission of SNIA.
 */

#define HAVE_STDINT_H 1
#include "mdadm.h"
#include "mdmon.h"
#include "sha1.h"
#include <values.h>

/* a non-official T10 name for creation GUIDs */
static char T10[] = "Linux-MD";

/* DDF timestamps are 1980 based, so we need to add
 * second-in-decade-of-seventies to convert to linux timestamps.
 * 10 years with 2 leap years.
 */
#define DECADE (3600*24*(365*10+2))
unsigned long crc32(
	unsigned long crc,
	const unsigned char *buf,
	unsigned len);

/* The DDF metadata handling.
 * DDF metadata lives at the end of the device.
 * The last 512 byte block provides an 'anchor' which is used to locate
 * the rest of the metadata which usually lives immediately behind the anchor.
 *
 * Note:
 *  - all multibyte numeric fields are bigendian.
 *  - all strings are space padded.
 *
 */

/* Primary Raid Level (PRL) */
#define	DDF_RAID0	0x00
#define	DDF_RAID1	0x01
#define	DDF_RAID3	0x03
#define	DDF_RAID4	0x04
#define	DDF_RAID5	0x05
#define	DDF_RAID1E	0x11
#define	DDF_JBOD	0x0f
#define	DDF_CONCAT	0x1f
#define	DDF_RAID5E	0x15
#define	DDF_RAID5EE	0x25
#define	DDF_RAID6	0x06

/* Raid Level Qualifier (RLQ) */
#define	DDF_RAID0_SIMPLE	0x00
#define	DDF_RAID1_SIMPLE	0x00 /* just 2 devices in this plex */
#define	DDF_RAID1_MULTI		0x01 /* exactly 3 devices in this plex */
#define	DDF_RAID3_0		0x00 /* parity in first extent */
#define	DDF_RAID3_N		0x01 /* parity in last extent */
#define	DDF_RAID4_0		0x00 /* parity in first extent */
#define	DDF_RAID4_N		0x01 /* parity in last extent */
/* these apply to raid5e and raid5ee as well */
#define	DDF_RAID5_0_RESTART	0x00 /* same as 'right asymmetric' - layout 1 */
#define	DDF_RAID6_0_RESTART	0x01 /* raid6 different from raid5 here!!! */
#define	DDF_RAID5_N_RESTART	0x02 /* same as 'left asymmetric' - layout 0 */
#define	DDF_RAID5_N_CONTINUE	0x03 /* same as 'left symmetric' - layout 2 */

#define	DDF_RAID1E_ADJACENT	0x00 /* raid10 nearcopies==2 */
#define	DDF_RAID1E_OFFSET	0x01 /* raid10 offsetcopies==2 */

/* Secondary RAID Level (SRL) */
#define	DDF_2STRIPED	0x00	/* This is weirder than RAID0 !! */
#define	DDF_2MIRRORED	0x01
#define	DDF_2CONCAT	0x02
#define	DDF_2SPANNED	0x03	/* This is also weird - be careful */

/* Magic numbers */
#define	DDF_HEADER_MAGIC	__cpu_to_be32(0xDE11DE11)
#define	DDF_CONTROLLER_MAGIC	__cpu_to_be32(0xAD111111)
#define	DDF_PHYS_RECORDS_MAGIC	__cpu_to_be32(0x22222222)
#define	DDF_PHYS_DATA_MAGIC	__cpu_to_be32(0x33333333)
#define	DDF_VIRT_RECORDS_MAGIC	__cpu_to_be32(0xDDDDDDDD)
#define	DDF_VD_CONF_MAGIC	__cpu_to_be32(0xEEEEEEEE)
#define	DDF_SPARE_ASSIGN_MAGIC	__cpu_to_be32(0x55555555)
#define	DDF_VU_CONF_MAGIC	__cpu_to_be32(0x88888888)
#define	DDF_VENDOR_LOG_MAGIC	__cpu_to_be32(0x01dBEEF0)
#define	DDF_BBM_LOG_MAGIC	__cpu_to_be32(0xABADB10C)

#define	DDF_GUID_LEN	24
#define DDF_REVISION_0	"01.00.00"
#define DDF_REVISION_2	"01.02.00"

struct ddf_header {
	__u32	magic;		/* DDF_HEADER_MAGIC */
	__u32	crc;
	char	guid[DDF_GUID_LEN];
	char	revision[8];	/* 01.02.00 */
	__u32	seq;		/* starts at '1' */
	__u32	timestamp;
	__u8	openflag;
	__u8	foreignflag;
	__u8	enforcegroups;
	__u8	pad0;		/* 0xff */
	__u8	pad1[12];	/* 12 * 0xff */
	/* 64 bytes so far */
	__u8	header_ext[32];	/* reserved: fill with 0xff */
	__u64	primary_lba;
	__u64	secondary_lba;
	__u8	type;
	__u8	pad2[3];	/* 0xff */
	__u32	workspace_len;	/* sectors for vendor space -
				 * at least 32768(sectors) */
	__u64	workspace_lba;
	__u16	max_pd_entries;	/* one of 15, 63, 255, 1023, 4095 */
	__u16	max_vd_entries; /* 2^(4,6,8,10,12)-1 : i.e. as above */
	__u16	max_partitions; /* i.e. max num of configuration
				   record entries per disk */
	__u16	config_record_len; /* 1 +ROUNDUP(max_primary_element_entries
				                 *12/512) */
	__u16	max_primary_element_entries; /* 16, 64, 256, 1024, or 4096 */
	__u8	pad3[54];	/* 0xff */
	/* 192 bytes so far */
	__u32	controller_section_offset;
	__u32	controller_section_length;
	__u32	phys_section_offset;
	__u32	phys_section_length;
	__u32	virt_section_offset;
	__u32	virt_section_length;
	__u32	config_section_offset;
	__u32	config_section_length;
	__u32	data_section_offset;
	__u32	data_section_length;
	__u32	bbm_section_offset;
	__u32	bbm_section_length;
	__u32	diag_space_offset;
	__u32	diag_space_length;
	__u32	vendor_offset;
	__u32	vendor_length;
	/* 256 bytes so far */
	__u8	pad4[256];	/* 0xff */
};

/* type field */
#define	DDF_HEADER_ANCHOR	0x00
#define	DDF_HEADER_PRIMARY	0x01
#define	DDF_HEADER_SECONDARY	0x02

/* The content of the 'controller section' - global scope */
struct ddf_controller_data {
	__u32	magic;			/* DDF_CONTROLLER_MAGIC */
	__u32	crc;
	char	guid[DDF_GUID_LEN];
	struct controller_type {
		__u16 vendor_id;
		__u16 device_id;
		__u16 sub_vendor_id;
		__u16 sub_device_id;
	} type;
	char	product_id[16];
	__u8	pad[8];	/* 0xff */
	__u8	vendor_data[448];
};

/* The content of phys_section - global scope */
struct phys_disk {
	__u32	magic;		/* DDF_PHYS_RECORDS_MAGIC */
	__u32	crc;
	__u16	used_pdes;
	__u16	max_pdes;
	__u8	pad[52];
	struct phys_disk_entry {
		char	guid[DDF_GUID_LEN];
		__u32	refnum;
		__u16	type;
		__u16	state;
		__u64	config_size; /* DDF structures must be after here */
		char	path[18];	/* another horrible structure really */
		__u8	pad[6];
	} entries[0];
};

/* phys_disk_entry.type is a bitmap - bigendian remember */
#define	DDF_Forced_PD_GUID		1
#define	DDF_Active_in_VD		2
#define	DDF_Global_Spare		4 /* VD_CONF records are ignored */
#define	DDF_Spare			8 /* overrides Global_spare */
#define	DDF_Foreign			16
#define	DDF_Legacy			32 /* no DDF on this device */

#define	DDF_Interface_mask		0xf00
#define	DDF_Interface_SCSI		0x100
#define	DDF_Interface_SAS		0x200
#define	DDF_Interface_SATA		0x300
#define	DDF_Interface_FC		0x400

/* phys_disk_entry.state is a bigendian bitmap */
#define	DDF_Online			1
#define	DDF_Failed			2 /* overrides  1,4,8 */
#define	DDF_Rebuilding			4
#define	DDF_Transition			8
#define	DDF_SMART			16
#define	DDF_ReadErrors			32
#define	DDF_Missing			64

/* The content of the virt_section global scope */
struct virtual_disk {
	__u32	magic;		/* DDF_VIRT_RECORDS_MAGIC */
	__u32	crc;
	__u16	populated_vdes;
	__u16	max_vdes;
	__u8	pad[52];
	struct virtual_entry {
		char	guid[DDF_GUID_LEN];
		__u16	unit;
		__u16	pad0;	/* 0xffff */
		__u16	guid_crc;
		__u16	type;
		__u8	state;
		__u8	init_state;
		__u8	pad1[14];
		char	name[16];
	} entries[0];
};

/* virtual_entry.type is a bitmap - bigendian */
#define	DDF_Shared		1
#define	DDF_Enforce_Groups	2
#define	DDF_Unicode		4
#define	DDF_Owner_Valid		8

/* virtual_entry.state is a bigendian bitmap */
#define	DDF_state_mask		0x7
#define	DDF_state_optimal	0x0
#define	DDF_state_degraded	0x1
#define	DDF_state_deleted	0x2
#define	DDF_state_missing	0x3
#define	DDF_state_failed	0x4
#define	DDF_state_part_optimal	0x5

#define	DDF_state_morphing	0x8
#define	DDF_state_inconsistent	0x10

/* virtual_entry.init_state is a bigendian bitmap */
#define	DDF_initstate_mask	0x03
#define	DDF_init_not		0x00
#define	DDF_init_quick		0x01 /* initialisation is progress.
				      * i.e. 'state_inconsistent' */
#define	DDF_init_full		0x02

#define	DDF_access_mask		0xc0
#define	DDF_access_rw		0x00
#define	DDF_access_ro		0x80
#define	DDF_access_blocked	0xc0

/* The content of the config_section - local scope
 * It has multiple records each config_record_len sectors
 * They can be vd_config or spare_assign
 */

struct vd_config {
	__u32	magic;		/* DDF_VD_CONF_MAGIC */
	__u32	crc;
	char	guid[DDF_GUID_LEN];
	__u32	timestamp;
	__u32	seqnum;
	__u8	pad0[24];
	__u16	prim_elmnt_count;
	__u8	chunk_shift;	/* 0 == 512, 1==1024 etc */
	__u8	prl;
	__u8	rlq;
	__u8	sec_elmnt_count;
	__u8	sec_elmnt_seq;
	__u8	srl;
	__u64	blocks;		/* blocks per component could be different
				 * on different component devices...(only
				 * for concat I hope) */
	__u64	array_blocks;	/* blocks in array */
	__u8	pad1[8];
	__u32	spare_refs[8];
	__u8	cache_pol[8];
	__u8	bg_rate;
	__u8	pad2[3];
	__u8	pad3[52];
	__u8	pad4[192];
	__u8	v0[32];	/* reserved- 0xff */
	__u8	v1[32];	/* reserved- 0xff */
	__u8	v2[16];	/* reserved- 0xff */
	__u8	v3[16];	/* reserved- 0xff */
	__u8	vendor[32];
	__u32	phys_refnum[0];	/* refnum of each disk in sequence */
      /*__u64	lba_offset[0];  LBA offset in each phys.  Note extents in a
				bvd are always the same size */
};

/* vd_config.cache_pol[7] is a bitmap */
#define	DDF_cache_writeback	1	/* else writethrough */
#define	DDF_cache_wadaptive	2	/* only applies if writeback */
#define	DDF_cache_readahead	4
#define	DDF_cache_radaptive	8	/* only if doing read-ahead */
#define	DDF_cache_ifnobatt	16	/* even to write cache if battery is poor */
#define	DDF_cache_wallowed	32	/* enable write caching */
#define	DDF_cache_rallowed	64	/* enable read caching */

struct spare_assign {
	__u32	magic;		/* DDF_SPARE_ASSIGN_MAGIC */
	__u32	crc;
	__u32	timestamp;
	__u8	reserved[7];
	__u8	type;
	__u16	populated;	/* SAEs used */
	__u16	max;		/* max SAEs */
	__u8	pad[8];
	struct spare_assign_entry {
		char	guid[DDF_GUID_LEN];
		__u16	secondary_element;
		__u8	pad[6];
	} spare_ents[0];
};
/* spare_assign.type is a bitmap */
#define	DDF_spare_dedicated	0x1	/* else global */
#define	DDF_spare_revertible	0x2	/* else committable */
#define	DDF_spare_active	0x4	/* else not active */
#define	DDF_spare_affinity	0x8	/* enclosure affinity */

/* The data_section contents - local scope */
struct disk_data {
	__u32	magic;		/* DDF_PHYS_DATA_MAGIC */
	__u32	crc;
	char	guid[DDF_GUID_LEN];
	__u32	refnum;		/* crc of some magic drive data ... */
	__u8	forced_ref;	/* set when above was not result of magic */
	__u8	forced_guid;	/* set if guid was forced rather than magic */
	__u8	vendor[32];
	__u8	pad[442];
};

/* bbm_section content */
struct bad_block_log {
	__u32	magic;
	__u32	crc;
	__u16	entry_count;
	__u32	spare_count;
	__u8	pad[10];
	__u64	first_spare;
	struct mapped_block {
		__u64	defective_start;
		__u32	replacement_start;
		__u16	remap_count;
		__u8	pad[2];
	} entries[0];
};

/* Struct for internally holding ddf structures */
/* The DDF structure stored on each device is potentially
 * quite different, as some data is global and some is local.
 * The global data is:
 *   - ddf header
 *   - controller_data
 *   - Physical disk records
 *   - Virtual disk records
 * The local data is:
 *   - Configuration records
 *   - Physical Disk data section
 *  (  and Bad block and vendor which I don't care about yet).
 *
 * The local data is parsed into separate lists as it is read
 * and reconstructed for writing.  This means that we only need
 * to make config changes once and they are automatically
 * propagated to all devices.
 * Note that the ddf_super has space of the conf and disk data
 * for this disk and also for a list of all such data.
 * The list is only used for the superblock that is being
 * built in Create or Assemble to describe the whole array.
 */
struct ddf_super {
	struct ddf_header anchor, primary, secondary;
	struct ddf_controller_data controller;
	struct ddf_header *active;
	struct phys_disk	*phys;
	struct virtual_disk	*virt;
	int pdsize, vdsize;
	unsigned int max_part, mppe, conf_rec_len;
	int currentdev;
	int updates_pending;
	struct vcl {
		union {
			char space[512];
			struct {
				struct vcl	*next;
				__u64		*lba_offset; /* location in 'conf' of
							      * the lba table */
				unsigned int	vcnum; /* index into ->virt */
				__u64		*block_sizes; /* NULL if all the same */
			};
		};
		struct vd_config conf;
	} *conflist, *currentconf;
	struct dl {
		union {
			char space[512];
			struct {
				struct dl	*next;
				int major, minor;
				char *devname;
				int fd;
				unsigned long long size; /* sectors */
				int pdnum;	/* index in ->phys */
				struct spare_assign *spare;
				void *mdupdate; /* hold metadata update */

				/* These fields used by auto-layout */
				int raiddisk; /* slot to fill in autolayout */
				__u64 esize;
			};
		};
		struct disk_data disk;
		struct vcl *vlist[0]; /* max_part in size */
	} *dlist, *add_list;
};

#ifndef offsetof
#define offsetof(t,f) ((size_t)&(((t*)0)->f))
#endif


static unsigned int calc_crc(void *buf, int len)
{
	/* crcs are always at the same place as in the ddf_header */
	struct ddf_header *ddf = buf;
	__u32 oldcrc = ddf->crc;
	__u32 newcrc;
	ddf->crc = 0xffffffff;

	newcrc = crc32(0, buf, len);
	ddf->crc = oldcrc;
	/* The crc is store (like everything) bigendian, so convert
	 * here for simplicity
	 */
	return __cpu_to_be32(newcrc);
}

static int load_ddf_header(int fd, unsigned long long lba,
			   unsigned long long size,
			   int type,
			   struct ddf_header *hdr, struct ddf_header *anchor)
{
	/* read a ddf header (primary or secondary) from fd/lba
	 * and check that it is consistent with anchor
	 * Need to check:
	 *   magic, crc, guid, rev, and LBA's header_type, and
	 *  everything after header_type must be the same
	 */
	if (lba >= size-1)
		return 0;

	if (lseek64(fd, lba<<9, 0) < 0)
		return 0;

	if (read(fd, hdr, 512) != 512)
		return 0;

	if (hdr->magic != DDF_HEADER_MAGIC)
		return 0;
	if (calc_crc(hdr, 512) != hdr->crc)
		return 0;
	if (memcmp(anchor->guid, hdr->guid, DDF_GUID_LEN) != 0 ||
	    memcmp(anchor->revision, hdr->revision, 8) != 0 ||
	    anchor->primary_lba != hdr->primary_lba ||
	    anchor->secondary_lba != hdr->secondary_lba ||
	    hdr->type != type ||
	    memcmp(anchor->pad2, hdr->pad2, 512 -
		   offsetof(struct ddf_header, pad2)) != 0)
		return 0;

	/* Looks good enough to me... */
	return 1;
}

static void *load_section(int fd, struct ddf_super *super, void *buf,
			  __u32 offset_be, __u32 len_be, int check)
{
	unsigned long long offset = __be32_to_cpu(offset_be);
	unsigned long long len = __be32_to_cpu(len_be);
	int dofree = (buf == NULL);

	if (check)
		if (len != 2 && len != 8 && len != 32
		    && len != 128 && len != 512)
			return NULL;

	if (len > 1024)
		return NULL;
	if (buf) {
		/* All pre-allocated sections are a single block */
		if (len != 1)
			return NULL;
	} else if (posix_memalign(&buf, 512, len<<9) != 0)
		buf = NULL;

	if (!buf)
		return NULL;

	if (super->active->type == 1)
		offset += __be64_to_cpu(super->active->primary_lba);
	else
		offset += __be64_to_cpu(super->active->secondary_lba);

	if ((unsigned long long)lseek64(fd, offset<<9, 0) != (offset<<9)) {
		if (dofree)
			free(buf);
		return NULL;
	}
	if ((unsigned long long)read(fd, buf, len<<9) != (len<<9)) {
		if (dofree)
			free(buf);
		return NULL;
	}
	return buf;
}

static int load_ddf_headers(int fd, struct ddf_super *super, char *devname)
{
	unsigned long long dsize;

	get_dev_size(fd, NULL, &dsize);

	if (lseek64(fd, dsize-512, 0) < 0) {
		if (devname)
			fprintf(stderr,
				Name": Cannot seek to anchor block on %s: %s\n",
				devname, strerror(errno));
		return 1;
	}
	if (read(fd, &super->anchor, 512) != 512) {
		if (devname)
			fprintf(stderr,
				Name ": Cannot read anchor block on %s: %s\n",
				devname, strerror(errno));
		return 1;
	}
	if (super->anchor.magic != DDF_HEADER_MAGIC) {
		if (devname)
			fprintf(stderr, Name ": no DDF anchor found on %s\n",
				devname);
		return 2;
	}
	if (calc_crc(&super->anchor, 512) != super->anchor.crc) {
		if (devname)
			fprintf(stderr, Name ": bad CRC on anchor on %s\n",
				devname);
		return 2;
	}
	if (memcmp(super->anchor.revision, DDF_REVISION_0, 8) != 0 &&
	    memcmp(super->anchor.revision, DDF_REVISION_2, 8) != 0) {
		if (devname)
			fprintf(stderr, Name ": can only support super revision"
				" %.8s and earlier, not %.8s on %s\n",
				DDF_REVISION_2, super->anchor.revision,devname);
		return 2;
	}
	if (load_ddf_header(fd, __be64_to_cpu(super->anchor.primary_lba),
			    dsize >> 9,  1,
			    &super->primary, &super->anchor) == 0) {
		if (devname)
			fprintf(stderr,
				Name ": Failed to load primary DDF header "
				"on %s\n", devname);
		return 2;
	}
	super->active = &super->primary;
	if (load_ddf_header(fd, __be64_to_cpu(super->anchor.secondary_lba),
			    dsize >> 9,  2,
			    &super->secondary, &super->anchor)) {
		if ((__be32_to_cpu(super->primary.seq)
		     < __be32_to_cpu(super->secondary.seq) &&
		     !super->secondary.openflag)
		    || (__be32_to_cpu(super->primary.seq)
			== __be32_to_cpu(super->secondary.seq) &&
			super->primary.openflag && !super->secondary.openflag)
			)
			super->active = &super->secondary;
	}
	return 0;
}

static int load_ddf_global(int fd, struct ddf_super *super, char *devname)
{
	void *ok;
	ok = load_section(fd, super, &super->controller,
			  super->active->controller_section_offset,
			  super->active->controller_section_length,
			  0);
	super->phys = load_section(fd, super, NULL,
				   super->active->phys_section_offset,
				   super->active->phys_section_length,
				   1);
	super->pdsize = __be32_to_cpu(super->active->phys_section_length) * 512;

	super->virt = load_section(fd, super, NULL,
				   super->active->virt_section_offset,
				   super->active->virt_section_length,
				   1);
	super->vdsize = __be32_to_cpu(super->active->virt_section_length) * 512;
	if (!ok ||
	    !super->phys ||
	    !super->virt) {
		free(super->phys);
		free(super->virt);
		super->phys = NULL;
		super->virt = NULL;
		return 2;
	}
	super->conflist = NULL;
	super->dlist = NULL;

	super->max_part = __be16_to_cpu(super->active->max_partitions);
	super->mppe = __be16_to_cpu(super->active->max_primary_element_entries);
	super->conf_rec_len = __be16_to_cpu(super->active->config_record_len);
	return 0;
}

static int load_ddf_local(int fd, struct ddf_super *super,
			  char *devname, int keep)
{
	struct dl *dl;
	struct stat stb;
	char *conf;
	unsigned int i;
	unsigned int confsec;
	int vnum;
	unsigned int max_virt_disks = __be16_to_cpu(super->active->max_vd_entries);
	unsigned long long dsize;

	/* First the local disk info */
	if (posix_memalign((void**)&dl, 512,
		       sizeof(*dl) +
		       (super->max_part) * sizeof(dl->vlist[0])) != 0) {
		fprintf(stderr, Name ": %s could not allocate disk info buffer\n",
			__func__);
		return 1;
	}

	load_section(fd, super, &dl->disk,
		     super->active->data_section_offset,
		     super->active->data_section_length,
		     0);
	dl->devname = devname ? strdup(devname) : NULL;

	fstat(fd, &stb);
	dl->major = major(stb.st_rdev);
	dl->minor = minor(stb.st_rdev);
	dl->next = super->dlist;
	dl->fd = keep ? fd : -1;

	dl->size = 0;
	if (get_dev_size(fd, devname, &dsize))
		dl->size = dsize >> 9;
	dl->spare = NULL;
	for (i = 0 ; i < super->max_part ; i++)
		dl->vlist[i] = NULL;
	super->dlist = dl;
	dl->pdnum = -1;
	for (i = 0; i < __be16_to_cpu(super->active->max_pd_entries); i++)
		if (memcmp(super->phys->entries[i].guid,
			   dl->disk.guid, DDF_GUID_LEN) == 0)
			dl->pdnum = i;

	/* Now the config list. */
	/* 'conf' is an array of config entries, some of which are
	 * probably invalid.  Those which are good need to be copied into
	 * the conflist
	 */

	conf = load_section(fd, super, NULL,
			    super->active->config_section_offset,
			    super->active->config_section_length,
			    0);

	vnum = 0;
	for (confsec = 0;
	     confsec < __be32_to_cpu(super->active->config_section_length);
	     confsec += super->conf_rec_len) {
		struct vd_config *vd =
			(struct vd_config *)((char*)conf + confsec*512);
		struct vcl *vcl;

		if (vd->magic == DDF_SPARE_ASSIGN_MAGIC) {
			if (dl->spare)
				continue;
			if (posix_memalign((void**)&dl->spare, 512,
				       super->conf_rec_len*512) != 0) {
				fprintf(stderr, Name
					": %s could not allocate spare info buf\n",
					__func__);
				return 1;
			}
				
			memcpy(dl->spare, vd, super->conf_rec_len*512);
			continue;
		}
		if (vd->magic != DDF_VD_CONF_MAGIC)
			continue;
		for (vcl = super->conflist; vcl; vcl = vcl->next) {
			if (memcmp(vcl->conf.guid,
				   vd->guid, DDF_GUID_LEN) == 0)
				break;
		}

		if (vcl) {
			dl->vlist[vnum++] = vcl;
			if (__be32_to_cpu(vd->seqnum) <=
			    __be32_to_cpu(vcl->conf.seqnum))
				continue;
		} else {
			if (posix_memalign((void**)&vcl, 512,
				       (super->conf_rec_len*512 +
					offsetof(struct vcl, conf))) != 0) {
				fprintf(stderr, Name
					": %s could not allocate vcl buf\n",
					__func__);
				return 1;
			}
			vcl->next = super->conflist;
			vcl->block_sizes = NULL; /* FIXME not for CONCAT */
			super->conflist = vcl;
			dl->vlist[vnum++] = vcl;
		}
		memcpy(&vcl->conf, vd, super->conf_rec_len*512);
		vcl->lba_offset = (__u64*)
			&vcl->conf.phys_refnum[super->mppe];

		for (i=0; i < max_virt_disks ; i++)
			if (memcmp(super->virt->entries[i].guid,
				   vcl->conf.guid, DDF_GUID_LEN)==0)
				break;
		if (i < max_virt_disks)
			vcl->vcnum = i;
	}
	free(conf);

	return 0;
}

#ifndef MDASSEMBLE
static int load_super_ddf_all(struct supertype *st, int fd,
			      void **sbp, char *devname);
#endif

static void free_super_ddf(struct supertype *st);

static int load_super_ddf(struct supertype *st, int fd,
			  char *devname)
{
	unsigned long long dsize;
	struct ddf_super *super;
	int rv;

	if (get_dev_size(fd, devname, &dsize) == 0)
		return 1;

	if (test_partition(fd))
		/* DDF is not allowed on partitions */
		return 1;

	/* 32M is a lower bound */
	if (dsize <= 32*1024*1024) {
		if (devname)
			fprintf(stderr,
				Name ": %s is too small for ddf: "
				"size is %llu sectors.\n",
				devname, dsize>>9);
		return 1;
	}
	if (dsize & 511) {
		if (devname)
			fprintf(stderr,
				Name ": %s is an odd size for ddf: "
				"size is %llu bytes.\n",
				devname, dsize);
		return 1;
	}

	free_super_ddf(st);

	if (posix_memalign((void**)&super, 512, sizeof(*super))!= 0) {
		fprintf(stderr, Name ": malloc of %zu failed.\n",
			sizeof(*super));
		return 1;
	}
	memset(super, 0, sizeof(*super));

	rv = load_ddf_headers(fd, super, devname);
	if (rv) {
		free(super);
		return rv;
	}

	/* Have valid headers and have chosen the best. Let's read in the rest*/

	rv = load_ddf_global(fd, super, devname);

	if (rv) {
		if (devname)
			fprintf(stderr,
				Name ": Failed to load all information "
				"sections on %s\n", devname);
		free(super);
		return rv;
	}

	rv = load_ddf_local(fd, super, devname, 0);

	if (rv) {
		if (devname)
			fprintf(stderr,
				Name ": Failed to load all information "
				"sections on %s\n", devname);
		free(super);
		return rv;
	}

	/* Should possibly check the sections .... */

	st->sb = super;
	if (st->ss == NULL) {
		st->ss = &super_ddf;
		st->minor_version = 0;
		st->max_devs = 512;
	}
	return 0;

}

static void free_super_ddf(struct supertype *st)
{
	struct ddf_super *ddf = st->sb;
	if (ddf == NULL)
		return;
	free(ddf->phys);
	free(ddf->virt);
	while (ddf->conflist) {
		struct vcl *v = ddf->conflist;
		ddf->conflist = v->next;
		if (v->block_sizes)
			free(v->block_sizes);
		free(v);
	}
	while (ddf->dlist) {
		struct dl *d = ddf->dlist;
		ddf->dlist = d->next;
		if (d->fd >= 0)
			close(d->fd);
		if (d->spare)
			free(d->spare);
		free(d);
	}
	while (ddf->add_list) {
		struct dl *d = ddf->add_list;
		ddf->add_list = d->next;
		if (d->fd >= 0)
			close(d->fd);
		if (d->spare)
			free(d->spare);
		free(d);
	}
	free(ddf);
	st->sb = NULL;
}

static struct supertype *match_metadata_desc_ddf(char *arg)
{
	/* 'ddf' only support containers */
	struct supertype *st;
	if (strcmp(arg, "ddf") != 0 &&
	    strcmp(arg, "default") != 0
		)
		return NULL;

	st = malloc(sizeof(*st));
	memset(st, 0, sizeof(*st));
	st->container_dev = NoMdDev;
	st->ss = &super_ddf;
	st->max_devs = 512;
	st->minor_version = 0;
	st->sb = NULL;
	return st;
}


#ifndef MDASSEMBLE

static mapping_t ddf_state[] = {
	{ "Optimal", 0},
	{ "Degraded", 1},
	{ "Deleted", 2},
	{ "Missing", 3},
	{ "Failed", 4},
	{ "Partially Optimal", 5},
	{ "-reserved-", 6},
	{ "-reserved-", 7},
	{ NULL, 0}
};

static mapping_t ddf_init_state[] = {
	{ "Not Initialised", 0},
	{ "QuickInit in Progress", 1},
	{ "Fully Initialised", 2},
	{ "*UNKNOWN*", 3},
	{ NULL, 0}
};
static mapping_t ddf_access[] = {
	{ "Read/Write", 0},
	{ "Reserved", 1},
	{ "Read Only", 2},
	{ "Blocked (no access)", 3},
	{ NULL ,0}
};

static mapping_t ddf_level[] = {
	{ "RAID0", DDF_RAID0},
	{ "RAID1", DDF_RAID1},
	{ "RAID3", DDF_RAID3},
	{ "RAID4", DDF_RAID4},
	{ "RAID5", DDF_RAID5},
	{ "RAID1E",DDF_RAID1E},
	{ "JBOD",  DDF_JBOD},
	{ "CONCAT",DDF_CONCAT},
	{ "RAID5E",DDF_RAID5E},
	{ "RAID5EE",DDF_RAID5EE},
	{ "RAID6", DDF_RAID6},
	{ NULL, 0}
};
static mapping_t ddf_sec_level[] = {
	{ "Striped", DDF_2STRIPED},
	{ "Mirrored", DDF_2MIRRORED},
	{ "Concat", DDF_2CONCAT},
	{ "Spanned", DDF_2SPANNED},
	{ NULL, 0}
};
#endif

struct num_mapping {
	int num1, num2;
};
static struct num_mapping ddf_level_num[] = {
	{ DDF_RAID0, 0 },
	{ DDF_RAID1, 1 },
	{ DDF_RAID3, LEVEL_UNSUPPORTED },
	{ DDF_RAID4, 4 },
	{ DDF_RAID5, 5 },
	{ DDF_RAID1E, LEVEL_UNSUPPORTED },
	{ DDF_JBOD, LEVEL_UNSUPPORTED },
	{ DDF_CONCAT, LEVEL_LINEAR },
	{ DDF_RAID5E, LEVEL_UNSUPPORTED },
	{ DDF_RAID5EE, LEVEL_UNSUPPORTED },
	{ DDF_RAID6, 6},
	{ MAXINT, MAXINT }
};

static int map_num1(struct num_mapping *map, int num)
{
	int i;
	for (i=0 ; map[i].num1 != MAXINT; i++)
		if (map[i].num1 == num)
			break;
	return map[i].num2;
}

static int all_ff(char *guid)
{
	int i;
	for (i = 0; i < DDF_GUID_LEN; i++)
		if (guid[i] != (char)0xff)
			return 0;
	return 1;
}

#ifndef MDASSEMBLE
static void print_guid(char *guid, int tstamp)
{
	/* A GUIDs are part (or all) ASCII and part binary.
	 * They tend to be space padded.
	 * We print the GUID in HEX, then in parentheses add
	 * any initial ASCII sequence, and a possible
	 * time stamp from bytes 16-19
	 */
	int l = DDF_GUID_LEN;
	int i;

	for (i=0 ; i<DDF_GUID_LEN ; i++) {
		if ((i&3)==0 && i != 0) printf(":");
		printf("%02X", guid[i]&255);
	}

	printf("\n                  (");
	while (l && guid[l-1] == ' ')
		l--;
	for (i=0 ; i<l ; i++) {
		if (guid[i] >= 0x20 && guid[i] < 0x7f)
			fputc(guid[i], stdout);
		else
			break;
	}
	if (tstamp) {
		time_t then = __be32_to_cpu(*(__u32*)(guid+16)) + DECADE;
		char tbuf[100];
		struct tm *tm;
		tm = localtime(&then);
		strftime(tbuf, 100, " %D %T",tm);
		fputs(tbuf, stdout);
	}
	printf(")");
}

static void examine_vd(int n, struct ddf_super *sb, char *guid)
{
	int crl = sb->conf_rec_len;
	struct vcl *vcl;

	for (vcl = sb->conflist ; vcl ; vcl = vcl->next) {
		unsigned int i;
		struct vd_config *vc = &vcl->conf;

		if (calc_crc(vc, crl*512) != vc->crc)
			continue;
		if (memcmp(vc->guid, guid, DDF_GUID_LEN) != 0)
			continue;

		/* Ok, we know about this VD, let's give more details */
		printf(" Raid Devices[%d] : %d (", n,
		       __be16_to_cpu(vc->prim_elmnt_count));
		for (i = 0; i < __be16_to_cpu(vc->prim_elmnt_count); i++) {
			int j;
			int cnt = __be16_to_cpu(sb->phys->used_pdes);
			for (j=0; j<cnt; j++)
				if (vc->phys_refnum[i] == sb->phys->entries[j].refnum)
					break;
			if (i) printf(" ");
			if (j < cnt)
				printf("%d", j);
			else
				printf("--");
		}
		printf(")\n");
		if (vc->chunk_shift != 255)
		printf("   Chunk Size[%d] : %d sectors\n", n,
		       1 << vc->chunk_shift);
		printf("   Raid Level[%d] : %s\n", n,
		       map_num(ddf_level, vc->prl)?:"-unknown-");
		if (vc->sec_elmnt_count != 1) {
			printf("  Secondary Position[%d] : %d of %d\n", n,
			       vc->sec_elmnt_seq, vc->sec_elmnt_count);
			printf("  Secondary Level[%d] : %s\n", n,
			       map_num(ddf_sec_level, vc->srl) ?: "-unknown-");
		}
		printf("  Device Size[%d] : %llu\n", n,
		       (unsigned long long)__be64_to_cpu(vc->blocks)/2);
		printf("   Array Size[%d] : %llu\n", n,
		       (unsigned long long)__be64_to_cpu(vc->array_blocks)/2);
	}
}

static void examine_vds(struct ddf_super *sb)
{
	int cnt = __be16_to_cpu(sb->virt->populated_vdes);
	int i;
	printf("  Virtual Disks : %d\n", cnt);

	for (i=0; i<cnt; i++) {
		struct virtual_entry *ve = &sb->virt->entries[i];
		printf("\n");
		printf("      VD GUID[%d] : ", i); print_guid(ve->guid, 1);
		printf("\n");
		printf("         unit[%d] : %d\n", i, __be16_to_cpu(ve->unit));
		printf("        state[%d] : %s, %s%s\n", i,
		       map_num(ddf_state, ve->state & 7),
		       (ve->state & 8) ? "Morphing, ": "",
		       (ve->state & 16)? "Not Consistent" : "Consistent");
		printf("   init state[%d] : %s\n", i,
		       map_num(ddf_init_state, ve->init_state&3));
		printf("       access[%d] : %s\n", i,
		       map_num(ddf_access, (ve->init_state>>6) & 3));
		printf("         Name[%d] : %.16s\n", i, ve->name);
		examine_vd(i, sb, ve->guid);
	}
	if (cnt) printf("\n");
}

static void examine_pds(struct ddf_super *sb)
{
	int cnt = __be16_to_cpu(sb->phys->used_pdes);
	int i;
	struct dl *dl;
	printf(" Physical Disks : %d\n", cnt);
	printf("      Number    RefNo      Size       Device      Type/State\n");

	for (i=0 ; i<cnt ; i++) {
		struct phys_disk_entry *pd = &sb->phys->entries[i];
		int type = __be16_to_cpu(pd->type);
		int state = __be16_to_cpu(pd->state);

		//printf("      PD GUID[%d] : ", i); print_guid(pd->guid, 0);
		//printf("\n");
		printf("       %3d    %08x  ", i,
		       __be32_to_cpu(pd->refnum));
		printf("%8lluK ", 
		       (unsigned long long)__be64_to_cpu(pd->config_size)>>1);
		for (dl = sb->dlist; dl ; dl = dl->next) {
			if (dl->disk.refnum == pd->refnum) {
				char *dv = map_dev(dl->major, dl->minor, 0);
				if (dv) {
					printf("%-15s", dv);
					break;
				}
			}
		}
		if (!dl)
			printf("%15s","");
		printf(" %s%s%s%s%s",
		       (type&2) ? "active":"",
		       (type&4) ? "Global-Spare":"",
		       (type&8) ? "spare" : "",
		       (type&16)? ", foreign" : "",
		       (type&32)? "pass-through" : "");
		if (state & DDF_Failed)
			/* This over-rides these three */
			state &= ~(DDF_Online|DDF_Rebuilding|DDF_Transition);
		printf("/%s%s%s%s%s%s%s",
		       (state&1)? "Online": "Offline",
		       (state&2)? ", Failed": "",
		       (state&4)? ", Rebuilding": "",
		       (state&8)? ", in-transition": "",
		       (state&16)? ", SMART-errors": "",
		       (state&32)? ", Unrecovered-Read-Errors": "",
		       (state&64)? ", Missing" : "");
		printf("\n");
	}
}

static void examine_super_ddf(struct supertype *st, char *homehost)
{
	struct ddf_super *sb = st->sb;

	printf("          Magic : %08x\n", __be32_to_cpu(sb->anchor.magic));
	printf("        Version : %.8s\n", sb->anchor.revision);
	printf("Controller GUID : "); print_guid(sb->controller.guid, 0);
	printf("\n");
	printf(" Container GUID : "); print_guid(sb->anchor.guid, 1);
	printf("\n");
	printf("            Seq : %08x\n", __be32_to_cpu(sb->active->seq));
	printf("  Redundant hdr : %s\n", sb->secondary.magic == DDF_HEADER_MAGIC
	       ?"yes" : "no");
	examine_vds(sb);
	examine_pds(sb);
}

static void getinfo_super_ddf(struct supertype *st, struct mdinfo *info, char *map);

static void uuid_from_super_ddf(struct supertype *st, int uuid[4]);

static void brief_examine_super_ddf(struct supertype *st, int verbose)
{
	/* We just write a generic DDF ARRAY entry
	 */
	struct mdinfo info;
	char nbuf[64];
	getinfo_super_ddf(st, &info, NULL);
	fname_from_uuid(st, &info, nbuf, ':');

	printf("ARRAY metadata=ddf UUID=%s\n", nbuf + 5);
}

static void brief_examine_subarrays_ddf(struct supertype *st, int verbose)
{
	/* We just write a generic DDF ARRAY entry
	 */
	struct ddf_super *ddf = st->sb;
	struct mdinfo info;
	unsigned int i;
	char nbuf[64];
	getinfo_super_ddf(st, &info, NULL);
	fname_from_uuid(st, &info, nbuf, ':');

	for (i = 0; i < __be16_to_cpu(ddf->virt->max_vdes); i++) {
		struct virtual_entry *ve = &ddf->virt->entries[i];
		struct vcl vcl;
		char nbuf1[64];
		if (all_ff(ve->guid))
			continue;
		memcpy(vcl.conf.guid, ve->guid, DDF_GUID_LEN);
		ddf->currentconf =&vcl;
		uuid_from_super_ddf(st, info.uuid);
		fname_from_uuid(st, &info, nbuf1, ':');
		printf("ARRAY container=%s member=%d UUID=%s\n",
		       nbuf+5, i, nbuf1+5);
	}
}

static void export_examine_super_ddf(struct supertype *st)
{
	struct mdinfo info;
	char nbuf[64];
	getinfo_super_ddf(st, &info, NULL);
	fname_from_uuid(st, &info, nbuf, ':');
	printf("MD_METADATA=ddf\n");
	printf("MD_LEVEL=container\n");
	printf("MD_UUID=%s\n", nbuf+5);
}
	

static void detail_super_ddf(struct supertype *st, char *homehost)
{
	/* FIXME later
	 * Could print DDF GUID
	 * Need to find which array
	 *  If whole, briefly list all arrays
	 *  If one, give name
	 */
}

static void brief_detail_super_ddf(struct supertype *st)
{
	/* FIXME I really need to know which array we are detailing.
	 * Can that be stored in ddf_super??
	 */
//	struct ddf_super *ddf = st->sb;
	struct mdinfo info;
	char nbuf[64];
	getinfo_super_ddf(st, &info, NULL);
	fname_from_uuid(st, &info, nbuf,':');
	printf(" UUID=%s", nbuf + 5);
}
#endif

static int match_home_ddf(struct supertype *st, char *homehost)
{
	/* It matches 'this' host if the controller is a
	 * Linux-MD controller with vendor_data matching
	 * the hostname
	 */
	struct ddf_super *ddf = st->sb;
	unsigned int len;

	if (!homehost)
		return 0;
	len = strlen(homehost);

	return (memcmp(ddf->controller.guid, T10, 8) == 0 &&
		len < sizeof(ddf->controller.vendor_data) &&
		memcmp(ddf->controller.vendor_data, homehost,len) == 0 &&
		ddf->controller.vendor_data[len] == 0);
}

#ifndef MDASSEMBLE
static struct vd_config *find_vdcr(struct ddf_super *ddf, unsigned int inst)
{
	struct vcl *v;

	for (v = ddf->conflist; v; v = v->next)
		if (inst == v->vcnum)
			return &v->conf;
	return NULL;
}
#endif

static int find_phys(struct ddf_super *ddf, __u32 phys_refnum)
{
	/* Find the entry in phys_disk which has the given refnum
	 * and return it's index
	 */
	unsigned int i;
	for (i = 0; i < __be16_to_cpu(ddf->phys->max_pdes); i++)
		if (ddf->phys->entries[i].refnum == phys_refnum)
			return i;
	return -1;
}

static void uuid_from_super_ddf(struct supertype *st, int uuid[4])
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
	 * In the case of SVD we assume the BVD is of interest,
	 * though that might be the case if a bitmap were made for
	 * a mirrored SVD - worry about that later.
	 * So we need to find the VD configuration record for the
	 * relevant BVD and extract the GUID and Secondary_Element_Seq.
	 * The first 16 bytes of the sha1 of these is used.
	 */
	struct ddf_super *ddf = st->sb;
	struct vcl *vcl = ddf->currentconf;
	char *guid;
	char buf[20];
	struct sha1_ctx ctx;

	if (vcl)
		guid = vcl->conf.guid;
	else
		guid = ddf->anchor.guid;

	sha1_init_ctx(&ctx);
	sha1_process_bytes(guid, DDF_GUID_LEN, &ctx);
	sha1_finish_ctx(&ctx, buf);
	memcpy(uuid, buf, 4*4);
}

static void getinfo_super_ddf_bvd(struct supertype *st, struct mdinfo *info, char *map);

static void getinfo_super_ddf(struct supertype *st, struct mdinfo *info, char *map)
{
	struct ddf_super *ddf = st->sb;
	int map_disks = info->array.raid_disks;
	__u32 *cptr;

	if (ddf->currentconf) {
		getinfo_super_ddf_bvd(st, info, map);
		return;
	}
	memset(info, 0, sizeof(*info));

	info->array.raid_disks    = __be16_to_cpu(ddf->phys->used_pdes);
	info->array.level	  = LEVEL_CONTAINER;
	info->array.layout	  = 0;
	info->array.md_minor	  = -1;
	cptr = (__u32 *)(ddf->anchor.guid + 16);
	info->array.ctime	  = DECADE + __be32_to_cpu(*cptr);

	info->array.utime	  = 0;
	info->array.chunk_size	  = 0;
	info->container_enough	  = 1;


	info->disk.major = 0;
	info->disk.minor = 0;
	if (ddf->dlist) {
		info->disk.number = __be32_to_cpu(ddf->dlist->disk.refnum);
		info->disk.raid_disk = find_phys(ddf, ddf->dlist->disk.refnum);

		info->data_offset = __be64_to_cpu(ddf->phys->
					  entries[info->disk.raid_disk].
					  config_size);
		info->component_size = ddf->dlist->size - info->data_offset;
	} else {
		info->disk.number = -1;
		info->disk.raid_disk = -1;
//		info->disk.raid_disk = find refnum in the table and use index;
	}
	info->disk.state = (1 << MD_DISK_SYNC) | (1 << MD_DISK_ACTIVE);


	info->recovery_start = MaxSector;
	info->reshape_active = 0;
	info->recovery_blocked = 0;
	info->name[0] = 0;

	info->array.major_version = -1;
	info->array.minor_version = -2;
	strcpy(info->text_version, "ddf");
	info->safe_mode_delay = 0;

	uuid_from_super_ddf(st, info->uuid);

	if (map) {
		int i;
		for (i = 0 ; i < map_disks; i++) {
			if (i < info->array.raid_disks &&
			    (__be16_to_cpu(ddf->phys->entries[i].state) & DDF_Online) &&
			    !(__be16_to_cpu(ddf->phys->entries[i].state) & DDF_Failed))
				map[i] = 1;
			else
				map[i] = 0;
		}
	}
}

static int rlq_to_layout(int rlq, int prl, int raiddisks);

static void getinfo_super_ddf_bvd(struct supertype *st, struct mdinfo *info, char *map)
{
	struct ddf_super *ddf = st->sb;
	struct vcl *vc = ddf->currentconf;
	int cd = ddf->currentdev;
	int j;
	struct dl *dl;
	int map_disks = info->array.raid_disks;
	__u32 *cptr;

	memset(info, 0, sizeof(*info));
	/* FIXME this returns BVD info - what if we want SVD ?? */

	info->array.raid_disks    = __be16_to_cpu(vc->conf.prim_elmnt_count);
	info->array.level	  = map_num1(ddf_level_num, vc->conf.prl);
	info->array.layout	  = rlq_to_layout(vc->conf.rlq, vc->conf.prl,
						  info->array.raid_disks);
	info->array.md_minor	  = -1;
	cptr = (__u32 *)(vc->conf.guid + 16);
	info->array.ctime	  = DECADE + __be32_to_cpu(*cptr);
	info->array.utime	  = DECADE + __be32_to_cpu(vc->conf.timestamp);
	info->array.chunk_size	  = 512 << vc->conf.chunk_shift;
	info->custom_array_size	  = 0;

	if (cd >= 0 && (unsigned)cd < ddf->mppe) {
		info->data_offset	  = __be64_to_cpu(vc->lba_offset[cd]);
		if (vc->block_sizes)
			info->component_size = vc->block_sizes[cd];
		else
			info->component_size = __be64_to_cpu(vc->conf.blocks);
	}

	for (dl = ddf->dlist; dl ; dl = dl->next)
		if (dl->raiddisk == ddf->currentdev)
			break;

	info->disk.major = 0;
	info->disk.minor = 0;
	info->disk.state = 0;
	if (dl) {
		info->disk.major = dl->major;
		info->disk.minor = dl->minor;
		info->disk.raid_disk = dl->raiddisk;
		info->disk.number = dl->pdnum;
		info->disk.state = (1<<MD_DISK_SYNC)|(1<<MD_DISK_ACTIVE);
	}

	info->container_member = ddf->currentconf->vcnum;

	info->recovery_start = MaxSector;
	info->resync_start = 0;
	info->reshape_active = 0;
	info->recovery_blocked = 0;
	if (!(ddf->virt->entries[info->container_member].state
	      & DDF_state_inconsistent)  &&
	    (ddf->virt->entries[info->container_member].init_state
	     & DDF_initstate_mask)
	    == DDF_init_full)
		info->resync_start = MaxSector;

	uuid_from_super_ddf(st, info->uuid);

	info->array.major_version = -1;
	info->array.minor_version = -2;
	sprintf(info->text_version, "/%s/%d",
		devnum2devname(st->container_dev),
		info->container_member);
	info->safe_mode_delay = 200;

	memcpy(info->name, ddf->virt->entries[info->container_member].name, 16);
	info->name[16]=0;
	for(j=0; j<16; j++)
		if (info->name[j] == ' ')
			info->name[j] = 0;

	if (map)
		for (j = 0; j < map_disks; j++) {
			map[j] = 0;
			if (j <  info->array.raid_disks) {
				int i = find_phys(ddf, vc->conf.phys_refnum[j]);
				if (i >= 0 && 
				    (__be16_to_cpu(ddf->phys->entries[i].state) & DDF_Online) &&
				    !(__be16_to_cpu(ddf->phys->entries[i].state) & DDF_Failed))
					map[i] = 1;
			}
		}
}


static int update_super_ddf(struct supertype *st, struct mdinfo *info,
			    char *update,
			    char *devname, int verbose,
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
	 *  uuid:  Change the uuid of the array to match what is given
	 *  homehost:  update the recorded homehost
	 *  name:  update the name - preserving the homehost
	 *  _reshape_progress: record new reshape_progress position.
	 *
	 * Following are not relevant for this version:
	 *  sparc2.2 : update from old dodgey metadata
	 *  super-minor: change the preferred_minor number
	 *  summaries:  update redundant counters.
	 */
	int rv = 0;
//	struct ddf_super *ddf = st->sb;
//	struct vd_config *vd = find_vdcr(ddf, info->container_member);
//	struct virtual_entry *ve = find_ve(ddf);

	/* we don't need to handle "force-*" or "assemble" as
	 * there is no need to 'trick' the kernel.  We the metadata is
	 * first updated to activate the array, all the implied modifications
	 * will just happen.
	 */

	if (strcmp(update, "grow") == 0) {
		/* FIXME */
	} else if (strcmp(update, "resync") == 0) {
//		info->resync_checkpoint = 0;
	} else if (strcmp(update, "homehost") == 0) {
		/* homehost is stored in controller->vendor_data,
		 * or it is when we are the vendor
		 */
//		if (info->vendor_is_local)
//			strcpy(ddf->controller.vendor_data, homehost);
		rv = -1;
	} else if (strcmp(update, "name") == 0) {
		/* name is stored in virtual_entry->name */
//		memset(ve->name, ' ', 16);
//		strncpy(ve->name, info->name, 16);
		rv = -1;
	} else if (strcmp(update, "_reshape_progress") == 0) {
		/* We don't support reshape yet */
	} else if (strcmp(update, "assemble") == 0 ) {
		/* Do nothing, just succeed */
		rv = 0;
	} else
		rv = -1;

//	update_all_csum(ddf);

	return rv;
}

static void make_header_guid(char *guid)
{
	__u32 stamp;
	/* Create a DDF Header of Virtual Disk GUID */

	/* 24 bytes of fiction required.
	 * first 8 are a 'vendor-id'  - "Linux-MD"
	 * next 8 are controller type.. how about 0X DEAD BEEF 0000 0000
	 * Remaining 8 random number plus timestamp
	 */
	memcpy(guid, T10, sizeof(T10));
	stamp = __cpu_to_be32(0xdeadbeef);
	memcpy(guid+8, &stamp, 4);
	stamp = __cpu_to_be32(0);
	memcpy(guid+12, &stamp, 4);
	stamp = __cpu_to_be32(time(0) - DECADE);
	memcpy(guid+16, &stamp, 4);
	stamp = random32();
	memcpy(guid+20, &stamp, 4);
}

static int init_super_ddf_bvd(struct supertype *st,
			      mdu_array_info_t *info,
			      unsigned long long size,
			      char *name, char *homehost,
			      int *uuid);

static int init_super_ddf(struct supertype *st,
			  mdu_array_info_t *info,
			  unsigned long long size, char *name, char *homehost,
			  int *uuid)
{
	/* This is primarily called by Create when creating a new array.
	 * We will then get add_to_super called for each component, and then
	 * write_init_super called to write it out to each device.
	 * For DDF, Create can create on fresh devices or on a pre-existing
	 * array.
	 * To create on a pre-existing array a different method will be called.
	 * This one is just for fresh drives.
	 *
	 * We need to create the entire 'ddf' structure which includes:
	 *  DDF headers - these are easy.
	 *  Controller data - a Sector describing this controller .. not that
	 *                  this is a controller exactly.
	 *  Physical Disk Record - one entry per device, so
	 *			leave plenty of space.
	 *  Virtual Disk Records - again, just leave plenty of space.
	 *                   This just lists VDs, doesn't give details
	 *  Config records - describes the VDs that use this disk
	 *  DiskData  - describes 'this' device.
	 *  BadBlockManagement - empty
	 *  Diag Space - empty
	 *  Vendor Logs - Could we put bitmaps here?
	 *
	 */
	struct ddf_super *ddf;
	char hostname[17];
	int hostlen;
	int max_phys_disks, max_virt_disks;
	unsigned long long sector;
	int clen;
	int i;
	int pdsize, vdsize;
	struct phys_disk *pd;
	struct virtual_disk *vd;

	if (st->sb)
		return init_super_ddf_bvd(st, info, size, name, homehost, uuid);

	if (posix_memalign((void**)&ddf, 512, sizeof(*ddf)) != 0) {
		fprintf(stderr, Name ": %s could not allocate superblock\n", __func__);
		return 0;
	}
	memset(ddf, 0, sizeof(*ddf));
	ddf->dlist = NULL; /* no physical disks yet */
	ddf->conflist = NULL; /* No virtual disks yet */
	st->sb = ddf;

	if (info == NULL) {
		/* zeroing superblock */
		return 0;
	}

	/* At least 32MB *must* be reserved for the ddf.  So let's just
	 * start 32MB from the end, and put the primary header there.
	 * Don't do secondary for now.
	 * We don't know exactly where that will be yet as it could be
	 * different on each device.  To just set up the lengths.
	 *
	 */

	ddf->anchor.magic = DDF_HEADER_MAGIC;
	make_header_guid(ddf->anchor.guid);

	memcpy(ddf->anchor.revision, DDF_REVISION_2, 8);
	ddf->anchor.seq = __cpu_to_be32(1);
	ddf->anchor.timestamp = __cpu_to_be32(time(0) - DECADE);
	ddf->anchor.openflag = 0xFF;
	ddf->anchor.foreignflag = 0;
	ddf->anchor.enforcegroups = 0; /* Is this best?? */
	ddf->anchor.pad0 = 0xff;
	memset(ddf->anchor.pad1, 0xff, 12);
	memset(ddf->anchor.header_ext, 0xff, 32);
	ddf->anchor.primary_lba = ~(__u64)0;
	ddf->anchor.secondary_lba = ~(__u64)0;
	ddf->anchor.type = DDF_HEADER_ANCHOR;
	memset(ddf->anchor.pad2, 0xff, 3);
	ddf->anchor.workspace_len = __cpu_to_be32(32768); /* Must be reserved */
	ddf->anchor.workspace_lba = ~(__u64)0; /* Put this at bottom
						  of 32M reserved.. */
	max_phys_disks = 1023;   /* Should be enough */
	ddf->anchor.max_pd_entries = __cpu_to_be16(max_phys_disks);
	max_virt_disks = 255;
	ddf->anchor.max_vd_entries = __cpu_to_be16(max_virt_disks); /* ?? */
	ddf->anchor.max_partitions = __cpu_to_be16(64); /* ?? */
	ddf->max_part = 64;
	ddf->mppe = 256;
	ddf->conf_rec_len = 1 + ROUND_UP(ddf->mppe * (4+8), 512)/512;
	ddf->anchor.config_record_len = __cpu_to_be16(ddf->conf_rec_len);
	ddf->anchor.max_primary_element_entries = __cpu_to_be16(ddf->mppe);
	memset(ddf->anchor.pad3, 0xff, 54);
	/* controller sections is one sector long immediately
	 * after the ddf header */
	sector = 1;
	ddf->anchor.controller_section_offset = __cpu_to_be32(sector);
	ddf->anchor.controller_section_length = __cpu_to_be32(1);
	sector += 1;

	/* phys is 8 sectors after that */
	pdsize = ROUND_UP(sizeof(struct phys_disk) +
			  sizeof(struct phys_disk_entry)*max_phys_disks,
			  512);
	switch(pdsize/512) {
	case 2: case 8: case 32: case 128: case 512: break;
	default: abort();
	}
	ddf->anchor.phys_section_offset = __cpu_to_be32(sector);
	ddf->anchor.phys_section_length =
		__cpu_to_be32(pdsize/512); /* max_primary_element_entries/8 */
	sector += pdsize/512;

	/* virt is another 32 sectors */
	vdsize = ROUND_UP(sizeof(struct virtual_disk) +
			  sizeof(struct virtual_entry) * max_virt_disks,
			  512);
	switch(vdsize/512) {
	case 2: case 8: case 32: case 128: case 512: break;
	default: abort();
	}
	ddf->anchor.virt_section_offset = __cpu_to_be32(sector);
	ddf->anchor.virt_section_length =
		__cpu_to_be32(vdsize/512); /* max_vd_entries/8 */
	sector += vdsize/512;

	clen = ddf->conf_rec_len * (ddf->max_part+1);
	ddf->anchor.config_section_offset = __cpu_to_be32(sector);
	ddf->anchor.config_section_length = __cpu_to_be32(clen);
	sector += clen;

	ddf->anchor.data_section_offset = __cpu_to_be32(sector);
	ddf->anchor.data_section_length = __cpu_to_be32(1);
	sector += 1;

	ddf->anchor.bbm_section_length = __cpu_to_be32(0);
	ddf->anchor.bbm_section_offset = __cpu_to_be32(0xFFFFFFFF);
	ddf->anchor.diag_space_length = __cpu_to_be32(0);
	ddf->anchor.diag_space_offset = __cpu_to_be32(0xFFFFFFFF);
	ddf->anchor.vendor_length = __cpu_to_be32(0);
	ddf->anchor.vendor_offset = __cpu_to_be32(0xFFFFFFFF);

	memset(ddf->anchor.pad4, 0xff, 256);

	memcpy(&ddf->primary, &ddf->anchor, 512);
	memcpy(&ddf->secondary, &ddf->anchor, 512);

	ddf->primary.openflag = 1; /* I guess.. */
	ddf->primary.type = DDF_HEADER_PRIMARY;

	ddf->secondary.openflag = 1; /* I guess.. */
	ddf->secondary.type = DDF_HEADER_SECONDARY;

	ddf->active = &ddf->primary;

	ddf->controller.magic = DDF_CONTROLLER_MAGIC;

	/* 24 more bytes of fiction required.
	 * first 8 are a 'vendor-id'  - "Linux-MD"
	 * Remaining 16 are serial number.... maybe a hostname would do?
	 */
	memcpy(ddf->controller.guid, T10, sizeof(T10));
	gethostname(hostname, sizeof(hostname));
	hostname[sizeof(hostname) - 1] = 0;
	hostlen = strlen(hostname);
	memcpy(ddf->controller.guid + 24 - hostlen, hostname, hostlen);
	for (i = strlen(T10) ; i+hostlen < 24; i++)
		ddf->controller.guid[i] = ' ';

	ddf->controller.type.vendor_id = __cpu_to_be16(0xDEAD);
	ddf->controller.type.device_id = __cpu_to_be16(0xBEEF);
	ddf->controller.type.sub_vendor_id = 0;
	ddf->controller.type.sub_device_id = 0;
	memcpy(ddf->controller.product_id, "What Is My PID??", 16);
	memset(ddf->controller.pad, 0xff, 8);
	memset(ddf->controller.vendor_data, 0xff, 448);
	if (homehost && strlen(homehost) < 440)
		strcpy((char*)ddf->controller.vendor_data, homehost);

	if (posix_memalign((void**)&pd, 512, pdsize) != 0) {
		fprintf(stderr, Name ": %s could not allocate pd\n", __func__);
		return 0;
	}
	ddf->phys = pd;
	ddf->pdsize = pdsize;

	memset(pd, 0xff, pdsize);
	memset(pd, 0, sizeof(*pd));
	pd->magic = DDF_PHYS_RECORDS_MAGIC;
	pd->used_pdes = __cpu_to_be16(0);
	pd->max_pdes = __cpu_to_be16(max_phys_disks);
	memset(pd->pad, 0xff, 52);

	if (posix_memalign((void**)&vd, 512, vdsize) != 0) {
		fprintf(stderr, Name ": %s could not allocate vd\n", __func__);
		return 0;
	}
	ddf->virt = vd;
	ddf->vdsize = vdsize;
	memset(vd, 0, vdsize);
	vd->magic = DDF_VIRT_RECORDS_MAGIC;
	vd->populated_vdes = __cpu_to_be16(0);
	vd->max_vdes = __cpu_to_be16(max_virt_disks);
	memset(vd->pad, 0xff, 52);

	for (i=0; i<max_virt_disks; i++)
		memset(&vd->entries[i], 0xff, sizeof(struct virtual_entry));

	st->sb = ddf;
	ddf->updates_pending = 1;
	return 1;
}

static int chunk_to_shift(int chunksize)
{
	return ffs(chunksize/512)-1;
}

static int level_to_prl(int level)
{
	switch (level) {
	case LEVEL_LINEAR: return DDF_CONCAT;
	case 0: return DDF_RAID0;
	case 1: return DDF_RAID1;
	case 4: return DDF_RAID4;
	case 5: return DDF_RAID5;
	case 6: return DDF_RAID6;
	default: return -1;
	}
}
static int layout_to_rlq(int level, int layout, int raiddisks)
{
	switch(level) {
	case 0:
		return DDF_RAID0_SIMPLE;
	case 1:
		switch(raiddisks) {
		case 2: return DDF_RAID1_SIMPLE;
		case 3: return DDF_RAID1_MULTI;
		default: return -1;
		}
	case 4:
		switch(layout) {
		case 0: return DDF_RAID4_N;
		}
		break;
	case 5:
		switch(layout) {
		case ALGORITHM_LEFT_ASYMMETRIC:
			return DDF_RAID5_N_RESTART;
		case ALGORITHM_RIGHT_ASYMMETRIC:
			return DDF_RAID5_0_RESTART;
		case ALGORITHM_LEFT_SYMMETRIC:
			return DDF_RAID5_N_CONTINUE;
		case ALGORITHM_RIGHT_SYMMETRIC:
			return -1; /* not mentioned in standard */
		}
	case 6:
		switch(layout) {
		case ALGORITHM_ROTATING_N_RESTART:
			return DDF_RAID5_N_RESTART;
		case ALGORITHM_ROTATING_ZERO_RESTART:
			return DDF_RAID6_0_RESTART;
		case ALGORITHM_ROTATING_N_CONTINUE:
			return DDF_RAID5_N_CONTINUE;
		}
	}
	return -1;
}

static int rlq_to_layout(int rlq, int prl, int raiddisks)
{
	switch(prl) {
	case DDF_RAID0:
		return 0; /* hopefully rlq == DDF_RAID0_SIMPLE */
	case DDF_RAID1:
		return 0; /* hopefully rlq == SIMPLE or MULTI depending
			     on raiddisks*/
	case DDF_RAID4:
		switch(rlq) {
		case DDF_RAID4_N:
			return 0;
		default:
			/* not supported */
			return -1; /* FIXME this isn't checked */
		}
	case DDF_RAID5:
		switch(rlq) {
		case DDF_RAID5_N_RESTART:
			return ALGORITHM_LEFT_ASYMMETRIC;
		case DDF_RAID5_0_RESTART:
			return ALGORITHM_RIGHT_ASYMMETRIC;
		case DDF_RAID5_N_CONTINUE:
			return ALGORITHM_LEFT_SYMMETRIC;
		default:
			return -1;
		}
	case DDF_RAID6:
		switch(rlq) {
		case DDF_RAID5_N_RESTART:
			return ALGORITHM_ROTATING_N_RESTART;
		case DDF_RAID6_0_RESTART:
			return ALGORITHM_ROTATING_ZERO_RESTART;
		case DDF_RAID5_N_CONTINUE:
			return ALGORITHM_ROTATING_N_CONTINUE;
		default:
			return -1;
		}
	}
	return -1;
}

#ifndef MDASSEMBLE
struct extent {
	unsigned long long start, size;
};
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

static struct extent *get_extents(struct ddf_super *ddf, struct dl *dl)
{
	/* find a list of used extents on the give physical device
	 * (dnum) of the given ddf.
	 * Return a malloced array of 'struct extent'

FIXME ignore DDF_Legacy devices?

	 */
	struct extent *rv;
	int n = 0;
	unsigned int i, j;

	rv = malloc(sizeof(struct extent) * (ddf->max_part + 2));
	if (!rv)
		return NULL;

	for (i = 0; i < ddf->max_part; i++) {
		struct vcl *v = dl->vlist[i];
		if (v == NULL)
			continue;
		for (j = 0; j < v->conf.prim_elmnt_count; j++)
			if (v->conf.phys_refnum[j] == dl->disk.refnum) {
				/* This device plays role 'j' in  'v'. */
				rv[n].start = __be64_to_cpu(v->lba_offset[j]);
				rv[n].size = __be64_to_cpu(v->conf.blocks);
				n++;
				break;
			}
	}
	qsort(rv, n, sizeof(*rv), cmp_extent);

	rv[n].start = __be64_to_cpu(ddf->phys->entries[dl->pdnum].config_size);
	rv[n].size = 0;
	return rv;
}
#endif

static int init_super_ddf_bvd(struct supertype *st,
			      mdu_array_info_t *info,
			      unsigned long long size,
			      char *name, char *homehost,
			      int *uuid)
{
	/* We are creating a BVD inside a pre-existing container.
	 * so st->sb is already set.
	 * We need to create a new vd_config and a new virtual_entry
	 */
	struct ddf_super *ddf = st->sb;
	unsigned int venum;
	struct virtual_entry *ve;
	struct vcl *vcl;
	struct vd_config *vc;

	if (__be16_to_cpu(ddf->virt->populated_vdes)
	    >= __be16_to_cpu(ddf->virt->max_vdes)) {
		fprintf(stderr, Name": This ddf already has the "
			"maximum of %d virtual devices\n",
			__be16_to_cpu(ddf->virt->max_vdes));
		return 0;
	}

	if (name)
		for (venum = 0; venum < __be16_to_cpu(ddf->virt->max_vdes); venum++)
			if (!all_ff(ddf->virt->entries[venum].guid)) {
				char *n = ddf->virt->entries[venum].name;

				if (strncmp(name, n, 16) == 0) {
					fprintf(stderr, Name ": This ddf already"
						" has an array called %s\n",
						name);
					return 0;
				}
			}

	for (venum = 0; venum < __be16_to_cpu(ddf->virt->max_vdes); venum++)
		if (all_ff(ddf->virt->entries[venum].guid))
			break;
	if (venum == __be16_to_cpu(ddf->virt->max_vdes)) {
		fprintf(stderr, Name ": Cannot find spare slot for "
			"virtual disk - DDF is corrupt\n");
		return 0;
	}
	ve = &ddf->virt->entries[venum];

	/* A Virtual Disk GUID contains the T10 Vendor ID, controller type,
	 * timestamp, random number
	 */
	make_header_guid(ve->guid);
	ve->unit = __cpu_to_be16(info->md_minor);
	ve->pad0 = 0xFFFF;
	ve->guid_crc = crc32(0, (unsigned char*)ddf->anchor.guid, DDF_GUID_LEN);
	ve->type = 0;
	ve->state = DDF_state_degraded; /* Will be modified as devices are added */
	if (info->state & 1) /* clean */
		ve->init_state = DDF_init_full;
	else
		ve->init_state = DDF_init_not;

	memset(ve->pad1, 0xff, 14);
	memset(ve->name, ' ', 16);
	if (name)
		strncpy(ve->name, name, 16);
	ddf->virt->populated_vdes =
		__cpu_to_be16(__be16_to_cpu(ddf->virt->populated_vdes)+1);

	/* Now create a new vd_config */
	if (posix_memalign((void**)&vcl, 512,
		           (offsetof(struct vcl, conf) + ddf->conf_rec_len * 512)) != 0) {
		fprintf(stderr, Name ": %s could not allocate vd_config\n", __func__);
		return 0;
	}
	vcl->lba_offset = (__u64*) &vcl->conf.phys_refnum[ddf->mppe];
	vcl->vcnum = venum;
	vcl->block_sizes = NULL; /* FIXME not for CONCAT */

	vc = &vcl->conf;

	vc->magic = DDF_VD_CONF_MAGIC;
	memcpy(vc->guid, ve->guid, DDF_GUID_LEN);
	vc->timestamp = __cpu_to_be32(time(0)-DECADE);
	vc->seqnum = __cpu_to_be32(1);
	memset(vc->pad0, 0xff, 24);
	vc->prim_elmnt_count = __cpu_to_be16(info->raid_disks);
	vc->chunk_shift = chunk_to_shift(info->chunk_size);
	vc->prl = level_to_prl(info->level);
	vc->rlq = layout_to_rlq(info->level, info->layout, info->raid_disks);
	vc->sec_elmnt_count = 1;
	vc->sec_elmnt_seq = 0;
	vc->srl = 0;
	vc->blocks = __cpu_to_be64(info->size * 2);
	vc->array_blocks = __cpu_to_be64(
		calc_array_size(info->level, info->raid_disks, info->layout,
				info->chunk_size, info->size*2));
	memset(vc->pad1, 0xff, 8);
	vc->spare_refs[0] = 0xffffffff;
	vc->spare_refs[1] = 0xffffffff;
	vc->spare_refs[2] = 0xffffffff;
	vc->spare_refs[3] = 0xffffffff;
	vc->spare_refs[4] = 0xffffffff;
	vc->spare_refs[5] = 0xffffffff;
	vc->spare_refs[6] = 0xffffffff;
	vc->spare_refs[7] = 0xffffffff;
	memset(vc->cache_pol, 0, 8);
	vc->bg_rate = 0x80;
	memset(vc->pad2, 0xff, 3);
	memset(vc->pad3, 0xff, 52);
	memset(vc->pad4, 0xff, 192);
	memset(vc->v0, 0xff, 32);
	memset(vc->v1, 0xff, 32);
	memset(vc->v2, 0xff, 16);
	memset(vc->v3, 0xff, 16);
	memset(vc->vendor, 0xff, 32);

	memset(vc->phys_refnum, 0xff, 4*ddf->mppe);
	memset(vc->phys_refnum+ddf->mppe, 0x00, 8*ddf->mppe);

	vcl->next = ddf->conflist;
	ddf->conflist = vcl;
	ddf->currentconf = vcl;
	ddf->updates_pending = 1;
	return 1;
}

#ifndef MDASSEMBLE
static void add_to_super_ddf_bvd(struct supertype *st,
				 mdu_disk_info_t *dk, int fd, char *devname)
{
	/* fd and devname identify a device with-in the ddf container (st).
	 * dk identifies a location in the new BVD.
	 * We need to find suitable free space in that device and update
	 * the phys_refnum and lba_offset for the newly created vd_config.
	 * We might also want to update the type in the phys_disk
	 * section.
	 *
	 * Alternately: fd == -1 and we have already chosen which device to
	 * use and recorded in dlist->raid_disk;
	 */
	struct dl *dl;
	struct ddf_super *ddf = st->sb;
	struct vd_config *vc;
	__u64 *lba_offset;
	unsigned int working;
	unsigned int i;
	unsigned long long blocks, pos, esize;
	struct extent *ex;

	if (fd == -1) {
		for (dl = ddf->dlist; dl ; dl = dl->next)
			if (dl->raiddisk == dk->raid_disk)
				break;
	} else {
		for (dl = ddf->dlist; dl ; dl = dl->next)
			if (dl->major == dk->major &&
			    dl->minor == dk->minor)
				break;
	}
	if (!dl || ! (dk->state & (1<<MD_DISK_SYNC)))
		return;

	vc = &ddf->currentconf->conf;
	lba_offset = ddf->currentconf->lba_offset;

	ex = get_extents(ddf, dl);
	if (!ex)
		return;

	i = 0; pos = 0;
	blocks = __be64_to_cpu(vc->blocks);
	if (ddf->currentconf->block_sizes)
		blocks = ddf->currentconf->block_sizes[dk->raid_disk];

	do {
		esize = ex[i].start - pos;
		if (esize >= blocks)
			break;
		pos = ex[i].start + ex[i].size;
		i++;
	} while (ex[i-1].size);

	free(ex);
	if (esize < blocks)
		return;

	ddf->currentdev = dk->raid_disk;
	vc->phys_refnum[dk->raid_disk] = dl->disk.refnum;
	lba_offset[dk->raid_disk] = __cpu_to_be64(pos);

	for (i = 0; i < ddf->max_part ; i++)
		if (dl->vlist[i] == NULL)
			break;
	if (i == ddf->max_part)
		return;
	dl->vlist[i] = ddf->currentconf;

	if (fd >= 0)
		dl->fd = fd;
	if (devname)
		dl->devname = devname;

	/* Check how many working raid_disks, and if we can mark
	 * array as optimal yet
	 */
	working = 0;

	for (i = 0; i < __be16_to_cpu(vc->prim_elmnt_count); i++)
		if (vc->phys_refnum[i] != 0xffffffff)
			working++;

	/* Find which virtual_entry */
	i = ddf->currentconf->vcnum;
	if (working == __be16_to_cpu(vc->prim_elmnt_count))
		ddf->virt->entries[i].state =
			(ddf->virt->entries[i].state & ~DDF_state_mask)
			| DDF_state_optimal;

	if (vc->prl == DDF_RAID6 &&
	    working+1 == __be16_to_cpu(vc->prim_elmnt_count))
		ddf->virt->entries[i].state =
			(ddf->virt->entries[i].state & ~DDF_state_mask)
			| DDF_state_part_optimal;

	ddf->phys->entries[dl->pdnum].type &= ~__cpu_to_be16(DDF_Global_Spare);
	ddf->phys->entries[dl->pdnum].type |= __cpu_to_be16(DDF_Active_in_VD);
	ddf->updates_pending = 1;
}

/* add a device to a container, either while creating it or while
 * expanding a pre-existing container
 */
static int add_to_super_ddf(struct supertype *st,
			     mdu_disk_info_t *dk, int fd, char *devname)
{
	struct ddf_super *ddf = st->sb;
	struct dl *dd;
	time_t now;
	struct tm *tm;
	unsigned long long size;
	struct phys_disk_entry *pde;
	unsigned int n, i;
	struct stat stb;
	__u32 *tptr;

	if (ddf->currentconf) {
		add_to_super_ddf_bvd(st, dk, fd, devname);
		return 0;
	}

	/* This is device numbered dk->number.  We need to create
	 * a phys_disk entry and a more detailed disk_data entry.
	 */
	fstat(fd, &stb);
	if (posix_memalign((void**)&dd, 512,
		           sizeof(*dd) + sizeof(dd->vlist[0]) * ddf->max_part) != 0) {
		fprintf(stderr, Name
			": %s could allocate buffer for new disk, aborting\n",
			__func__);
		return 1;
	}
	dd->major = major(stb.st_rdev);
	dd->minor = minor(stb.st_rdev);
	dd->devname = devname;
	dd->fd = fd;
	dd->spare = NULL;

	dd->disk.magic = DDF_PHYS_DATA_MAGIC;
	now = time(0);
	tm = localtime(&now);
	sprintf(dd->disk.guid, "%8s%04d%02d%02d",
		T10, tm->tm_year+1900, tm->tm_mon+1, tm->tm_mday);
	tptr = (__u32 *)(dd->disk.guid + 16);
	*tptr++ = random32();
	*tptr = random32();

	do {
		/* Cannot be bothered finding a CRC of some irrelevant details*/
		dd->disk.refnum = random32();
		for (i = __be16_to_cpu(ddf->active->max_pd_entries);
		     i > 0; i--)
			if (ddf->phys->entries[i-1].refnum == dd->disk.refnum)
				break;
	} while (i > 0);

	dd->disk.forced_ref = 1;
	dd->disk.forced_guid = 1;
	memset(dd->disk.vendor, ' ', 32);
	memcpy(dd->disk.vendor, "Linux", 5);
	memset(dd->disk.pad, 0xff, 442);
	for (i = 0; i < ddf->max_part ; i++)
		dd->vlist[i] = NULL;

	n = __be16_to_cpu(ddf->phys->used_pdes);
	pde = &ddf->phys->entries[n];
	dd->pdnum = n;

	if (st->update_tail) {
		int len = (sizeof(struct phys_disk) +
			   sizeof(struct phys_disk_entry));
		struct phys_disk *pd;

		pd = malloc(len);
		pd->magic = DDF_PHYS_RECORDS_MAGIC;
		pd->used_pdes = __cpu_to_be16(n);
		pde = &pd->entries[0];
		dd->mdupdate = pd;
	} else {
		n++;
		ddf->phys->used_pdes = __cpu_to_be16(n);
	}

	memcpy(pde->guid, dd->disk.guid, DDF_GUID_LEN);
	pde->refnum = dd->disk.refnum;
	pde->type = __cpu_to_be16(DDF_Forced_PD_GUID | DDF_Global_Spare);
	pde->state = __cpu_to_be16(DDF_Online);
	get_dev_size(fd, NULL, &size);
	/* We are required to reserve 32Meg, and record the size in sectors */
	pde->config_size = __cpu_to_be64( (size - 32*1024*1024) / 512);
	sprintf(pde->path, "%17.17s","Information: nil") ;
	memset(pde->pad, 0xff, 6);

	dd->size = size >> 9;
	if (st->update_tail) {
		dd->next = ddf->add_list;
		ddf->add_list = dd;
	} else {
		dd->next = ddf->dlist;
		ddf->dlist = dd;
		ddf->updates_pending = 1;
	}

	return 0;
}

static int remove_from_super_ddf(struct supertype *st, mdu_disk_info_t *dk)
{
	struct ddf_super *ddf = st->sb;
	struct dl *dl;

	/* mdmon has noticed that this disk (dk->major/dk->minor) has
	 * disappeared from the container.
	 * We need to arrange that it disappears from the metadata and
	 * internal data structures too.
	 * Most of the work is done by ddf_process_update which edits
	 * the metadata and closes the file handle and attaches the memory
	 * where free_updates will free it.
	 */
	for (dl = ddf->dlist; dl ; dl = dl->next)
		if (dl->major == dk->major &&
		    dl->minor == dk->minor)
			break;
	if (!dl)
		return -1;

	if (st->update_tail) {
		int len = (sizeof(struct phys_disk) +
			   sizeof(struct phys_disk_entry));
		struct phys_disk *pd;

		pd = malloc(len);
		pd->magic = DDF_PHYS_RECORDS_MAGIC;
		pd->used_pdes = __cpu_to_be16(dl->pdnum);
		pd->entries[0].state = __cpu_to_be16(DDF_Missing);
		append_metadata_update(st, pd, len);
	}
	return 0;
}

/*
 * This is the write_init_super method for a ddf container.  It is
 * called when creating a container or adding another device to a
 * container.
 */
#define NULL_CONF_SZ	4096

static int __write_init_super_ddf(struct supertype *st)
{

	struct ddf_super *ddf = st->sb;
	int i;
	struct dl *d;
	int n_config;
	int conf_size;
	int attempts = 0;
	int successes = 0;
	unsigned long long size, sector;
	char *null_aligned;

	if (posix_memalign((void**)&null_aligned, 4096, NULL_CONF_SZ) != 0) {
		return -ENOMEM;
	}
	memset(null_aligned, 0xff, NULL_CONF_SZ);

	/* try to write updated metadata,
	 * if we catch a failure move on to the next disk
	 */
	for (d = ddf->dlist; d; d=d->next) {
		int fd = d->fd;

		if (fd < 0)
			continue;

		attempts++;
		/* We need to fill in the primary, (secondary) and workspace
		 * lba's in the headers, set their checksums,
		 * Also checksum phys, virt....
		 *
		 * Then write everything out, finally the anchor is written.
		 */
		get_dev_size(fd, NULL, &size);
		size /= 512;
		ddf->anchor.workspace_lba = __cpu_to_be64(size - 32*1024*2);
		ddf->anchor.primary_lba = __cpu_to_be64(size - 16*1024*2);
		ddf->anchor.seq = __cpu_to_be32(1);
		memcpy(&ddf->primary, &ddf->anchor, 512);
		memcpy(&ddf->secondary, &ddf->anchor, 512);

		ddf->anchor.openflag = 0xFF; /* 'open' means nothing */
		ddf->anchor.seq = 0xFFFFFFFF; /* no sequencing in anchor */
		ddf->anchor.crc = calc_crc(&ddf->anchor, 512);

		ddf->primary.openflag = 0;
		ddf->primary.type = DDF_HEADER_PRIMARY;

		ddf->secondary.openflag = 0;
		ddf->secondary.type = DDF_HEADER_SECONDARY;

		ddf->primary.crc = calc_crc(&ddf->primary, 512);
		ddf->secondary.crc = calc_crc(&ddf->secondary, 512);

		sector = size - 16*1024*2;
		lseek64(fd, sector<<9, 0);
		if (write(fd, &ddf->primary, 512) < 0)
			continue;

		ddf->controller.crc = calc_crc(&ddf->controller, 512);
		if (write(fd, &ddf->controller, 512) < 0)
			continue;

		ddf->phys->crc = calc_crc(ddf->phys, ddf->pdsize);

		if (write(fd, ddf->phys, ddf->pdsize) < 0)
			continue;

		ddf->virt->crc = calc_crc(ddf->virt, ddf->vdsize);
		if (write(fd, ddf->virt, ddf->vdsize) < 0)
			continue;

		/* Now write lots of config records. */
		n_config = ddf->max_part;
		conf_size = ddf->conf_rec_len * 512;
		for (i = 0 ; i <= n_config ; i++) {
			struct vcl *c = d->vlist[i];
			if (i == n_config)
				c = (struct vcl*)d->spare;

			if (c) {
				c->conf.crc = calc_crc(&c->conf, conf_size);
				if (write(fd, &c->conf, conf_size) < 0)
					break;
			} else {
				unsigned int togo = conf_size;
				while (togo > NULL_CONF_SZ) {
					if (write(fd, null_aligned, NULL_CONF_SZ) < 0)
						break;
					togo -= NULL_CONF_SZ;
				}
				if (write(fd, null_aligned, togo) < 0)
					break;
			}
		}
		if (i <= n_config)
			continue;
		d->disk.crc = calc_crc(&d->disk, 512);
		if (write(fd, &d->disk, 512) < 0)
			continue;

		/* Maybe do the same for secondary */

		lseek64(fd, (size-1)*512, SEEK_SET);
		if (write(fd, &ddf->anchor, 512) < 0)
			continue;
		successes++;
	}
	free(null_aligned);

	return attempts != successes;
}

static int write_init_super_ddf(struct supertype *st)
{
	struct ddf_super *ddf = st->sb;
	struct vcl *currentconf = ddf->currentconf;

	/* we are done with currentconf reset it to point st at the container */
	ddf->currentconf = NULL;

	if (st->update_tail) {
		/* queue the virtual_disk and vd_config as metadata updates */
		struct virtual_disk *vd;
		struct vd_config *vc;
		int len;

		if (!currentconf) {
			int len = (sizeof(struct phys_disk) +
				   sizeof(struct phys_disk_entry));

			/* adding a disk to the container. */
			if (!ddf->add_list)
				return 0;

			append_metadata_update(st, ddf->add_list->mdupdate, len);
			ddf->add_list->mdupdate = NULL;
			return 0;
		}

		/* Newly created VD */

		/* First the virtual disk.  We have a slightly fake header */
		len = sizeof(struct virtual_disk) + sizeof(struct virtual_entry);
		vd = malloc(len);
		*vd = *ddf->virt;
		vd->entries[0] = ddf->virt->entries[currentconf->vcnum];
		vd->populated_vdes = __cpu_to_be16(currentconf->vcnum);
		append_metadata_update(st, vd, len);

		/* Then the vd_config */
		len = ddf->conf_rec_len * 512;
		vc = malloc(len);
		memcpy(vc, &currentconf->conf, len);
		append_metadata_update(st, vc, len);

		/* FIXME I need to close the fds! */
		return 0;
	} else {	
		struct dl *d;
		for (d = ddf->dlist; d; d=d->next)
			while (Kill(d->devname, NULL, 0, 1, 1) == 0);
		return __write_init_super_ddf(st);
	}
}

#endif

static __u64 avail_size_ddf(struct supertype *st, __u64 devsize)
{
	/* We must reserve the last 32Meg */
	if (devsize <= 32*1024*2)
		return 0;
	return devsize - 32*1024*2;
}

#ifndef MDASSEMBLE

static int reserve_space(struct supertype *st, int raiddisks,
			 unsigned long long size, int chunk,
			 unsigned long long *freesize)
{
	/* Find 'raiddisks' spare extents at least 'size' big (but
	 * only caring about multiples of 'chunk') and remember
	 * them.
	 * If the cannot be found, fail.
	 */
	struct dl *dl;
	struct ddf_super *ddf = st->sb;
	int cnt = 0;

	for (dl = ddf->dlist; dl ; dl=dl->next) {
		dl->raiddisk = -1;	
		dl->esize = 0;
	}
	/* Now find largest extent on each device */
	for (dl = ddf->dlist ; dl ; dl=dl->next) {
		struct extent *e = get_extents(ddf, dl);
		unsigned long long pos = 0;
		int i = 0;
		int found = 0;
		unsigned long long minsize = size;

		if (size == 0)
			minsize = chunk;

		if (!e)
			continue;
		do {
			unsigned long long esize;
			esize = e[i].start - pos;
			if (esize >= minsize) {
				found = 1;
				minsize = esize;
			}
			pos = e[i].start + e[i].size;
			i++;
		} while (e[i-1].size);
		if (found) {
			cnt++;
			dl->esize = minsize;
		}
		free(e);
	}
	if (cnt < raiddisks) {
		fprintf(stderr, Name ": not enough devices with space to create array.\n");
		return 0; /* No enough free spaces large enough */
	}
	if (size == 0) {
		/* choose the largest size of which there are at least 'raiddisk' */
		for (dl = ddf->dlist ; dl ; dl=dl->next) {
			struct dl *dl2;
			if (dl->esize <= size)
				continue;
			/* This is bigger than 'size', see if there are enough */
			cnt = 0;
			for (dl2 = ddf->dlist; dl2 ; dl2=dl2->next)
				if (dl2->esize >= dl->esize)
					cnt++;
			if (cnt >= raiddisks)
				size = dl->esize;
		}
		if (chunk) {
			size = size / chunk;
			size *= chunk;
		}
		*freesize = size;
		if (size < 32) {
			fprintf(stderr, Name ": not enough spare devices to create array.\n");
			return 0;
		}
	}
	/* We have a 'size' of which there are enough spaces.
	 * We simply do a first-fit */
	cnt = 0;
	for (dl = ddf->dlist ; dl && cnt < raiddisks ; dl=dl->next) {
		if (dl->esize < size)
			continue;
		
		dl->raiddisk = cnt;
		cnt++;
	}
	return 1;
}



static int
validate_geometry_ddf_container(struct supertype *st,
				int level, int layout, int raiddisks,
				int chunk, unsigned long long size,
				char *dev, unsigned long long *freesize,
				int verbose);

static int validate_geometry_ddf_bvd(struct supertype *st,
				     int level, int layout, int raiddisks,
				     int *chunk, unsigned long long size,
				     char *dev, unsigned long long *freesize,
				     int verbose);

static int validate_geometry_ddf(struct supertype *st,
				 int level, int layout, int raiddisks,
				 int *chunk, unsigned long long size,
				 char *dev, unsigned long long *freesize,
				 int verbose)
{
	int fd;
	struct mdinfo *sra;
	int cfd;

	/* ddf potentially supports lots of things, but it depends on
	 * what devices are offered (and maybe kernel version?)
	 * If given unused devices, we will make a container.
	 * If given devices in a container, we will make a BVD.
	 * If given BVDs, we make an SVD, changing all the GUIDs in the process.
	 */

	if (chunk && *chunk == UnSet)
		*chunk = DEFAULT_CHUNK;


	if (level == LEVEL_CONTAINER) {
		/* Must be a fresh device to add to a container */
		return validate_geometry_ddf_container(st, level, layout,
						       raiddisks, chunk?*chunk:0,
						       size, dev, freesize,
						       verbose);
	}

	if (!dev) {
		/* Initial sanity check.  Exclude illegal levels. */
		int i;
		for (i=0; ddf_level_num[i].num1 != MAXINT; i++)
			if (ddf_level_num[i].num2 == level)
				break;
		if (ddf_level_num[i].num1 == MAXINT) {
			if (verbose)
				fprintf(stderr, Name ": DDF does not support level %d arrays\n",
					level);
			return 0;
		}
		/* Should check layout? etc */

		if (st->sb && freesize) {
			/* --create was given a container to create in.
			 * So we need to check that there are enough
			 * free spaces and return the amount of space.
			 * We may as well remember which drives were
			 * chosen so that add_to_super/getinfo_super
			 * can return them.
			 */
			return reserve_space(st, raiddisks, size, chunk?*chunk:0, freesize);
		}
		return 1;
	}

	if (st->sb) {
		/* A container has already been opened, so we are
		 * creating in there.  Maybe a BVD, maybe an SVD.
		 * Should make a distinction one day.
		 */
		return validate_geometry_ddf_bvd(st, level, layout, raiddisks,
						 chunk, size, dev, freesize,
						 verbose);
	}
	/* This is the first device for the array.
	 * If it is a container, we read it in and do automagic allocations,
	 * no other devices should be given.
	 * Otherwise it must be a member device of a container, and we
	 * do manual allocation.
	 * Later we should check for a BVD and make an SVD.
	 */
	fd = open(dev, O_RDONLY|O_EXCL, 0);
	if (fd >= 0) {
		sra = sysfs_read(fd, 0, GET_VERSION);
		close(fd);
		if (sra && sra->array.major_version == -1 &&
		    strcmp(sra->text_version, "ddf") == 0) {

			/* load super */
			/* find space for 'n' devices. */
			/* remember the devices */
			/* Somehow return the fact that we have enough */
		}

		if (verbose)
			fprintf(stderr,
				Name ": ddf: Cannot create this array "
				"on device %s - a container is required.\n",
				dev);
		return 0;
	}
	if (errno != EBUSY || (fd = open(dev, O_RDONLY, 0)) < 0) {
		if (verbose)
			fprintf(stderr, Name ": ddf: Cannot open %s: %s\n",
				dev, strerror(errno));
		return 0;
	}
	/* Well, it is in use by someone, maybe a 'ddf' container. */
	cfd = open_container(fd);
	if (cfd < 0) {
		close(fd);
		if (verbose)
			fprintf(stderr, Name ": ddf: Cannot use %s: %s\n",
				dev, strerror(EBUSY));
		return 0;
	}
	sra = sysfs_read(cfd, 0, GET_VERSION);
	close(fd);
	if (sra && sra->array.major_version == -1 &&
	    strcmp(sra->text_version, "ddf") == 0) {
		/* This is a member of a ddf container.  Load the container
		 * and try to create a bvd
		 */
		struct ddf_super *ddf;
		if (load_super_ddf_all(st, cfd, (void **)&ddf, NULL) == 0) {
			st->sb = ddf;
			st->container_dev = fd2devnum(cfd);
			close(cfd);
			return validate_geometry_ddf_bvd(st, level, layout,
							 raiddisks, chunk, size,
							 dev, freesize,
							 verbose);
		}
		close(cfd);
	} else /* device may belong to a different container */
		return 0;

	return 1;
}

static int
validate_geometry_ddf_container(struct supertype *st,
				int level, int layout, int raiddisks,
				int chunk, unsigned long long size,
				char *dev, unsigned long long *freesize,
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
			fprintf(stderr, Name ": ddf: Cannot open %s: %s\n",
				dev, strerror(errno));
		return 0;
	}
	if (!get_dev_size(fd, dev, &ldsize)) {
		close(fd);
		return 0;
	}
	close(fd);

	*freesize = avail_size_ddf(st, ldsize >> 9);
	if (*freesize == 0)
		return 0;

	return 1;
}

static int validate_geometry_ddf_bvd(struct supertype *st,
				     int level, int layout, int raiddisks,
				     int *chunk, unsigned long long size,
				     char *dev, unsigned long long *freesize,
				     int verbose)
{
	struct stat stb;
	struct ddf_super *ddf = st->sb;
	struct dl *dl;
	unsigned long long pos = 0;
	unsigned long long maxsize;
	struct extent *e;
	int i;
	/* ddf/bvd supports lots of things, but not containers */
	if (level == LEVEL_CONTAINER) {
		if (verbose)
			fprintf(stderr, Name ": DDF cannot create a container within an container\n");
		return 0;
	}
	/* We must have the container info already read in. */
	if (!ddf)
		return 0;

	if (!dev) {
		/* General test:  make sure there is space for
		 * 'raiddisks' device extents of size 'size'.
		 */
		unsigned long long minsize = size;
		int dcnt = 0;
		if (minsize == 0)
			minsize = 8;
		for (dl = ddf->dlist; dl ; dl = dl->next)
		{
			int found = 0;
			pos = 0;

			i = 0;
			e = get_extents(ddf, dl);
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
				fprintf(stderr,
					Name ": ddf: Not enough devices with "
					"space for this array (%d < %d)\n",
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
	for (dl = ddf->dlist ; dl ; dl = dl->next) {
		if (dl->major == (int)major(stb.st_rdev) &&
		    dl->minor == (int)minor(stb.st_rdev))
			break;
	}
	if (!dl) {
		if (verbose)
			fprintf(stderr, Name ": ddf: %s is not in the "
				"same DDF set\n",
				dev);
		return 0;
	}
	e = get_extents(ddf, dl);
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
	// FIXME here I am

	return 1;
}

static int load_super_ddf_all(struct supertype *st, int fd,
			      void **sbp, char *devname)
{
	struct mdinfo *sra;
	struct ddf_super *super;
	struct mdinfo *sd, *best = NULL;
	int bestseq = 0;
	int seq;
	char nm[20];
	int dfd;

	sra = sysfs_read(fd, 0, GET_LEVEL|GET_VERSION|GET_DEVS|GET_STATE);
	if (!sra)
		return 1;
	if (sra->array.major_version != -1 ||
	    sra->array.minor_version != -2 ||
	    strcmp(sra->text_version, "ddf") != 0)
		return 1;

	if (posix_memalign((void**)&super, 512, sizeof(*super)) != 0)
		return 1;
	memset(super, 0, sizeof(*super));

	/* first, try each device, and choose the best ddf */
	for (sd = sra->devs ; sd ; sd = sd->next) {
		int rv;
		sprintf(nm, "%d:%d", sd->disk.major, sd->disk.minor);
		dfd = dev_open(nm, O_RDONLY);
		if (dfd < 0)
			return 2;
		rv = load_ddf_headers(dfd, super, NULL);
		close(dfd);
		if (rv == 0) {
			seq = __be32_to_cpu(super->active->seq);
			if (super->active->openflag)
				seq--;
			if (!best || seq > bestseq) {
				bestseq = seq;
				best = sd;
			}
		}
	}
	if (!best)
		return 1;
	/* OK, load this ddf */
	sprintf(nm, "%d:%d", best->disk.major, best->disk.minor);
	dfd = dev_open(nm, O_RDONLY);
	if (dfd < 0)
		return 1;
	load_ddf_headers(dfd, super, NULL);
	load_ddf_global(dfd, super, NULL);
	close(dfd);
	/* Now we need the device-local bits */
	for (sd = sra->devs ; sd ; sd = sd->next) {
		int rv;

		sprintf(nm, "%d:%d", sd->disk.major, sd->disk.minor);
		dfd = dev_open(nm, O_RDWR);
		if (dfd < 0)
			return 2;
		rv = load_ddf_headers(dfd, super, NULL);
		if (rv == 0)
			rv = load_ddf_local(dfd, super, NULL, 1);
		if (rv)
			return 1;
	}

	*sbp = super;
	if (st->ss == NULL) {
		st->ss = &super_ddf;
		st->minor_version = 0;
		st->max_devs = 512;
	}
	st->container_dev = fd2devnum(fd);
	return 0;
}

static int load_container_ddf(struct supertype *st, int fd,
			      char *devname)
{
	return load_super_ddf_all(st, fd, &st->sb, devname);
}

#endif /* MDASSEMBLE */

static struct mdinfo *container_content_ddf(struct supertype *st, char *subarray)
{
	/* Given a container loaded by load_super_ddf_all,
	 * extract information about all the arrays into
	 * an mdinfo tree.
	 *
	 * For each vcl in conflist: create an mdinfo, fill it in,
	 *  then look for matching devices (phys_refnum) in dlist
	 *  and create appropriate device mdinfo.
	 */
	struct ddf_super *ddf = st->sb;
	struct mdinfo *rest = NULL;
	struct vcl *vc;

	for (vc = ddf->conflist ; vc ; vc=vc->next)
	{
		unsigned int i;
		unsigned int j;
		struct mdinfo *this;
		char *ep;
		__u32 *cptr;

		if (subarray &&
		    (strtoul(subarray, &ep, 10) != vc->vcnum ||
		     *ep != '\0'))
			continue;

		this = malloc(sizeof(*this));
		memset(this, 0, sizeof(*this));
		this->next = rest;
		rest = this;

		this->array.level = map_num1(ddf_level_num, vc->conf.prl);
		this->array.raid_disks =
			__be16_to_cpu(vc->conf.prim_elmnt_count);
		this->array.layout = rlq_to_layout(vc->conf.rlq, vc->conf.prl,
						   this->array.raid_disks);
		this->array.md_minor      = -1;
		this->array.major_version = -1;
		this->array.minor_version = -2;
		cptr = (__u32 *)(vc->conf.guid + 16);
		this->array.ctime         = DECADE + __be32_to_cpu(*cptr);
		this->array.utime	  = DECADE +
			__be32_to_cpu(vc->conf.timestamp);
		this->array.chunk_size	  = 512 << vc->conf.chunk_shift;

		i = vc->vcnum;
		if ((ddf->virt->entries[i].state & DDF_state_inconsistent) ||
		    (ddf->virt->entries[i].init_state & DDF_initstate_mask) !=
		    DDF_init_full) {
			this->array.state = 0;
			this->resync_start = 0;
		} else {
			this->array.state = 1;
			this->resync_start = MaxSector;
		}
		memcpy(this->name, ddf->virt->entries[i].name, 16);
		this->name[16]=0;
		for(j=0; j<16; j++)
			if (this->name[j] == ' ')
				this->name[j] = 0;

		memset(this->uuid, 0, sizeof(this->uuid));
		this->component_size = __be64_to_cpu(vc->conf.blocks);
		this->array.size = this->component_size / 2;
		this->container_member = i;

		ddf->currentconf = vc;
		uuid_from_super_ddf(st, this->uuid);
		ddf->currentconf = NULL;

		sprintf(this->text_version, "/%s/%d",
			devnum2devname(st->container_dev),
			this->container_member);

		for (i = 0 ; i < ddf->mppe ; i++) {
			struct mdinfo *dev;
			struct dl *d;
			int stt;
			int pd;

			if (vc->conf.phys_refnum[i] == 0xFFFFFFFF)
				continue;

			for (pd = __be16_to_cpu(ddf->phys->used_pdes);
			     pd--;)
				if (ddf->phys->entries[pd].refnum
				    == vc->conf.phys_refnum[i])
					break;
			if (pd < 0)
				continue;

			stt = __be16_to_cpu(ddf->phys->entries[pd].state);
			if ((stt & (DDF_Online|DDF_Failed|DDF_Rebuilding))
			    != DDF_Online)
				continue;

			this->array.working_disks++;

			for (d = ddf->dlist; d ; d=d->next)
				if (d->disk.refnum == vc->conf.phys_refnum[i])
					break;
			if (d == NULL)
				/* Haven't found that one yet, maybe there are others */
				continue;

			dev = malloc(sizeof(*dev));
			memset(dev, 0, sizeof(*dev));
			dev->next = this->devs;
			this->devs = dev;

			dev->disk.number = __be32_to_cpu(d->disk.refnum);
			dev->disk.major = d->major;
			dev->disk.minor = d->minor;
			dev->disk.raid_disk = i;
			dev->disk.state = (1<<MD_DISK_SYNC)|(1<<MD_DISK_ACTIVE);
			dev->recovery_start = MaxSector;

			dev->events = __be32_to_cpu(ddf->primary.seq);
			dev->data_offset = __be64_to_cpu(vc->lba_offset[i]);
			dev->component_size = __be64_to_cpu(vc->conf.blocks);
			if (d->devname)
				strcpy(dev->name, d->devname);
		}
	}
	return rest;
}

static int store_super_ddf(struct supertype *st, int fd)
{
	struct ddf_super *ddf = st->sb;
	unsigned long long dsize;
	void *buf;
	int rc;

	if (!ddf)
		return 1;

	/* ->dlist and ->conflist will be set for updates, currently not
	 * supported
	 */
	if (ddf->dlist || ddf->conflist)
		return 1;

	if (!get_dev_size(fd, NULL, &dsize))
		return 1;

	if (posix_memalign(&buf, 512, 512) != 0)
		return 1;
	memset(buf, 0, 512);

	lseek64(fd, dsize-512, 0);
	rc = write(fd, buf, 512);
	free(buf);
	if (rc < 0)
		return 1;
	return 0;
}

static int compare_super_ddf(struct supertype *st, struct supertype *tst)
{
	/*
	 * return:
	 *  0 same, or first was empty, and second was copied
	 *  1 second had wrong number
	 *  2 wrong uuid
	 *  3 wrong other info
	 */
	struct ddf_super *first = st->sb;
	struct ddf_super *second = tst->sb;

	if (!first) {
		st->sb = tst->sb;
		tst->sb = NULL;
		return 0;
	}

	if (memcmp(first->anchor.guid, second->anchor.guid, DDF_GUID_LEN) != 0)
		return 2;

	/* FIXME should I look at anything else? */
	return 0;
}

#ifndef MDASSEMBLE
/*
 * A new array 'a' has been started which claims to be instance 'inst'
 * within container 'c'.
 * We need to confirm that the array matches the metadata in 'c' so
 * that we don't corrupt any metadata.
 */
static int ddf_open_new(struct supertype *c, struct active_array *a, char *inst)
{
	dprintf("ddf: open_new %s\n", inst);
	a->info.container_member = atoi(inst);
	return 0;
}

/*
 * The array 'a' is to be marked clean in the metadata.
 * If '->resync_start' is not ~(unsigned long long)0, then the array is only
 * clean up to the point (in sectors).  If that cannot be recorded in the
 * metadata, then leave it as dirty.
 *
 * For DDF, we need to clear the DDF_state_inconsistent bit in the
 * !global! virtual_disk.virtual_entry structure.
 */
static int ddf_set_array_state(struct active_array *a, int consistent)
{
	struct ddf_super *ddf = a->container->sb;
	int inst = a->info.container_member;
	int old = ddf->virt->entries[inst].state;
	if (consistent == 2) {
		/* Should check if a recovery should be started FIXME */
		consistent = 1;
		if (!is_resync_complete(&a->info))
			consistent = 0;
	}
	if (consistent)
		ddf->virt->entries[inst].state &= ~DDF_state_inconsistent;
	else
		ddf->virt->entries[inst].state |= DDF_state_inconsistent;
	if (old != ddf->virt->entries[inst].state)
		ddf->updates_pending = 1;

	old = ddf->virt->entries[inst].init_state;
	ddf->virt->entries[inst].init_state &= ~DDF_initstate_mask;
	if (is_resync_complete(&a->info))
		ddf->virt->entries[inst].init_state |= DDF_init_full;
	else if (a->info.resync_start == 0)
		ddf->virt->entries[inst].init_state |= DDF_init_not;
	else
		ddf->virt->entries[inst].init_state |= DDF_init_quick;
	if (old != ddf->virt->entries[inst].init_state)
		ddf->updates_pending = 1;

	dprintf("ddf mark %d %s %llu\n", inst, consistent?"clean":"dirty",
		a->info.resync_start);
	return consistent;
}

#define container_of(ptr, type, member) ({                      \
        const typeof( ((type *)0)->member ) *__mptr = (ptr);    \
        (type *)( (char *)__mptr - offsetof(type,member) );})
/*
 * The state of each disk is stored in the global phys_disk structure
 * in phys_disk.entries[n].state.
 * This makes various combinations awkward.
 * - When a device fails in any array, it must be failed in all arrays
 *   that include a part of this device.
 * - When a component is rebuilding, we cannot include it officially in the
 *   array unless this is the only array that uses the device.
 *
 * So: when transitioning:
 *   Online -> failed,  just set failed flag.  monitor will propagate
 *   spare -> online,   the device might need to be added to the array.
 *   spare -> failed,   just set failed.  Don't worry if in array or not.
 */
static void ddf_set_disk(struct active_array *a, int n, int state)
{
	struct ddf_super *ddf = a->container->sb;
	unsigned int inst = a->info.container_member;
	struct vd_config *vc = find_vdcr(ddf, inst);
	int pd = find_phys(ddf, vc->phys_refnum[n]);
	int i, st, working;
	struct mdinfo *mdi;
	struct dl *dl;

	if (vc == NULL) {
		dprintf("ddf: cannot find instance %d!!\n", inst);
		return;
	}
	/* Find the matching slot in 'info'. */
	for (mdi = a->info.devs; mdi; mdi = mdi->next)
		if (mdi->disk.raid_disk == n)
			break;
	if (!mdi)
		return;

	/* and find the 'dl' entry corresponding to that. */
	for (dl = ddf->dlist; dl; dl = dl->next)
		if (mdi->state_fd >= 0 &&
		    mdi->disk.major == dl->major &&
		    mdi->disk.minor == dl->minor)
			break;
	if (!dl)
		return;

	if (pd < 0 || pd != dl->pdnum) {
		/* disk doesn't currently exist or has changed.
		 * If it is now in_sync, insert it. */
		if ((state & DS_INSYNC) && ! (state & DS_FAULTY)) {
			struct vcl *vcl;
			pd = dl->pdnum;
			vc->phys_refnum[n] = dl->disk.refnum;
			vcl = container_of(vc, struct vcl, conf);
			vcl->lba_offset[n] = mdi->data_offset;
			ddf->phys->entries[pd].type &=
				~__cpu_to_be16(DDF_Global_Spare);
			ddf->phys->entries[pd].type |=
				__cpu_to_be16(DDF_Active_in_VD);
			ddf->updates_pending = 1;
		}
	} else {
		int old = ddf->phys->entries[pd].state;
		if (state & DS_FAULTY)
			ddf->phys->entries[pd].state  |= __cpu_to_be16(DDF_Failed);
		if (state & DS_INSYNC) {
			ddf->phys->entries[pd].state  |= __cpu_to_be16(DDF_Online);
			ddf->phys->entries[pd].state  &= __cpu_to_be16(~DDF_Rebuilding);
		}
		if (old != ddf->phys->entries[pd].state)
			ddf->updates_pending = 1;
	}

	dprintf("ddf: set_disk %d to %x\n", n, state);

	/* Now we need to check the state of the array and update
	 * virtual_disk.entries[n].state.
	 * It needs to be one of "optimal", "degraded", "failed".
	 * I don't understand 'deleted' or 'missing'.
	 */
	working = 0;
	for (i=0; i < a->info.array.raid_disks; i++) {
		pd = find_phys(ddf, vc->phys_refnum[i]);
		if (pd < 0)
			continue;
		st = __be16_to_cpu(ddf->phys->entries[pd].state);
		if ((st & (DDF_Online|DDF_Failed|DDF_Rebuilding))
		    == DDF_Online)
			working++;
	}
	state = DDF_state_degraded;
	if (working == a->info.array.raid_disks)
		state = DDF_state_optimal;
	else switch(vc->prl) {
	case DDF_RAID0:
	case DDF_CONCAT:
	case DDF_JBOD:
		state = DDF_state_failed;
		break;
	case DDF_RAID1:
		if (working == 0)
			state = DDF_state_failed;
		else if (working == 2 && state == DDF_state_degraded)
			state = DDF_state_part_optimal;
		break;
	case DDF_RAID4:
	case DDF_RAID5:
		if (working < a->info.array.raid_disks-1)
			state = DDF_state_failed;
		break;
	case DDF_RAID6:
		if (working < a->info.array.raid_disks-2)
			state = DDF_state_failed;
		else if (working == a->info.array.raid_disks-1)
			state = DDF_state_part_optimal;
		break;
	}

	if (ddf->virt->entries[inst].state !=
	    ((ddf->virt->entries[inst].state & ~DDF_state_mask)
	     | state)) {

		ddf->virt->entries[inst].state =
			(ddf->virt->entries[inst].state & ~DDF_state_mask)
			| state;
		ddf->updates_pending = 1;
	}

}

static void ddf_sync_metadata(struct supertype *st)
{

	/*
	 * Write all data to all devices.
	 * Later, we might be able to track whether only local changes
	 * have been made, or whether any global data has been changed,
	 * but ddf is sufficiently weird that it probably always
	 * changes global data ....
	 */
	struct ddf_super *ddf = st->sb;
	if (!ddf->updates_pending)
		return;
	ddf->updates_pending = 0;
	__write_init_super_ddf(st);
	dprintf("ddf: sync_metadata\n");
}

static void ddf_process_update(struct supertype *st,
			       struct metadata_update *update)
{
	/* Apply this update to the metadata.
	 * The first 4 bytes are a DDF_*_MAGIC which guides
	 * our actions.
	 * Possible update are:
	 *  DDF_PHYS_RECORDS_MAGIC
	 *    Add a new physical device or remove an old one.
	 *    Changes to this record only happen implicitly.
	 *    used_pdes is the device number.
	 *  DDF_VIRT_RECORDS_MAGIC
	 *    Add a new VD.  Possibly also change the 'access' bits.
	 *    populated_vdes is the entry number.
	 *  DDF_VD_CONF_MAGIC
	 *    New or updated VD.  the VIRT_RECORD must already
	 *    exist.  For an update, phys_refnum and lba_offset
	 *    (at least) are updated, and the VD_CONF must
	 *    be written to precisely those devices listed with
	 *    a phys_refnum.
	 *  DDF_SPARE_ASSIGN_MAGIC
	 *    replacement Spare Assignment Record... but for which device?
	 *
	 * So, e.g.:
	 *  - to create a new array, we send a VIRT_RECORD and
	 *    a VD_CONF.  Then assemble and start the array.
	 *  - to activate a spare we send a VD_CONF to add the phys_refnum
	 *    and offset.  This will also mark the spare as active with
	 *    a spare-assignment record.
	 */
	struct ddf_super *ddf = st->sb;
	__u32 *magic = (__u32*)update->buf;
	struct phys_disk *pd;
	struct virtual_disk *vd;
	struct vd_config *vc;
	struct vcl *vcl;
	struct dl *dl;
	unsigned int mppe;
	unsigned int ent;
	unsigned int pdnum, pd2;

	dprintf("Process update %x\n", *magic);

	switch (*magic) {
	case DDF_PHYS_RECORDS_MAGIC:

		if (update->len != (sizeof(struct phys_disk) +
				    sizeof(struct phys_disk_entry)))
			return;
		pd = (struct phys_disk*)update->buf;

		ent = __be16_to_cpu(pd->used_pdes);
		if (ent >= __be16_to_cpu(ddf->phys->max_pdes))
			return;
		if (pd->entries[0].state & __cpu_to_be16(DDF_Missing)) {
			struct dl **dlp;
			/* removing this disk. */
			ddf->phys->entries[ent].state |= __cpu_to_be16(DDF_Missing);
			for (dlp = &ddf->dlist; *dlp; dlp = &(*dlp)->next) {
				struct dl *dl = *dlp;
				if (dl->pdnum == (signed)ent) {
					close(dl->fd);
					dl->fd = -1;
					/* FIXME this doesn't free
					 * dl->devname */
					update->space = dl;
					*dlp = dl->next;
					break;
				}
			}
			ddf->updates_pending = 1;
			return;
		}
		if (!all_ff(ddf->phys->entries[ent].guid))
			return;
		ddf->phys->entries[ent] = pd->entries[0];
		ddf->phys->used_pdes = __cpu_to_be16(1 +
					   __be16_to_cpu(ddf->phys->used_pdes));
		ddf->updates_pending = 1;
		if (ddf->add_list) {
			struct active_array *a;
			struct dl *al = ddf->add_list;
			ddf->add_list = al->next;

			al->next = ddf->dlist;
			ddf->dlist = al;

			/* As a device has been added, we should check
			 * for any degraded devices that might make
			 * use of this spare */
			for (a = st->arrays ; a; a=a->next)
				a->check_degraded = 1;
		}
		break;

	case DDF_VIRT_RECORDS_MAGIC:

		if (update->len != (sizeof(struct virtual_disk) +
				    sizeof(struct virtual_entry)))
			return;
		vd = (struct virtual_disk*)update->buf;

		ent = __be16_to_cpu(vd->populated_vdes);
		if (ent >= __be16_to_cpu(ddf->virt->max_vdes))
			return;
		if (!all_ff(ddf->virt->entries[ent].guid))
			return;
		ddf->virt->entries[ent] = vd->entries[0];
		ddf->virt->populated_vdes = __cpu_to_be16(1 +
			      __be16_to_cpu(ddf->virt->populated_vdes));
		ddf->updates_pending = 1;
		break;

	case DDF_VD_CONF_MAGIC:
		dprintf("len %d %d\n", update->len, ddf->conf_rec_len);

		mppe = __be16_to_cpu(ddf->anchor.max_primary_element_entries);
		if ((unsigned)update->len != ddf->conf_rec_len * 512)
			return;
		vc = (struct vd_config*)update->buf;
		for (vcl = ddf->conflist; vcl ; vcl = vcl->next)
			if (memcmp(vcl->conf.guid, vc->guid, DDF_GUID_LEN) == 0)
				break;
		dprintf("vcl = %p\n", vcl);
		if (vcl) {
			/* An update, just copy the phys_refnum and lba_offset
			 * fields
			 */
			memcpy(vcl->conf.phys_refnum, vc->phys_refnum,
			       mppe * (sizeof(__u32) + sizeof(__u64)));
		} else {
			/* A new VD_CONF */
			if (!update->space)
				return;
			vcl = update->space;
			update->space = NULL;
			vcl->next = ddf->conflist;
			memcpy(&vcl->conf, vc, update->len);
			vcl->lba_offset = (__u64*)
				&vcl->conf.phys_refnum[mppe];
			for (ent = 0;
			     ent < __be16_to_cpu(ddf->virt->populated_vdes);
			     ent++)
				if (memcmp(vc->guid, ddf->virt->entries[ent].guid,
					   DDF_GUID_LEN) == 0) {
					vcl->vcnum = ent;
					break;
				}
			ddf->conflist = vcl;
		}
		/* Set DDF_Transition on all Failed devices - to help
		 * us detect those that are no longer in use
		 */
		for (pdnum = 0; pdnum < __be16_to_cpu(ddf->phys->used_pdes); pdnum++)
			if (ddf->phys->entries[pdnum].state
			    & __be16_to_cpu(DDF_Failed))
				ddf->phys->entries[pdnum].state
					|= __be16_to_cpu(DDF_Transition);
		/* Now make sure vlist is correct for each dl. */
		for (dl = ddf->dlist; dl; dl = dl->next) {
			unsigned int dn;
			unsigned int vn = 0;
			int in_degraded = 0;
			for (vcl = ddf->conflist; vcl ; vcl = vcl->next)
				for (dn=0; dn < ddf->mppe ; dn++)
					if (vcl->conf.phys_refnum[dn] ==
					    dl->disk.refnum) {
						int vstate;
						dprintf("dev %d has %p at %d\n",
							dl->pdnum, vcl, vn);
						/* Clear the Transition flag */
						if (ddf->phys->entries[dl->pdnum].state
						    & __be16_to_cpu(DDF_Failed))
							ddf->phys->entries[dl->pdnum].state &=
								~__be16_to_cpu(DDF_Transition);

						dl->vlist[vn++] = vcl;
						vstate = ddf->virt->entries[vcl->vcnum].state
							& DDF_state_mask;
						if (vstate == DDF_state_degraded ||
						    vstate == DDF_state_part_optimal)
							in_degraded = 1;
						break;
					}
			while (vn < ddf->max_part)
				dl->vlist[vn++] = NULL;
			if (dl->vlist[0]) {
				ddf->phys->entries[dl->pdnum].type &=
					~__cpu_to_be16(DDF_Global_Spare);
				if (!(ddf->phys->entries[dl->pdnum].type &
				      __cpu_to_be16(DDF_Active_in_VD))) {
					    ddf->phys->entries[dl->pdnum].type |=
						    __cpu_to_be16(DDF_Active_in_VD);
					    if (in_degraded)
						    ddf->phys->entries[dl->pdnum].state |=
							    __cpu_to_be16(DDF_Rebuilding);
				    }
			}
			if (dl->spare) {
				ddf->phys->entries[dl->pdnum].type &=
					~__cpu_to_be16(DDF_Global_Spare);
				ddf->phys->entries[dl->pdnum].type |=
					__cpu_to_be16(DDF_Spare);
			}
			if (!dl->vlist[0] && !dl->spare) {
				ddf->phys->entries[dl->pdnum].type |=
					__cpu_to_be16(DDF_Global_Spare);
				ddf->phys->entries[dl->pdnum].type &=
					~__cpu_to_be16(DDF_Spare |
						       DDF_Active_in_VD);
			}
		}

		/* Now remove any 'Failed' devices that are not part
		 * of any VD.  They will have the Transition flag set.
		 * Once done, we need to update all dl->pdnum numbers.
		 */
		pd2 = 0;
		for (pdnum = 0; pdnum < __be16_to_cpu(ddf->phys->used_pdes); pdnum++)
			if ((ddf->phys->entries[pdnum].state
			     & __be16_to_cpu(DDF_Failed))
			    && (ddf->phys->entries[pdnum].state
				& __be16_to_cpu(DDF_Transition)))
				/* skip this one */;
			else if (pdnum == pd2)
				pd2++;
			else {
				ddf->phys->entries[pd2] = ddf->phys->entries[pdnum];
				for (dl = ddf->dlist; dl; dl = dl->next)
					if (dl->pdnum == (int)pdnum)
						dl->pdnum = pd2;
				pd2++;
			}
		ddf->phys->used_pdes = __cpu_to_be16(pd2);
		while (pd2 < pdnum) {
			memset(ddf->phys->entries[pd2].guid, 0xff, DDF_GUID_LEN);
			pd2++;
		}

		ddf->updates_pending = 1;
		break;
	case DDF_SPARE_ASSIGN_MAGIC:
	default: break;
	}
}

static void ddf_prepare_update(struct supertype *st,
			       struct metadata_update *update)
{
	/* This update arrived at managemon.
	 * We are about to pass it to monitor.
	 * If a malloc is needed, do it here.
	 */
	struct ddf_super *ddf = st->sb;
	__u32 *magic = (__u32*)update->buf;
	if (*magic == DDF_VD_CONF_MAGIC)
		if (posix_memalign(&update->space, 512,
			       offsetof(struct vcl, conf)
			       + ddf->conf_rec_len * 512) != 0)
			update->space = NULL;
}

/*
 * Check if the array 'a' is degraded but not failed.
 * If it is, find as many spares as are available and needed and
 * arrange for their inclusion.
 * We only choose devices which are not already in the array,
 * and prefer those with a spare-assignment to this array.
 * otherwise we choose global spares - assuming always that
 * there is enough room.
 * For each spare that we assign, we return an 'mdinfo' which
 * describes the position for the device in the array.
 * We also add to 'updates' a DDF_VD_CONF_MAGIC update with
 * the new phys_refnum and lba_offset values.
 *
 * Only worry about BVDs at the moment.
 */
static struct mdinfo *ddf_activate_spare(struct active_array *a,
					 struct metadata_update **updates)
{
	int working = 0;
	struct mdinfo *d;
	struct ddf_super *ddf = a->container->sb;
	int global_ok = 0;
	struct mdinfo *rv = NULL;
	struct mdinfo *di;
	struct metadata_update *mu;
	struct dl *dl;
	int i;
	struct vd_config *vc;
	__u64 *lba;

	for (d = a->info.devs ; d ; d = d->next) {
		if ((d->curr_state & DS_FAULTY) &&
			d->state_fd >= 0)
			/* wait for Removal to happen */
			return NULL;
		if (d->state_fd >= 0)
			working ++;
	}

	dprintf("ddf_activate: working=%d (%d) level=%d\n", working, a->info.array.raid_disks,
		a->info.array.level);
	if (working == a->info.array.raid_disks)
		return NULL; /* array not degraded */
	switch (a->info.array.level) {
	case 1:
		if (working == 0)
			return NULL; /* failed */
		break;
	case 4:
	case 5:
		if (working < a->info.array.raid_disks - 1)
			return NULL; /* failed */
		break;
	case 6:
		if (working < a->info.array.raid_disks - 2)
			return NULL; /* failed */
		break;
	default: /* concat or stripe */
		return NULL; /* failed */
	}

	/* For each slot, if it is not working, find a spare */
	dl = ddf->dlist;
	for (i = 0; i < a->info.array.raid_disks; i++) {
		for (d = a->info.devs ; d ; d = d->next)
			if (d->disk.raid_disk == i)
				break;
		dprintf("found %d: %p %x\n", i, d, d?d->curr_state:0);
		if (d && (d->state_fd >= 0))
			continue;

		/* OK, this device needs recovery.  Find a spare */
	again:
		for ( ; dl ; dl = dl->next) {
			unsigned long long esize;
			unsigned long long pos;
			struct mdinfo *d2;
			int is_global = 0;
			int is_dedicated = 0;
			struct extent *ex;
			unsigned int j;
			/* If in this array, skip */
			for (d2 = a->info.devs ; d2 ; d2 = d2->next)
				if (d2->state_fd >= 0 &&
				    d2->disk.major == dl->major &&
				    d2->disk.minor == dl->minor) {
					dprintf("%x:%x already in array\n", dl->major, dl->minor);
					break;
				}
			if (d2)
				continue;
			if (ddf->phys->entries[dl->pdnum].type &
			    __cpu_to_be16(DDF_Spare)) {
				/* Check spare assign record */
				if (dl->spare) {
					if (dl->spare->type & DDF_spare_dedicated) {
						/* check spare_ents for guid */
						for (j = 0 ;
						     j < __be16_to_cpu(dl->spare->populated);
						     j++) {
							if (memcmp(dl->spare->spare_ents[j].guid,
								   ddf->virt->entries[a->info.container_member].guid,
								   DDF_GUID_LEN) == 0)
								is_dedicated = 1;
						}
					} else
						is_global = 1;
				}
			} else if (ddf->phys->entries[dl->pdnum].type &
				   __cpu_to_be16(DDF_Global_Spare)) {
				is_global = 1;
			}
			if ( ! (is_dedicated ||
				(is_global && global_ok))) {
				dprintf("%x:%x not suitable: %d %d\n", dl->major, dl->minor,
				       is_dedicated, is_global);
				continue;
			}

			/* We are allowed to use this device - is there space?
			 * We need a->info.component_size sectors */
			ex = get_extents(ddf, dl);
			if (!ex) {
				dprintf("cannot get extents\n");
				continue;
			}
			j = 0; pos = 0;
			esize = 0;

			do {
				esize = ex[j].start - pos;
				if (esize >= a->info.component_size)
					break;
				pos = ex[j].start + ex[j].size;
				j++;
			} while (ex[j-1].size);

			free(ex);
			if (esize < a->info.component_size) {
				dprintf("%x:%x has no room: %llu %llu\n",
					dl->major, dl->minor,
					esize, a->info.component_size);
				/* No room */
				continue;
			}

			/* Cool, we have a device with some space at pos */
			di = malloc(sizeof(*di));
			if (!di)
				continue;
			memset(di, 0, sizeof(*di));
			di->disk.number = i;
			di->disk.raid_disk = i;
			di->disk.major = dl->major;
			di->disk.minor = dl->minor;
			di->disk.state = 0;
			di->recovery_start = 0;
			di->data_offset = pos;
			di->component_size = a->info.component_size;
			di->container_member = dl->pdnum;
			di->next = rv;
			rv = di;
			dprintf("%x:%x to be %d at %llu\n", dl->major, dl->minor,
				i, pos);

			break;
		}
		if (!dl && ! global_ok) {
			/* not enough dedicated spares, try global */
			global_ok = 1;
			dl = ddf->dlist;
			goto again;
		}
	}

	if (!rv)
		/* No spares found */
		return rv;
	/* Now 'rv' has a list of devices to return.
	 * Create a metadata_update record to update the
	 * phys_refnum and lba_offset values
	 */
	mu = malloc(sizeof(*mu));
	if (mu && posix_memalign(&mu->space, 512, sizeof(struct vcl)) != 0) {
		free(mu);
		mu = NULL;
	}
	if (!mu) {
		while (rv) {
			struct mdinfo *n = rv->next;

			free(rv);
			rv = n;
		}
		return NULL;
	}
		
	mu->buf = malloc(ddf->conf_rec_len * 512);
	mu->len = ddf->conf_rec_len * 512;
	mu->space = NULL;
	mu->space_list = NULL;
	mu->next = *updates;
	vc = find_vdcr(ddf, a->info.container_member);
	memcpy(mu->buf, vc, ddf->conf_rec_len * 512);

	vc = (struct vd_config*)mu->buf;
	lba = (__u64*)&vc->phys_refnum[ddf->mppe];
	for (di = rv ; di ; di = di->next) {
		vc->phys_refnum[di->disk.raid_disk] =
			ddf->phys->entries[dl->pdnum].refnum;
		lba[di->disk.raid_disk] = di->data_offset;
	}
	*updates = mu;
	return rv;
}
#endif /* MDASSEMBLE */

static int ddf_level_to_layout(int level)
{
	switch(level) {
	case 0:
	case 1:
		return 0;
	case 5:
		return ALGORITHM_LEFT_SYMMETRIC;
	case 6:
		return ALGORITHM_ROTATING_N_CONTINUE;
	case 10:
		return 0x102;
	default:
		return UnSet;
	}
}

static void default_geometry_ddf(struct supertype *st, int *level, int *layout, int *chunk)
{
	if (level && *level == UnSet)
		*level = LEVEL_CONTAINER;

	if (level && layout && *layout == UnSet)
		*layout = ddf_level_to_layout(*level);
}

struct superswitch super_ddf = {
#ifndef	MDASSEMBLE
	.examine_super	= examine_super_ddf,
	.brief_examine_super = brief_examine_super_ddf,
	.brief_examine_subarrays = brief_examine_subarrays_ddf,
	.export_examine_super = export_examine_super_ddf,
	.detail_super	= detail_super_ddf,
	.brief_detail_super = brief_detail_super_ddf,
	.validate_geometry = validate_geometry_ddf,
	.write_init_super = write_init_super_ddf,
	.add_to_super	= add_to_super_ddf,
	.remove_from_super = remove_from_super_ddf,
	.load_container	= load_container_ddf,
#endif
	.match_home	= match_home_ddf,
	.uuid_from_super= uuid_from_super_ddf,
	.getinfo_super  = getinfo_super_ddf,
	.update_super	= update_super_ddf,

	.avail_size	= avail_size_ddf,

	.compare_super	= compare_super_ddf,

	.load_super	= load_super_ddf,
	.init_super	= init_super_ddf,
	.store_super	= store_super_ddf,
	.free_super	= free_super_ddf,
	.match_metadata_desc = match_metadata_desc_ddf,
	.container_content = container_content_ddf,
	.default_geometry = default_geometry_ddf,

	.external	= 1,

#ifndef MDASSEMBLE
/* for mdmon */
	.open_new       = ddf_open_new,
	.set_array_state= ddf_set_array_state,
	.set_disk       = ddf_set_disk,
	.sync_metadata  = ddf_sync_metadata,
	.process_update	= ddf_process_update,
	.prepare_update	= ddf_prepare_update,
	.activate_spare = ddf_activate_spare,
#endif
	.name = "ddf",
};
