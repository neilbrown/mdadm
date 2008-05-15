/*
 * mdadm - manage Linux "md" devices aka RAID arrays.
 *
 * Copyright (C) 2006-2007 Neil Brown <neilb@suse.de>
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
#include "sha1.h"
#include <values.h>

static inline int ROUND_UP(int a, int base)
{
	return ((a+base-1)/base)*base;
}

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
#define	DDF_RAID6	0x16	/* Vendor unique layout */

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
#define DDF_REVISION	"01.00.00"

struct ddf_header {
	__u32	magic;
	__u32	crc;
	char	guid[DDF_GUID_LEN];
	char	revision[8];	/* 01.00.00 */
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
	__u32	magic;
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
	__u32	magic;
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
#define	DDF_Global_Spare		4
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
	__u32	magic;
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

#define	DDF_state_morphing	0x8
#define	DDF_state_inconsistent	0x10

/* virtual_entry.init_state is a bigendian bitmap */
#define	DDF_initstate_mask	0x03
#define	DDF_init_not		0x00
#define	DDF_init_quick		0x01
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
	__u32	magic;
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
	__u64	blocks;
	__u64	array_blocks;
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
	__u32	magic;
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
	__u32	magic;
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
	struct ddf_header anchor, primary, secondary, *active;
	struct ddf_controller_data controller;
	struct phys_disk	*phys;
	struct virtual_disk	*virt;
	int pdsize, vdsize;
	int max_part;
	struct vcl {
		struct vcl	*next;
		__u64		*lba_offset; /* location in 'conf' of
					      * the lba table */
		struct vd_config conf;
	} *conflist, *newconf;
	struct dl {
		struct dl	*next;
		struct disk_data disk;
		int major, minor;
		char *devname;
		int fd;
		struct vcl *vlist[0]; /* max_part+1 in size */
	} *dlist;
};

#ifndef offsetof
#define offsetof(t,f) ((size_t)&(((t*)0)->f))
#endif

struct superswitch super_ddf_container, super_ddf_bvd;

static int calc_crc(void *buf, int len)
{
	/* crcs are always at the same place as in the ddf_header */
	struct ddf_header *ddf = buf;
	__u32 oldcrc = ddf->crc;
	__u32 newcrc;
	ddf->crc = 0xffffffff;

	newcrc = crc32(0, buf, len);
	ddf->crc = oldcrc;
	return newcrc;
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
	} else
		buf = malloc(len<<9);
	if (!buf)
		return NULL;

	if (super->active->type == 1)
		offset += __be64_to_cpu(super->active->primary_lba);
	else
		offset += __be64_to_cpu(super->active->secondary_lba);

	if (lseek64(fd, offset<<9, 0) != (offset<<9)) {
		if (dofree)
			free(buf);
		return NULL;
	}
	if (read(fd, buf, len<<9) != (len<<9)) {
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
	if (memcmp(super->anchor.revision, DDF_REVISION, 8) != 0) {
		if (devname)
			fprintf(stderr, Name ": can only support super revision"
				" %.8s, not %.8s on %s\n",
				DDF_REVISION, super->anchor.revision, devname);
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
		return 2;
	}
	super->conflist = NULL;
	super->dlist = NULL;
	return 0;
}

static int load_ddf_local(int fd, struct ddf_super *super,
			  char *devname, int keep)
{
	struct dl *dl;
	struct stat stb;
	char *conf;
	int i;
	int conflen;

	/* First the local disk info */
	super->max_part = __be16_to_cpu(super->active->max_partitions);
	dl = malloc(sizeof(*dl) +
		    (super->max_part+1) * sizeof(dl->vlist[0]));

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
	for (i=0 ; i < super->max_part + 1 ; i++)
		dl->vlist[i] = NULL;
	super->dlist = dl;

	/* Now the config list. */
	/* 'conf' is an array of config entries, some of which are
	 * probably invalid.  Those which are good need to be copied into
	 * the conflist
	 */
	conflen =  __be16_to_cpu(super->active->config_record_len);

	conf = load_section(fd, super, NULL,
			    super->active->config_section_offset,
			    super->active->config_section_length,
			    0);

	for (i = 0;
	     i < __be32_to_cpu(super->active->config_section_length);
	     i += conflen) {
		struct vd_config *vd =
			(struct vd_config *)((char*)conf + i*512);
		struct vcl *vcl;

		if (vd->magic != DDF_VD_CONF_MAGIC)
			continue;
		for (vcl = super->conflist; vcl; vcl = vcl->next) {
			if (memcmp(vcl->conf.guid,
				   vd->guid, DDF_GUID_LEN) == 0)
				break;
		}

		if (vcl) {
			dl->vlist[i/conflen] = vcl;
			if (__be32_to_cpu(vd->seqnum) <=
			    __be32_to_cpu(vcl->conf.seqnum))
				continue;
 		} else {
			vcl = malloc(conflen*512 + offsetof(struct vcl, conf));
			vcl->next = super->conflist;
			super->conflist = vcl;
		}
		memcpy(&vcl->conf, vd, conflen*512);
		vcl->lba_offset = (__u64*)
			&vcl->conf.phys_refnum[super->max_part+1];
		dl->vlist[i/conflen] = vcl;
	}
	free(conf);

	return 0;
}

#ifndef MDASSEMBLE
static int load_super_ddf_all(struct supertype *st, int fd,
			      void **sbp, char *devname, int keep_fd);
#endif
static int load_super_ddf(struct supertype *st, int fd,
			  char *devname)
{
	unsigned long long dsize;
	struct ddf_super *super;
	int rv;

#ifndef MDASSEMBLE
	if (load_super_ddf_all(st, fd, &st->sb, devname, 0) == 0)
		return 0;
#endif

	if (get_dev_size(fd, devname, &dsize) == 0)
		return 1;

	/* 32M is a lower bound */
	if (dsize <= 32*1024*1024) {
		if (devname) {
			fprintf(stderr,
				Name ": %s is too small for ddf: "
				"size is %llu sectors.\n",
				devname, dsize>>9);
			return 1;
		}
	}
	if (dsize & 511) {
		if (devname) {
			fprintf(stderr,
				Name ": %s is an odd size for ddf: "
				"size is %llu bytes.\n",
				devname, dsize);
			return 1;
		}
	}

	super = malloc(sizeof(*super));
	if (!super) {
		fprintf(stderr, Name ": malloc of %zu failed.\n",
			sizeof(*super));
		return 1;
	}

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

	load_ddf_local(fd, super, devname, 0);

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
		free(v);
	}
	while (ddf->dlist) {
		struct dl *d = ddf->dlist;
		ddf->dlist = d->next;
		if (d->fd >= 0)
			close(d->fd);
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
	st->ss = &super_ddf;
	st->max_devs = 512;
	st->minor_version = 0;
	st->sb = NULL;
	return st;
}

static struct supertype *match_metadata_desc_ddf_bvd(char *arg)
{
	struct supertype *st;
	if (strcmp(arg, "ddf/bvd") != 0 &&
	    strcmp(arg, "bvd") != 0 &&
	    strcmp(arg, "default") != 0
		)
		return NULL;

	st = malloc(sizeof(*st));
	st->ss = &super_ddf_bvd;
	st->max_devs = 512;
	st->minor_version = 0;
	st->sb = NULL;
	return st;
}
static struct supertype *match_metadata_desc_ddf_svd(char *arg)
{
	struct supertype *st;
	if (strcmp(arg, "ddf/svd") != 0 &&
	    strcmp(arg, "svd") != 0 &&
	    strcmp(arg, "default") != 0
		)
		return NULL;

	st = malloc(sizeof(*st));
	st->ss = &super_ddf_svd;
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
	{ DDF_RAID5, 4 },
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

#ifndef MDASSEMBLE
static void print_guid(char *guid, int tstamp)
{
	/* A GUIDs are part (or all) ASCII and part binary.
	 * They tend to be space padded.
	 * We ignore trailing spaces and print numbers
	 * <0x20 and >=0x7f as \xXX
	 * Some GUIDs have a time stamp in bytes 16-19.
	 * We print that if appropriate
	 */
	int l = DDF_GUID_LEN;
	int i;
	while (l && guid[l-1] == ' ')
		l--;
	for (i=0 ; i<l ; i++) {
		if (guid[i] >= 0x20 && guid[i] < 0x7f)
			fputc(guid[i], stdout);
		else
			fprintf(stdout, "\\x%02x", guid[i]&255);
	}
	if (tstamp) {
		time_t then = __be32_to_cpu(*(__u32*)(guid+16)) + DECADE;
		char tbuf[100];
		struct tm *tm;
		tm = localtime(&then);
		strftime(tbuf, 100, " (%D %T)",tm);
		fputs(tbuf, stdout);
	}
}

static void examine_vd(int n, struct ddf_super *sb, char *guid)
{
	int crl = __be16_to_cpu(sb->anchor.config_record_len);
	struct vcl *vcl;

	for (vcl = sb->conflist ; vcl ; vcl = vcl->next) {
		struct vd_config *vc = &vcl->conf;

		if (calc_crc(vc, crl*512) != vc->crc)
			continue;
		if (memcmp(vc->guid, guid, DDF_GUID_LEN) != 0)
			continue;

		/* Ok, we know about this VD, let's give more details */
		printf(" Raid Devices[%d] : %d\n", n,
		       __be16_to_cpu(vc->prim_elmnt_count));
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
		       __be64_to_cpu(vc->blocks)/2);
		printf("   Array Size[%d] : %llu\n", n,
		       __be64_to_cpu(vc->array_blocks)/2);
	}
}

static void examine_vds(struct ddf_super *sb)
{
	int cnt = __be16_to_cpu(sb->virt->populated_vdes);
	int i;
	printf("  Virtual Disks : %d\n", cnt);

	for (i=0; i<cnt; i++) {
		struct virtual_entry *ve = &sb->virt->entries[i];
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

	for (i=0 ; i<cnt ; i++) {
		struct phys_disk_entry *pd = &sb->phys->entries[i];
		int type = __be16_to_cpu(pd->type);
		int state = __be16_to_cpu(pd->state);

		printf("      PD GUID[%d] : ", i); print_guid(pd->guid, 0);
		printf("\n");
		printf("          ref[%d] : %08x\n", i,
		       __be32_to_cpu(pd->refnum));
		printf("         mode[%d] : %s%s%s%s%s\n", i,
		       (type&2) ? "active":"",
		       (type&4) ? "Global Spare":"",
		       (type&8) ? "spare" : "",
		       (type&16)? ", foreign" : "",
		       (type&32)? "pass-through" : "");
		printf("        state[%d] : %s%s%s%s%s%s%s\n", i,
		       (state&1)? "Online": "Offline",
		       (state&2)? ", Failed": "",
		       (state&4)? ", Rebuilding": "",
		       (state&8)? ", in-transition": "",
		       (state&16)? ", SMART errors": "",
		       (state&32)? ", Unrecovered Read Errors": "",
		       (state&64)? ", Missing" : "");
		printf("   Avail Size[%d] : %llu K\n", i,
		       __be64_to_cpu(pd->config_size)>>1);
		for (dl = sb->dlist; dl ; dl = dl->next) {
			if (dl->disk.refnum == pd->refnum) {
				char *dv = map_dev(dl->major, dl->minor, 0);
				if (dv)
					printf("       Device[%d] : %s\n",
					       i, dv);
			}
		}
		printf("\n");
	}
}

static void examine_super_ddf(struct supertype *st, char *homehost)
{
	struct ddf_super *sb = st->sb;

	printf("          Magic : %08x\n", __be32_to_cpu(sb->anchor.magic));
	printf("        Version : %.8s\n", sb->anchor.revision);
	printf("Controller GUID : "); print_guid(sb->anchor.guid, 1);
	printf("\n");
	printf("            Seq : %08x\n", __be32_to_cpu(sb->active->seq));
	printf("  Redundant hdr : %s\n", sb->secondary.magic == DDF_HEADER_MAGIC
	       ?"yes" : "no");
	examine_vds(sb);
	examine_pds(sb);
}

static void brief_examine_super_ddf(struct supertype *st)
{
	/* We just write a generic DDF ARRAY entry
	 * The uuid is all hex, 6 groups of 4 bytes
	 */
	struct ddf_super *ddf = st->sb;
	int i;
	printf("ARRAY /dev/ddf UUID=");
	for (i = 0; i < DDF_GUID_LEN; i++) {
		printf("%02x", ddf->anchor.guid[i]);
		if ((i&3) == 0 && i != 0)
			printf(":");
	}
	printf("\n");
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
}


#endif

static int match_home_ddf(struct supertype *st, char *homehost)
{
	/* It matches 'this' host if the controller is a
	 * Linux-MD controller with vendor_data matching
	 * the hostname
	 */
	struct ddf_super *ddf = st->sb;
	int len = strlen(homehost);

	return (memcmp(ddf->controller.guid, T10, 8) == 0 &&
		len < sizeof(ddf->controller.vendor_data) &&
		memcmp(ddf->controller.vendor_data, homehost,len) == 0 &&
		ddf->controller.vendor_data[len] == 0);
}

static struct vd_config *find_vdcr(struct ddf_super *ddf)
{
	/* FIXME this just picks off the first one */
	return &ddf->conflist->conf;
}

static void uuid_from_super_ddf(struct supertype *st, int uuid[4])
{
	/* The uuid returned here is used for:
	 *  uuid to put into bitmap file (Create, Grow)
	 *  uuid for backup header when saving critical section (Grow)
	 *  comparing uuids when re-adding a device into an array
	 * For each of these we can make do with a truncated
	 * or hashed uuid rather than the original, as long as
	 * everyone agrees.
	 * In each case the uuid required is that of the data-array,
	 * not the device-set.
	 * In the case of SVD we assume the BVD is of interest,
	 * though that might be the case if a bitmap were made for
	 * a mirrored SVD - worry about that later.
	 * So we need to find the VD configuration record for the
	 * relevant BVD and extract the GUID and Secondary_Element_Seq.
	 * The first 16 bytes of the sha1 of these is used.
	 */
	struct ddf_super *ddf = st->sb;
	struct vd_config *vd = find_vdcr(ddf);

	if (!vd)
		memset(uuid, 0, sizeof (uuid));
	else {
		char buf[20];
		struct sha1_ctx ctx;
		sha1_init_ctx(&ctx);
		sha1_process_bytes(&vd->guid, DDF_GUID_LEN, &ctx);
		if (vd->sec_elmnt_count > 1)
			sha1_process_bytes(&vd->sec_elmnt_seq, 1, &ctx);
		sha1_finish_ctx(&ctx, buf);
		memcpy(uuid, buf, sizeof(uuid));
	}
}

static void getinfo_super_ddf(struct supertype *st, struct mdinfo *info)
{
	struct ddf_super *ddf = st->sb;
	int i;

	info->array.major_version = 1000;
	info->array.minor_version = 0; /* FIXME use ddf->revision somehow */
	info->array.patch_version = 0;
	info->array.raid_disks    = __be16_to_cpu(ddf->phys->used_pdes);
	info->array.level	  = LEVEL_CONTAINER;
	info->array.layout	  = 0;
	info->array.md_minor	  = -1;
	info->array.ctime	  = DECADE + __be32_to_cpu(*(__u32*)
							 (ddf->anchor.guid+16));
	info->array.utime	  = 0;
	info->array.chunk_size	  = 0;

//	info->data_offset	  = ???;
//	info->component_size	  = ???;

	info->disk.major = 0;
	info->disk.minor = 0;
	info->disk.number = __be32_to_cpu(ddf->dlist->disk.refnum);
//	info->disk.raid_disk = find refnum in the table and use index;
	info->disk.raid_disk = -1;
	for (i = 0; i < __be16_to_cpu(ddf->phys->max_pdes) ; i++)
		if (ddf->phys->entries[i].refnum == ddf->dlist->disk.refnum) {
			info->disk.raid_disk = i;
			break;
		}
	info->disk.state = (1 << MD_DISK_SYNC);

	info->reshape_active = 0;

//	uuid_from_super_ddf(info->uuid, sbv);

//	info->name[] ?? ;
}

static void getinfo_super_ddf_bvd(struct supertype *st, struct mdinfo *info)
{
	struct ddf_super *ddf = st->sb;
	struct vd_config *vd = find_vdcr(ddf);

	/* FIXME this returns BVD info - what if we want SVD ?? */

	info->array.major_version = 1000;
	info->array.minor_version = 0; /* FIXME use ddf->revision somehow */
	info->array.patch_version = 0;
	info->array.raid_disks    = __be16_to_cpu(vd->prim_elmnt_count);
	info->array.level	  = map_num1(ddf_level_num, vd->prl);
	info->array.layout	  = vd->rlq; /* FIXME should this be mapped */
	info->array.md_minor	  = -1;
	info->array.ctime	  = DECADE + __be32_to_cpu(*(__u32*)(vd->guid+16));
	info->array.utime	  = DECADE + __be32_to_cpu(vd->timestamp);
	info->array.chunk_size	  = 512 << vd->chunk_shift;

//	info->data_offset	  = ???;
//	info->component_size	  = ???;

	info->disk.major = 0;
	info->disk.minor = 0;
//	info->disk.number = __be32_to_cpu(ddf->disk.refnum);
//	info->disk.raid_disk = find refnum in the table and use index;
//	info->disk.state = ???;

	uuid_from_super_ddf(st, info->uuid);

//	info->name[] ?? ;
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
	 *  uuid:  Change the uuid of the array to match watch is given
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
//	struct vd_config *vd = find_vdcr(ddf);
//	struct virtual_entry *ve = find_ve(ddf);


	/* we don't need to handle "force-*" or "assemble" as
	 * there is no need to 'trick' the kernel.  We the metadata is
	 * first updated to activate the array, all the implied modifications
	 * will just happen.
	 */

	if (strcmp(update, "grow") == 0) {
		/* FIXME */
	}
	if (strcmp(update, "resync") == 0) {
//		info->resync_checkpoint = 0;
	}
	/* We ignore UUID updates as they make even less sense
	 * with DDF
	 */
	if (strcmp(update, "homehost") == 0) {
		/* homehost is stored in controller->vendor_data,
		 * or it is when we are the vendor
		 */
//		if (info->vendor_is_local)
//			strcpy(ddf->controller.vendor_data, homehost);
	}
	if (strcmp(update, "name") == 0) {
		/* name is stored in virtual_entry->name */
//		memset(ve->name, ' ', 16);
//		strncpy(ve->name, info->name, 16);
	}
	if (strcmp(update, "_reshape_progress") == 0) {
		/* We don't support reshape yet */
	}

//	update_all_csum(ddf);

	return rv;
}

static void make_header_guid(char *guid)
{
	__u32 stamp;
	int rfd;
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
	rfd = open("/dev/urandom", O_RDONLY);
	if (rfd < 0 || read(rfd, &stamp, 4) != 4)
		stamp = random();
	memcpy(guid+20, &stamp, 4);
	if (rfd >= 0) close(rfd);
}
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

	ddf = malloc(sizeof(*ddf));
	ddf->dlist = NULL; /* no physical disks yet */
	ddf->conflist = NULL; /* No virtual disks yet */

	/* At least 32MB *must* be reserved for the ddf.  So let's just
	 * start 32MB from the end, and put the primary header there.
	 * Don't do secondary for now.
	 * We don't know exactly where that will be yet as it could be
	 * different on each device.  To just set up the lengths.
	 *
	 */

	ddf->anchor.magic = DDF_HEADER_MAGIC;
	make_header_guid(ddf->anchor.guid);

	memcpy(ddf->anchor.revision, DDF_REVISION, 8);
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
	ddf->anchor.config_record_len = __cpu_to_be16(1 + 256*12/512);
	ddf->anchor.max_primary_element_entries = __cpu_to_be16(256);
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

	clen = (1 + 256*12/512) * (64+1);
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
	gethostname(hostname, 17);
	hostname[17] = 0;
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

	pd = ddf->phys = malloc(pdsize);
	ddf->pdsize = pdsize;

	memset(pd, 0xff, pdsize);
	memset(pd, 0, sizeof(*pd));
	pd->magic = DDF_PHYS_DATA_MAGIC;
	pd->used_pdes = __cpu_to_be16(0);
	pd->max_pdes = __cpu_to_be16(max_phys_disks);
	memset(pd->pad, 0xff, 52);

	vd = ddf->virt = malloc(vdsize);
	ddf->vdsize = vdsize;
	memset(vd, 0, vdsize);
	vd->magic = DDF_VIRT_RECORDS_MAGIC;
	vd->populated_vdes = __cpu_to_be16(0);
	vd->max_vdes = __cpu_to_be16(max_virt_disks);
	memset(vd->pad, 0xff, 52);

	for (i=0; i<max_virt_disks; i++)
		memset(&vd->entries[i], 0xff, sizeof(struct virtual_entry));

	st->sb = ddf;
	return 1;
}

static int all_ff(char *guid)
{
	int i;
	for (i = 0; i < DDF_GUID_LEN; i++)
		if (guid[i] != (char)0xff)
			return 0;
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
	case 6:
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
	}
	return -1;
}

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
	int venum;
	struct virtual_entry *ve;
	struct vcl *vcl;
	struct vd_config *vc;
	int mppe;
	int conflen;

	if (__be16_to_cpu(ddf->virt->populated_vdes)
	    >= __be16_to_cpu(ddf->virt->max_vdes)) {
		fprintf(stderr, Name": This ddf already has the "
			"maximum of %d virtual devices\n",
			__be16_to_cpu(ddf->virt->max_vdes));
		return 0;
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
	ve->state = 0;
	ve->init_state = 0;
	if (!(info->state & 1))
		ve->init_state = DDF_state_inconsistent;
	memset(ve->pad1, 0xff, 14);
	memset(ve->name, ' ', 16);
	if (name)
		strncpy(ve->name, name, 16);
	ddf->virt->populated_vdes =
		__cpu_to_be16(__be16_to_cpu(ddf->virt->populated_vdes)+1);

	/* Now create a new vd_config */
	conflen =  __be16_to_cpu(ddf->active->config_record_len);
	vcl = malloc(offsetof(struct vcl, conf) + conflen * 512);
	vcl->lba_offset = (__u64*) &vcl->conf.phys_refnum[ddf->max_part+1];

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
	mppe = __be16_to_cpu(ddf->anchor.max_primary_element_entries);
	memset(vc->phys_refnum, 0xff, 4*mppe);
	memset(vc->phys_refnum+mppe, 0x00, 8*mppe);

	vcl->next = ddf->conflist;
	ddf->conflist = vcl;
	ddf->newconf = vcl;
	return 1;
}

static void add_to_super_ddf_bvd(struct supertype *st,
				 mdu_disk_info_t *dk, int fd, char *devname)
{
	/* fd and devname identify a device with-in the ddf container (st).
	 * dk identifies a location in the new BVD.
	 * We need to find suitable free space in that device and update
	 * the phys_refnum and lba_offset for the newly created vd_config.
	 * We might also want to update the type in the phys_disk
	 * section. FIXME
	 */
	struct dl *dl;
	struct ddf_super *ddf = st->sb;
	struct vd_config *vc;
	__u64 *lba_offset;
	int mppe;

	for (dl = ddf->dlist; dl ; dl = dl->next)
		if (dl->major == dk->major &&
		    dl->minor == dk->minor)
			break;
	if (!dl || ! (dk->state & (1<<MD_DISK_SYNC)))
		return;

	vc = &ddf->newconf->conf;
	vc->phys_refnum[dk->raid_disk] = dl->disk.refnum;
	mppe = __be16_to_cpu(ddf->anchor.max_primary_element_entries);
	lba_offset = (__u64*)(vc->phys_refnum + mppe);
	lba_offset[dk->raid_disk] = 0; /* FIXME */

	dl->vlist[0] =ddf->newconf; /* FIXME */

	dl->fd = fd;
	dl->devname = devname;
}

/* add a device to a container, either while creating it or while
 * expanding a pre-existing container
 */
static void add_to_super_ddf(struct supertype *st,
			     mdu_disk_info_t *dk, int fd, char *devname)
{
	struct ddf_super *ddf = st->sb;
	struct dl *dd;
	time_t now;
	struct tm *tm;
	unsigned long long size;
	struct phys_disk_entry *pde;
	int n, i;
	struct stat stb;

	/* This is device numbered dk->number.  We need to create
	 * a phys_disk entry and a more detailed disk_data entry.
	 */
	fstat(fd, &stb);
	dd = malloc(sizeof(*dd) + sizeof(dd->vlist[0]) * (ddf->max_part+1));
	dd->major = major(stb.st_rdev);
	dd->minor = minor(stb.st_rdev);
	dd->devname = devname;
	dd->next = ddf->dlist;
	dd->fd = fd;

	dd->disk.magic = DDF_PHYS_DATA_MAGIC;
	now = time(0);
	tm = localtime(&now);
	sprintf(dd->disk.guid, "%8s%04d%02d%02d",
		T10, tm->tm_year+1900, tm->tm_mon+1, tm->tm_mday);
	*(__u32*)(dd->disk.guid + 16) = random();
	*(__u32*)(dd->disk.guid + 20) = random();

	dd->disk.refnum = random(); /* and hope for the best FIXME check this is unique!!*/
	dd->disk.forced_ref = 1;
	dd->disk.forced_guid = 1;
	memset(dd->disk.vendor, ' ', 32);
	memcpy(dd->disk.vendor, "Linux", 5);
	memset(dd->disk.pad, 0xff, 442);
	for (i = 0; i < ddf->max_part+1 ; i++)
		dd->vlist[i] = NULL;

	n = __be16_to_cpu(ddf->phys->used_pdes);
	pde = &ddf->phys->entries[n];
	n++;
	ddf->phys->used_pdes = __cpu_to_be16(n);

	memcpy(pde->guid, dd->disk.guid, DDF_GUID_LEN);
	pde->refnum = dd->disk.refnum;
	pde->type = __cpu_to_be16(DDF_Forced_PD_GUID |DDF_Global_Spare);
	pde->state = __cpu_to_be16(DDF_Online);
	get_dev_size(fd, NULL, &size);
	/* We are required to reserve 32Meg, and record the size in sectors */
	pde->config_size = __cpu_to_be64( (size - 32*1024*1024) / 512);
	sprintf(pde->path, "%17.17s","Information: nil") ;
	memset(pde->pad, 0xff, 6);

	ddf->dlist = dd;
}

/*
 * This is the write_init_super method for a ddf container.  It is
 * called when creating a container or adding another device to a
 * container.
 */

#ifndef MDASSEMBLE
static int write_init_super_ddf(struct supertype *st)
{

	struct ddf_super *ddf = st->sb;
	int i;
	struct dl *d;
	int n_config;
	int conf_size;

	unsigned long long size, sector;

	for (d = ddf->dlist; d; d=d->next) {
		int fd = d->fd;

		if (fd < 0)
			continue;

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
		write(fd, &ddf->primary, 512);

		ddf->controller.crc = calc_crc(&ddf->controller, 512);
		write(fd, &ddf->controller, 512);

		ddf->phys->crc = calc_crc(ddf->phys, ddf->pdsize);

		write(fd, ddf->phys, ddf->pdsize);

		ddf->virt->crc = calc_crc(ddf->virt, ddf->vdsize);
		write(fd, ddf->virt, ddf->vdsize);

		/* Now write lots of config records. */
		n_config = __be16_to_cpu(ddf->active->max_partitions);
		conf_size = __be16_to_cpu(ddf->active->config_record_len) * 512;
		for (i = 0 ; i <= n_config ; i++) {
			struct vcl *c = d->vlist[i];

			if (c) {
				c->conf.crc = calc_crc(&c->conf, conf_size);
				write(fd, &c->conf, conf_size);
			} else {
				__u32 sig = 0xffffffff;
				write(fd, &sig, 4);
				lseek64(fd, conf_size-4, SEEK_CUR);
			}
		}
		d->disk.crc = calc_crc(&d->disk, 512);
		write(fd, &d->disk, 512);

		/* Maybe do the same for secondary */

		lseek64(fd, (size-1)*512, SEEK_SET);
		write(fd, &ddf->anchor, 512);
		close(fd);
	}
	return 1;
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
int validate_geometry_ddf(struct supertype *st,
			  int level, int layout, int raiddisks,
			  int chunk, unsigned long long size,
			  char *dev, unsigned long long *freesize)
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

	if (level == LEVEL_CONTAINER) {
		st->ss = &super_ddf_container;
		if (dev) {
			int rv =st->ss->validate_geometry(st, level, layout,
							  raiddisks, chunk,
							  size,
							  NULL, freesize);
			if (rv)
				return rv;
		}
		return st->ss->validate_geometry(st, level, layout, raiddisks,
						 chunk, size, dev, freesize);
	}

	if (st->sb) {
		/* creating in a given container */
		st->ss = &super_ddf_bvd;
		if (dev) {
			int rv =st->ss->validate_geometry(st, level, layout,
							  raiddisks, chunk,
							  size,
							  NULL, freesize);
			if (rv)
				return rv;
		}
		return st->ss->validate_geometry(st, level, layout, raiddisks,
						 chunk, size, dev, freesize);
	}
	/* FIXME should exclude MULTIPATH, or more appropriately, allow
	 * only known levels.
	 */
	if (!dev)
		return 1;

	/* This device needs to be either a device in a 'ddf' container,
	 * or it needs to be a 'ddf-bvd' array.
	 */

	fd = open(dev, O_RDONLY|O_EXCL, 0);
	if (fd >= 0) {
		sra = sysfs_read(fd, 0, GET_VERSION);
		close(fd);
		if (sra && sra->array.major_version == -1 &&
		    strcmp(sra->text_version, "ddf-bvd") == 0) {
			st->ss = &super_ddf_svd;
			return st->ss->validate_geometry(st, level, layout,
							 raiddisks, chunk, size,
							 dev, freesize);
		}

		fprintf(stderr,
			Name ": Cannot create this array on device %s\n",
			dev);
		return 0;
	}
	if (errno != EBUSY || (fd = open(dev, O_RDONLY, 0)) < 0) {
		fprintf(stderr, Name ": Cannot open %s: %s\n",
			dev, strerror(errno));
		return 0;
	}
	/* Well, it is in use by someone, maybe a 'ddf' container. */
	cfd = open_container(fd);
	if (cfd < 0) {
		close(fd);
		fprintf(stderr, Name ": Cannot use %s: It is busy\n",
			dev);
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
		st->ss = &super_ddf_bvd;
		if (load_super_ddf_all(st, cfd, (void **)&ddf, NULL, 1) == 0) {
			st->sb = ddf;
			close(cfd);
			return st->ss->validate_geometry(st, level, layout,
							 raiddisks, chunk, size,
							 dev, freesize);
		}
		close(cfd);
	}
	fprintf(stderr, Name ": Cannot use %s: Already in use\n",
		dev);
	return 1;
}

int validate_geometry_ddf_container(struct supertype *st,
				    int level, int layout, int raiddisks,
				    int chunk, unsigned long long size,
				    char *dev, unsigned long long *freesize)
{
	int fd;
	unsigned long long ldsize;

	if (level != LEVEL_CONTAINER)
		return 0;
	if (!dev)
		return 1;

	fd = open(dev, O_RDONLY|O_EXCL, 0);
	if (fd < 0) {
		fprintf(stderr, Name ": Cannot open %s: %s\n",
			dev, strerror(errno));
		return 0;
	}
	if (!get_dev_size(fd, dev, &ldsize)) {
		close(fd);
		return 0;
	}
	close(fd);

	*freesize = avail_size_ddf(st, ldsize);

	return 1;
}

struct extent {
	unsigned long long start, size;
};
int cmp_extent(const void *av, const void *bv)
{
	const struct extent *a = av;
	const struct extent *b = bv;
	if (a->start < b->start)
		return -1;
	if (a->start > b->start)
		return 1;
	return 0;
}

struct extent *get_extents(struct ddf_super *ddf, struct dl *dl)
{
	/* find a list of used extents on the give physical device
	 * (dnum) or the given ddf.
	 * Return a malloced array of 'struct extent'

FIXME ignore DDF_Legacy devices?

	 */
	struct extent *rv;
	int n = 0;
	int dnum;
	int i, j;

	for (dnum = 0; dnum < ddf->phys->used_pdes; dnum++)
		if (memcmp(dl->disk.guid,
			   ddf->phys->entries[dnum].guid,
			   DDF_GUID_LEN) == 0)
			break;

	if (dnum == ddf->phys->used_pdes)
		return NULL;

	rv = malloc(sizeof(struct extent) * (ddf->max_part + 2));
	if (!rv)
		return NULL;

	for (i = 0; i < ddf->max_part+1; i++) {
		struct vcl *v = dl->vlist[i];
		if (v == NULL)
			continue;
		for (j=0; j < v->conf.prim_elmnt_count; j++)
			if (v->conf.phys_refnum[j] == dl->disk.refnum) {
				/* This device plays role 'j' in  'v'. */
				rv[n].start = __be64_to_cpu(v->lba_offset[j]);
				rv[n].size = __be64_to_cpu(v->conf.blocks);
				n++;
				break;
			}
	}
	qsort(rv, n, sizeof(*rv), cmp_extent);

	rv[n].start = __be64_to_cpu(ddf->phys->entries[dnum].config_size);
	rv[n].size = 0;
	return rv;
}

int validate_geometry_ddf_bvd(struct supertype *st,
			      int level, int layout, int raiddisks,
			      int chunk, unsigned long long size,
			      char *dev, unsigned long long *freesize)
{
	struct stat stb;
	struct ddf_super *ddf = st->sb;
	struct dl *dl;
	unsigned long long pos = 0;
	unsigned long long maxsize;
	struct extent *e;
	int i;
	/* ddf/bvd supports lots of things, but not containers */
	if (level == LEVEL_CONTAINER)
		return 0;
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
			fprintf(stderr, Name ": Not enough devices with space "
				"for this array (%d < %d)\n",
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
		if (dl->major == major(stb.st_rdev) &&
		    dl->minor == minor(stb.st_rdev))
			break;
	}
	if (!dl) {
		fprintf(stderr, Name ": %s is not in the same DDF set\n",
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
int validate_geometry_ddf_svd(struct supertype *st,
			      int level, int layout, int raiddisks,
			      int chunk, unsigned long long size,
			      char *dev, unsigned long long *freesize)
{
	/* dd/svd only supports striped, mirrored, concat, spanned... */
	if (level != LEVEL_LINEAR &&
	    level != 0 &&
	    level != 1)
		return 0;
	return 1;
}


static int load_super_ddf_all(struct supertype *st, int fd,
			      void **sbp, char *devname, int keep_fd)
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

	super = malloc(sizeof(*super));
	if (!super)
		return 1;

	/* first, try each device, and choose the best ddf */
	for (sd = sra->devs ; sd ; sd = sd->next) {
		int rv;
		sprintf(nm, "%d:%d", sd->disk.major, sd->disk.minor);
		dfd = dev_open(nm, keep_fd? O_RDWR : O_RDONLY);
		if (!dfd)
			return 2;
		rv = load_ddf_headers(dfd, super, NULL);
		if (!keep_fd) close(dfd);
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
	if (!dfd)
		return 1;
	load_ddf_headers(dfd, super, NULL);
	load_ddf_global(dfd, super, NULL);
	close(dfd);
	/* Now we need the device-local bits */
	for (sd = sra->devs ; sd ; sd = sd->next) {
		sprintf(nm, "%d:%d", sd->disk.major, sd->disk.minor);
		dfd = dev_open(nm, keep_fd? O_RDWR : O_RDONLY);
		if (!dfd)
			return 2;
		seq = load_ddf_local(dfd, super, NULL, keep_fd);
		if (!keep_fd) close(dfd);
	}
	*sbp = super;
	if (st->ss == NULL) {
		st->ss = &super_ddf;
		st->minor_version = 0;
		st->max_devs = 512;
	}
	return 0;
}
#endif



static int init_zero_ddf(struct supertype *st,
			 mdu_array_info_t *info,
			 unsigned long long size, char *name,
			 char *homehost, int *uuid)
{
	st->sb = NULL;
	return 0;
}

static int store_zero_ddf(struct supertype *st, int fd)
{
	unsigned long long dsize;
	char buf[512];
	memset(buf, 0, 512);


	if (!get_dev_size(fd, NULL, &dsize))
		return 1;

	lseek64(fd, dsize-512, 0);
	write(fd, buf, 512);
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

struct superswitch super_ddf = {
#ifndef	MDASSEMBLE
	.examine_super	= examine_super_ddf,
	.brief_examine_super = brief_examine_super_ddf,
	.detail_super	= detail_super_ddf,
	.brief_detail_super = brief_detail_super_ddf,
	.validate_geometry = validate_geometry_ddf,
#endif
	.match_home	= match_home_ddf,
	.uuid_from_super= uuid_from_super_ddf,
	.getinfo_super  = getinfo_super_ddf,
	.update_super	= update_super_ddf,

	.avail_size	= avail_size_ddf,

	.compare_super	= compare_super_ddf,

	.load_super	= load_super_ddf,
	.init_super	= init_zero_ddf,
	.store_super	= store_zero_ddf,
	.free_super	= free_super_ddf,
	.match_metadata_desc = match_metadata_desc_ddf,


	.major		= 1000,
	.swapuuid	= 0,
	.external	= 1,
	.text_version	= "ddf",
};

/* Super_ddf_container is set by validate_geometry_ddf when given a
 * device that is not part of any array
 */
struct superswitch super_ddf_container = {
#ifndef MDASSEMBLE
	.validate_geometry = validate_geometry_ddf_container,
	.write_init_super = write_init_super_ddf,
#endif

	.init_super	= init_super_ddf,
	.add_to_super	= add_to_super_ddf,

	.free_super	= free_super_ddf,

	.major		= 1000,
	.swapuuid	= 0,
	.external	= 1,
	.text_version	= "ddf",
};

struct superswitch super_ddf_bvd = {
#ifndef	MDASSEMBLE
//	.detail_super	= detail_super_ddf_bvd,
//	.brief_detail_super = brief_detail_super_ddf_bvd,
	.validate_geometry = validate_geometry_ddf_bvd,
	.write_init_super = write_init_super_ddf,
#endif
	.update_super	= update_super_ddf,
	.init_super	= init_super_ddf_bvd,
	.add_to_super	= add_to_super_ddf_bvd,
	.getinfo_super  = getinfo_super_ddf_bvd,

	.load_super	= load_super_ddf,
	.free_super	= free_super_ddf,
	.match_metadata_desc = match_metadata_desc_ddf_bvd,


	.major		= 1001,
	.swapuuid	= 0,
	.external	= 2,
	.text_version	= "ddf",
};

struct superswitch super_ddf_svd = {
#ifndef	MDASSEMBLE
//	.detail_super	= detail_super_ddf_svd,
//	.brief_detail_super = brief_detail_super_ddf_svd,
	.validate_geometry = validate_geometry_ddf_svd,
#endif
	.update_super	= update_super_ddf,
	.init_super	= init_super_ddf,

	.load_super	= load_super_ddf,
	.free_super	= free_super_ddf,
	.match_metadata_desc = match_metadata_desc_ddf_svd,

	.major		= 1002,
	.swapuuid	= 0,
	.external	= 2,
	.text_version	= "ddf",
};
