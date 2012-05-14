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

#include "mdadm.h"
/*
 * The version-1 superblock :
 * All numeric fields are little-endian.
 *
 * total size: 256 bytes plus 2 per device.
 *  1K allows 384 devices.
 */
struct mdp_superblock_1 {
	/* constant array information - 128 bytes */
	__u32	magic;		/* MD_SB_MAGIC: 0xa92b4efc - little endian */
	__u32	major_version;	/* 1 */
	__u32	feature_map;	/* 0 for now */
	__u32	pad0;		/* always set to 0 when writing */

	__u8	set_uuid[16];	/* user-space generated. */
	char	set_name[32];	/* set and interpreted by user-space */

	__u64	ctime;		/* lo 40 bits are seconds, top 24 are microseconds or 0*/
	__u32	level;		/* -4 (multipath), -1 (linear), 0,1,4,5 */
	__u32	layout;		/* only for raid5 currently */
	__u64	size;		/* used size of component devices, in 512byte sectors */

	__u32	chunksize;	/* in 512byte sectors */
	__u32	raid_disks;
	__u32	bitmap_offset;	/* sectors after start of superblock that bitmap starts
				 * NOTE: signed, so bitmap can be before superblock
				 * only meaningful of feature_map[0] is set.
				 */

	/* These are only valid with feature bit '4' */
	__u32	new_level;	/* new level we are reshaping to		*/
	__u64	reshape_position;	/* next address in array-space for reshape */
	__u32	delta_disks;	/* change in number of raid_disks		*/
	__u32	new_layout;	/* new layout					*/
	__u32	new_chunk;	/* new chunk size (bytes)			*/
	__u8	pad1[128-124];	/* set to 0 when written */

	/* constant this-device information - 64 bytes */
	__u64	data_offset;	/* sector start of data, often 0 */
	__u64	data_size;	/* sectors in this device that can be used for data */
	__u64	super_offset;	/* sector start of this superblock */
	__u64	recovery_offset;/* sectors before this offset (from data_offset) have been recovered */
	__u32	dev_number;	/* permanent identifier of this  device - not role in raid */
	__u32	cnt_corrected_read; /* number of read errors that were corrected by re-writing */
	__u8	device_uuid[16]; /* user-space setable, ignored by kernel */
        __u8    devflags;        /* per-device flags.  Only one defined...*/
#define WriteMostly1    1        /* mask for writemostly flag in above */
	__u8	pad2[64-57];	/* set to 0 when writing */

	/* array state information - 64 bytes */
	__u64	utime;		/* 40 bits second, 24 btes microseconds */
	__u64	events;		/* incremented when superblock updated */
	__u64	resync_offset;	/* data before this offset (from data_offset) known to be in sync */
	__u32	sb_csum;	/* checksum upto dev_roles[max_dev] */
	__u32	max_dev;	/* size of dev_roles[] array to consider */
	__u8	pad3[64-32];	/* set to 0 when writing */

	/* device state information. Indexed by dev_number.
	 * 2 bytes per device
	 * Note there are no per-device state flags. State information is rolled
	 * into the 'roles' value.  If a device is spare or faulty, then it doesn't
	 * have a meaningful role.
	 */
	__u16	dev_roles[0];	/* role in array, or 0xffff for a spare, or 0xfffe for faulty */
};

#define MAX_SB_SIZE 4096
/* bitmap super size is 256, but we round up to a sector for alignment */
#define BM_SUPER_SIZE 512
#define MAX_DEVS ((int)(MAX_SB_SIZE - sizeof(struct mdp_superblock_1)) / 2)
#define SUPER1_SIZE	(MAX_SB_SIZE + BM_SUPER_SIZE \
			 + sizeof(struct misc_dev_info))

struct misc_dev_info {
	__u64 device_size;
};

/* feature_map bits */
#define MD_FEATURE_BITMAP_OFFSET	1
#define	MD_FEATURE_RECOVERY_OFFSET	2 /* recovery_offset is present and
					   * must be honoured
					   */
#define	MD_FEATURE_RESHAPE_ACTIVE	4

#define	MD_FEATURE_ALL			(1|2|4)

#ifndef offsetof
#define offsetof(t,f) ((size_t)&(((t*)0)->f))
#endif
static unsigned int calc_sb_1_csum(struct mdp_superblock_1 * sb)
{
	unsigned int disk_csum, csum;
	unsigned long long newcsum;
	int size = sizeof(*sb) + __le32_to_cpu(sb->max_dev)*2;
	unsigned int *isuper = (unsigned int*)sb;

/* make sure I can count... */
	if (offsetof(struct mdp_superblock_1,data_offset) != 128 ||
	    offsetof(struct mdp_superblock_1, utime) != 192 ||
	    sizeof(struct mdp_superblock_1) != 256) {
		fprintf(stderr, "WARNING - superblock isn't sized correctly\n");
	}

	disk_csum = sb->sb_csum;
	sb->sb_csum = 0;
	newcsum = 0;
	for (; size>=4; size -= 4 ) {
		newcsum += __le32_to_cpu(*isuper);
		isuper++;
	}

	if (size == 2)
		newcsum += __le16_to_cpu(*(unsigned short*) isuper);

	csum = (newcsum & 0xffffffff) + (newcsum >> 32);
	sb->sb_csum = disk_csum;
	return __cpu_to_le32(csum);
}

/*
 * Information related to file descriptor used for aligned reads/writes.
 * Cache the block size.
 */
struct align_fd {
	int fd;
	int blk_sz;
};

static void init_afd(struct align_fd *afd, int fd)
{
	afd->fd = fd;

	if (ioctl(afd->fd, BLKSSZGET, &afd->blk_sz) != 0)
		afd->blk_sz = 512;
}

static char abuf[4096+4096];
static int aread(struct align_fd *afd, void *buf, int len)
{
	/* aligned read.
	 * On devices with a 4K sector size, we need to read
	 * the full sector and copy relevant bits into
	 * the buffer
	 */
	int bsize, iosize;
	char *b;
	int n;

	bsize = afd->blk_sz;

	if (!bsize || bsize > 4096 || len > 4096) {
		if (!bsize)
			fprintf(stderr, "WARNING - aread() called with "
				"invalid block size\n");
		return -1;
	}
	b = ROUND_UP_PTR((char *)abuf, 4096);

	for (iosize = 0; iosize < len; iosize += bsize)
		;
	n = read(afd->fd, b, iosize);
	if (n <= 0)
		return n;
	lseek(afd->fd, len - n, 1);
	if (n > len)
		n = len;
	memcpy(buf, b, n);
	return n;
}

static int awrite(struct align_fd *afd, void *buf, int len)
{
	/* aligned write.
	 * On devices with a 4K sector size, we need to write
	 * the full sector.  We pre-read if the sector is larger
	 * than the write.
	 * The address must be sector-aligned.
	 */
	int bsize, iosize;
	char *b;
	int n;

	bsize = afd->blk_sz;
	if (!bsize || bsize > 4096 || len > 4096) {
		if (!bsize)
			fprintf(stderr, "WARNING - awrite() called with "
				"invalid block size\n");
		return -1;
	}
	b = ROUND_UP_PTR((char *)abuf, 4096);

	for (iosize = 0; iosize < len ; iosize += bsize)
		;

	if (len != iosize) {
		n = read(afd->fd, b, iosize);
		if (n <= 0)
			return n;
		lseek(afd->fd, -n, 1);
	}

	memcpy(b, buf, len);
	n = write(afd->fd, b, iosize);
	if (n <= 0)
		return n;
	lseek(afd->fd, len - n, 1);
	return len;
}

#ifndef MDASSEMBLE
static void examine_super1(struct supertype *st, char *homehost)
{
	struct mdp_superblock_1 *sb = st->sb;
	time_t atime;
	unsigned int d;
	int role;
	int delta_extra = 0;
	int i;
	char *c;
	int l = homehost ? strlen(homehost) : 0;
	int layout;
	unsigned long long sb_offset;

	printf("          Magic : %08x\n", __le32_to_cpu(sb->magic));
	printf("        Version : 1");
	sb_offset = __le64_to_cpu(sb->super_offset);
	if (sb_offset <= 4)
		printf(".1\n");
	else if (sb_offset <= 8)
		printf(".2\n");
	else
		printf(".0\n");
	printf("    Feature Map : 0x%x\n", __le32_to_cpu(sb->feature_map));
	printf("     Array UUID : ");
	for (i=0; i<16; i++) {
		if ((i&3)==0 && i != 0) printf(":");
		printf("%02x", sb->set_uuid[i]);
	}
	printf("\n");
	printf("           Name : %.32s", sb->set_name);
	if (l > 0 && l < 32 &&
	    sb->set_name[l] == ':' &&
	    strncmp(sb->set_name, homehost, l) == 0)
		printf("  (local to host %s)", homehost);
	printf("\n");
	atime = __le64_to_cpu(sb->ctime) & 0xFFFFFFFFFFULL;
	printf("  Creation Time : %.24s\n", ctime(&atime));
	c=map_num(pers, __le32_to_cpu(sb->level));
	printf("     Raid Level : %s\n", c?c:"-unknown-");
	printf("   Raid Devices : %d\n", __le32_to_cpu(sb->raid_disks));
	printf("\n");
	printf(" Avail Dev Size : %llu%s\n",
	       (unsigned long long)__le64_to_cpu(sb->data_size),
	       human_size(__le64_to_cpu(sb->data_size)<<9));
	if (__le32_to_cpu(sb->level) > 0) {
		int ddsks = 0, ddsks_denom = 1;
		switch(__le32_to_cpu(sb->level)) {
		case 1: ddsks=1;break;
		case 4:
		case 5: ddsks = __le32_to_cpu(sb->raid_disks)-1; break;
		case 6: ddsks = __le32_to_cpu(sb->raid_disks)-2; break;
		case 10:
			layout = __le32_to_cpu(sb->layout);
			ddsks = __le32_to_cpu(sb->raid_disks);
			ddsks_denom = (layout&255) * ((layout>>8)&255);
		}
		if (ddsks) {
			long long asize = __le64_to_cpu(sb->size);
			asize = (asize << 9) * ddsks / ddsks_denom;
			printf("     Array Size : %llu%s\n",
			       asize >> 10,  human_size(asize));
		}
		if (sb->size != sb->data_size)
			printf("  Used Dev Size : %llu%s\n",
			       (unsigned long long)__le64_to_cpu(sb->size),
			       human_size(__le64_to_cpu(sb->size)<<9));
	}
	if (sb->data_offset)
		printf("    Data Offset : %llu sectors\n",
		       (unsigned long long)__le64_to_cpu(sb->data_offset));
	printf("   Super Offset : %llu sectors\n",
	       (unsigned long long)__le64_to_cpu(sb->super_offset));
	if (__le32_to_cpu(sb->feature_map) & MD_FEATURE_RECOVERY_OFFSET)
		printf("Recovery Offset : %llu sectors\n", (unsigned long long)__le64_to_cpu(sb->recovery_offset));
	printf("          State : %s\n", (__le64_to_cpu(sb->resync_offset)+1)? "active":"clean");
	printf("    Device UUID : ");
	for (i=0; i<16; i++) {
		if ((i&3)==0 && i != 0) printf(":");
		printf("%02x", sb->device_uuid[i]);
	}
	printf("\n");
	printf("\n");
	if (sb->feature_map & __cpu_to_le32(MD_FEATURE_BITMAP_OFFSET)) {
		printf("Internal Bitmap : %ld sectors from superblock\n",
		       (long)(int32_t)__le32_to_cpu(sb->bitmap_offset));
	}
	if (sb->feature_map & __le32_to_cpu(MD_FEATURE_RESHAPE_ACTIVE)) {
		printf("  Reshape pos'n : %llu%s\n", (unsigned long long)__le64_to_cpu(sb->reshape_position)/2,
		       human_size(__le64_to_cpu(sb->reshape_position)<<9));
		if (__le32_to_cpu(sb->delta_disks)) {
			printf("  Delta Devices : %d", __le32_to_cpu(sb->delta_disks));
			printf(" (%d->%d)\n",
			       __le32_to_cpu(sb->raid_disks)-__le32_to_cpu(sb->delta_disks),
			       __le32_to_cpu(sb->raid_disks));
			if ((int)__le32_to_cpu(sb->delta_disks) < 0)
				delta_extra = -__le32_to_cpu(sb->delta_disks);
		}
		if (__le32_to_cpu(sb->new_level) != __le32_to_cpu(sb->level)) {
			c = map_num(pers, __le32_to_cpu(sb->new_level));
			printf("      New Level : %s\n", c?c:"-unknown-");
		}
		if (__le32_to_cpu(sb->new_layout) != __le32_to_cpu(sb->layout)) {
			if (__le32_to_cpu(sb->level) == 5) {
				c = map_num(r5layout, __le32_to_cpu(sb->new_layout));
				printf("     New Layout : %s\n", c?c:"-unknown-");
			}
			if (__le32_to_cpu(sb->level) == 6) {
				c = map_num(r6layout, __le32_to_cpu(sb->new_layout));
				printf("     New Layout : %s\n", c?c:"-unknown-");
			}
			if (__le32_to_cpu(sb->level) == 10) {
				printf("     New Layout :");
				print_r10_layout(__le32_to_cpu(sb->new_layout));
				printf("\n");
			}
		}
		if (__le32_to_cpu(sb->new_chunk) != __le32_to_cpu(sb->chunksize))
			printf("  New Chunksize : %dK\n", __le32_to_cpu(sb->new_chunk)/2);
		printf("\n");
	}
	if (sb->devflags) {
		printf("          Flags :");
		if (sb->devflags & WriteMostly1)
			printf(" write-mostly");
		printf("\n");
	}

	atime = __le64_to_cpu(sb->utime) & 0xFFFFFFFFFFULL;
	printf("    Update Time : %.24s\n", ctime(&atime));

	if (calc_sb_1_csum(sb) == sb->sb_csum)
		printf("       Checksum : %x - correct\n", __le32_to_cpu(sb->sb_csum));
	else
		printf("       Checksum : %x - expected %x\n", __le32_to_cpu(sb->sb_csum),
		       __le32_to_cpu(calc_sb_1_csum(sb)));
	printf("         Events : %llu\n", (unsigned long long)__le64_to_cpu(sb->events));
	printf("\n");
	if (__le32_to_cpu(sb->level) == 5) {
		c = map_num(r5layout, __le32_to_cpu(sb->layout));
		printf("         Layout : %s\n", c?c:"-unknown-");
	}
	if (__le32_to_cpu(sb->level) == 6) {
		c = map_num(r6layout, __le32_to_cpu(sb->layout));
		printf("         Layout : %s\n", c?c:"-unknown-");
	}
	if (__le32_to_cpu(sb->level) == 10) {
		int lo = __le32_to_cpu(sb->layout);
		printf("         Layout :");
		print_r10_layout(lo);
		printf("\n");
	}
	switch(__le32_to_cpu(sb->level)) {
	case 0:
	case 4:
	case 5:
	case 6:
	case 10:
		printf("     Chunk Size : %dK\n", __le32_to_cpu(sb->chunksize)/2);
		break;
	case -1:
		printf("       Rounding : %dK\n", __le32_to_cpu(sb->chunksize)/2);
		break;
	default: break;
	}
	printf("\n");
#if 0
	/* This turns out to just be confusing */
	printf("    Array Slot : %d (", __le32_to_cpu(sb->dev_number));
	for (i= __le32_to_cpu(sb->max_dev); i> 0 ; i--)
		if (__le16_to_cpu(sb->dev_roles[i-1]) != 0xffff)
			break;
	for (d=0; d < i; d++) {
		int role = __le16_to_cpu(sb->dev_roles[d]);
		if (d) printf(", ");
		if (role == 0xffff) printf("empty");
		else if(role == 0xfffe) printf("failed");
		else printf("%d", role);
	}
	printf(")\n");
#endif
	printf("   Device Role : ");
	d = __le32_to_cpu(sb->dev_number);
	if (d < __le32_to_cpu(sb->max_dev))
		role = __le16_to_cpu(sb->dev_roles[d]);
	else
		role = 0xFFFF;
	if (role >= 0xFFFE)
		printf("spare\n");
	else
		printf("Active device %d\n", role);

	printf("   Array State : ");
	for (d=0; d<__le32_to_cpu(sb->raid_disks) + delta_extra; d++) {
		int cnt = 0;
		unsigned int i;
		for (i=0; i< __le32_to_cpu(sb->max_dev); i++) {
			unsigned int role = __le16_to_cpu(sb->dev_roles[i]);
			if (role == d)
				cnt++;
		}
		if (cnt > 1) printf("?");
		else if (cnt == 1) printf("A");
		else printf (".");
	}
#if 0
	/* This is confusing too */
	faulty = 0;
	for (i=0; i< __le32_to_cpu(sb->max_dev); i++) {
		int role = __le16_to_cpu(sb->dev_roles[i]);
		if (role == 0xFFFE)
			faulty++;
	}
	if (faulty) printf(" %d failed", faulty);
#endif
	printf(" ('A' == active, '.' == missing)");
	printf("\n");
}


static void brief_examine_super1(struct supertype *st, int verbose)
{
	struct mdp_superblock_1 *sb = st->sb;
	int i;
	unsigned long long sb_offset;
	char *nm;
	char *c=map_num(pers, __le32_to_cpu(sb->level));

	nm = strchr(sb->set_name, ':');
	if (nm)
		nm++;
	else if (sb->set_name[0])
		nm = sb->set_name;
	else
		nm = NULL;

	printf("ARRAY%s%s", nm ? " /dev/md/":"", nm);
	if (verbose && c)
		printf(" level=%s", c);
	sb_offset = __le64_to_cpu(sb->super_offset);
	if (sb_offset <= 4)
		printf(" metadata=1.1 ");
	else if (sb_offset <= 8)
		printf(" metadata=1.2 ");
	else
		printf(" metadata=1.0 ");
	if (verbose)
		printf("num-devices=%d ", __le32_to_cpu(sb->raid_disks));
	printf("UUID=");
	for (i=0; i<16; i++) {
		if ((i&3)==0 && i != 0) printf(":");
		printf("%02x", sb->set_uuid[i]);
	}
	if (sb->set_name[0])
		printf(" name=%.32s", sb->set_name);
	printf("\n");
}

static void export_examine_super1(struct supertype *st)
{
	struct mdp_superblock_1 *sb = st->sb;
	int i;
	int len = 32;

	printf("MD_LEVEL=%s\n", map_num(pers, __le32_to_cpu(sb->level)));
	printf("MD_DEVICES=%d\n", __le32_to_cpu(sb->raid_disks));
	for (i=0; i<32; i++)
		if (sb->set_name[i] == '\n' ||
		    sb->set_name[i] == '\0') {
			len = i;
			break;
		}
	if (len)
		printf("MD_NAME=%.*s\n", len, sb->set_name);
	printf("MD_UUID=");
	for (i=0; i<16; i++) {
		if ((i&3)==0 && i != 0) printf(":");
		printf("%02x", sb->set_uuid[i]);
	}
	printf("\n");
	printf("MD_UPDATE_TIME=%llu\n",
	       __le64_to_cpu(sb->utime) & 0xFFFFFFFFFFULL);
	printf("MD_DEV_UUID=");
	for (i=0; i<16; i++) {
		if ((i&3)==0 && i != 0) printf(":");
		printf("%02x", sb->device_uuid[i]);
	}
	printf("\n");
	printf("MD_EVENTS=%llu\n",
	       (unsigned long long)__le64_to_cpu(sb->events));
}

static void detail_super1(struct supertype *st, char *homehost)
{
	struct mdp_superblock_1 *sb = st->sb;
	int i;
	int l = homehost ? strlen(homehost) : 0;

	printf("           Name : %.32s", sb->set_name);
	if (l > 0 && l < 32 &&
	    sb->set_name[l] == ':' &&
	    strncmp(sb->set_name, homehost, l) == 0)
		printf("  (local to host %s)", homehost);
	printf("\n           UUID : ");
	for (i=0; i<16; i++) {
		if ((i&3)==0 && i != 0) printf(":");
		printf("%02x", sb->set_uuid[i]);
	}
	printf("\n         Events : %llu\n\n", (unsigned long long)__le64_to_cpu(sb->events));
}

static void brief_detail_super1(struct supertype *st)
{
	struct mdp_superblock_1 *sb = st->sb;
	int i;

	if (sb->set_name[0])
		printf(" name=%.32s", sb->set_name);
	printf(" UUID=");
	for (i=0; i<16; i++) {
		if ((i&3)==0 && i != 0) printf(":");
		printf("%02x", sb->set_uuid[i]);
	}
}

static void export_detail_super1(struct supertype *st)
{
	struct mdp_superblock_1 *sb = st->sb;
	int i;
	int len = 32;

	for (i=0; i<32; i++)
		if (sb->set_name[i] == '\n' ||
		    sb->set_name[i] == '\0') {
			len = i;
			break;
		}
	if (len)
		printf("MD_NAME=%.*s\n", len, sb->set_name);
}

#endif

static int match_home1(struct supertype *st, char *homehost)
{
	struct mdp_superblock_1 *sb = st->sb;
	int l = homehost ? strlen(homehost) : 0;

	return (l > 0 && l < 32 &&
		sb->set_name[l] == ':' &&
		strncmp(sb->set_name, homehost, l) == 0);
}

static void uuid_from_super1(struct supertype *st, int uuid[4])
{
	struct mdp_superblock_1 *super = st->sb;
	char *cuuid = (char*)uuid;
	int i;
	for (i=0; i<16; i++)
		cuuid[i] = super->set_uuid[i];
}

static void getinfo_super1(struct supertype *st, struct mdinfo *info, char *map)
{
	struct mdp_superblock_1 *sb = st->sb;
	int working = 0;
	unsigned int i;
	unsigned int role;
	unsigned int map_disks = info->array.raid_disks;

	memset(info, 0, sizeof(*info));
	info->array.major_version = 1;
	info->array.minor_version = st->minor_version;
	info->array.patch_version = 0;
	info->array.raid_disks = __le32_to_cpu(sb->raid_disks);
	info->array.level = __le32_to_cpu(sb->level);
	info->array.layout = __le32_to_cpu(sb->layout);
	info->array.md_minor = -1;
	info->array.ctime = __le64_to_cpu(sb->ctime);
	info->array.utime = __le64_to_cpu(sb->utime);
	info->array.chunk_size = __le32_to_cpu(sb->chunksize)*512;
	info->array.state =
		(__le64_to_cpu(sb->resync_offset) == MaxSector)
		? 1 : 0;

	info->data_offset = __le64_to_cpu(sb->data_offset);
	info->component_size = __le64_to_cpu(sb->size);
	if (sb->feature_map & __le32_to_cpu(MD_FEATURE_BITMAP_OFFSET))
		info->bitmap_offset = (int32_t)__le32_to_cpu(sb->bitmap_offset);

	info->disk.major = 0;
	info->disk.minor = 0;
	info->disk.number = __le32_to_cpu(sb->dev_number);
	if (__le32_to_cpu(sb->dev_number) >= __le32_to_cpu(sb->max_dev) ||
	    __le32_to_cpu(sb->dev_number) >= MAX_DEVS)
		role = 0xfffe;
	else
		role = __le16_to_cpu(sb->dev_roles[__le32_to_cpu(sb->dev_number)]);

	info->disk.raid_disk = -1;
	switch(role) {
	case 0xFFFF:
		info->disk.state = 0; /* spare: not active, not sync, not faulty */
		break;
	case 0xFFFE:
		info->disk.state = 1; /* faulty */
		break;
	default:
		info->disk.state = 6; /* active and in sync */
		info->disk.raid_disk = role;
	}
	if (sb->devflags & WriteMostly1)
		info->disk.state |= (1 << MD_DISK_WRITEMOSTLY);
	info->events = __le64_to_cpu(sb->events);
	sprintf(info->text_version, "1.%d", st->minor_version);
	info->safe_mode_delay = 200;

	memcpy(info->uuid, sb->set_uuid, 16);

	strncpy(info->name, sb->set_name, 32);
	info->name[32] = 0;

	if (sb->feature_map & __le32_to_cpu(MD_FEATURE_RECOVERY_OFFSET))
		info->recovery_start = __le32_to_cpu(sb->recovery_offset);
	else
		info->recovery_start = MaxSector;

	if (sb->feature_map & __le32_to_cpu(MD_FEATURE_RESHAPE_ACTIVE)) {
		info->reshape_active = 1;
		info->reshape_progress = __le64_to_cpu(sb->reshape_position);
		info->new_level = __le32_to_cpu(sb->new_level);
		info->delta_disks = __le32_to_cpu(sb->delta_disks);
		info->new_layout = __le32_to_cpu(sb->new_layout);
		info->new_chunk = __le32_to_cpu(sb->new_chunk)<<9;
		if (info->delta_disks < 0)
			info->array.raid_disks -= info->delta_disks;
	} else
		info->reshape_active = 0;

	info->recovery_blocked = info->reshape_active;

	if (map)
		for (i=0; i<map_disks; i++)
			map[i] = 0;
	for (i = 0; i < __le32_to_cpu(sb->max_dev); i++) {
		role = __le16_to_cpu(sb->dev_roles[i]);
		if (/*role == 0xFFFF || */role < (unsigned) info->array.raid_disks) {
			working++;
			if (map && role < map_disks)
				map[role] = 1;
		}
	}

	info->array.working_disks = working;
}

static struct mdinfo *container_content1(struct supertype *st, char *subarray)
{
	struct mdinfo *info;

	if (subarray)
		return NULL;

	info = malloc(sizeof(*info));
	getinfo_super1(st, info, NULL);
	return info;
}

static int update_super1(struct supertype *st, struct mdinfo *info,
			 char *update,
			 char *devname, int verbose,
			 int uuid_set, char *homehost)
{
	/* NOTE: for 'assemble' and 'force' we need to return non-zero
	 * if any change was made.  For others, the return value is
	 * ignored.
	 */
	int rv = 0;
	struct mdp_superblock_1 *sb = st->sb;

	if (strcmp(update, "force-one")==0) {
		/* Not enough devices for a working array,
		 * so bring this one up-to-date
		 */
		if (sb->events != __cpu_to_le64(info->events))
			rv = 1;
		sb->events = __cpu_to_le64(info->events);
	} else if (strcmp(update, "force-array")==0) {
		/* Degraded array and 'force' requests to
		 * maybe need to mark it 'clean'.
		 */
		switch(__le32_to_cpu(sb->level)) {
		case 5: case 4: case 6:
			/* need to force clean */
			if (sb->resync_offset != MaxSector)
				rv = 1;
			sb->resync_offset = MaxSector;
		}
	} else if (strcmp(update, "assemble")==0) {
		int d = info->disk.number;
		int want;
		if (info->disk.state == 6)
			want = info->disk.raid_disk;
		else
			want = 0xFFFF;
		if (sb->dev_roles[d] != __cpu_to_le16(want)) {
			sb->dev_roles[d] = __cpu_to_le16(want);
			rv = 1;
		}
		if (info->reshape_active &&
		    sb->feature_map & __le32_to_cpu(MD_FEATURE_RESHAPE_ACTIVE) &&
		    info->delta_disks >= 0 &&
		    info->reshape_progress < __le64_to_cpu(sb->reshape_position)) {
			sb->reshape_position = __cpu_to_le64(info->reshape_progress);
			rv = 1;
		}
		if (info->reshape_active &&
		    sb->feature_map & __le32_to_cpu(MD_FEATURE_RESHAPE_ACTIVE) &&
		    info->delta_disks < 0 &&
		    info->reshape_progress > __le64_to_cpu(sb->reshape_position)) {
			sb->reshape_position = __cpu_to_le64(info->reshape_progress);
			rv = 1;
		}
	} else if (strcmp(update, "linear-grow-new") == 0) {
		unsigned int i;
		int rfd, fd;
		unsigned int max = __le32_to_cpu(sb->max_dev);

		for (i=0 ; i < max ; i++)
			if (__le16_to_cpu(sb->dev_roles[i]) >= 0xfffe)
				break;
		sb->dev_number = __cpu_to_le32(i);
		info->disk.number = i;
		if (max >= __le32_to_cpu(sb->max_dev))
			sb->max_dev = __cpu_to_le32(max+1);

		if ((rfd = open("/dev/urandom", O_RDONLY)) < 0 ||
		    read(rfd, sb->device_uuid, 16) != 16) {
			__u32 r[4] = {random(), random(), random(), random()};
			memcpy(sb->device_uuid, r, 16);
		}
		if (rfd >= 0)
			close(rfd);

		sb->dev_roles[i] =
			__cpu_to_le16(info->disk.raid_disk);

		fd = open(devname, O_RDONLY);
		if (fd >= 0) {
			unsigned long long ds;
			get_dev_size(fd, devname, &ds);
			close(fd);
			ds >>= 9;
			if (__le64_to_cpu(sb->super_offset) <
			    __le64_to_cpu(sb->data_offset)) {
				sb->data_size = __cpu_to_le64(
					ds - __le64_to_cpu(sb->data_offset));
			} else {
				ds -= 8*2;
				ds &= ~(unsigned long long)(4*2-1);
				sb->super_offset = __cpu_to_le64(ds);
				sb->data_size = __cpu_to_le64(
					ds - __le64_to_cpu(sb->data_offset));
			}
		}
	} else if (strcmp(update, "linear-grow-update") == 0) {
		sb->raid_disks = __cpu_to_le32(info->array.raid_disks);
		sb->dev_roles[info->disk.number] =
			__cpu_to_le16(info->disk.raid_disk);
	} else if (strcmp(update, "resync") == 0) {
		/* make sure resync happens */
		sb->resync_offset = 0ULL;
	} else if (strcmp(update, "uuid") == 0) {
		copy_uuid(sb->set_uuid, info->uuid, super1.swapuuid);

		if (__le32_to_cpu(sb->feature_map)&MD_FEATURE_BITMAP_OFFSET) {
			struct bitmap_super_s *bm;
			bm = (struct bitmap_super_s*)(st->sb+MAX_SB_SIZE);
			memcpy(bm->uuid, sb->set_uuid, 16);
		}
	} else if (strcmp(update, "no-bitmap") == 0) {
		sb->feature_map &= ~__cpu_to_le32(MD_FEATURE_BITMAP_OFFSET);
	} else if (strcmp(update, "homehost") == 0 &&
		   homehost) {
		char *c;
		update = "name";
		c = strchr(sb->set_name, ':');
		if (c)
			strncpy(info->name, c+1, 31 - (c-sb->set_name));
		else
			strncpy(info->name, sb->set_name, 32);
		info->name[32] = 0;
	} else if (strcmp(update, "name") == 0) {
		if (info->name[0] == 0)
			sprintf(info->name, "%d", info->array.md_minor);
		memset(sb->set_name, 0, sizeof(sb->set_name));
		if (homehost &&
		    strchr(info->name, ':') == NULL &&
		    strlen(homehost)+1+strlen(info->name) < 32) {
			strcpy(sb->set_name, homehost);
			strcat(sb->set_name, ":");
			strcat(sb->set_name, info->name);
		} else
			strcpy(sb->set_name, info->name);
	} else if (strcmp(update, "devicesize") == 0 &&
	    __le64_to_cpu(sb->super_offset) <
	    __le64_to_cpu(sb->data_offset)) {
		/* set data_size to device size less data_offset */
		struct misc_dev_info *misc = (struct misc_dev_info*)
			(st->sb + MAX_SB_SIZE + BM_SUPER_SIZE);
		printf("Size was %llu\n", (unsigned long long)
		       __le64_to_cpu(sb->data_size));
		sb->data_size = __cpu_to_le64(
			misc->device_size - __le64_to_cpu(sb->data_offset));
		printf("Size is %llu\n", (unsigned long long)
		       __le64_to_cpu(sb->data_size));
	} else if (strcmp(update, "_reshape_progress")==0)
		sb->reshape_position = __cpu_to_le64(info->reshape_progress);
	else if (strcmp(update, "writemostly")==0)
		sb->devflags |= WriteMostly1;
	else if (strcmp(update, "readwrite")==0)
		sb->devflags &= ~WriteMostly1;
	else
		rv = -1;

	sb->sb_csum = calc_sb_1_csum(sb);
	return rv;
}

static int init_super1(struct supertype *st, mdu_array_info_t *info,
		       unsigned long long size, char *name, char *homehost, int *uuid)
{
	struct mdp_superblock_1 *sb;
	int spares;
	int rfd;
	char defname[10];
	int sbsize;

	if (posix_memalign((void**)&sb, 4096, SUPER1_SIZE) != 0) {
		fprintf(stderr, Name
			": %s could not allocate superblock\n", __func__);
		return 0;
	}
	memset(sb, 0, SUPER1_SIZE);

	st->sb = sb;
	if (info == NULL) {
		/* zeroing superblock */
		return 0;
	}

	spares = info->working_disks - info->active_disks;
	if (info->raid_disks + spares  > MAX_DEVS) {
		fprintf(stderr, Name ": too many devices requested: %d+%d > %d\n",
			info->raid_disks , spares, MAX_DEVS);
		return 0;
	}

	sb->magic = __cpu_to_le32(MD_SB_MAGIC);
	sb->major_version = __cpu_to_le32(1);
	sb->feature_map = 0;
	sb->pad0 = 0;

	if (uuid)
		copy_uuid(sb->set_uuid, uuid, super1.swapuuid);
	else {
		if ((rfd = open("/dev/urandom", O_RDONLY)) < 0 ||
		    read(rfd, sb->set_uuid, 16) != 16) {
			__u32 r[4] = {random(), random(), random(), random()};
			memcpy(sb->set_uuid, r, 16);
		}
		if (rfd >= 0) close(rfd);
	}

	if (name == NULL || *name == 0) {
		sprintf(defname, "%d", info->md_minor);
		name = defname;
	}
	if (homehost &&
	    strchr(name, ':')== NULL &&
	    strlen(homehost)+1+strlen(name) < 32) {
		strcpy(sb->set_name, homehost);
		strcat(sb->set_name, ":");
		strcat(sb->set_name, name);
	} else
		strcpy(sb->set_name, name);

	sb->ctime = __cpu_to_le64((unsigned long long)time(0));
	sb->level = __cpu_to_le32(info->level);
	sb->layout = __cpu_to_le32(info->layout);
	sb->size = __cpu_to_le64(size*2ULL);
	sb->chunksize = __cpu_to_le32(info->chunk_size>>9);
	sb->raid_disks = __cpu_to_le32(info->raid_disks);

	sb->data_offset = __cpu_to_le64(0);
	sb->data_size = __cpu_to_le64(0);
	sb->super_offset = __cpu_to_le64(0);
	sb->recovery_offset = __cpu_to_le64(0);

	sb->utime = sb->ctime;
	sb->events = __cpu_to_le64(1);
	if (info->state & (1<<MD_SB_CLEAN))
		sb->resync_offset = MaxSector;
	else
		sb->resync_offset = 0;
	sbsize = sizeof(struct mdp_superblock_1) + 2 * (info->raid_disks + spares);
	sbsize = ROUND_UP(sbsize, 512);
	sb->max_dev = __cpu_to_le32((sbsize - sizeof(struct mdp_superblock_1)) / 2);

	memset(sb->dev_roles, 0xff, MAX_SB_SIZE - sizeof(struct mdp_superblock_1));

	return 1;
}

struct devinfo {
	int fd;
	char *devname;
	mdu_disk_info_t disk;
	struct devinfo *next;
};
#ifndef MDASSEMBLE
/* Add a device to the superblock being created */
static int add_to_super1(struct supertype *st, mdu_disk_info_t *dk,
			  int fd, char *devname)
{
	struct mdp_superblock_1 *sb = st->sb;
	__u16 *rp = sb->dev_roles + dk->number;
	struct devinfo *di, **dip;

	if ((dk->state & 6) == 6) /* active, sync */
		*rp = __cpu_to_le16(dk->raid_disk);
	else if ((dk->state & ~2) == 0) /* active or idle -> spare */
		*rp = 0xffff;
	else
		*rp = 0xfffe;

	if (dk->number >= (int)__le32_to_cpu(sb->max_dev) &&
	    __le32_to_cpu(sb->max_dev) < MAX_DEVS)
		sb->max_dev = __cpu_to_le32(dk->number+1);

	sb->dev_number = __cpu_to_le32(dk->number);
	sb->devflags = 0; /* don't copy another disks flags */
	sb->sb_csum = calc_sb_1_csum(sb);

	dip = (struct devinfo **)&st->info;
	while (*dip)
		dip = &(*dip)->next;
	di = malloc(sizeof(struct devinfo));
	di->fd = fd;
	di->devname = devname;
	di->disk = *dk;
	di->next = NULL;
	*dip = di;

	return 0;
}
#endif

static void locate_bitmap1(struct supertype *st, int fd);

static int store_super1(struct supertype *st, int fd)
{
	struct mdp_superblock_1 *sb = st->sb;
	unsigned long long sb_offset;
	struct align_fd afd;
	int sbsize;
	unsigned long long dsize;

	if (!get_dev_size(fd, NULL, &dsize))
		return 1;

	dsize >>= 9;

	if (dsize < 24)
		return 2;

	init_afd(&afd, fd);

	/*
	 * Calculate the position of the superblock.
	 * It is always aligned to a 4K boundary and
	 * depending on minor_version, it can be:
	 * 0: At least 8K, but less than 12K, from end of device
	 * 1: At start of device
	 * 2: 4K from start of device.
	 */
	switch(st->minor_version) {
	case 0:
		sb_offset = dsize;
		sb_offset -= 8*2;
		sb_offset &= ~(4*2-1);
		break;
	case 1:
		sb_offset = 0;
		break;
	case 2:
		sb_offset = 4*2;
		break;
	default:
		return -EINVAL;
	}



	if (sb_offset != __le64_to_cpu(sb->super_offset) &&
	    0 != __le64_to_cpu(sb->super_offset)
		) {
		fprintf(stderr, Name ": internal error - sb_offset is wrong\n");
		abort();
	}

	if (lseek64(fd, sb_offset << 9, 0)< 0LL)
		return 3;

	sbsize = ROUND_UP(sizeof(*sb) + 2 * __le32_to_cpu(sb->max_dev), 512);

	if (awrite(&afd, sb, sbsize) != sbsize)
		return 4;

	if (sb->feature_map & __cpu_to_le32(MD_FEATURE_BITMAP_OFFSET)) {
		struct bitmap_super_s *bm = (struct bitmap_super_s*)
			(((char*)sb)+MAX_SB_SIZE);
		if (__le32_to_cpu(bm->magic) == BITMAP_MAGIC) {
			locate_bitmap1(st, fd);
			if (awrite(&afd, bm, sizeof(*bm)) != sizeof(*bm))
				return 5;
		}
	}
	fsync(fd);
	return 0;
}

static int load_super1(struct supertype *st, int fd, char *devname);

static unsigned long choose_bm_space(unsigned long devsize)
{
	/* if the device is bigger than 8Gig, save 64k for bitmap usage,
	 * if bigger than 200Gig, save 128k
	 * NOTE: result must be multiple of 4K else bad things happen
	 * on 4K-sector devices.
	 */
	if (devsize < 64*2) return 0;
	if (devsize - 64*2 >= 200*1024*1024*2)
		return 128*2;
	if (devsize - 4*2 > 8*1024*1024*2)
		return 64*2;
	return 4*2;
}

static void free_super1(struct supertype *st);

#ifndef MDASSEMBLE
static int write_init_super1(struct supertype *st)
{
	struct mdp_superblock_1 *sb = st->sb;
	struct supertype *refst;
	int rfd;
	int rv = 0;
	unsigned long long bm_space;
	unsigned long long reserved;
	struct devinfo *di;
	unsigned long long dsize, array_size;
	unsigned long long sb_offset, headroom;

	for (di = st->info; di && ! rv ; di = di->next) {
		if (di->disk.state == 1)
			continue;
		if (di->fd < 0)
			continue;

		while (Kill(di->devname, NULL, 0, 1, 1) == 0)
			;

		sb->dev_number = __cpu_to_le32(di->disk.number);
		if (di->disk.state & (1<<MD_DISK_WRITEMOSTLY))
			sb->devflags |= WriteMostly1;
		else
			sb->devflags &= ~WriteMostly1;

		if ((rfd = open("/dev/urandom", O_RDONLY)) < 0 ||
		    read(rfd, sb->device_uuid, 16) != 16) {
			__u32 r[4] = {random(), random(), random(), random()};
			memcpy(sb->device_uuid, r, 16);
		}
		if (rfd >= 0)
			close(rfd);

		sb->events = 0;

		refst = dup_super(st);
 		if (load_super1(refst, di->fd, NULL)==0) {
			struct mdp_superblock_1 *refsb = refst->sb;

			memcpy(sb->device_uuid, refsb->device_uuid, 16);
			if (memcmp(sb->set_uuid, refsb->set_uuid, 16)==0) {
				/* same array, so preserve events and
				 * dev_number */
				sb->events = refsb->events;
				/* bugs in 2.6.17 and earlier mean the
				 * dev_number chosen in Manage must be preserved
				 */
				if (get_linux_version() >= 2006018)
					sb->dev_number = refsb->dev_number;
			}
			free_super1(refst);
		}
		free(refst);

		if (!get_dev_size(di->fd, NULL, &dsize)) {
			rv = 1;
			goto error_out;
		}
		dsize >>= 9;

		if (dsize < 24) {
			close(di->fd);
			rv = 2;
			goto error_out;
		}


		/*
		 * Calculate the position of the superblock.
		 * It is always aligned to a 4K boundary and
		 * depending on minor_version, it can be:
		 * 0: At least 8K, but less than 12K, from end of device
		 * 1: At start of device
		 * 2: 4K from start of device.
		 * Depending on the array size, we might leave extra space
		 * for a bitmap.
		 */
		array_size = __le64_to_cpu(sb->size);
		/* work out how much space we left for a bitmap */
		bm_space = choose_bm_space(array_size);

		/* We try to leave 0.1% at the start for reshape
		 * operations, but limit this to 128Meg (0.1% of 10Gig)
		 * which is plenty for efficient reshapes
		 */
		headroom = 128 * 1024 * 2;
		while  (headroom << 10 > array_size)
			headroom >>= 1;

		switch(st->minor_version) {
		case 0:
			sb_offset = dsize;
			sb_offset -= 8*2;
			sb_offset &= ~(4*2-1);
			sb->super_offset = __cpu_to_le64(sb_offset);
			sb->data_offset = __cpu_to_le64(0);
			if (sb_offset < array_size + bm_space)
				bm_space = sb_offset - array_size;
			sb->data_size = __cpu_to_le64(sb_offset - bm_space);
			break;
		case 1:
			sb->super_offset = __cpu_to_le64(0);
			reserved = bm_space + 4*2;
			if (reserved < headroom)
				reserved = headroom;
			if (reserved + array_size > dsize)
				reserved = dsize - array_size;
			/* Try for multiple of 1Meg so it is nicely aligned */
			#define ONE_MEG (2*1024)
			if (reserved > ONE_MEG)
				reserved = (reserved/ONE_MEG) * ONE_MEG;

			/* force 4K alignment */
			reserved &= ~7ULL;

			sb->data_offset = __cpu_to_le64(reserved);
			sb->data_size = __cpu_to_le64(dsize - reserved);
			break;
		case 2:
			sb_offset = 4*2;
			sb->super_offset = __cpu_to_le64(4*2);
			if (4*2 + 4*2 + bm_space + array_size
			    > dsize)
				bm_space = dsize - array_size
					- 4*2 - 4*2;

			reserved = bm_space + 4*2 + 4*2;
			if (reserved < headroom)
				reserved = headroom;
			if (reserved + array_size > dsize)
				reserved = dsize - array_size;
			/* Try for multiple of 1Meg so it is nicely aligned */
			#define ONE_MEG (2*1024)
			if (reserved > ONE_MEG)
				reserved = (reserved/ONE_MEG) * ONE_MEG;

			/* force 4K alignment */
			reserved &= ~7ULL;

			sb->data_offset = __cpu_to_le64(reserved);
			sb->data_size = __cpu_to_le64(dsize - reserved);
			break;
		default:
			fprintf(stderr,	Name ": Failed to write invalid "
				"metadata format 1.%i to %s\n",
				st->minor_version, di->devname);
			rv = -EINVAL;
			goto out;
		}

		sb->sb_csum = calc_sb_1_csum(sb);
		rv = store_super1(st, di->fd);
		if (rv == 0 && (__le32_to_cpu(sb->feature_map) & 1))
			rv = st->ss->write_bitmap(st, di->fd);
		close(di->fd);
		di->fd = -1;
	}
error_out:
	if (rv)
		fprintf(stderr,	Name ": Failed to write metadata to %s\n",
			di->devname);
out:
	return rv;
}
#endif

static int compare_super1(struct supertype *st, struct supertype *tst)
{
	/*
	 * return:
	 *  0 same, or first was empty, and second was copied
	 *  1 second had wrong number
	 *  2 wrong uuid
	 *  3 wrong other info
	 */
	struct mdp_superblock_1 *first = st->sb;
	struct mdp_superblock_1 *second = tst->sb;

	if (second->magic != __cpu_to_le32(MD_SB_MAGIC))
		return 1;
	if (second->major_version != __cpu_to_le32(1))
		return 1;

	if (!first) {
		if (posix_memalign((void**)&first, 4096, SUPER1_SIZE) != 0) {
			fprintf(stderr, Name
				": %s could not allocate superblock\n", __func__);
			return 1;
		}
		memcpy(first, second, SUPER1_SIZE);
		st->sb = first;
		return 0;
	}
	if (memcmp(first->set_uuid, second->set_uuid, 16)!= 0)
		return 2;

	if (first->ctime      != second->ctime     ||
	    first->level      != second->level     ||
	    first->layout     != second->layout    ||
	    first->size       != second->size      ||
	    first->chunksize  != second->chunksize ||
	    first->raid_disks != second->raid_disks)
		return 3;
	return 0;
}

static int load_super1(struct supertype *st, int fd, char *devname)
{
	unsigned long long dsize;
	unsigned long long sb_offset;
	struct mdp_superblock_1 *super;
	int uuid[4];
	struct bitmap_super_s *bsb;
	struct misc_dev_info *misc;
	struct align_fd afd;

	free_super1(st);

	init_afd(&afd, fd);

	if (st->ss == NULL || st->minor_version == -1) {
		int bestvers = -1;
		struct supertype tst;
		__u64 bestctime = 0;
		/* guess... choose latest ctime */
		memset(&tst, 0, sizeof(tst));
		tst.ss = &super1;
		for (tst.minor_version = 0; tst.minor_version <= 2 ; tst.minor_version++) {
			switch(load_super1(&tst, fd, devname)) {
			case 0: super = tst.sb;
				if (bestvers == -1 ||
				    bestctime < __le64_to_cpu(super->ctime)) {
					bestvers = tst.minor_version;
					bestctime = __le64_to_cpu(super->ctime);
				}
				free(super);
				tst.sb = NULL;
				break;
			case 1: return 1; /*bad device */
			case 2: break; /* bad, try next */
			}
		}
		if (bestvers != -1) {
			int rv;
			tst.minor_version = bestvers;
			tst.ss = &super1;
			tst.max_devs = MAX_DEVS;
			rv = load_super1(&tst, fd, devname);
			if (rv == 0)
				*st = tst;
			return rv;
		}
		return 2;
	}
	if (!get_dev_size(fd, devname, &dsize))
		return 1;
	dsize >>= 9;

	if (dsize < 24) {
		if (devname)
			fprintf(stderr, Name ": %s is too small for md: size is %llu sectors.\n",
				devname, dsize);
		return 1;
	}

	/*
	 * Calculate the position of the superblock.
	 * It is always aligned to a 4K boundary and
	 * depending on minor_version, it can be:
	 * 0: At least 8K, but less than 12K, from end of device
	 * 1: At start of device
	 * 2: 4K from start of device.
	 */
	switch(st->minor_version) {
	case 0:
		sb_offset = dsize;
		sb_offset -= 8*2;
		sb_offset &= ~(4*2-1);
		break;
	case 1:
		sb_offset = 0;
		break;
	case 2:
		sb_offset = 4*2;
		break;
	default:
		return -EINVAL;
	}

	ioctl(fd, BLKFLSBUF, 0); /* make sure we read current data */


	if (lseek64(fd, sb_offset << 9, 0)< 0LL) {
		if (devname)
			fprintf(stderr, Name ": Cannot seek to superblock on %s: %s\n",
				devname, strerror(errno));
		return 1;
	}

	if (posix_memalign((void**)&super, 4096, SUPER1_SIZE) != 0) {
		fprintf(stderr, Name ": %s could not allocate superblock\n",
			__func__);
		return 1;
	}

	if (aread(&afd, super, MAX_SB_SIZE) != MAX_SB_SIZE) {
		if (devname)
			fprintf(stderr, Name ": Cannot read superblock on %s\n",
				devname);
		free(super);
		return 1;
	}

	if (__le32_to_cpu(super->magic) != MD_SB_MAGIC) {
		if (devname)
			fprintf(stderr, Name ": No super block found on %s (Expected magic %08x, got %08x)\n",
				devname, MD_SB_MAGIC, __le32_to_cpu(super->magic));
		free(super);
		return 2;
	}

	if (__le32_to_cpu(super->major_version) != 1) {
		if (devname)
			fprintf(stderr, Name ": Cannot interpret superblock on %s - version is %d\n",
				devname, __le32_to_cpu(super->major_version));
		free(super);
		return 2;
	}
	if (__le64_to_cpu(super->super_offset) != sb_offset) {
		if (devname)
			fprintf(stderr, Name ": No superblock found on %s (super_offset is wrong)\n",
				devname);
		free(super);
		return 2;
	}
	st->sb = super;

	bsb = (struct bitmap_super_s *)(((char*)super)+MAX_SB_SIZE);

	misc = (struct misc_dev_info*) (((char*)super)+MAX_SB_SIZE+BM_SUPER_SIZE);
	misc->device_size = dsize;

	/* Now check on the bitmap superblock */
	if ((__le32_to_cpu(super->feature_map)&MD_FEATURE_BITMAP_OFFSET) == 0)
		return 0;
	/* Read the bitmap superblock and make sure it looks
	 * valid.  If it doesn't clear the bit.  An --assemble --force
	 * should get that written out.
	 */
	locate_bitmap1(st, fd);
	if (aread(&afd, bsb, 512) != 512)
		goto no_bitmap;

	uuid_from_super1(st, uuid);
	if (__le32_to_cpu(bsb->magic) != BITMAP_MAGIC ||
	    memcmp(bsb->uuid, uuid, 16) != 0)
		goto no_bitmap;
	return 0;

 no_bitmap:
	super->feature_map = __cpu_to_le32(__le32_to_cpu(super->feature_map)
					   & ~MD_FEATURE_BITMAP_OFFSET);
	return 0;
}


static struct supertype *match_metadata_desc1(char *arg)
{
	struct supertype *st = calloc(1, sizeof(*st));
	if (!st)
		return st;

	st->container_dev = NoMdDev;
	st->ss = &super1;
	st->max_devs = MAX_DEVS;
	st->sb = NULL;
	/* leading zeros can be safely ignored.  --detail generates them. */
	while (*arg == '0')
		arg++;
	if (strcmp(arg, "1.0") == 0 ||
	    strcmp(arg, "1.00") == 0) {
		st->minor_version = 0;
		return st;
	}
	if (strcmp(arg, "1.1") == 0 ||
	    strcmp(arg, "1.01") == 0
		) {
		st->minor_version = 1;
		return st;
	}
	if (strcmp(arg, "1.2") == 0 ||
#ifndef DEFAULT_OLD_METADATA /* ifdef in super0.c */
	    strcmp(arg, "default") == 0 ||
#endif /* DEFAULT_OLD_METADATA */
	    strcmp(arg, "1.02") == 0) {
		st->minor_version = 2;
		return st;
	}
	if (strcmp(arg, "1") == 0 ||
	    strcmp(arg, "default") == 0) {
		st->minor_version = -1;
		return st;
	}

	free(st);
	return NULL;
}

/* find available size on device with this devsize, using
 * superblock type st, and reserving 'reserve' sectors for
 * a possible bitmap
 */
static __u64 avail_size1(struct supertype *st, __u64 devsize)
{
	struct mdp_superblock_1 *super = st->sb;
	if (devsize < 24)
		return 0;

	if (super == NULL)
		/* creating:  allow suitable space for bitmap */
		devsize -= choose_bm_space(devsize);
#ifndef MDASSEMBLE
	else if (__le32_to_cpu(super->feature_map)&MD_FEATURE_BITMAP_OFFSET) {
		/* hot-add. allow for actual size of bitmap */
		struct bitmap_super_s *bsb;
		bsb = (struct bitmap_super_s *)(((char*)super)+MAX_SB_SIZE);
		devsize -= bitmap_sectors(bsb);
	}
#endif

	if (st->minor_version < 0)
		/* not specified, so time to set default */
		st->minor_version = 2;
	if (super == NULL && st->minor_version > 0) {
		/* haven't committed to a size yet, so allow some
		 * slack for space for reshape.
		 * Limit slack to 128M, but aim for about 0.1%
		 */
		unsigned long long headroom = 128*1024*2;
		while ((headroom << 10) > devsize)
			headroom >>= 1;
		devsize -= headroom;
	}
	switch(st->minor_version) {
	case 0:
		/* at end */
		return ((devsize - 8*2 ) & ~(4*2-1));
	case 1:
		/* at start, 4K for superblock and possible bitmap */
		return devsize - 4*2;
	case 2:
		/* 4k from start, 4K for superblock and possible bitmap */
		return devsize - (4+4)*2;
	}
	return 0;
}

static int
add_internal_bitmap1(struct supertype *st,
		     int *chunkp, int delay, int write_behind,
		     unsigned long long size,
		     int may_change, int major)
{
	/*
	 * If not may_change, then this is a 'Grow' without sysfs support for
	 * bitmaps, and the bitmap must fit after the superblock at 1K offset.
	 * If may_change, then this is create or a Grow with sysfs syupport,
	 * and we can put the bitmap wherever we like.
	 *
	 * size is in sectors,  chunk is in bytes !!!
	 */

	unsigned long long bits;
	unsigned long long max_bits;
	unsigned long long min_chunk;
	long offset;
	unsigned long long chunk = *chunkp;
	int room = 0;
	int creating = 0;
	struct mdp_superblock_1 *sb = st->sb;
	bitmap_super_t *bms = (bitmap_super_t*)(((char*)sb) + MAX_SB_SIZE);
	int uuid[4];

	if (__le64_to_cpu(sb->data_size) == 0)
		/* Must be creating the array, else data_size would be non-zero */
		creating = 1;
	switch(st->minor_version) {
	case 0:
		/* either 3K after the superblock (when hot-add),
		 * or some amount of space before.
		 */
		if (creating) {
			/* We are creating array, so we *know* how much room has
			 * been left.
			 */
			offset = 0;
			room = choose_bm_space(__le64_to_cpu(sb->size));
		} else {
			room = __le64_to_cpu(sb->super_offset)
				- __le64_to_cpu(sb->data_offset)
				- __le64_to_cpu(sb->data_size);

			if (!may_change || (room < 3*2 &&
				  __le32_to_cpu(sb->max_dev) <= 384)) {
				room = 3*2;
				offset = 1*2;
			} else {
				offset = 0; /* means movable offset */
			}
		}
		break;
	case 1:
	case 2: /* between superblock and data */
		if (creating) {
			offset = 4*2;
			room = choose_bm_space(__le64_to_cpu(sb->size));
		} else {
			room = __le64_to_cpu(sb->data_offset)
				- __le64_to_cpu(sb->super_offset);
			if (!may_change) {
				room -= 2; /* Leave 1K for superblock */
				offset = 2;
			} else {
				room -= 4*2; /* leave 4K for superblock */
				offset = 4*2;
			}
		}
		break;
	default:
		return 0;
	}

	if (chunk == UnSet && room > 128*2)
		/* Limit to 128K of bitmap when chunk size not requested */
		room = 128*2;

	max_bits = (room * 512 - sizeof(bitmap_super_t)) * 8;

	min_chunk = 4096; /* sub-page chunks don't work yet.. */
	bits = (size*512)/min_chunk +1;
	while (bits > max_bits) {
		min_chunk *= 2;
		bits = (bits+1)/2;
	}
	if (chunk == UnSet) {
		/* For practical purpose, 64Meg is a good
		 * default chunk size for internal bitmaps.
		 */
		chunk = min_chunk;
		if (chunk < 64*1024*1024)
			chunk = 64*1024*1024;
	} else if (chunk < min_chunk)
		return 0; /* chunk size too small */
	if (chunk == 0) /* rounding problem */
		return 0;

	if (offset == 0) {
		/* start bitmap on a 4K boundary with enough space for
		 * the bitmap
		 */
		bits = (size*512) / chunk + 1;
		room = ((bits+7)/8 + sizeof(bitmap_super_t) +4095)/4096;
		room *= 8; /* convert 4K blocks to sectors */
		offset = -room;
	}

	sb->bitmap_offset = (int32_t)__cpu_to_le32(offset);

	sb->feature_map = __cpu_to_le32(__le32_to_cpu(sb->feature_map)
					| MD_FEATURE_BITMAP_OFFSET);
	memset(bms, 0, sizeof(*bms));
	bms->magic = __cpu_to_le32(BITMAP_MAGIC);
	bms->version = __cpu_to_le32(major);
	uuid_from_super1(st, uuid);
	memcpy(bms->uuid, uuid, 16);
	bms->chunksize = __cpu_to_le32(chunk);
	bms->daemon_sleep = __cpu_to_le32(delay);
	bms->sync_size = __cpu_to_le64(size);
	bms->write_behind = __cpu_to_le32(write_behind);

	*chunkp = chunk;
	return 1;
}

static void locate_bitmap1(struct supertype *st, int fd)
{
	unsigned long long offset;
	struct mdp_superblock_1 *sb;
	int mustfree = 0;

	if (!st->sb) {
		if (st->ss->load_super(st, fd, NULL))
			return; /* no error I hope... */
		mustfree = 1;
	}
	sb = st->sb;

	offset = __le64_to_cpu(sb->super_offset);
	offset += (int32_t) __le32_to_cpu(sb->bitmap_offset);
	if (mustfree)
		free(sb);
	lseek64(fd, offset<<9, 0);
}

static int write_bitmap1(struct supertype *st, int fd)
{
	struct mdp_superblock_1 *sb = st->sb;
	bitmap_super_t *bms = (bitmap_super_t*)(((char*)sb)+MAX_SB_SIZE);
	int rv = 0;
	void *buf;
	int towrite, n;
	struct align_fd afd;

	init_afd(&afd, fd);

	locate_bitmap1(st, fd);

	if (posix_memalign(&buf, 4096, 4096))
		return -ENOMEM;

	memset(buf, 0xff, 4096);
	memcpy(buf, (char *)bms, sizeof(bitmap_super_t));

	towrite = __le64_to_cpu(bms->sync_size) / (__le32_to_cpu(bms->chunksize)>>9);
	towrite = (towrite+7) >> 3; /* bits to bytes */
	towrite += sizeof(bitmap_super_t);
	towrite = ROUND_UP(towrite, 512);
	while (towrite > 0) {
		n = towrite;
		if (n > 4096)
			n = 4096;
		n = awrite(&afd, buf, n);
		if (n > 0)
			towrite -= n;
		else
			break;
		memset(buf, 0xff, 4096);
	}
	fsync(fd);
	if (towrite)
		rv = -2;

	free(buf);
	return rv;
}

static void free_super1(struct supertype *st)
{
	if (st->sb)
		free(st->sb);
	while (st->info) {
		struct devinfo *di = st->info;
		st->info = di->next;
		if (di->fd >= 0)
			close(di->fd);
		free(di);
	}
	st->sb = NULL;
}

#ifndef MDASSEMBLE
static int validate_geometry1(struct supertype *st, int level,
			      int layout, int raiddisks,
			      int *chunk, unsigned long long size,
			      char *subdev, unsigned long long *freesize,
			      int verbose)
{
	unsigned long long ldsize;
	int fd;

	if (level == LEVEL_CONTAINER) {
		if (verbose)
			fprintf(stderr, Name ": 1.x metadata does not support containers\n");
		return 0;
	}
	if (chunk && *chunk == UnSet)
		*chunk = DEFAULT_CHUNK;

	if (!subdev)
		return 1;

	fd = open(subdev, O_RDONLY|O_EXCL, 0);
	if (fd < 0) {
		if (verbose)
			fprintf(stderr, Name ": super1.x cannot open %s: %s\n",
				subdev, strerror(errno));
		return 0;
	}

	if (!get_dev_size(fd, subdev, &ldsize)) {
		close(fd);
		return 0;
	}
	close(fd);

	*freesize = avail_size1(st, ldsize >> 9);
	return 1;
}
#endif /* MDASSEMBLE */

struct superswitch super1 = {
#ifndef MDASSEMBLE
	.examine_super = examine_super1,
	.brief_examine_super = brief_examine_super1,
	.export_examine_super = export_examine_super1,
	.detail_super = detail_super1,
	.brief_detail_super = brief_detail_super1,
	.export_detail_super = export_detail_super1,
	.write_init_super = write_init_super1,
	.validate_geometry = validate_geometry1,
	.add_to_super = add_to_super1,
#endif
	.match_home = match_home1,
	.uuid_from_super = uuid_from_super1,
	.getinfo_super = getinfo_super1,
	.container_content = container_content1,
	.update_super = update_super1,
	.init_super = init_super1,
	.store_super = store_super1,
	.compare_super = compare_super1,
	.load_super = load_super1,
	.match_metadata_desc = match_metadata_desc1,
	.avail_size = avail_size1,
	.add_internal_bitmap = add_internal_bitmap1,
	.locate_bitmap = locate_bitmap1,
	.write_bitmap = write_bitmap1,
	.free_super = free_super1,
#if __BYTE_ORDER == BIG_ENDIAN
	.swapuuid = 0,
#else
	.swapuuid = 1,
#endif
	.name = "1.x",
};
