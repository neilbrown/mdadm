/*
 * mdadm - manage Linux "md" devices aka RAID arrays.
 *
 * Copyright (C) 2001-2004 Neil Brown <neilb@cse.unsw.edu.au>
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
 *    Email: <neilb@cse.unsw.edu.au>
 *    Paper: Neil Brown
 *           School of Computer Science and Engineering
 *           The University of New South Wales
 *           Sydney, 2052
 *           Australia
 */

#include "mdadm.h"

#include "asm/byteorder.h"
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
	__u8	pad1[128-96];	/* set to 0 when written */

	/* constant this-device information - 64 bytes */
	__u64	data_offset;	/* sector start of data, often 0 */
	__u64	data_size;	/* sectors in this device that can be used for data */
	__u64	super_offset;	/* sector start of this superblock */
	__u64	recovery_offset;/* sectors before this offset (from data_offset) have been recovered */
	__u32	dev_number;	/* permanent identifier of this  device - not role in raid */
	__u32	cnt_corrected_read; /* number of read errors that were corrected by re-writing */
	__u8	device_uuid[16]; /* user-space setable, ignored by kernel */
	__u8	pad2[64-56];	/* set to 0 when writing */

	/* array state information - 64 bytes */
	__u64	utime;		/* 40 bits second, 24 btes microseconds */
	__u64	events;		/* incremented when superblock updated */
	__u64	resync_offset;	/* data before this offset (from data_offset) known to be in sync */
	__u32	sb_csum;	/* checksum upto devs[max_dev] */
	__u32	max_dev;	/* size of devs[] array to consider */
	__u8	pad3[64-32];	/* set to 0 when writing */

	/* device state information. Indexed by dev_number.
	 * 2 bytes per device
	 * Note there are no per-device state flags. State information is rolled
	 * into the 'roles' value.  If a device is spare or faulty, then it doesn't
	 * have a meaningful role.
	 */
	__u16	dev_roles[0];	/* role in array, or 0xffff for a spare, or 0xfffe for faulty */
};

#ifndef offsetof
#define offsetof(t,f) ((int)&(((t*)0)->f))
#endif
static unsigned int calc_sb_1_csum(struct mdp_superblock_1 * sb)
{
	unsigned int disk_csum, csum;
	unsigned long long newcsum;
	int size = sizeof(*sb) + __le32_to_cpu(sb->max_dev)*2;
	unsigned int *isuper = (unsigned int*)sb;
	int i;

/* make sure I can count... */
	if (offsetof(struct mdp_superblock_1,data_offset) != 128 ||
	    offsetof(struct mdp_superblock_1, utime) != 192 ||
	    sizeof(struct mdp_superblock_1) != 256) {
		fprintf(stderr, "WARNING - superblock isn't sized correctly\n");
	}

	disk_csum = sb->sb_csum;
	sb->sb_csum = 0;
	newcsum = 0;
	for (i=0; size>=4; size -= 4 )
		newcsum += __le32_to_cpu(*isuper++);

	if (size == 2)
		newcsum += __le16_to_cpu(*(unsigned short*) isuper);

	csum = (newcsum & 0xffffffff) + (newcsum >> 32);
	sb->sb_csum = disk_csum;
	return csum;
}


static void examine_super1(void *sbv)
{
	struct mdp_superblock_1 *sb = sbv;
	time_t atime;
	int d;
	int spares, faulty;
	int i;
	char *c;

	printf("          Magic : %08x\n", __le32_to_cpu(sb->magic));
	printf("        Version : %02d.%02d\n", 1, __le32_to_cpu(sb->feature_map));
	printf("     Array UUID : ");
	for (i=0; i<16; i++) {
		printf("%02x", sb->set_uuid[i]);
		if ((i&3)==0 && i != 0) printf(":");
	}
	printf("\n");
	printf("           Name : %.32s\n", sb->set_name);

	atime = __le64_to_cpu(sb->ctime) & 0xFFFFFFFFFFULL;
	printf("  Creation Time : %.24s\n", ctime(&atime));
	c=map_num(pers, __le32_to_cpu(sb->level));
	printf("     Raid Level : %s\n", c?c:"-unknown-");
	printf("   Raid Devices : %d\n", __le32_to_cpu(sb->raid_disks));
	printf("\n");
	printf("    Device Size : %llu%s\n", (unsigned long long)sb->data_size, human_size(sb->data_size<<9));
	if (sb->data_offset)
		printf("    Data Offset : %llu sectors\n", (unsigned long long)__le64_to_cpu(sb->data_offset));
	if (sb->super_offset)
		printf("   Super Offset : %llu sectors\n", (unsigned long long)__le64_to_cpu(sb->super_offset));
	printf("          State : %s\n", (__le64_to_cpu(sb->resync_offset)+1)? "active":"clean");
	printf("    Device UUID : ");
	for (i=0; i<16; i++) {
		printf("%02x", sb->set_uuid[i]);
		if ((i&3)==0 && i != 0) printf(":");
	}
	printf("\n");

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
	switch(__le32_to_cpu(sb->level)) {
	case 0:
	case 4:
	case 5:
		printf("     Chunk Size : %dK\n", __le32_to_cpu(sb->chunksize/2));
		break;
	case -1:
		printf("       Rounding : %dK\n", __le32_to_cpu(sb->chunksize/2));
		break;
	default: break;
	}
	printf("\n");
	printf("   Array State : ");
	for (d=0; d<__le32_to_cpu(sb->raid_disks); d++) {
		int cnt = 0;
		int me = 0;
		int i;
		for (i=0; i< __le32_to_cpu(sb->max_dev); i++) {
			int role = __le16_to_cpu(sb->dev_roles[i]);
			if (role == d) {
				if (i == __le32_to_cpu(sb->dev_number))
					me = 1;
				cnt++;
			}
		}
		if (cnt > 1) printf("?");
		else if (cnt == 1 && me) printf("U");
		else if (cnt == 1) printf("u");
		else printf ("_");
	}
	spares = faulty = 0;
	for (i=0; i< __le32_to_cpu(sb->max_dev); i++) {
		int role = __le16_to_cpu(sb->dev_roles[i]);
		switch (role) {
		case 0xFFFF: spares++; break;
		case 0xFFFE: faulty++;
		}
	}
	if (spares) printf(" %d spares", spares);
	if (faulty) printf(" %d failed", faulty);
	printf("\n");
}


static void brief_examine_super1(void *sbv)
{
	struct mdp_superblock_1 *sb = sbv;
	int i;

	char *c=map_num(pers, __le32_to_cpu(sb->level));

	printf("ARRAY /dev/?? level=%s metadata=1 num-devices=%d UUID=",
	       c?c:"-unknown-", sb->raid_disks);
	for (i=0; i<16; i++) {
		printf("%02x", sb->set_uuid[i]);
		if ((i&3)==0 && i != 0) printf(":");
	}
	printf("\n");
}

static void detail_super1(void *sbv)
{
	struct mdp_superblock_1 *sb = sbv;
	int i;

	printf("           UUID : ");
	for (i=0; i<16; i++) {
		printf("%02x", sb->set_uuid[i]);
		if ((i&3)==0 && i != 0) printf(":");
	}
	printf("\n         Events : %llu\n\n", (unsigned long long)__le64_to_cpu(sb->events));
}

static void brief_detail_super1(void *sbv)
{
	struct mdp_superblock_1 *sb = sbv;
	int i;

	printf(" UUID=");
	for (i=0; i<16; i++) {
		printf("%02x", sb->set_uuid[i]);
		if ((i&3)==0 && i != 0) printf(":");
	}
}

static void uuid_from_super1(int uuid[4], void * sbv)
{
	struct mdp_superblock_1 *super = sbv;
	char *cuuid = (char*)uuid;
	int i;
	for (i=0; i<16; i++)
		cuuid[i] = super->set_uuid[i];
}

static void getinfo_super1(struct mdinfo *info, void *sbv)
{
	struct mdp_superblock_1 *sb = sbv;
	int working = 0;
	int i;
	int role;

	info->array.major_version = 1;
	info->array.minor_version = __le32_to_cpu(sb->feature_map);
	info->array.patch_version = 0;
	info->array.raid_disks = __le32_to_cpu(sb->raid_disks);
	info->array.level = __le32_to_cpu(sb->level);
	info->array.md_minor = -1;
	info->array.ctime = __le64_to_cpu(sb->ctime);

	info->disk.major = 0;
	info->disk.minor = 0;

	if (__le32_to_cpu(sb->dev_number) >= __le32_to_cpu(sb->max_dev) ||
	    __le32_to_cpu(sb->max_dev) > 512)
		role = 0xfffe;
	else
		role = __le16_to_cpu(sb->dev_roles[__le32_to_cpu(sb->dev_number)]);

	info->disk.raid_disk = -1;
	switch(role) {
	case 0xFFFF:
		info->disk.state = 2; /* spare: ACTIVE, not sync, not faulty */
		break;
	case 0xFFFE:
		info->disk.state = 1; /* faulty */
		break;
	default:
		info->disk.state = 6; /* active and in sync */
		info->disk.raid_disk = role;
	}
	info->events = __le64_to_cpu(sb->events);

	memcpy(info->uuid, sb->set_uuid, 16);

	for (i=0; i< __le32_to_cpu(sb->max_dev); i++) {
		role = __le16_to_cpu(sb->dev_roles[i]);
		if (role == 0xFFFF || role < info->array.raid_disks)
			working++;
	}

	info->array.working_disks = working;
}

static int update_super1(struct mdinfo *info, void *sbv, char *update, char *devname, int verbose)
{
	int rv = 0;
	struct mdp_superblock_1 *sb = sbv;

	if (strcmp(update, "force")==0) {
		sb->events = __cpu_to_le32(info->events);
		switch(__le32_to_cpu(sb->level)) {
		case 5: case 4: case 6:
			/* need to force clean */
			sb->resync_offset = ~0ULL;
		}
	}
	if (strcmp(update, "assemble")==0) {
		int d = info->disk.number;
		int want;
		if (info->disk.state == 6)
			want = __cpu_to_le32(info->disk.raid_disk);
		else
			want = 0xFFFF;
		if (sb->dev_roles[d] != want) {
			sb->dev_roles[d] = want;
			rv = 1;
		}
	}
#if 0
	if (strcmp(update, "newdev") == 0) {
		int d = info->disk.number;
		memset(&sb->disks[d], 0, sizeof(sb->disks[d]));
		sb->disks[d].number = d;
		sb->disks[d].major = info->disk.major;
		sb->disks[d].minor = info->disk.minor;
		sb->disks[d].raid_disk = info->disk.raid_disk;
		sb->disks[d].state = info->disk.state;
		sb->this_disk = sb->disks[d];
	}
#endif
	if (strcmp(update, "grow") == 0) {
		sb->raid_disks = __cpu_to_le32(info->array.raid_disks);
		/* FIXME */
	}
	if (strcmp(update, "resync") == 0) {
		/* make sure resync happens */
		sb->resync_offset = ~0ULL;
	}

	sb->sb_csum = calc_sb_1_csum(sb);
	return rv;
}


static __u64 event_super1(void *sbv)
{
	struct mdp_superblock_1 *sb = sbv;
	return __le64_to_cpu(sb->events);
}

static int init_super1(void **sbp, mdu_array_info_t *info)
{
	struct mdp_superblock_1 *sb = malloc(1024);
	int spares;
	memset(sb, 0, 1024);

	if (info->major_version == -1)
		/* zeroing superblock */
		return 0;

	spares = info->working_disks - info->active_disks;
	if (info->raid_disks + spares  > 384) {
		fprintf(stderr, Name ": too many devices requested: %d+%d > %d\n",
			info->raid_disks , spares, 384);
		return 0;
	}


	sb->magic = __cpu_to_le32(MD_SB_MAGIC);
	sb->major_version = __cpu_to_le32(1);
	sb->feature_map = 0;
	sb->pad0 = 0;
	*(__u32*)(sb->set_uuid) = random();
	*(__u32*)(sb->set_uuid+4) = random();
	*(__u32*)(sb->set_uuid+8) = random();
	*(__u32*)(sb->set_uuid+12) = random();

	/* FIXME name */

	sb->ctime = __cpu_to_le64((unsigned long long)time(0));
	sb->level = __cpu_to_le32(info->level);
	sb->layout = __cpu_to_le32(info->layout);
	sb->size = __cpu_to_le64(info->size*2ULL);
	sb->chunksize = __cpu_to_le32(info->chunk_size>>9);
	sb->raid_disks = __cpu_to_le32(info->raid_disks);

	sb->data_offset = __cpu_to_le64(0);
	sb->data_size = __cpu_to_le64(0);
	sb->super_offset = __cpu_to_le64(0);
	sb->recovery_offset = __cpu_to_le64(0);

	sb->utime = sb->ctime;
	sb->events = __cpu_to_le64(1);
	if (info->state & (1<<MD_SB_CLEAN))
		sb->resync_offset = ~0ULL;
	else
		sb->resync_offset = 0;
	sb->max_dev = __cpu_to_le32((1024- sizeof(struct mdp_superblock_1))/ 
				    sizeof(sb->dev_roles[0]));
	memset(sb->pad3, 0, sizeof(sb->pad3));

	memset(sb->dev_roles, 0xff, 1024 - sizeof(struct mdp_superblock_1));

	*sbp = sb;
	return 1;
}

/* Add a device to the superblock being created */
static void add_to_super1(void *sbv, mdu_disk_info_t *dk)
{
	struct mdp_superblock_1 *sb = sbv;
	__u16 *rp = sb->dev_roles + dk->number;
	if (dk->state == 6) /* active, sync */
		*rp = __cpu_to_le16(dk->raid_disk);
	else if (dk->state == 2) /* active -> spare */
		*rp = 0xffff;
	else 
		*rp = 0xfffe;
}

static int store_super1(int fd, void *sbv)
{
	struct mdp_superblock_1 *sb = sbv;
	long long sb_offset;
	int sbsize;

    
	sb_offset = __le64_to_cpu(sb->super_offset) << 9;

	if (lseek64(fd, sb_offset, 0)< 0LL)
		return 3;

	sbsize = sizeof(*sb) + 2 * __le32_to_cpu(sb->max_dev);

	if (write(fd, sb, sbsize) != sbsize)
		return 4;

	fsync(fd);
	return 0;
}

static int load_super1(struct supertype *st, int fd, void **sbp, char *devname);

static int write_init_super1(struct supertype *st, void *sbv, mdu_disk_info_t *dinfo, char *devname)
{
	struct mdp_superblock_1 *sb = sbv;
	struct mdp_superblock_1 *refsb = NULL;
	int fd = open(devname, O_RDWR | O_EXCL);
	int rfd;
	int rv;

	long size;
	long long sb_offset;


	if (fd < 0) {
		fprintf(stderr, Name ": Failed to open %s to write superblock\n",
			devname);
		return -1;
	}

	sb->dev_number = __cpu_to_le32(dinfo->number);

	if ((rfd = open("/dev/urandom", O_RDONLY)) < 0 ||
	    read(rfd, sb->device_uuid, 16) != 16) {
		*(__u32*)(sb->device_uuid) = random();
		*(__u32*)(sb->device_uuid+4) = random();
		*(__u32*)(sb->device_uuid+8) = random();
		*(__u32*)(sb->device_uuid+12) = random();
	}
	if (rfd >= 0) close(rfd);
	sb->events = 0;

	if (load_super1(st, fd, (void**)&refsb, NULL)==0) {
		memcpy(sb->device_uuid, refsb->device_uuid, 16);
		if (memcmp(sb->set_uuid, refsb->set_uuid, 16)==0) {
			/* same array, so preserve events and dev_number */
			sb->events = refsb->events;
			sb->dev_number = refsb->dev_number;
		}
		free(refsb);
	}
    
	if (ioctl(fd, BLKGETSIZE, &size)) {
		close(fd);
		return 1;
	}

	if (size < 24) {
		close(fd);
		return 2;
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
		sb_offset = size;
		sb_offset -= 8*2;
		sb_offset &= ~(4*2-1);
		sb->super_offset = __cpu_to_le64(sb_offset);
		sb->data_offset = __cpu_to_le64(0);
		sb->data_size = sb->super_offset;
		break;
	case 1:
		sb->super_offset = __cpu_to_le64(0);
		sb->data_offset = __cpu_to_le64(2);
		sb->data_size = __cpu_to_le64(size - 2);
		break;
	case 2:
		sb_offset = 4*2;
		sb->super_offset = __cpu_to_le64(sb_offset);
		sb->data_offset = __cpu_to_le64(sb_offset+2);
		sb->data_size = __cpu_to_le64(size - 4*2 - 2);
		break;
	default:
		return -EINVAL;
	}


	sb->sb_csum = calc_sb_1_csum(sb);
	rv = store_super1(fd, sb);
	if (rv)
		fprintf(stderr, Name ": failed to write superblock to %s\n", devname);
	close(fd);
	return rv;
}

static int compare_super1(void **firstp, void *secondv)
{
	/*
	 * return:
	 *  0 same, or first was empty, and second was copied
	 *  1 second had wrong number
	 *  2 wrong uuid
	 *  3 wrong other info
	 */
	struct mdp_superblock_1 *first = *firstp;
	struct mdp_superblock_1 *second = secondv;

	if (second->magic != __cpu_to_le32(MD_SB_MAGIC))
		return 1;
	if (second->major_version != __cpu_to_le32(1))
		return 1;

	if (!first) {
		first = malloc(1024);
		memcpy(first, second, 1024);
		*firstp = first;
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

static int load_super1(struct supertype *st, int fd, void **sbp, char *devname)
{
	unsigned long size;
	unsigned long long sb_offset;
	struct mdp_superblock_1 *super;



	if (st->ss == NULL) {
		int bestvers = -1;
		__u64 bestctime = 0;
		/* guess... choose latest ctime */
		st->ss = &super1;
		for (st->minor_version = 0; st->minor_version <= 2 ; st->minor_version++) {
			switch(load_super1(st, fd, sbp, devname)) {
			case 0: super = *sbp;
				if (bestvers == -1 ||
				    bestctime < __le64_to_cpu(super->ctime)) {
					bestvers = st->minor_version;
					bestctime = __le64_to_cpu(super->ctime);
				}
				free(super);
				*sbp = NULL;
				break;
			case 1: st->ss = NULL; return 1; /*bad device */
			case 2: break; /* bad, try next */
			}
		}
		if (bestvers != -1) {
			int rv;
			st->minor_version = bestvers;
			st->ss = &super1;
			st->max_devs = 384;
			rv = load_super1(st, fd, sbp, devname);
			if (rv) st->ss = NULL;
			return rv;
		}
		st->ss = NULL;
		return 2;
	}
	if (ioctl(fd, BLKGETSIZE, &size)) {
		if (devname) 
			fprintf(stderr, Name ": cannot find device size for %s: %s\n",
				devname, strerror(errno));
		return 1;
	}

	if (size < 24) {
		if (devname)
			fprintf(stderr, Name ": %s is too small for md: size is %lu sectors.\n",
				devname, size);
		return 1;
	}

	/*
	 * Calculate the position of the superblock.
	 * It is always aligned to a 4K boundary and
	 * depeding on minor_version, it can be:
	 * 0: At least 8K, but less than 12K, from end of device
	 * 1: At start of device
	 * 2: 4K from start of device.
	 */
	switch(st->minor_version) {
	case 0:
		sb_offset = size;
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

	super = malloc(1024);

	if (read(fd, super, 1024) != 1024) {
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
	*sbp = super;
	return 0;
}


static struct supertype *match_metadata_desc1(char *arg)
{
	struct supertype *st = malloc(sizeof(*st));
	if (!st) return st;

	st->ss = &super1;
	st->max_devs = 384;
	if (strcmp(arg, "1") == 0 ||
	    strcmp(arg, "1.0") == 0) {
		st->minor_version = 0;
		return st;
	}
	if (strcmp(arg, "1.1") == 0) {
		st->minor_version = 1;
		return st;
	}
	if (strcmp(arg, "1.2") == 0) {
		st->minor_version = 2;
		return st;
	}

	free(st);
	return NULL;
}

static __u64 avail_size1(__u64 devsize)
{
	if (devsize < 24)
		return 0;

	return (devsize - 8*2 ) & ~(4*2-1);
}

struct superswitch super1 = {
	.examine_super = examine_super1,
	.brief_examine_super = brief_examine_super1,
	.detail_super = detail_super1,
	.brief_detail_super = brief_detail_super1,
	.uuid_from_super = uuid_from_super1,
	.getinfo_super = getinfo_super1,
	.update_super = update_super1,
	.event_super = event_super1,
	.init_super = init_super1,
	.add_to_super = add_to_super1,
	.store_super = store_super1,
	.write_init_super = write_init_super1,
	.compare_super = compare_super1,
	.load_super = load_super1,
	.match_metadata_desc = match_metadata_desc1,
	.avail_size = avail_size1,
	.major = 1,
};
