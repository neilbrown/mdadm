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

/*
 * All handling for the 0.90.0 version superblock is in
 * this file.
 * This includes:
 *   - finding, loading, and writing the superblock.
 *   - initialising a new superblock
 *   - printing the superblock for --examine
 *   - printing part of the superblock for --detail
 * .. other stuff 
 */


static unsigned long calc_sb0_csum(mdp_super_t *super)
{
	unsigned long csum = super->sb_csum;
	unsigned long newcsum;
	super->sb_csum= 0 ;
	newcsum = calc_csum(super, MD_SB_BYTES);
	super->sb_csum = csum;
	return newcsum;
}

void examine_super0(void *sbv)
{
	mdp_super_t *sb = sbv;
	time_t atime;
	int d;
	char *c;

	printf("          Magic : %08x\n", sb->md_magic);
	printf("        Version : %02d.%02d.%02d\n", sb->major_version, sb->minor_version,
	       sb->patch_version);
	if (sb->minor_version >= 90)
		printf("           UUID : %08x:%08x:%08x:%08x\n", sb->set_uuid0, sb->set_uuid1,
		       sb->set_uuid2, sb->set_uuid3);
	else
		printf("           UUID : %08x\n", sb->set_uuid0);

	atime = sb->ctime;
	printf("  Creation Time : %.24s\n", ctime(&atime));
	c=map_num(pers, sb->level);
	printf("     Raid Level : %s\n", c?c:"-unknown-");
	if ((int)sb->level >= 0)
		printf("    Device Size : %d%s\n", sb->size, human_size((long long)sb->size<<10));
	printf("   Raid Devices : %d\n", sb->raid_disks);
	printf("  Total Devices : %d\n", sb->nr_disks);
	printf("Preferred Minor : %d\n", sb->md_minor);
	printf("\n");
	atime = sb->utime;
	printf("    Update Time : %.24s\n", ctime(&atime));
	printf("          State : %s\n",
	       (sb->state&(1<<MD_SB_CLEAN))?"clean":"active");
	printf(" Active Devices : %d\n", sb->active_disks);
	printf("Working Devices : %d\n", sb->working_disks);
	printf(" Failed Devices : %d\n", sb->failed_disks);
	printf("  Spare Devices : %d\n", sb->spare_disks);
	if (calc_sb0_csum(sb) == sb->sb_csum)
		printf("       Checksum : %x - correct\n", sb->sb_csum);
	else
		printf("       Checksum : %x - expected %lx\n", sb->sb_csum, calc_sb0_csum(sb));
	printf("         Events : %d.%d\n", sb->events_hi, sb->events_lo);
	printf("\n");
	if (sb->level == 5) {
		c = map_num(r5layout, sb->layout);
		printf("         Layout : %s\n", c?c:"-unknown-");
	}
	switch(sb->level) {
	case 0:
	case 4:
	case 5:
		printf("     Chunk Size : %dK\n", sb->chunk_size/1024);
		break;
	case -1:
		printf("       Rounding : %dK\n", sb->chunk_size/1024);
		break;
	default: break;		
	}
	printf("\n");
	printf("      Number   Major   Minor   RaidDevice State\n");
	for (d= -1; d<(signed int)(sb->raid_disks+sb->spare_disks); d++) {
		mdp_disk_t *dp;
		char *dv;
		char nb[5];
		if (d>=0) dp = &sb->disks[d];
		else dp = &sb->this_disk;
		sprintf(nb, "%4d", d);
		printf("%4s %5d   %5d    %5d    %5d     ", d < 0 ? "this" :  nb,
		       dp->number, dp->major, dp->minor, dp->raid_disk);
		if (dp->state & (1<<MD_DISK_FAULTY)) printf(" faulty");
		if (dp->state & (1<<MD_DISK_ACTIVE)) printf(" active");
		if (dp->state & (1<<MD_DISK_SYNC)) printf(" sync");
		if (dp->state & (1<<MD_DISK_REMOVED)) printf(" removed");
		if (dp->state == 0) printf(" spare");
		if ((dv=map_dev(dp->major, dp->minor)))
			printf("   %s", dv);
		printf("\n");
		if (d == -1) printf("\n");
	}
}

void brief_examine_super0(void *sbv)
{
	mdp_super_t *sb = sbv;
	char *c=map_num(pers, sb->level);

	printf("ARRAY %s level=%s num-devices=%d UUID=",
	       get_md_name(sb->md_minor),
	       c?c:"-unknown-", sb->raid_disks);
	if (sb->minor_version >= 90)
		printf("%08x:%08x:%08x:%08x", sb->set_uuid0, sb->set_uuid1,
		       sb->set_uuid2, sb->set_uuid3);
	else
		printf("%08x", sb->set_uuid0);
	printf("\n");
}

void detail_super0(void *sbv)
{
	mdp_super_t *sb = sbv;
	printf("           UUID : ");
	if (sb->minor_version >= 90)
		printf("%08x:%08x:%08x:%08x", sb->set_uuid0, sb->set_uuid1,
		       sb->set_uuid2, sb->set_uuid3);
	else
		printf("%08x", sb->set_uuid0);
	printf("\n         Events : %d.%d\n\n", sb->events_hi, sb->events_lo);
}

void brief_detail_super0(void *sbv)
{
	mdp_super_t *sb = sbv;
	printf(" UUID=");
	if (sb->minor_version >= 90)
		printf("%08x:%08x:%08x:%08x", sb->set_uuid0, sb->set_uuid1,
		       sb->set_uuid2, sb->set_uuid3);
	else
		printf("%08x", sb->set_uuid0);
}

void uuid_from_super0(int uuid[4], void * sbv)
{
	mdp_super_t *super = sbv;
	uuid[0] = super->set_uuid0;
	if (super->minor_version >= 90) {
		uuid[1] = super->set_uuid1;
		uuid[2] = super->set_uuid2;
		uuid[3] = super->set_uuid3;
	} else {
		uuid[1] = 0;
		uuid[2] = 0;
		uuid[3] = 0;
	}
}

void getinfo_super0(struct mdinfo *info, void *sbv)
{
	mdp_super_t *sb = sbv;
	int working = 0;
	int i;

	info->array.major_version = sb->major_version;
	info->array.minor_version = sb->minor_version;
	info->array.patch_version = sb->patch_version;
	info->array.raid_disks = sb->raid_disks;
	info->array.level = sb->level;
	info->array.md_minor = sb->md_minor;
	info->array.ctime = sb->ctime;

	info->disk.state = sb->this_disk.state;
	info->disk.major = sb->this_disk.major;
	info->disk.minor = sb->this_disk.minor;
	info->disk.raid_disk = sb->this_disk.raid_disk;

	info->events = md_event(sb);

	uuid_from_super0(info->uuid, sbv);
	
	/* work_disks is calculated rather than read directly */
	for (i=0; i < MD_SB_DISKS; i++)
		if ((sb->disks[i].state & (1<<MD_DISK_SYNC)) &&
		    (sb->disks[i].state & (1<<MD_DISK_ACTIVE)) &&
		    !(sb->disks[i].state & (1<<MD_DISK_FAULTY)))
			working ++;
	info->array.working_disks = working;
}


int update_super0(struct mdinfo *info, void *sbv, char *update, char *devname, int verbose)
{
	int rv = 0;
	mdp_super_t *sb = sbv;
	if (strcmp(update, "sparc2.2")==0 ) {
		/* 2.2 sparc put the events in the wrong place
		 * So we copy the tail of the superblock
		 * up 4 bytes before continuing
		 */
		__u32 *sb32 = (__u32*)sb;
		memcpy(sb32+MD_SB_GENERIC_CONSTANT_WORDS+7,
		       sb32+MD_SB_GENERIC_CONSTANT_WORDS+7+1,
		       (MD_SB_WORDS - (MD_SB_GENERIC_CONSTANT_WORDS+7+1))*4);
		fprintf (stderr, Name ": adjusting superblock of %s for 2.2/sparc compatability.\n",
			 devname);
	}
	if (strcmp(update, "super-minor") ==0) {
		sb->md_minor = info->array.md_minor;
		if (verbose)
			fprintf(stderr, Name ": updating superblock of %s with minor number %d\n",
				devname, info->array.md_minor);
	}
	if (strcmp(update, "summaries") == 0) {
		int i;
		/* set nr_disks, active_disks, working_disks,
		 * failed_disks, spare_disks based on disks[] 
		 * array in superblock.
		 * Also make sure extra slots aren't 'failed'
		 */
		sb->nr_disks = sb->active_disks =
			sb->working_disks = sb->failed_disks =
			sb->spare_disks = 0;
		for (i=0; i < MD_SB_DISKS ; i++) 
			if (sb->disks[i].major ||
			    sb->disks[i].minor) {
				int state = sb->disks[i].state;
				if (state & (1<<MD_DISK_REMOVED))
					continue;
				sb->nr_disks++;
				if (state & (1<<MD_DISK_ACTIVE))
					sb->active_disks++;
				if (state & (1<<MD_DISK_FAULTY))
					sb->failed_disks++;
				else
					sb->working_disks++;
				if (state == 0)
					sb->spare_disks++;
			} else if (i >= sb->raid_disks && sb->disks[i].number == 0)
				sb->disks[i].state = 0;
	}
	if (strcmp(update, "force")==0) {
		sb->events_hi = (info->events>>32) & 0xFFFFFFFF;
		sb->events_lo = (info->events) & 0xFFFFFFFF;
		if (sb->level == 5 || sb->level == 4 || sb->level == 6)
			/* need to force clean */
			sb->state |= (1 << MD_SB_CLEAN);
	}
	if (strcmp(update, "assemble")==0) {
		int d = info->disk.number;
		if (sb->disks[d].state != info->disk.state) {
			sb->disks[d].state = info->disk.state;
			rv = 1;
		}
	}
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
	if (strcmp(update, "grow") == 0) {
		sb->raid_disks = info->array.raid_disks;
		sb->nr_disks = info->array.nr_disks;
		sb->active_disks = info->array.active_disks;
		sb->working_disks = info->array.working_disks;
		memset(&sb->disks[info->disk.number], 0, sizeof(sb->disks[0]));
		sb->disks[info->disk.number].number = info->disk.number;
		sb->disks[info->disk.number].major = info->disk.major;
		sb->disks[info->disk.number].minor = info->disk.minor;
		sb->disks[info->disk.number].raid_disk = info->disk.raid_disk;
		sb->disks[info->disk.number].state = info->disk.state;
		if (sb->this_disk.number == info->disk.number)
			sb->this_disk = sb->disks[info->disk.number];
	}
	if (strcmp(update, "resync") == 0) {
		/* make sure resync happens */
		sb->state &= ~(1<<MD_SB_CLEAN);
		sb->recovery_cp = 0;
	}

	sb->sb_csum = calc_sb0_csum(sb);
	return rv;
}

__u64 event_super0(void *sbv)
{
	mdp_super_t *sb = sbv;
	return md_event(sb);
}



void init_super0(void **sbp, mdu_array_info_t *info)
{
	mdp_super_t *sb = malloc(MD_SB_BYTES);
	memset(sb, 0, MD_SB_BYTES);

	sb->md_magic = MD_SB_MAGIC;
	sb->major_version = 0;
	sb->minor_version = 90;
	sb->patch_version = 0;
	sb->gvalid_words = 0; /* ignored */
	sb->set_uuid0 = random();
	sb->ctime = time(0);
	sb->level = info->level;
	sb->size = info->size;
	sb->nr_disks = info->nr_disks;
	sb->raid_disks = info->raid_disks;
	sb->md_minor = info->md_minor;
	sb->not_persistent = 0;
	sb->set_uuid1 = random();
	sb->set_uuid2 = random();
	sb->set_uuid3 = random();

	sb->utime = sb->ctime;
	sb->state = info->state;
	sb->active_disks = info->active_disks;
	sb->working_disks = info->working_disks;
	sb->failed_disks = info->failed_disks;
	sb->events_hi = 0;
	sb->events_lo = 1;

	sb->layout = info->layout;
	sb->chunk_size = info->chunk_size;

	*sbp = sb;
}

/* Add a device to the superblock being created */
void add_to_super0(void *sbv, mdu_disk_info_t *dinfo)
{
	mdp_super_t *sb = sbv;
	mdp_disk_t *dk = &sb->disks[dinfo->number];
	
	dk->number = dinfo->number;
	dk->major = dinfo->major;
	dk->minor = dinfo->minor;
	dk->raid_disk = dinfo->raid_disk;
	dk->state = dinfo->state;
}

int store_super0(int fd, mdp_super_t *super)
{
	unsigned long size;
	unsigned long long dsize;
	unsigned long long offset;
    
#ifdef BLKGETSIZE64
	if (ioctl(fd, BLKGETSIZE64, &dsize) != 0)
#endif
	{
		if (ioctl(fd, BLKGETSIZE, &size))
			return 1;
		else
			dsize = ((unsigned long long)size)<<9;
	}

	if (dsize < MD_RESERVED_SECTORS*2)
		return 2;
	
	offset = MD_NEW_SIZE_SECTORS(dsize>>9);

	offset *= 512;

	if (lseek64(fd, offset, 0)< 0LL)
		return 3;

	if (write(fd, super, sizeof(*super)) != sizeof(*super))
		return 4;

	return 0;
}

int write_init_super0(void *sbv, mdu_disk_info_t *dinfo, char *devname)
{
	mdp_super_t *sb = sbv;
	int fd = open(devname, O_RDWR, O_EXCL);
	int rv;

	if (fd < 0) {
		fprintf(stderr, Name ": Failed to open %s to write superblock\n", devname);
		return -1;
	}

	sb->this_disk = sb->disks[dinfo->number];
	sb->sb_csum = calc_sb0_csum(sb);
	rv = store_super0(fd, sb);
	close(fd);
	if (rv)
		fprintf(stderr, Name ": failed to write superblock to %s\n", devname);
	return rv;
}

int compare_super0(void **firstp, void *secondv)
{
	/*
	 * return:
	 *  0 same, or first was empty, and second was copied
	 *  1 second had wrong number
	 *  2 wrong uuid
	 *  3 wrong other info
	 */
	mdp_super_t *first = *firstp;
	mdp_super_t *second = secondv;

	int uuid1[4], uuid2[4];
	if (second->md_magic != MD_SB_MAGIC)
		return 1;
	if (!first) {
		first = malloc(MD_SB_BYTES);
		memcpy(first, second, MD_SB_BYTES);
		*firstp = first;
		return 0;
	}

	uuid_from_super0(uuid1, first);
	uuid_from_super0(uuid2, second);
	if (!same_uuid(uuid1, uuid2))
		return 2;
	if (first->major_version != second->major_version ||
	    first->minor_version != second->minor_version ||
	    first->patch_version != second->patch_version ||
	    first->gvalid_words  != second->gvalid_words  ||
	    first->ctime         != second->ctime         ||
	    first->level         != second->level         ||
	    first->size          != second->size          ||
	    first->raid_disks    != second->raid_disks    )
		return 3;

	return 0;
}


int load_super0(int fd, void **sbp, char *devname)
{
	/* try to read in the superblock
	 * Return:
	 *  0 on success
	 *  1 on cannot get superblock
	 *  2 on superblock meaningless
	 */
	unsigned long size;
	unsigned long long dsize;
	unsigned long long offset;
	mdp_super_t *super;
    
#ifdef BLKGETSIZE64
	if (ioctl(fd, BLKGETSIZE64, &dsize) != 0)
#endif
	{
		if (ioctl(fd, BLKGETSIZE, &size)) {
			if (devname)
				fprintf(stderr, Name ": cannot find device size for %s: %s\n",
					devname, strerror(errno));
			return 1;
		} else
			dsize = size << 9;
	}

	if (dsize < MD_RESERVED_SECTORS*2) {
		if (devname)
			fprintf(stderr, Name ": %s is too small for md: size is %ld sectors.\n",
				devname, size);
		return 1;
	}
	
	offset = MD_NEW_SIZE_SECTORS(dsize>>9);

	offset *= 512;

	ioctl(fd, BLKFLSBUF, 0); /* make sure we read current data */

	if (lseek64(fd, offset, 0)< 0LL) {
		if (devname)
			fprintf(stderr, Name ": Cannot seek to superblock on %s: %s\n",
				devname, strerror(errno));
		return 1;
	}

	super = malloc(MD_SB_BYTES);

	if (read(fd, super, sizeof(*super)) != MD_SB_BYTES) {
		if (devname)
			fprintf(stderr, Name ": Cannot read superblock on %s\n",
				devname);
		free(super);
		return 1;
	}

	if (super->md_magic != MD_SB_MAGIC) {
		if (devname)
			fprintf(stderr, Name ": No super block found on %s (Expected magic %08x, got %08x)\n",
				devname, MD_SB_MAGIC, super->md_magic);
		free(super);
		return 2;
	}

	if (super->major_version != 0) {
		if (devname)
			fprintf(stderr, Name ": Cannot interpret superblock on %s - version is %d\n",
				devname, super->major_version);
		free(super);
		return 2;
	}
	*sbp = super;
	return 0;
}
