/*
 * mdadm - manage Linux "md" devices aka RAID arrays.
 *
 * Copyright (C) 2001-2002 Neil Brown <neilb@cse.unsw.edu.au>
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

#include	"mdadm.h"
#include	"dlink.h"

#if ! defined(__BIG_ENDIAN) && ! defined(__LITTLE_ENDIAN)
#error no endian defined
#endif
#include	"md_u.h"
#include	"md_p.h"
int Examine(mddev_dev_t devlist, int brief, int scan, int SparcAdjust)
{

	/* Read the raid superblock from a device and
	 * display important content.
	 *
	 * If cannot be found, print reason: too small, bad magic
	 *
	 * Print:
	 *   version, ctime, level, size, raid+spare+
	 *   prefered minor
	 *   uuid
	 *
	 *   utime, state etc
	 *
	 * If (brief) gather devices for same array and just print a mdadm.conf line including devices=
	 * if devlist==NULL, use conf_get_devs(
	 */
	int fd; 
	time_t atime;
	mdp_super_t super;
	int d;
	char *c;
	int rv = 0;
	int err;
	int spares = 0;

	struct array {
		mdp_super_t super;
		void *devs;
		struct array *next;
	} *arrays = NULL;

	for (; devlist ; devlist=devlist->next) {
		fd = open(devlist->devname, O_RDONLY);
		if (fd < 0) {
			if (!scan)
				fprintf(stderr,Name ": cannot open %s: %s\n",
					devlist->devname, strerror(errno));
			err = 1;
		}
		else {
			err = load_super(fd, &super);
			close(fd);
		}
		if (err && (brief||scan))
			continue;
		if (err) rv =1;
		switch(err) {
		case 1:
			fprintf(stderr, Name ": cannot find device size for %s: %s\n",
				devlist->devname, strerror(errno));
			continue;
		case 2:
/*		fprintf(stderr, Name ": %s is too small for md: size is %ld sectors\n",
		devlist->devname, size);
*/
			fprintf(stderr, Name ": %s is too small for md\n",
				devlist->devname);
			continue;
		case 3:
			fprintf(stderr, Name ": Cannot seek to superblock on %s: %s\n",
				devlist->devname, strerror(errno));
			continue;
		case 4:
			fprintf(stderr, Name ": Cannot read superblock on %s\n",
				devlist->devname);
			continue;
		case 5:
			fprintf(stderr, Name ": No super block found on %s (Expected magic %08x, got %08x)\n",
				devlist->devname, MD_SB_MAGIC, super.md_magic);
			continue;
		case 6:
			fprintf(stderr, Name ": Cannot interpret superblock on %s - version is %d\n",
				devlist->devname, super.major_version);
			continue;
		}
    
		/* Ok, its good enough to try, though the checksum could be wrong */
		if (brief) {
			struct array *ap;
			char *d;
			for (ap=arrays; ap; ap=ap->next) {
				if (compare_super(&ap->super, &super)==0)
					break;
			}
			if (!ap) {
				ap = malloc(sizeof(*ap));
				ap->super = super;
				ap->devs = dl_head();
				ap->next = arrays;
				arrays = ap;
			}
			d = dl_strdup(devlist->devname);
			dl_add(ap->devs, d);
		} else {
			printf("%s:\n",devlist->devname);
			printf("          Magic : %08x\n", super.md_magic);
			printf("        Version : %02d.%02d.%02d\n", super.major_version, super.minor_version,
			       super.patch_version);
			if (super.minor_version >= 90)
				printf("           UUID : %08x:%08x:%08x:%08x\n", super.set_uuid0, super.set_uuid1,
				       super.set_uuid2, super.set_uuid3);
			else
				printf("           UUID : %08x\n", super.set_uuid0);

			atime = super.ctime;
			printf("  Creation Time : %.24s\n", ctime(&atime));
			c=map_num(pers, super.level);
			printf("     Raid Level : %s\n", c?c:"-unknown-");
			if (super.level <= 0)
				printf("    Device Size : %u%s\n", super.size, human_size((long long)super.size<<10));
			printf("   Raid Devices : %d\n", super.raid_disks);
			printf("  Total Devices : %d\n", super.nr_disks);
			printf("Preferred Minor : %d\n", super.md_minor);
			printf("\n");
			atime = super.utime;
			printf("    Update Time : %.24s\n", ctime(&atime));
			printf("          State : %s\n",
			       (super.state&(1<<MD_SB_CLEAN))?"clean":"dirty");
			printf(" Active Devices : %d\n", super.active_disks);
			printf("Working Devices : %d\n", super.working_disks);
			printf(" Failed Devices : %d\n", super.failed_disks);
			printf("  Spare Devices : %d\n", super.spare_disks);
			if (calc_sb_csum(&super) == super.sb_csum)
				printf("       Checksum : %x - correct\n", super.sb_csum);
			else
				printf("       Checksum : %x - expected %lx\n", super.sb_csum, calc_sb_csum(&super));
			if (SparcAdjust) {
				/* 2.2 sparc put the events in the wrong place
				 * So we copy the tail of the superblock
				 * up 4 bytes before continuing
				 */
				__u32 *sb32 = (__u32*)&super;
				memcpy(sb32+MD_SB_GENERIC_CONSTANT_WORDS+7,
				       sb32+MD_SB_GENERIC_CONSTANT_WORDS+7+1,
				       (MD_SB_WORDS - (MD_SB_GENERIC_CONSTANT_WORDS+7+1))*4);
				printf (" --- adjusting superblock for 2.2/sparc compatability ---\n");
			}
			printf("         Events : %d.%d\n", super.events_hi, super.events_lo);
			if (super.events_hi == super.cp_events_hi &&
			    super.events_lo == super.cp_events_lo &&
			    super.recovery_cp > 0 &&
			    (super.state & (1<<MD_SB_CLEAN)) == 0 )
				printf("Sync checkpoint : %d KB (%d%%)\n", super.recovery_cp/2, super.recovery_cp/(super.size/100*2));
			printf("\n");
			if (super.level == 5) {
				c = map_num(r5layout, super.layout);
				printf("         Layout : %s\n", c?c:"-unknown-");
			}
			if (super.level == 10)
				printf("         Layout : near=%d, far=%d\n",
				       super.layout&255, (super.layout>>8) & 255);

			switch(super.level) {
			case 0:
			case 4:
			case 5:
				printf("     Chunk Size : %dK\n", super.chunk_size/1024);
				break;
			case -1:
				printf("       Rounding : %dK\n", super.chunk_size/1024);
				break;
			default: break;		
			}
			printf("\n");
			printf("      Number   Major   Minor   RaidDevice State\n");
			for (d= -1; d<(signed int)(super.raid_disks+super.spare_disks); d++) {
				mdp_disk_t *dp;
				char *dv;
				char nb[5];
				if (d>=0) dp = &super.disks[d];
				else dp = &super.this_disk;
				sprintf(nb, "%4d", d);
				printf("%4s %5d   %5d    %5d    %5d     ", d < 0 ? "this" :  nb,
				       dp->number, dp->major, dp->minor, dp->raid_disk);
				if (dp->state & (1<<MD_DISK_FAULTY)) printf(" faulty");
				if (dp->state & (1<<MD_DISK_ACTIVE)) printf(" active");
				if (dp->state & (1<<MD_DISK_SYNC)) printf(" sync");
				if (dp->state & (1<<MD_DISK_REMOVED)) printf(" removed");
				if (dp->state == 0) { printf(" spare"); spares++; }
				if ((dv=map_dev(dp->major, dp->minor)))
					printf("   %s", dv);
				printf("\n");
				if (d == -1) printf("\n");
			}
		}
		if (SparcAdjust == 2) {
			printf(" ----- updating superblock on device ----\n");
			fd = open(devlist->devname, O_RDWR);
			if (fd < 0) {
				fprintf(stderr, Name ": cannot open %s to update superblock: %s\n",
					devlist->devname, strerror(errno));
				err = 1;
			} else {
				super.sb_csum = calc_sb_csum(&super);
				if (store_super(fd, &super)) {
					fprintf(stderr, Name ": Count not re-write superblock on %s\n",
						devlist->devname);
					err = 1;
				}
				close(fd);
			}
		}
	}
	if (brief) {
		struct array *ap;
		for (ap=arrays; ap; ap=ap->next) {
			char sep='=';
			char *c=map_num(pers, ap->super.level);
			char *d;
			printf("ARRAY %s level=%s num-devices=%d UUID=",
			       get_md_name(ap->super.md_minor),
			       c?c:"-unknown-", ap->super.raid_disks);
			if (spares) printf(" spares=%d", spares);
			if (ap->super.minor_version >= 90)
				printf("%08x:%08x:%08x:%08x", ap->super.set_uuid0, ap->super.set_uuid1,
				       ap->super.set_uuid2, ap->super.set_uuid3);
			else
				printf("%08x", ap->super.set_uuid0);
			printf("\n   devices");
			for (d=dl_next(ap->devs); d!= ap->devs; d=dl_next(d)) {
				printf("%c%s", sep, d);
				sep=',';
			}
			printf("\n");
		}
	}
	return rv;
}
