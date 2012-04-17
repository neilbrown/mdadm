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

#include	"mdadm.h"
#include	"md_p.h"
#include	<sys/socket.h>
#include	<sys/utsname.h>
#include	<sys/wait.h>
#include	<sys/un.h>
#include	<ctype.h>
#include	<dirent.h>
#include	<signal.h>

int __offroot;

/*
 * following taken from linux/blkpg.h because they aren't
 * anywhere else and it isn't safe to #include linux/ * stuff.
 */

#define BLKPG      _IO(0x12,105)

/* The argument structure */
struct blkpg_ioctl_arg {
        int op;
        int flags;
        int datalen;
        void *data;
};

/* The subfunctions (for the op field) */
#define BLKPG_ADD_PARTITION	1
#define BLKPG_DEL_PARTITION	2

/* Sizes of name fields. Unused at present. */
#define BLKPG_DEVNAMELTH	64
#define BLKPG_VOLNAMELTH	64

/* The data structure for ADD_PARTITION and DEL_PARTITION */
struct blkpg_partition {
	long long start;		/* starting offset in bytes */
	long long length;		/* length in bytes */
	int pno;			/* partition number */
	char devname[BLKPG_DEVNAMELTH];	/* partition name, like sda5 or c0d1p2,
					   to be used in kernel messages */
	char volname[BLKPG_VOLNAMELTH];	/* volume label */
};

#include "part.h"

/* Force a compilation error if condition is true */
#define BUILD_BUG_ON(condition) ((void)BUILD_BUG_ON_ZERO(condition))

/* Force a compilation error if condition is true, but also produce a
   result (of value 0 and type size_t), so the expression can be used
   e.g. in a structure initializer (or where-ever else comma expressions
   aren't permitted). */
#define BUILD_BUG_ON_ZERO(e) (sizeof(struct { int:-!!(e); }))

/*
 * Parse a 128 bit uuid in 4 integers
 * format is 32 hexx nibbles with options :.<space> separator
 * If not exactly 32 hex digits are found, return 0
 * else return 1
 */
int parse_uuid(char *str, int uuid[4])
{
	int hit = 0; /* number of Hex digIT */
	int i;
	char c;
	for (i=0; i<4; i++) uuid[i]=0;

	while ((c= *str++)) {
		int n;
		if (c>='0' && c<='9')
			n = c-'0';
		else if (c>='a' && c <= 'f')
			n = 10 + c - 'a';
		else if (c>='A' && c <= 'F')
			n = 10 + c - 'A';
		else if (strchr(":. -", c))
			continue;
		else return 0;

		if (hit<32) {
			uuid[hit/8] <<= 4;
			uuid[hit/8] += n;
		}
		hit++;
	}
	if (hit == 32)
		return 1;
	return 0;
}


/*
 * Get the md version number.
 * We use the RAID_VERSION ioctl if it is supported
 * If not, but we have a block device with major '9', we assume
 * 0.36.0
 *
 * Return version number as 24 but number - assume version parts
 * always < 255
 */

int md_get_version(int fd)
{
    struct stat stb;
    mdu_version_t vers;

    if (fstat(fd, &stb)<0)
	return -1;
    if ((S_IFMT&stb.st_mode) != S_IFBLK)
	return -1;

    if (ioctl(fd, RAID_VERSION, &vers) == 0)
	return  (vers.major*10000) + (vers.minor*100) + vers.patchlevel;
    if (errno == EACCES)
	    return -1;
    if (major(stb.st_rdev) == MD_MAJOR)
	return (3600);
    return -1;
}

int get_linux_version()
{
	struct utsname name;
	char *cp;
	int a = 0, b = 0,c = 0;
	if (uname(&name) <0)
		return -1;

	cp = name.release;
	a = strtoul(cp, &cp, 10);
	if (*cp == '.')
		b = strtoul(cp+1, &cp, 10);
	if (*cp == '.')
		c = strtoul(cp+1, &cp, 10);

	return (a*1000000)+(b*1000)+c;
}

#ifndef MDASSEMBLE
int mdadm_version(char *version)
{
	int a, b, c;
	char *cp;

	if (!version)
		version = Version;

	cp = strchr(version, '-');
	if (!cp || *(cp+1) != ' ' || *(cp+2) != 'v')
		return -1;
	cp += 3;
	a = strtoul(cp, &cp, 10);
	if (*cp != '.')
		return -1;
	b = strtoul(cp+1, &cp, 10);
	if (*cp == '.')
		c = strtoul(cp+1, &cp, 10);
	else
		c = 0;
	if (*cp != ' ' && *cp != '-')
		return -1;
	return (a*1000000)+(b*1000)+c;
}

long long parse_size(char *size)
{
	/* parse 'size' which should be a number optionally
	 * followed by 'K', 'M', or 'G'.
	 * Without a suffix, K is assumed.
	 * Number returned is in sectors (half-K)
	 * -1 returned on error.
	 */
	char *c;
	long long s = strtoll(size, &c, 10);
	if (s > 0) {
		switch (*c) {
		case 'K':
			c++;
		default:
			s *= 2;
			break;
		case 'M':
			c++;
			s *= 1024 * 2;
			break;
		case 'G':
			c++;
			s *= 1024 * 1024 * 2;
			break;
		}
	}
	if (*c)
		s = -1;
	return s;
}

int parse_layout_10(char *layout)
{
	int copies, rv;
	char *cp;
	/* Parse the layout string for raid10 */
	/* 'f', 'o' or 'n' followed by a number <= raid_disks */
	if ((layout[0] !=  'n' && layout[0] != 'f' && layout[0] != 'o') ||
	    (copies = strtoul(layout+1, &cp, 10)) < 1 ||
	    copies > 200 ||
	    *cp)
		return -1;
	if (layout[0] == 'n')
		rv = 256 + copies;
	else if (layout[0] == 'o')
		rv = 0x10000 + (copies<<8) + 1;
	else
		rv = 1 + (copies<<8);
	return rv;
}

int parse_layout_faulty(char *layout)
{
	/* Parse the layout string for 'faulty' */
	int ln = strcspn(layout, "0123456789");
	char *m = strdup(layout);
	int mode;
	m[ln] = 0;
	mode = map_name(faultylayout, m);
	if (mode == UnSet)
		return -1;

	return mode | (atoi(layout+ln)<< ModeShift);
}
#endif

void remove_partitions(int fd)
{
	/* remove partitions from this block devices.
	 * This is used for components added to an array
	 */
#ifdef BLKPG_DEL_PARTITION
	struct blkpg_ioctl_arg a;
	struct blkpg_partition p;

	a.op = BLKPG_DEL_PARTITION;
	a.data = (void*)&p;
	a.datalen = sizeof(p);
	a.flags = 0;
	memset(a.data, 0, a.datalen);
	for (p.pno=0; p.pno < 16; p.pno++)
		ioctl(fd, BLKPG, &a);
#endif
}

int test_partition(int fd)
{
	/* Check if fd is a whole-disk or a partition.
	 * BLKPG will return EINVAL on a partition, and BLKPG_DEL_PARTITION
	 * will return ENXIO on an invalid partition number.
	 */
	struct blkpg_ioctl_arg a;
	struct blkpg_partition p;
	a.op = BLKPG_DEL_PARTITION;
	a.data = (void*)&p;
	a.datalen = sizeof(p);
	a.flags = 0;
	memset(a.data, 0, a.datalen);
	p.pno = 1<<30;
	if (ioctl(fd, BLKPG, &a) == 0)
		/* Very unlikely, but not a partition */
		return 0;
	if (errno == ENXIO)
		/* not a partition */
		return 0;

	return 1;
}

int test_partition_from_id(dev_t id)
{
	char buf[20];
	int fd, rv;

	sprintf(buf, "%d:%d", major(id), minor(id));
	fd = dev_open(buf, O_RDONLY);
	if (fd < 0)
		return -1;
	rv = test_partition(fd);
	close(fd);
	return rv;
}

int enough(int level, int raid_disks, int layout, int clean, char *avail)
{
	int copies, first;
	int i;
	int avail_disks = 0;

	for (i = 0; i < raid_disks; i++)
		avail_disks += !!avail[i];

	switch (level) {
	case 10:
		/* This is the tricky one - we need to check
		 * which actual disks are present.
		 */
		copies = (layout&255)* ((layout>>8) & 255);
		first=0;
		do {
			/* there must be one of the 'copies' form 'first' */
			int n = copies;
			int cnt=0;
			while (n--) {
				if (avail[first])
					cnt++;
				first = (first+1) % raid_disks;
			}
			if (cnt == 0)
				return 0;

		} while (first != 0);
		return 1;

	case LEVEL_MULTIPATH:
		return avail_disks>= 1;
	case LEVEL_LINEAR:
	case 0:
		return avail_disks == raid_disks;
	case 1:
		return avail_disks >= 1;
	case 4:
	case 5:
		if (clean)
			return avail_disks >= raid_disks-1;
		else
			return avail_disks >= raid_disks;
	case 6:
		if (clean)
			return avail_disks >= raid_disks-2;
		else
			return avail_disks >= raid_disks;
	default:
		return 0;
	}
}

int enough_fd(int fd)
{
	struct mdu_array_info_s array;
	struct mdu_disk_info_s disk;
	int avail_disks = 0;
	int i, rv;
	char *avail;

	if (ioctl(fd, GET_ARRAY_INFO, &array) != 0 ||
	    array.raid_disks <= 0)
		return 0;
	avail = calloc(array.raid_disks, 1);
	for (i=0; i < MAX_DISKS && array.nr_disks > 0; i++) {
		disk.number = i;
		if (ioctl(fd, GET_DISK_INFO, &disk) != 0)
			continue;
		if (disk.major == 0 && disk.minor == 0)
			continue;
		array.nr_disks--;

		if (! (disk.state & (1<<MD_DISK_SYNC)))
			continue;
		if (disk.raid_disk < 0 || disk.raid_disk >= array.raid_disks)
			continue;
		avail_disks++;
		avail[disk.raid_disk] = 1;
	}
	/* This is used on an active array, so assume it is clean */
	rv = enough(array.level, array.raid_disks, array.layout,
		    1, avail);
	free(avail);
	return rv;
}


const int uuid_zero[4] = { 0, 0, 0, 0 };

int same_uuid(int a[4], int b[4], int swapuuid)
{
	if (swapuuid) {
		/* parse uuids are hostendian.
		 * uuid's from some superblocks are big-ending
		 * if there is a difference, we need to swap..
		 */
		unsigned char *ac = (unsigned char *)a;
		unsigned char *bc = (unsigned char *)b;
		int i;
		for (i=0; i<16; i+= 4) {
			if (ac[i+0] != bc[i+3] ||
			    ac[i+1] != bc[i+2] ||
			    ac[i+2] != bc[i+1] ||
			    ac[i+3] != bc[i+0])
				return 0;
		}
		return 1;
	} else {
		if (a[0]==b[0] &&
		    a[1]==b[1] &&
		    a[2]==b[2] &&
		    a[3]==b[3])
			return 1;
		return 0;
	}
}
void copy_uuid(void *a, int b[4], int swapuuid)
{
	if (swapuuid) {
		/* parse uuids are hostendian.
		 * uuid's from some superblocks are big-ending
		 * if there is a difference, we need to swap..
		 */
		unsigned char *ac = (unsigned char *)a;
		unsigned char *bc = (unsigned char *)b;
		int i;
		for (i=0; i<16; i+= 4) {
			ac[i+0] = bc[i+3];
			ac[i+1] = bc[i+2];
			ac[i+2] = bc[i+1];
			ac[i+3] = bc[i+0];
		}
	} else
		memcpy(a, b, 16);
}

char *__fname_from_uuid(int id[4], int swap, char *buf, char sep)
{
	int i, j;
	char uuid[16];
	char *c = buf;
	strcpy(c, "UUID-");
	c += strlen(c);
	copy_uuid(uuid, id, swap);
	for (i = 0; i < 4; i++) {
		if (i)
			*c++ = sep;
		for (j = 3; j >= 0; j--) {
			sprintf(c,"%02x", (unsigned char) uuid[j+4*i]);
			c+= 2;
		}
	}
	return buf;

}

char *fname_from_uuid(struct supertype *st, struct mdinfo *info, char *buf, char sep)
{
	// dirty hack to work around an issue with super1 superblocks...
	// super1 superblocks need swapuuid set in order for assembly to
	// work, but can't have it set if we want this printout to match
	// all the other uuid printouts in super1.c, so we force swapuuid
	// to 1 to make our printout match the rest of super1
	return __fname_from_uuid(info->uuid, (st->ss == &super1) ? 1 : st->ss->swapuuid, buf, sep);
}

#ifndef MDASSEMBLE
int check_ext2(int fd, char *name)
{
	/*
	 * Check for an ext2fs file system.
	 * Superblock is always 1K at 1K offset
	 *
	 * s_magic is le16 at 56 == 0xEF53
	 * report mtime - le32 at 44
	 * blocks - le32 at 4
	 * logblksize - le32 at 24
	 */
	unsigned char sb[1024];
	time_t mtime;
	int size, bsize;
	if (lseek(fd, 1024,0)!= 1024)
		return 0;
	if (read(fd, sb, 1024)!= 1024)
		return 0;
	if (sb[56] != 0x53 || sb[57] != 0xef)
		return 0;

	mtime = sb[44]|(sb[45]|(sb[46]|sb[47]<<8)<<8)<<8;
	bsize = sb[24]|(sb[25]|(sb[26]|sb[27]<<8)<<8)<<8;
	size = sb[4]|(sb[5]|(sb[6]|sb[7]<<8)<<8)<<8;
	fprintf(stderr, Name ": %s appears to contain an ext2fs file system\n",
		name);
	fprintf(stderr,"    size=%dK  mtime=%s",
		size*(1<<bsize), ctime(&mtime));
	return 1;
}

int check_reiser(int fd, char *name)
{
	/*
	 * superblock is at 64K
	 * size is 1024;
	 * Magic string "ReIsErFs" or "ReIsEr2Fs" at 52
	 *
	 */
	unsigned char sb[1024];
	unsigned long size;
	if (lseek(fd, 64*1024, 0) != 64*1024)
		return 0;
	if (read(fd, sb, 1024) != 1024)
		return 0;
	if (strncmp((char*)sb+52, "ReIsErFs",8)!=0 &&
	    strncmp((char*)sb+52, "ReIsEr2Fs",9)!=0)
		return 0;
	fprintf(stderr, Name ": %s appears to contain a reiserfs file system\n",name);
	size = sb[0]|(sb[1]|(sb[2]|sb[3]<<8)<<8)<<8;
	fprintf(stderr, "    size = %luK\n", size*4);

	return 1;
}

int check_raid(int fd, char *name)
{
	struct mdinfo info;
	time_t crtime;
	char *level;
	struct supertype *st = guess_super(fd);

	if (!st) return 0;
	st->ignore_hw_compat = 1;
	st->ss->load_super(st, fd, name);
	/* Looks like a raid array .. */
	fprintf(stderr, Name ": %s appears to be part of a raid array:\n",
		name);
	st->ss->getinfo_super(st, &info, NULL);
	st->ss->free_super(st);
	crtime = info.array.ctime;
	level = map_num(pers, info.array.level);
	if (!level) level = "-unknown-";
	fprintf(stderr, "    level=%s devices=%d ctime=%s",
		level, info.array.raid_disks, ctime(&crtime));
	return 1;
}

int ask(char *mesg)
{
	char *add = "";
	int i;
	for (i=0; i<5; i++) {
		char buf[100];
		fprintf(stderr, "%s%s", mesg, add);
		fflush(stderr);
		if (fgets(buf, 100, stdin)==NULL)
			return 0;
		if (buf[0]=='y' || buf[0]=='Y')
			return 1;
		if (buf[0]=='n' || buf[0]=='N')
			return 0;
		add = "(y/n) ";
	}
	fprintf(stderr, Name ": assuming 'no'\n");
	return 0;
}
#endif /* MDASSEMBLE */

int is_standard(char *dev, int *nump)
{
	/* tests if dev is a "standard" md dev name.
	 * i.e if the last component is "/dNN" or "/mdNN",
	 * where NN is a string of digits
	 * Returns 1 if a partitionable standard,
	 *   -1 if non-partitonable,
	 *   0 if not a standard name.
	 */
	char *d = strrchr(dev, '/');
	int type=0;
	int num;
	if (!d)
		return 0;
	if (strncmp(d, "/d",2)==0)
		d += 2, type=1; /* /dev/md/dN{pM} */
	else if (strncmp(d, "/md_d", 5)==0)
		d += 5, type=1; /* /dev/md_dN{pM} */
	else if (strncmp(d, "/md", 3)==0)
		d += 3, type=-1; /* /dev/mdN */
	else if (d-dev > 3 && strncmp(d-2, "md/", 3)==0)
		d += 1, type=-1; /* /dev/md/N */
	else
		return 0;
	if (!*d)
		return 0;
	num = atoi(d);
	while (isdigit(*d))
		d++;
	if (*d)
		return 0;
	if (nump) *nump = num;

	return type;
}

unsigned long calc_csum(void *super, int bytes)
{
	unsigned long long newcsum = 0;
	int i;
	unsigned int csum;
	unsigned int *superc = (unsigned int*) super;

	for(i=0; i<bytes/4; i++)
		newcsum+= superc[i];
	csum = (newcsum& 0xffffffff) + (newcsum>>32);
#ifdef __alpha__
/* The in-kernel checksum calculation is always 16bit on
 * the alpha, though it is 32 bit on i386...
 * I wonder what it is elsewhere... (it uses and API in
 * a way that it shouldn't).
 */
	csum = (csum & 0xffff) + (csum >> 16);
	csum = (csum & 0xffff) + (csum >> 16);
#endif
	return csum;
}

#ifndef MDASSEMBLE
char *human_size(long long bytes)
{
	static char buf[30];

	/* We convert bytes to either centi-M{ega,ibi}bytes or
	 * centi-G{igi,ibi}bytes, with appropriate rounding,
	 * and then print 1/100th of those as a decimal.
	 * We allow upto 2048Megabytes before converting to
	 * gigabytes, as that shows more precision and isn't
	 * too large a number.
	 * Terabytes are not yet handled.
	 */

	if (bytes < 5000*1024)
		buf[0]=0;
	else if (bytes < 2*1024LL*1024LL*1024LL) {
		long cMiB = (bytes / ( (1LL<<20) / 200LL ) +1) /2;
		long cMB  = (bytes / ( 1000000LL / 200LL ) +1) /2;
		snprintf(buf, sizeof(buf), " (%ld.%02ld MiB %ld.%02ld MB)",
			cMiB/100 , cMiB % 100,
			cMB/100, cMB % 100);
	} else {
		long cGiB = (bytes / ( (1LL<<30) / 200LL ) +1) /2;
		long cGB  = (bytes / (1000000000LL/200LL ) +1) /2;
		snprintf(buf, sizeof(buf), " (%ld.%02ld GiB %ld.%02ld GB)",
			cGiB/100 , cGiB % 100,
			cGB/100, cGB % 100);
	}
	return buf;
}

char *human_size_brief(long long bytes)
{
	static char buf[30];

	if (bytes < 5000*1024)
		snprintf(buf, sizeof(buf), "%ld.%02ldKiB",
			(long)(bytes>>10), (long)(((bytes&1023)*100+512)/1024)
			);
	else if (bytes < 2*1024LL*1024LL*1024LL)
		snprintf(buf, sizeof(buf), "%ld.%02ldMiB",
			(long)(bytes>>20),
			(long)((bytes&0xfffff)+0x100000/200)/(0x100000/100)
			);
	else
		snprintf(buf, sizeof(buf), "%ld.%02ldGiB",
			(long)(bytes>>30),
			(long)(((bytes>>10)&0xfffff)+0x100000/200)/(0x100000/100)
			);
	return buf;
}

void print_r10_layout(int layout)
{
	int near = layout & 255;
	int far = (layout >> 8) & 255;
	int offset = (layout&0x10000);
	char *sep = "";

	if (near != 1) {
		printf("%s near=%d", sep, near);
		sep = ",";
	}
	if (far != 1)
		printf("%s %s=%d", sep, offset?"offset":"far", far);
	if (near*far == 1)
		printf("NO REDUNDANCY");
}
#endif

unsigned long long calc_array_size(int level, int raid_disks, int layout,
				   int chunksize, unsigned long long devsize)
{
	if (level == 1)
		return devsize;
	devsize &= ~(unsigned long long)((chunksize>>9)-1);
	return get_data_disks(level, layout, raid_disks) * devsize;
}

int get_data_disks(int level, int layout, int raid_disks)
{
	int data_disks = 0;
	switch (level) {
	case 0: data_disks = raid_disks; break;
	case 1: data_disks = 1; break;
	case 4:
	case 5: data_disks = raid_disks - 1; break;
	case 6: data_disks = raid_disks - 2; break;
	case 10: data_disks = raid_disks / (layout & 255) / ((layout>>8)&255);
		break;
	}

	return data_disks;
}

#if !defined(MDASSEMBLE) || defined(MDASSEMBLE) && defined(MDASSEMBLE_AUTO)
char *get_md_name(int dev)
{
	/* find /dev/md%d or /dev/md/%d or make a device /dev/.tmp.md%d */
	/* if dev < 0, want /dev/md/d%d or find mdp in /proc/devices ... */
	static char devname[50];
	struct stat stb;
	dev_t rdev;
	char *dn;

	if (dev < 0) {
		int mdp =  get_mdp_major();
		if (mdp < 0) return NULL;
		rdev = makedev(mdp, (-1-dev)<<6);
		snprintf(devname, sizeof(devname), "/dev/md/d%d", -1-dev);
		if (stat(devname, &stb) == 0
		    && (S_IFMT&stb.st_mode) == S_IFBLK
		    && (stb.st_rdev == rdev))
			return devname;
	} else {
		rdev = makedev(MD_MAJOR, dev);
		snprintf(devname, sizeof(devname), "/dev/md%d", dev);
		if (stat(devname, &stb) == 0
		    && (S_IFMT&stb.st_mode) == S_IFBLK
		    && (stb.st_rdev == rdev))
			return devname;

		snprintf(devname, sizeof(devname), "/dev/md/%d", dev);
		if (stat(devname, &stb) == 0
		    && (S_IFMT&stb.st_mode) == S_IFBLK
		    && (stb.st_rdev == rdev))
			return devname;
	}
	dn = map_dev(major(rdev), minor(rdev), 0);
	if (dn)
		return dn;
	snprintf(devname, sizeof(devname), "/dev/.tmp.md%d", dev);
	if (mknod(devname, S_IFBLK | 0600, rdev) == -1)
		if (errno != EEXIST)
			return NULL;

	if (stat(devname, &stb) == 0
	    && (S_IFMT&stb.st_mode) == S_IFBLK
	    && (stb.st_rdev == rdev))
		return devname;
	unlink(devname);
	return NULL;
}

void put_md_name(char *name)
{
	if (strncmp(name, "/dev/.tmp.md", 12)==0)
		unlink(name);
}

int find_free_devnum(int use_partitions)
{
	int devnum;
	for (devnum = 127; devnum != 128;
	     devnum = devnum ? devnum-1 : (1<<20)-1) {
		char *dn;
		int _devnum;

		_devnum = use_partitions ? (-1-devnum) : devnum;
		if (mddev_busy(_devnum))
			continue;
		/* make sure it is new to /dev too, at least as a
		 * non-standard */
		dn = map_dev(dev2major(_devnum), dev2minor(_devnum), 0);
		if (dn && ! is_standard(dn, NULL))
			continue;
		break;
	}
	if (devnum == 128)
		return NoMdDev;
	return use_partitions ? (-1-devnum) : devnum;
}
#endif /* !defined(MDASSEMBLE) || defined(MDASSEMBLE) && defined(MDASSEMBLE_AUTO) */

int dev_open(char *dev, int flags)
{
	/* like 'open', but if 'dev' matches %d:%d, create a temp
	 * block device and open that
	 */
	char *e;
	int fd = -1;
	char devname[32];
	int major;
	int minor;

	if (!dev) return -1;
	flags |= O_DIRECT;

	major = strtoul(dev, &e, 0);
	if (e > dev && *e == ':' && e[1] &&
	    (minor = strtoul(e+1, &e, 0)) >= 0 &&
	    *e == 0) {
		char *path = map_dev(major, minor, 0);
		if (path)
			fd = open(path, flags);
		if (fd < 0) {
			snprintf(devname, sizeof(devname), "/dev/.tmp.md.%d:%d:%d",
				 (int)getpid(), major, minor);
			if (mknod(devname, S_IFBLK|0600, makedev(major, minor))==0) {
				fd = open(devname, flags);
				unlink(devname);
			}
		}
		if (fd < 0) {
			snprintf(devname, sizeof(devname), "/tmp/.tmp.md.%d:%d:%d",
				 (int)getpid(), major, minor);
			if (mknod(devname, S_IFBLK|0600, makedev(major, minor))==0) {
				fd = open(devname, flags);
				unlink(devname);
			}
		}
	} else
		fd = open(dev, flags);
	return fd;
}

int open_dev_flags(int devnum, int flags)
{
	char buf[20];

	sprintf(buf, "%d:%d", dev2major(devnum), dev2minor(devnum));
	return dev_open(buf, flags);
}

int open_dev(int devnum)
{
	return open_dev_flags(devnum, O_RDONLY);
}

int open_dev_excl(int devnum)
{
	char buf[20];
	int i;
	int flags = O_RDWR;

	sprintf(buf, "%d:%d", dev2major(devnum), dev2minor(devnum));
	for (i=0 ; i<25 ; i++) {
		int fd = dev_open(buf, flags|O_EXCL);
		if (fd >= 0)
			return fd;
		if (errno == EACCES && flags == O_RDWR) {
			flags = O_RDONLY;
			continue;
		}
		if (errno != EBUSY)
			return fd;
		usleep(200000);
	}
	return -1;
}

int same_dev(char *one, char *two)
{
	struct stat st1, st2;
	if (stat(one, &st1) != 0)
		return 0;
	if (stat(two, &st2) != 0)
		return 0;
	if ((st1.st_mode & S_IFMT) != S_IFBLK)
		return 0;
	if ((st2.st_mode & S_IFMT) != S_IFBLK)
		return 0;
	return st1.st_rdev == st2.st_rdev;
}

void wait_for(char *dev, int fd)
{
	int i;
	struct stat stb_want;

	if (fstat(fd, &stb_want) != 0 ||
	    (stb_want.st_mode & S_IFMT) != S_IFBLK)
		return;

	for (i=0 ; i<25 ; i++) {
		struct stat stb;
		if (stat(dev, &stb) == 0 &&
		    (stb.st_mode & S_IFMT) == S_IFBLK &&
		    (stb.st_rdev == stb_want.st_rdev))
			return;
		usleep(200000);
	}
	if (i == 25)
		dprintf("%s: timeout waiting for %s\n", __func__, dev);
}

struct superswitch *superlist[] =
{
	&super0, &super1,
	&super_ddf, &super_imsm,
	&mbr, &gpt,
	NULL };

#if !defined(MDASSEMBLE) || defined(MDASSEMBLE) && defined(MDASSEMBLE_AUTO)

struct supertype *super_by_fd(int fd, char **subarrayp)
{
	mdu_array_info_t array;
	int vers;
	int minor;
	struct supertype *st = NULL;
	struct mdinfo *sra;
	char *verstr;
	char version[20];
	int i;
	char *subarray = NULL;
	int container = NoMdDev;

	sra = sysfs_read(fd, 0, GET_VERSION);

	if (sra) {
		vers = sra->array.major_version;
		minor = sra->array.minor_version;
		verstr = sra->text_version;
	} else {
		if (ioctl(fd, GET_ARRAY_INFO, &array))
			array.major_version = array.minor_version = 0;
		vers = array.major_version;
		minor = array.minor_version;
		verstr = "";
	}

	if (vers != -1) {
		sprintf(version, "%d.%d", vers, minor);
		verstr = version;
	}
	if (minor == -2 && is_subarray(verstr)) {
		char *dev = verstr+1;

		subarray = strchr(dev, '/');
		if (subarray) {
			*subarray++ = '\0';
			subarray = strdup(subarray);
		}
		container = devname2devnum(dev);
		if (sra)
			sysfs_free(sra);
		sra = sysfs_read(-1, container, GET_VERSION);
		if (sra && sra->text_version[0])
			verstr = sra->text_version;
		else
			verstr = "-no-metadata-";
	}

	for (i = 0; st == NULL && superlist[i] ; i++)
		st = superlist[i]->match_metadata_desc(verstr);

	if (sra)
		sysfs_free(sra);
	if (st) {
		st->sb = NULL;
		if (subarrayp)
			*subarrayp = subarray;
		st->container_dev = container;
		st->devnum = fd2devnum(fd);
	} else
		free(subarray);

	return st;
}
#endif /* !defined(MDASSEMBLE) || defined(MDASSEMBLE) && defined(MDASSEMBLE_AUTO) */

int dev_size_from_id(dev_t id, unsigned long long *size)
{
	char buf[20];
	int fd;

	sprintf(buf, "%d:%d", major(id), minor(id));
	fd = dev_open(buf, O_RDONLY);
	if (fd < 0)
		return 0;
	if (get_dev_size(fd, NULL, size)) {
		close(fd);
		return 1;
	}
	close(fd);
	return 0;
}

struct supertype *dup_super(struct supertype *orig)
{
	struct supertype *st;

	if (!orig)
		return orig;
	st = malloc(sizeof(*st));
	if (!st)
		return st;
	memset(st, 0, sizeof(*st));
	st->ss = orig->ss;
	st->max_devs = orig->max_devs;
	st->minor_version = orig->minor_version;
	st->sb = NULL;
	st->info = NULL;
	return st;
}

struct supertype *guess_super_type(int fd, enum guess_types guess_type)
{
	/* try each load_super to find the best match,
	 * and return the best superswitch
	 */
	struct superswitch  *ss;
	struct supertype *st;
	time_t besttime = 0;
	int bestsuper = -1;
	int i;

	st = malloc(sizeof(*st));
	memset(st, 0, sizeof(*st));
	st->container_dev = NoMdDev;

	for (i=0 ; superlist[i]; i++) {
		int rv;
		ss = superlist[i];
		if (guess_type == guess_array && ss->add_to_super == NULL)
			continue;
		if (guess_type == guess_partitions && ss->add_to_super != NULL)
			continue;
		memset(st, 0, sizeof(*st));
		st->ignore_hw_compat = 1;
		rv = ss->load_super(st, fd, NULL);
		if (rv == 0) {
			struct mdinfo info;
			st->ss->getinfo_super(st, &info, NULL);
			if (bestsuper == -1 ||
			    besttime < info.array.ctime) {
				bestsuper = i;
				besttime = info.array.ctime;
			}
			ss->free_super(st);
		}
	}
	if (bestsuper != -1) {
		int rv;
		memset(st, 0, sizeof(*st));
		st->ignore_hw_compat = 1;
		rv = superlist[bestsuper]->load_super(st, fd, NULL);
		if (rv == 0) {
			superlist[bestsuper]->free_super(st);
			st->ignore_hw_compat = 0;
			return st;
		}
	}
	free(st);
	return NULL;
}

/* Return size of device in bytes */
int get_dev_size(int fd, char *dname, unsigned long long *sizep)
{
	unsigned long long ldsize;
	struct stat st;

	if (fstat(fd, &st) != -1 && S_ISREG(st.st_mode))
		ldsize = (unsigned long long)st.st_size;
	else
#ifdef BLKGETSIZE64
	if (ioctl(fd, BLKGETSIZE64, &ldsize) != 0)
#endif
	{
		unsigned long dsize;
		if (ioctl(fd, BLKGETSIZE, &dsize) == 0) {
			ldsize = dsize;
			ldsize <<= 9;
		} else {
			if (dname)
				fprintf(stderr, Name ": Cannot get size of %s: %s\b",
					dname, strerror(errno));
			return 0;
		}
	}
	*sizep = ldsize;
	return 1;
}

/* Return true if this can only be a container, not a member device.
 * i.e. is and md device and size is zero
 */
int must_be_container(int fd)
{
	unsigned long long size;
	if (md_get_version(fd) < 0)
		return 0;
	if (get_dev_size(fd, NULL, &size) == 0)
		return 1;
	if (size == 0)
		return 1;
	return 0;
}

/* Sets endofpart parameter to the last block used by the last GPT partition on the device.
 * Returns: 1 if successful
 *         -1 for unknown partition type
 *          0 for other errors
 */
static int get_gpt_last_partition_end(int fd, unsigned long long *endofpart)
{
	struct GPT gpt;
	unsigned char empty_gpt_entry[16]= {0};
	struct GPT_part_entry *part;
	char buf[512];
	unsigned long long curr_part_end;
	unsigned all_partitions, entry_size;
	unsigned part_nr;

	*endofpart = 0;

	BUILD_BUG_ON(sizeof(gpt) != 512);
	/* skip protective MBR */
	lseek(fd, 512, SEEK_SET);
	/* read GPT header */
	if (read(fd, &gpt, 512) != 512)
		return 0;

	/* get the number of partition entries and the entry size */
	all_partitions = __le32_to_cpu(gpt.part_cnt);
	entry_size = __le32_to_cpu(gpt.part_size);

	/* Check GPT signature*/
	if (gpt.magic != GPT_SIGNATURE_MAGIC)
		return -1;

	/* sanity checks */
	if (all_partitions > 1024 ||
	    entry_size > sizeof(buf))
		return -1;

	part = (struct GPT_part_entry *)buf;

	for (part_nr=0; part_nr < all_partitions; part_nr++) {
		/* read partition entry */
		if (read(fd, buf, entry_size) != (ssize_t)entry_size)
			return 0;

		/* is this valid partition? */
		if (memcmp(part->type_guid, empty_gpt_entry, 16) != 0) {
			/* check the last lba for the current partition */
			curr_part_end = __le64_to_cpu(part->ending_lba);
			if (curr_part_end > *endofpart)
				*endofpart = curr_part_end;
		}

	}
	return 1;
}

/* Sets endofpart parameter to the last block used by the last partition on the device.
 * Returns: 1 if successful
 *         -1 for unknown partition type
 *          0 for other errors
 */
static int get_last_partition_end(int fd, unsigned long long *endofpart)
{
	struct MBR boot_sect;
	struct MBR_part_record *part;
	unsigned long long curr_part_end;
	unsigned part_nr;
	int retval = 0;

	*endofpart = 0;

	BUILD_BUG_ON(sizeof(boot_sect) != 512);
	/* read MBR */
	lseek(fd, 0, 0);
	if (read(fd, &boot_sect, 512) != 512)
		goto abort;

	/* check MBP signature */
	if (boot_sect.magic == MBR_SIGNATURE_MAGIC) {
		retval = 1;
		/* found the correct signature */
		part = boot_sect.parts;

		for (part_nr=0; part_nr < MBR_PARTITIONS; part_nr++) {
			/* check for GPT type */
			if (part->part_type == MBR_GPT_PARTITION_TYPE) {
				retval = get_gpt_last_partition_end(fd, endofpart);
				break;
			}
			/* check the last used lba for the current partition  */
			curr_part_end = __le32_to_cpu(part->first_sect_lba) +
				__le32_to_cpu(part->blocks_num);
			if (curr_part_end > *endofpart)
				*endofpart = curr_part_end;

			part++;
		}
	} else {
		/* Unknown partition table */
		retval = -1;
	}
 abort:
	return retval;
}

int check_partitions(int fd, char *dname, unsigned long long freesize,
			unsigned long long size)
{
	/*
	 * Check where the last partition ends
	 */
	unsigned long long endofpart;
	int ret;

	if ((ret = get_last_partition_end(fd, &endofpart)) > 0) {
		/* There appears to be a partition table here */
		if (freesize == 0) {
			/* partitions will not be visible in new device */
			fprintf(stderr,
				Name ": partition table exists on %s but will be lost or\n"
				"       meaningless after creating array\n",
				dname);
			return 1;
		} else if (endofpart > freesize) {
			/* last partition overlaps metadata */
			fprintf(stderr,
				Name ": metadata will over-write last partition on %s.\n",
				dname);
			return 1;
		} else if (size && endofpart > size) {
			/* partitions will be truncated in new device */
			fprintf(stderr,
				Name ": array size is too small to cover all partitions on %s.\n",
				dname);
			return 1;
		}
	}
	return 0;
}

void get_one_disk(int mdfd, mdu_array_info_t *ainf, mdu_disk_info_t *disk)
{
	int d;

	ioctl(mdfd, GET_ARRAY_INFO, ainf);
	for (d = 0 ; d < MAX_DISKS ; d++) {
		if (ioctl(mdfd, GET_DISK_INFO, disk) == 0 &&
		    (disk->major || disk->minor))
			return;
	}
}

int open_container(int fd)
{
	/* 'fd' is a block device.  Find out if it is in use
	 * by a container, and return an open fd on that container.
	 */
	char path[256];
	char *e;
	DIR *dir;
	struct dirent *de;
	int dfd, n;
	char buf[200];
	int major, minor;
	struct stat st;

	if (fstat(fd, &st) != 0)
		return -1;
	sprintf(path, "/sys/dev/block/%d:%d/holders",
		(int)major(st.st_rdev), (int)minor(st.st_rdev));
	e = path + strlen(path);

	dir = opendir(path);
	if (!dir)
		return -1;
	while ((de = readdir(dir))) {
		if (de->d_ino == 0)
			continue;
		if (de->d_name[0] == '.')
			continue;
		sprintf(e, "/%s/dev", de->d_name);
		dfd = open(path, O_RDONLY);
		if (dfd < 0)
			continue;
		n = read(dfd, buf, sizeof(buf));
		close(dfd);
		if (n <= 0 || (unsigned)n >= sizeof(buf))
			continue;
		buf[n] = 0;
		if (sscanf(buf, "%d:%d", &major, &minor) != 2)
			continue;
		sprintf(buf, "%d:%d", major, minor);
		dfd = dev_open(buf, O_RDONLY);
		if (dfd >= 0) {
			closedir(dir);
			return dfd;
		}
	}
	closedir(dir);
	return -1;
}

struct superswitch *version_to_superswitch(char *vers)
{
	int i;

	for (i = 0; superlist[i]; i++) {
		struct superswitch *ss = superlist[i];

		if (strcmp(vers, ss->name) == 0)
			return ss;
	}

	return NULL;
}

int is_container_member(struct mdstat_ent *mdstat, char *container)
{
	if (mdstat->metadata_version == NULL ||
	    strncmp(mdstat->metadata_version, "external:", 9) != 0 ||
	    !is_subarray(mdstat->metadata_version+9) ||
	    strncmp(mdstat->metadata_version+10, container, strlen(container)) != 0 ||
	    mdstat->metadata_version[10+strlen(container)] != '/')
		return 0;

	return 1;
}

int is_subarray_active(char *subarray, char *container)
{
	struct mdstat_ent *mdstat = mdstat_read(0, 0);
	struct mdstat_ent *ent;

	for (ent = mdstat; ent; ent = ent->next)
		if (is_container_member(ent, container))
			if (strcmp(to_subarray(ent, container), subarray) == 0)
				break;

	free_mdstat(mdstat);

	return ent != NULL;
}

/* open_subarray - opens a subarray in a container
 * @dev: container device name
 * @st: empty supertype
 * @quiet: block reporting errors flag
 *
 * On success returns an fd to a container and fills in *st
 */
int open_subarray(char *dev, char *subarray, struct supertype *st, int quiet)
{
	struct mdinfo *mdi;
	struct mdinfo *info;
	int fd, err = 1;

	fd = open(dev, O_RDWR|O_EXCL);
	if (fd < 0) {
		if (!quiet)
			fprintf(stderr, Name ": Couldn't open %s, aborting\n",
				dev);
		return -1;
	}

	st->devnum = fd2devnum(fd);
	if (st->devnum == NoMdDev) {
		if (!quiet)
			fprintf(stderr,
				Name ": Failed to determine device number for %s\n",
				dev);
		goto close_fd;
	}

	mdi = sysfs_read(fd, st->devnum, GET_VERSION|GET_LEVEL);
	if (!mdi) {
		if (!quiet)
			fprintf(stderr, Name ": Failed to read sysfs for %s\n",
				dev);
		goto close_fd;
	}

	if (mdi->array.level != UnSet) {
		if (!quiet)
			fprintf(stderr, Name ": %s is not a container\n", dev);
		goto free_sysfs;
	}

	st->ss = version_to_superswitch(mdi->text_version);
	if (!st->ss) {
		if (!quiet)
			fprintf(stderr,
				Name ": Operation not supported for %s metadata\n",
				mdi->text_version);
		goto free_sysfs;
	}

	st->devname = devnum2devname(st->devnum);
	if (!st->devname) {
		if (!quiet)
			fprintf(stderr, Name ": Failed to allocate device name\n");
		goto free_sysfs;
	}

	if (!st->ss->load_container) {
		if (!quiet)
			fprintf(stderr, Name ": %s is not a container\n", dev);
		goto free_name;
	}

	if (st->ss->load_container(st, fd, NULL)) {
		if (!quiet)
			fprintf(stderr, Name ": Failed to load metadata for %s\n",
				dev);
		goto free_name;
	}

	info = st->ss->container_content(st, subarray);
	if (!info) {
		if (!quiet)
			fprintf(stderr, Name ": Failed to find subarray-%s in %s\n",
				subarray, dev);
		goto free_super;
	}
	free(info);

	err = 0;

 free_super:
	if (err)
		st->ss->free_super(st);
 free_name:
	if (err)
		free(st->devname);
 free_sysfs:
	sysfs_free(mdi);
 close_fd:
	if (err)
		close(fd);

	if (err)
		return -1;
	else
		return fd;
}

int add_disk(int mdfd, struct supertype *st,
	     struct mdinfo *sra, struct mdinfo *info)
{
	/* Add a device to an array, in one of 2 ways. */
	int rv;
#ifndef MDASSEMBLE
	if (st->ss->external) {
		if (info->disk.state & (1<<MD_DISK_SYNC))
			info->recovery_start = MaxSector;
		else
			info->recovery_start = 0;
		rv = sysfs_add_disk(sra, info, 0);
		if (! rv) {
			struct mdinfo *sd2;
			for (sd2 = sra->devs; sd2; sd2=sd2->next)
				if (sd2 == info)
					break;
			if (sd2 == NULL) {
				sd2 = malloc(sizeof(*sd2));
				*sd2 = *info;
				sd2->next = sra->devs;
				sra->devs = sd2;
			}
		}
	} else
#endif
		rv = ioctl(mdfd, ADD_NEW_DISK, &info->disk);
	return rv;
}

int remove_disk(int mdfd, struct supertype *st,
		struct mdinfo *sra, struct mdinfo *info)
{
	int rv;
	/* Remove the disk given by 'info' from the array */
#ifndef MDASSEMBLE
	if (st->ss->external)
		rv = sysfs_set_str(sra, info, "slot", "none");
	else
#endif
		rv = ioctl(mdfd, HOT_REMOVE_DISK, makedev(info->disk.major,
							  info->disk.minor));
	return rv;
}

int set_array_info(int mdfd, struct supertype *st, struct mdinfo *info)
{
	/* Initialise kernel's knowledge of array.
	 * This varies between externally managed arrays
	 * and older kernels
	 */
	int vers = md_get_version(mdfd);
	int rv;

#ifndef MDASSEMBLE
	if (st->ss->external)
		rv = sysfs_set_array(info, vers);
	else
#endif
		if ((vers % 100) >= 1) { /* can use different versions */
		mdu_array_info_t inf;
		memset(&inf, 0, sizeof(inf));
		inf.major_version = info->array.major_version;
		inf.minor_version = info->array.minor_version;
		rv = ioctl(mdfd, SET_ARRAY_INFO, &inf);
	} else
		rv = ioctl(mdfd, SET_ARRAY_INFO, NULL);
	return rv;
}

unsigned long long min_recovery_start(struct mdinfo *array)
{
	/* find the minimum recovery_start in an array for metadata
	 * formats that only record per-array recovery progress instead
	 * of per-device
	 */
	unsigned long long recovery_start = MaxSector;
	struct mdinfo *d;

	for (d = array->devs; d; d = d->next)
		recovery_start = min(recovery_start, d->recovery_start);

	return recovery_start;
}

int mdmon_pid(int devnum)
{
	char path[100];
	char pid[10];
	int fd;
	int n;
	char *devname = devnum2devname(devnum);

	sprintf(path, "%s/%s.pid", MDMON_DIR, devname);
	free(devname);

	fd = open(path, O_RDONLY | O_NOATIME, 0);

	if (fd < 0)
		return -1;
	n = read(fd, pid, 9);
	close(fd);
	if (n <= 0)
		return -1;
	return atoi(pid);
}

int mdmon_running(int devnum)
{
	int pid = mdmon_pid(devnum);
	if (pid <= 0)
		return 0;
	if (kill(pid, 0) == 0)
		return 1;
	return 0;
}

int start_mdmon(int devnum)
{
	int i, skipped;
	int len;
	pid_t pid;	
	int status;
	char pathbuf[1024];
	char *paths[4] = {
		pathbuf,
		"/sbin/mdmon",
		"mdmon",
		NULL
	};

	if (check_env("MDADM_NO_MDMON"))
		return 0;

	len = readlink("/proc/self/exe", pathbuf, sizeof(pathbuf)-1);
	if (len > 0) {
		char *sl;
		pathbuf[len] = 0;
		sl = strrchr(pathbuf, '/');
		if (sl)
			sl++;
		else
			sl = pathbuf;
		strcpy(sl, "mdmon");
	} else
		pathbuf[0] = '\0';

	switch(fork()) {
	case 0:
		/* FIXME yuk. CLOSE_EXEC?? */
		skipped = 0;
		for (i=3; skipped < 20; i++)
			if (close(i) < 0)
				skipped++;
			else
				skipped = 0;

		for (i=0; paths[i]; i++)
			if (paths[i][0]) {
				if (__offroot) {
					execl(paths[i], "mdmon", "--offroot",
					      devnum2devname(devnum),
					      NULL);
				} else {
					execl(paths[i], "mdmon",
					      devnum2devname(devnum),
					      NULL);
				}
			}
		exit(1);
	case -1: fprintf(stderr, Name ": cannot run mdmon. "
			 "Array remains readonly\n");
		return -1;
	default: /* parent - good */
		pid = wait(&status);
		if (pid < 0 || status != 0)
			return -1;
	}
	return 0;
}

int check_env(char *name)
{
	char *val = getenv(name);

	if (val && atoi(val) == 1)
		return 1;

	return 0;
}

__u32 random32(void)
{
	__u32 rv;
	int rfd = open("/dev/urandom", O_RDONLY);
	if (rfd < 0 || read(rfd, &rv, 4) != 4)
		rv = random();
	if (rfd >= 0)
		close(rfd);
	return rv;
}

#ifndef MDASSEMBLE
int flush_metadata_updates(struct supertype *st)
{
	int sfd;
	if (!st->updates) {
		st->update_tail = NULL;
		return -1;
	}

	sfd = connect_monitor(devnum2devname(st->container_dev));
	if (sfd < 0)
		return -1;

	while (st->updates) {
		struct metadata_update *mu = st->updates;
		st->updates = mu->next;

		send_message(sfd, mu, 0);
		wait_reply(sfd, 0);
		free(mu->buf);
		free(mu);
	}
	ack(sfd, 0);
	wait_reply(sfd, 0);
	close(sfd);
	st->update_tail = NULL;
	return 0;
}

void append_metadata_update(struct supertype *st, void *buf, int len)
{

	struct metadata_update *mu = malloc(sizeof(*mu));

	mu->buf = buf;
	mu->len = len;
	mu->space = NULL;
	mu->space_list = NULL;
	mu->next = NULL;
	*st->update_tail = mu;
	st->update_tail = &mu->next;
}
#endif /* MDASSEMBLE */

#ifdef __TINYC__
/* tinyc doesn't optimize this check in ioctl.h out ... */
unsigned int __invalid_size_argument_for_IOC = 0;
#endif

int experimental(void)
{
	if (check_env("MDADM_EXPERIMENTAL"))
		return 1;
	else {
		fprintf(stderr, Name ": To use this feature MDADM_EXPERIMENTAL"
				" environment variable has to be defined.\n");
		return 0;
	}
}

/* Pick all spares matching given criteria from a container
 * if min_size == 0 do not check size
 * if domlist == NULL do not check domains
 * if spare_group given add it to domains of each spare
 * metadata allows to test domains using metadata of destination array */
struct mdinfo *container_choose_spares(struct supertype *st,
				       unsigned long long min_size,
				       struct domainlist *domlist,
				       char *spare_group,
				       const char *metadata, int get_one)
{
	struct mdinfo *d, **dp, *disks = NULL;

	/* get list of all disks in container */
	if (st->ss->getinfo_super_disks)
		disks = st->ss->getinfo_super_disks(st);

	if (!disks)
		return disks;
	/* find spare devices on the list */
	dp = &disks->devs;
	disks->array.spare_disks = 0;
	while (*dp) {
		int found = 0;
		d = *dp;
		if (d->disk.state == 0) {
			/* check if size is acceptable */
			unsigned long long dev_size;
			dev_t dev = makedev(d->disk.major,d->disk.minor);

			if (!min_size ||
			   (dev_size_from_id(dev,  &dev_size) &&
			    dev_size >= min_size))
				found = 1;
			/* check if domain matches */
			if (found && domlist) {
				struct dev_policy *pol = devnum_policy(dev);
				if (spare_group)
					pol_add(&pol, pol_domain,
						spare_group, NULL);
				if (domain_test(domlist, pol, metadata) != 1)
					found = 0;
				dev_policy_free(pol);
			}
		}
		if (found) {
			dp = &d->next;
			disks->array.spare_disks++;
			if (get_one) {
				sysfs_free(*dp);
				d->next = NULL;
			}
		} else {
			*dp = d->next;
			d->next = NULL;
			sysfs_free(d);
		}
	}
	return disks;
}
