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
#include	"md_p.h"
#include	<sys/utsname.h>
#include	<ctype.h>

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
	int a,b,c;
	if (uname(&name) <0)
		return -1;

	cp = name.release;
	a = strtoul(cp, &cp, 10);
	if (*cp != '.') return -1;
	b = strtoul(cp+1, &cp, 10);
	if (*cp != '.') return -1;
	c = strtoul(cp+1, NULL, 10);

	return (a*1000000)+(b*1000)+c;
}

int enough(int level, int raid_disks, int avail_disks)
{
	switch (level) {
	case 10: return 1; /* a lie, but it is hard to tell */

	case -4:
		return avail_disks>= 1;
	case -1:
	case 0:
		return avail_disks == raid_disks;
	case 1:
		return avail_disks >= 1;
	case 4:
	case 5:
		return avail_disks >= raid_disks-1;
	case 6:
		return avail_disks >= raid_disks-2;
	default:
		return 0;
	}
}

int same_uuid(int a[4], int b[4])
{
    if (a[0]==b[0] &&
	a[1]==b[1] &&
	a[2]==b[2] &&
	a[3]==b[3])
	return 1;
    return 0;
}

void uuid_from_super(int uuid[4], mdp_super_t *super)
{
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

int compare_super(mdp_super_t *first, mdp_super_t *second)
{
    /*
     * return:
     *  0 same, or first was empty, and second was copied
     *  1 second had wrong number
     *  2 wrong uuid
     *  3 wrong other info
     */
    int uuid1[4], uuid2[4];
    if (second->md_magic != MD_SB_MAGIC)
	return 1;
    if (first-> md_magic != MD_SB_MAGIC) {
	memcpy(first, second, sizeof(*first));
	return 0;
    }

    uuid_from_super(uuid1, first);
    uuid_from_super(uuid2, second);
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

int load_super(int fd, mdp_super_t *super)
{
	/* try to read in the superblock
	 *
	 * return
	 *   0 - success
	 *   1 - no block size
	 *   2 - too small
	 *   3 - no seek
	 *   4 - no read
	 *   5 - no magic
	 *   6 - wrong major version
	 */
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
			dsize = size << 9;
	}

	if (dsize < MD_RESERVED_SECTORS*2)
		return 2;
	
	offset = MD_NEW_SIZE_SECTORS(dsize>>9);

	offset *= 512;

	ioctl(fd, BLKFLSBUF, 0); /* make sure we read current data */

	if (lseek64(fd, offset, 0)< 0LL)
		return 3;

	if (read(fd, super, sizeof(*super)) != sizeof(*super))
		return 4;

	if (super->md_magic != MD_SB_MAGIC)
		return 5;

	if (super->major_version != 0)
		return 6;
	return 0;
}

int store_super(int fd, mdp_super_t *super)
{
	unsigned long size;
	unsigned long long dsize;
	
	long long offset;
    
#ifdef BLKGETSIZE64
	if (ioctl(fd, BLKGETSIZE64, &dsize) != 0)
#endif
	{
		if (ioctl(fd, BLKGETSIZE, &size))
			return 1;
		else
			dsize = ((unsigned long long)size) << 9;
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
	int size;
	if (lseek(fd, 64*1024, 0) != 64*1024)
		return 0;
	if (read(fd, sb, 1024) != 1024)
		return 0;
	if (strncmp(sb+52, "ReIsErFs",8)!=0 &&
	    strncmp(sb+52, "ReIsEr2Fs",9)!=0)
		return 0;
	fprintf(stderr, Name ": %s appears to contain a reiserfs file system\n",name);
	size = sb[0]|(sb[1]|(sb[2]|sb[3]<<8)<<8)<<8;
	fprintf(stderr, "    size = %dK\n", size*4);
		
	return 1;
}

int check_raid(int fd, char *name)
{
	mdp_super_t super;
	time_t crtime;
	if (load_super(fd, &super))
		return 0;
	/* Looks like a raid array .. */
	fprintf(stderr, Name ": %s appears to be part of a raid array:\n",
		name);
	crtime = super.ctime;
	fprintf(stderr, "    level=%d devices=%d ctime=%s",
		super.level, super.raid_disks, ctime(&crtime));
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

char *map_num(mapping_t *map, int num)
{
	while (map->name) {
		if (map->num == num)
			return map->name;
		map++;
	}
	return NULL;
}

int map_name(mapping_t *map, char *name)
{
	while (map->name) {
		if (strcmp(map->name, name)==0)
			return map->num;
		map++;
	}
	return UnSet;
}


int is_standard(char *dev, int *nump)
{
	/* tests if dev is a "standard" md dev name.
	 * i.e if the last component is "/dNN" or "/mdNN",
	 * where NN is a string of digits 
	 */
	char *d = strrchr(dev, '/');
	int type=0;
	int num;
	if (!d)
		return 0;
	if (strncmp(d, "/d",2)==0)
		d += 2, type=1; /* /dev/md/dN{pM} */
	else if (strncmp(d, "/md_d", 5)==0)
		d += 5, type=1; /* /dev/md_dNpM */
	else if (strncmp(d, "/md", 3)==0)
		d += 3, type=-1; /* /dev/mdN */
	else if (d-dev > 3 && strncmp(d-2, "md/", 3)==0)
		type = -1; /* /dev/md/N */
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


/*
 * convert a major/minor pair for a block device into a name in /dev, if possible.
 * On the first call, walk /dev collecting name.
 * Put them in a simple linked listfor now.
 */
struct devmap {
    int major, minor;
    char *name;
    struct devmap *next;
} *devlist = NULL;
int devlist_ready = 0;

#ifdef UCLIBC
char *map_dev(int major, int minor)
{
#if 0
	fprintf(stderr, "Warning - fail to map %d,%d to a device name\n",
		major, minor);
#endif
	return NULL;
}
#else
#define  __USE_XOPEN_EXTENDED
#include <ftw.h>


#ifndef __dietlibc__
int add_dev(const char *name, const struct stat *stb, int flag, struct FTW *s)
#else
int add_dev(const char *name, const struct stat *stb, int flag)
#endif
{
    if ((stb->st_mode&S_IFMT)== S_IFBLK) {
	char *n = strdup(name);
	struct devmap *dm = malloc(sizeof(*dm));
	if (dm) {
	    dm->major = major(stb->st_rdev);
	    dm->minor = minor(stb->st_rdev);
	    dm->name = n;
	    dm->next = devlist;
	    devlist = dm;
	}
    }
    return 0;
}

/*
 * Find a block device with the right major/minor number.
 * Avoid /dev/mdNN and /dev/md/dNN if possible
 */
char *map_dev(int major, int minor)
{
	struct devmap *p;
	char *std = NULL;
	if (!devlist_ready) {
#ifndef __dietlibc__
		nftw("/dev", add_dev, 10, FTW_PHYS);
#else
		ftw("/dev", add_dev, 10);
#endif
		devlist_ready=1;
	}

	for (p=devlist; p; p=p->next)
		if (p->major == major &&
		    p->minor == minor) {
			if (is_standard(p->name, NULL))
				std = p->name;
			else
				return p->name;
		}
	return std;
}

#endif

unsigned long calc_sb_csum(mdp_super_t *super)
{
        unsigned int  oldcsum = super->sb_csum;
	unsigned long long newcsum = 0;
	unsigned long csum;
	int i;
	unsigned int *superc = (int*) super;
	super->sb_csum = 0;

	for(i=0; i<MD_SB_BYTES/4; i++)
		newcsum+= superc[i];
	csum = (newcsum& 0xffffffff) + (newcsum>>32);
	super->sb_csum = oldcsum;
	return csum;
}

char *human_size(long long bytes)
{
	static char buf[30];
	

	if (bytes < 5000*1024)
		buf[0]=0;
	else if (bytes < 2*1024LL*1024LL*1024LL)
		sprintf(buf, " (%ld.%02ld MiB %ld.%02ld MB)",
			(long)(bytes>>20),
			(long)((bytes&0xfffff)+0x100000/200)/(0x100000/100),
			(long)(bytes/1000/1000),
			(long)(((bytes%1000000)+5000)/10000)
			);
	else
		sprintf(buf, " (%ld.%02ld GiB %ld.%02ld GB)",
			(long)(bytes>>30),
			(long)(((bytes>>10)&0xfffff)+0x100000/200)/(0x100000/100),
			(long)(bytes/1000LL/1000LL/1000LL),
			(long)((((bytes/1000)%1000000)+5000)/10000)
			);
	return buf;
}

char *human_size_brief(long long bytes)
{
	static char buf[30];
	

	if (bytes < 5000*1024)
		sprintf(buf, "%ld.%02ldKiB",
			(long)(bytes>>10), (long)(((bytes&1023)*100+512)/1024)
			);
	else if (bytes < 2*1024LL*1024LL*1024LL)
		sprintf(buf, "%ld.%02ldMiB",
			(long)(bytes>>20),
			(long)((bytes&0xfffff)+0x100000/200)/(0x100000/100)
			);
	else
		sprintf(buf, "%ld.%02ldGiB",
			(long)(bytes>>30),
			(long)(((bytes>>10)&0xfffff)+0x100000/200)/(0x100000/100)
			);
	return buf;
}

int get_mdp_major(void)
{
static int mdp_major = -1;
	FILE *fl;
	char *w;
	int have_block = 0;
	int have_devices = 0;
	int last_num = -1;

	if (mdp_major != -1)
		return mdp_major;
	fl = fopen("/proc/devices", "r");
	if (!fl)
		return -1;
	while ((w = conf_word(fl, 1))) {
		if (have_block && strcmp(w, "devices:")==0)
			have_devices = 1;
		have_block =  (strcmp(w, "Block")==0);
		if (isdigit(w[0]))
			last_num = atoi(w);
		if (have_devices && strcmp(w, "mdp")==0)
			mdp_major = last_num;
		free(w);
	}
	fclose(fl);
	return mdp_major;
}



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
		sprintf(devname, "/dev/md/d%d", -1-dev);
		if (stat(devname, &stb) == 0
		    && (S_IFMT&stb.st_mode) == S_IFBLK
		    && (stb.st_rdev == rdev))
			return devname;
	} else {
		rdev = makedev(MD_MAJOR, dev);
		sprintf(devname, "/dev/md%d", dev);
		if (stat(devname, &stb) == 0
		    && (S_IFMT&stb.st_mode) == S_IFBLK
		    && (stb.st_rdev == rdev))
			return devname;

		sprintf(devname, "/dev/md/%d", dev);
		if (stat(devname, &stb) == 0
		    && (S_IFMT&stb.st_mode) == S_IFBLK
		    && (stb.st_rdev == rdev))
			return devname;
	}
	dn = map_dev(major(rdev), minor(rdev));
	if (dn)
		return dn;
	sprintf(devname, "/dev/.tmp.md%d", dev);
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
