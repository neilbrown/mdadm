/*
 * mdctl - manage Linux "md" devices aka RAID arrays.
 *
 * Copyright (C) 2001 Neil Brown <neilb@cse.unsw.edu.au>
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

#include	"mdctl.h"
#include	"md_p.h"
#include	<sys/utsname.h>

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

	uuid[hit/4] <<= 4;
	uuid[hit/4] += n;
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
	return  (vers.major<<16) | (vers.minor<<8) | vers.patchlevel;

    if (MAJOR(stb.st_rdev) == MD_MAJOR)
	return (36<<8);
    return -1;
}

    
int get_linux_version()
{
	struct utsname name;
	int a,b,c;
	if (uname(&name) <0)
		return -1;

	if (sscanf(name.release, "%d.%d.%d", &a,&b,&c)!= 3)
		return -1;
	return (a<<16)+(b<<8)+c;
}

int enough(int level, int raid_disks, int avail_disks)
{
	switch (level) {
	case -1:
	case 0:
		return avail_disks == raid_disks;
	case 1:
		return avail_disks >= 1;
	case 4:
	case 5:
		return avail_disks >= raid_disks-1;
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
	long size;
	long long offset;
    
	if (ioctl(fd, BLKGETSIZE, &size))
		return 1;

	if (size < MD_RESERVED_SECTORS*2)
		return 2;
	
	offset = MD_NEW_SIZE_SECTORS(size);

	offset *= 512;

	if (lseek64(fd, offset, 0)< 0LL)
		return 3;

	if (read(fd, &super, sizeof(super)) != sizeof(super))
		return 4;

	if (super->md_magic != MD_SB_MAGIC)
		return 5;

	if (super->major_version != 0)
		return 6;
	return 0;
}

