/*
 * mdadm - manage Linux "md" devices aka RAID arrays.
 *
 * Copyright (C) 2002 Neil Brown <neilb@cse.unsw.edu.au>
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
#include	"md_u.h"

int Query(char *dev)
{
	/* Give a brief description of the device,
	 * whether it is an md device and whether it has 
	 * a superblock
	 */
	int fd = open(dev, O_RDONLY, 0);
	int vers;
	int ioctlerr;
	int superror, superrno;
	mdp_super_t super;
	mdu_array_info_t array;
	unsigned long long larray_size;
	unsigned long array_size;
	struct stat stb;
	char *mddev;
	mdu_disk_info_t disc;
	char *activity;

	if (fd < 0){
		fprintf(stderr, Name ": cannot open %s: %s\n",
			dev, strerror(errno));
		return 1;
	}

	vers = md_get_version(fd);
	if (ioctl(fd, GET_ARRAY_INFO, &array)<0)
		ioctlerr = errno;
	else ioctlerr = 0;
	superror = load_super(fd, &super);
	superrno = errno;
 
	fstat(fd, &stb);

	if (vers>=9000 && !ioctlerr) {
#ifdef BLKGETSIZE64
		if (ioctl(fd, BLKGETSIZE64, &larray_size)==0)
			;
		else
#endif
			if (ioctl(fd, BLKGETSIZE, &array_size)==0) {
				larray_size = array_size;
				larray_size <<= 9;
			} else larray_size = 0;
	}
	close(fd);

	if (vers < 0) 
		printf("%s: is not an md array\n", dev);
	else if (vers < 9000)
		printf("%s: is an md device, but kernel cannot provide details\n", dev);
	else if (ioctlerr == ENODEV)
		printf("%s: is an md device which is not active\n", dev);
	else if (ioctlerr)
		printf("%s: is an md device, but gives \"%s\" when queried\n",
		       dev, strerror(ioctlerr));
	else {
		printf("%s: %s %s %d devices, %d spare%s. Use mdadm --detail for more detail.\n",
		       dev,
		       human_size_brief(larray_size),
		       map_num(pers, array.level),
		       array.raid_disks,
		       array.spare_disks, array.spare_disks==1?"":"s");
	}
	switch(superror) {
	case 1:
		printf("%s: cannot find device size: %s\n",
		       dev, strerror(superrno));
		break;
	case 2:
		printf("%s: is too small to be an md component.\n",
		       dev);
		break;
	case 3:
		printf("%s: Cannot seek to superblock: %s\n",
		       dev, strerror(superrno));
		break;
	case 4:
		printf("%s: Cannot read md superblock.\n",
		       dev);
		break;
	case 5:
		printf("%s: No md super block found, not an md component.\n",
		       dev);
		break;
	case 6:
		printf("%s: md superblock present with wrong version: %d\n",
		       dev, super.major_version);
		break;
	default:
		/* array might be active... */
		mddev = get_md_name(super.md_minor);
		disc.number = super.this_disk.number;
		activity = "inactive";
		if (mddev && (fd = open(mddev, O_RDONLY))>=0) {
			if (md_get_version(fd) >= 9000 &&	
			    ioctl(fd, GET_ARRAY_INFO, &array)>= 0) {
				if (ioctl(fd, GET_DISK_INFO, &disc) >= 0 &&
				    MKDEV((unsigned)disc.major,(unsigned)disc.minor) == stb.st_rdev)
					activity = "active";
				else
					activity = "mismatch";
			}
			close(fd);
		}
		printf("%s: device %d in %d device %s %s md%d.  Use mdadm --examine for more detail.\n",
		       dev, 
		       super.this_disk.number, super.raid_disks,
		       activity,
		       map_num(pers, super.level),
		       super.md_minor);
		break;
	}
	return 0;
}

	
