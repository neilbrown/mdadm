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

#define	__USE_LARGEFILE64
#include	<unistd.h>
extern __off64_t lseek64 __P ((int __fd, __off64_t __offset, int __whence));

#include	<sys/types.h>
#include	<sys/stat.h>
#include	<stdlib.h>
#include	<time.h>
#include	<getopt.h>
#include	<fcntl.h>
#include	<stdio.h>
#include	<errno.h>
#include	<string.h>

#include	<linux/kdev_t.h>
#include	<linux/fs.h>
#include	<sys/ioctl.h>
#define	MD_MAJOR 9


#include	"md_u.h"

#define Name "mdctl"

extern char short_options[];
extern struct option long_options[];
extern char Version[], Usage[], Help[], Help_create[], Help_build[], Help_assemble[];

/* structures read from config file */
/* List of mddevice names and uuids */
typedef struct mddev_uuid_s {
	char *devname;
	__u32 uuid[4];
	struct mddev_uuid_s *next;
} *mddev_uuid_t;

/* List of device names - wildcards expanded */
typedef struct mddev_dev_s {
	char *devname;
	struct mddev_dev_s *next;
} *mddev_dev_t;

typedef struct mapping {
	char *name;
	int num;
} mapping_t;

extern char *map_num(mapping_t *map, int num);
extern int map_name(mapping_t *map, char *name);
extern mapping_t r5layout[], pers[];



extern int Manage_ro(char *devname, int fd, int readonly);
extern int Manage_runstop(char *devname, int fd, int runstop);
extern int Manage_subdevs(char *devname, int fd,
			  int devcnt, char *devnames[], int devmodes[]);


extern int Assemble(char *mddev, int mdfd,
		    int uuid[4], int uuidset,
		    char *conffile, int scan,
		    int subdevs, char *subdev[],
		    int readonly, int runstop,
		    int verbose, int force);

extern int Build(char *mddev, int mdfd, int chunk, int level,
		 int raiddisks,
		 int subdevs, char *subdev[]);


extern int Create(char *mddev, int mdfd,
		  int chunk, int level, int layout, int size, int raiddisks, int sparedisks,
		  int subdevs, char *subdev[],
		  int runstop, int verbose);

extern int Detail(char *dev);
extern int Examine(char *dev);

extern int md_get_version(int fd);
extern int get_linux_version();
extern int parse_uuid(char *str, int uuid[4]);
extern int check_ext2(int fd, char *name);
extern int check_reiser(int fd, char *name);
extern int check_raid(int fd, char *name);

extern mddev_uuid_t conf_get_uuids(char *);
extern mddev_dev_t conf_get_devs(char *);
