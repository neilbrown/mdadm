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

/* I seem to need this to make BLKGETSIZE64 to work... */
#define u64 __u64


#include	"md_u.h"

#define Name "mdadm"

extern char short_options[];
extern struct option long_options[];
extern char Version[], Usage[], Help[], Help_create[], Help_build[], Help_assemble[];

/* structures read from config file */
/* List of mddevice names and identifiers
 * Identifiers can be:
 *    uuid=128-hex-uuid
 *    super-minor=decimal-minor-number-from-superblock
 *    devices=comma,separated,list,of,device,names,with,wildcards
 *
 * If multiple fields are present, the intersection of all matching
 * devices is considered
 */
typedef struct mddev_ident_s {
	char *devname;
	
	int uuid_set;
	__u32 uuid[4];

	int super_minor;	/* -1 if not set */

	char *devices;		/* comma separated list of device
				 * names with wild cards
				 */
	int level;		/* -10 if not set */
	int raid_disks;		/* -1 if not set */
	char *spare_group;
	struct mddev_ident_s *next;
} *mddev_ident_t;

/* List of device names - wildcards expanded */
typedef struct mddev_dev_s {
	char *devname;
	char disposition;	/* 'a' for add, 'r' for remove, 'f' for fail.
				 * Not set for names read from .config
				 */
	struct mddev_dev_s *next;
} *mddev_dev_t;

typedef struct mapping {
	char *name;
	int num;
} mapping_t;

#ifndef Sendmail
#define Sendmail "/usr/lib/sendmail -t"
#endif

extern char *map_num(mapping_t *map, int num);
extern int map_name(mapping_t *map, char *name);
extern mapping_t r5layout[], pers[];

extern char *map_dev(int major, int minor);


extern int Manage_ro(char *devname, int fd, int readonly);
extern int Manage_runstop(char *devname, int fd, int runstop);
extern int Manage_subdevs(char *devname, int fd,
			  mddev_dev_t devlist);


extern int Assemble(char *mddev, int mdfd,
		    mddev_ident_t ident,
		    char *conffile,
		    mddev_dev_t devlist,
		    int readonly, int runstop,
		    int verbose, int force);

extern int Build(char *mddev, int mdfd, int chunk, int level,
		 int raiddisks,
		 mddev_dev_t devlist);


extern int Create(char *mddev, int mdfd,
		  int chunk, int level, int layout, int size, int raiddisks, int sparedisks,
		  int subdevs, mddev_dev_t devlist,
		  int runstop, int verbose, int force);

extern int Detail(char *dev, int brief);
extern int Examine(mddev_dev_t devlist, int brief, char *conffile);
extern int Monitor(mddev_dev_t devlist,
		  char *mailaddr, char *alert_cmd,
		  int period,
		  char *config);

extern int Kill(char *dev, int force);

extern int md_get_version(int fd);
extern int get_linux_version();
extern int parse_uuid(char *str, int uuid[4]);
extern int check_ext2(int fd, char *name);
extern int check_reiser(int fd, char *name);
extern int check_raid(int fd, char *name);

extern mddev_ident_t conf_get_ident(char *, char*);
extern mddev_dev_t conf_get_devs(char *);

extern char *human_size(long long bytes);
