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
#ifndef __dietlibc__
extern __off64_t lseek64 __P ((int __fd, __off64_t __offset, int __whence));
#endif

#include	<sys/types.h>
#include	<sys/stat.h>
#include	<stdlib.h>
#include	<time.h>
#include	<getopt.h>
#include	<fcntl.h>
#include	<stdio.h>
#include	<errno.h>
#include	<string.h>
#ifdef __dietlibc__NONO
int strncmp(const char *s1, const char *s2, size_t n) __THROW __pure__;
char *strncpy(char *dest, const char *src, size_t n) __THROW;
#include    <strings.h>
#endif


#include	<linux/kdev_t.h>
/*#include	<linux/fs.h> */
#include	<sys/mount.h>
#include	<asm/types.h>
#include	<sys/ioctl.h>
#define	MD_MAJOR 9
#define MdpMinorShift 6

#ifndef BLKGETSIZE64
#define BLKGETSIZE64 _IOR(0x12,114,size_t) /* return device size in bytes (u64 *arg) */
#endif


#include	"md_u.h"
#include	"md_p.h"

#define Name "mdadm"

enum mode {
	ASSEMBLE=1,
	BUILD,
	CREATE,
	MANAGE,
	MISC,
	MONITOR,
	GROW,
};

extern char short_options[];
extern struct option long_options[];
extern char Version[], Usage[], Help[], OptionHelp[],
	Help_create[], Help_build[], Help_assemble[], Help_grow[],
	Help_manage[], Help_misc[], Help_monitor[], Help_config[];

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
#define UnSet (0xfffe)
typedef struct mddev_ident_s {
	char	*devname;
	
	int	uuid_set;
	__u32	uuid[4];

	unsigned int super_minor;

	char	*devices;	/* comma separated list of device
				 * names with wild cards
				 */
	int	level;
	unsigned int raid_disks;
	unsigned int spare_disks;
	int	autof;		/* 1 for normal, 2 for partitioned */
	char	*spare_group;

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


struct mdstat_ent {
	char		*dev;
	int		devnum;
	int		active;
	char		*level;
	char		*pattern; /* U or up, _ for down */
	int		percent; /* -1 if no resync */
	int		resync; /* 1 if resync, 0 if recovery */
	struct mdstat_ent *next;
};

extern struct mdstat_ent *mdstat_read(int);
extern void free_mdstat(struct mdstat_ent *ms);
extern void mdstat_wait(int seconds);

#ifndef Sendmail
#define Sendmail "/usr/lib/sendmail -t"
#endif

extern char *map_num(mapping_t *map, int num);
extern int map_name(mapping_t *map, char *name);
extern mapping_t r5layout[], pers[], modes[], faultylayout[];

extern char *map_dev(int major, int minor);


extern int Manage_ro(char *devname, int fd, int readonly);
extern int Manage_runstop(char *devname, int fd, int runstop);
extern int Manage_resize(char *devname, int fd, long long size, int raid_disks);
extern int Manage_reconfig(char *devname, int fd, int layout);
extern int Manage_subdevs(char *devname, int fd,
			  mddev_dev_t devlist);
extern int Grow_Add_device(char *devname, int fd, char *newdev);


extern int Assemble(char *mddev, int mdfd,
		    mddev_ident_t ident,
		    char *conffile,
		    mddev_dev_t devlist,
		    int readonly, int runstop,
		    char *update,
		    int verbose, int force);

extern int Build(char *mddev, int mdfd, int chunk, int level, int layout,
		 int raiddisks,
		 mddev_dev_t devlist, int assume_clean);


extern int Create(char *mddev, int mdfd,
		  int chunk, int level, int layout, unsigned long size, int raiddisks, int sparedisks,
		  int subdevs, mddev_dev_t devlist,
		  int runstop, int verbose, int force);

extern int Detail(char *dev, int brief, int test);
extern int Query(char *dev);
extern int Examine(mddev_dev_t devlist, int brief, int scan, int SparcAdjust);
extern int Monitor(mddev_dev_t devlist,
		   char *mailaddr, char *alert_cmd,
		   int period, int daemonise, int scan, int oneshot,
		   char *config, int test, char *pidfile);

extern int Kill(char *dev, int force);

extern int md_get_version(int fd);
extern int get_linux_version(void);
extern int parse_uuid(char *str, int uuid[4]);
extern int check_ext2(int fd, char *name);
extern int check_reiser(int fd, char *name);
extern int check_raid(int fd, char *name);

extern int get_mdp_major(void);
extern int is_standard(char *dev);


extern mddev_ident_t conf_get_ident(char *conffile, char *dev);
extern mddev_dev_t conf_get_devs(char *conffile);
extern char *conf_get_mailaddr(char *conffile);
extern char *conf_get_program(char *conffile);
extern char *conf_line(FILE *file);
extern char *conf_word(FILE *file, int allow_key);
extern void free_line(char *line);
extern int match_oneof(char *devices, char *devname);
extern int load_super(int fd, mdp_super_t *super);
extern void uuid_from_super(int uuid[4], mdp_super_t *super);
extern int same_uuid(int a[4], int b[4]);
extern int compare_super(mdp_super_t *first, mdp_super_t *second);
extern unsigned long calc_sb_csum(mdp_super_t *super);
extern int store_super(int fd, mdp_super_t *super);
extern int enough(int level, int raid_disks, int avail_disks);
extern int ask(char *mesg);


extern char *human_size(long long bytes);
char *human_size_brief(long long bytes);

extern void put_md_name(char *name);
extern char *get_md_name(int dev);

extern char DefaultConfFile[];

extern int open_mddev(char *dev, int autof);


#define	LEVEL_MULTIPATH		(-4)
#define	LEVEL_LINEAR		(-1)
#define	LEVEL_FAULTY		(-5)


/* faulty stuff */

#define	WriteTransient	0
#define	ReadTransient	1
#define	WritePersistent	2
#define	ReadPersistent	3
#define	WriteAll	4 /* doesn't go to device */
#define	ReadFixable	5
#define	Modes	6

#define	ClearErrors	31
#define	ClearFaults	30

#define AllPersist	100 /* internal use only */
#define	NoPersist	101

#define	ModeMask	0x1f
#define	ModeShift	5
