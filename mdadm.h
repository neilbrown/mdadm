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
#include	<sys/time.h>
#include	<getopt.h>
#include	<fcntl.h>
#include	<stdio.h>
#include	<errno.h>
#include	<string.h>
#include	<syslog.h>
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

#define DEFAULT_BITMAP_CHUNK 4096
#define DEFAULT_BITMAP_DELAY 5
#define DEFAULT_MAX_WRITE_BEHIND 256

#include	"md_u.h"
#include	"md_p.h"
#include	"bitmap.h"

#include <endian.h>
/* #include "asm/byteorder.h" Redhat don't like this so... */
#if __BYTE_ORDER == __LITTLE_ENDIAN
#  include <linux/byteorder/little_endian.h>
#elif __BYTE_ORDER == __BIG_ENDIAN
#  include <linux/byteorder/big_endian.h>
#elif __BYTE_ORDER == __PDP_ENDIAN
#  include <linux/byteorder/pdp_endian.h>
#else
#  error "unknown endianness."
#endif



/* general information that might be extracted from a superblock */
struct mdinfo {
	mdu_array_info_t	array;
	mdu_disk_info_t		disk;
	__u64			events;
	int			uuid[4];
	char			name[33];
	unsigned long long	data_offset;
	unsigned long long	component_size;
	int			reshape_active;
	unsigned long long	reshape_progress;
	int			new_level, delta_disks, new_layout, new_chunk;
};

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
extern char short_bitmap_auto_options[];
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
	int	uuid[4];
	char	name[33];

	unsigned int super_minor;

	char	*devices;	/* comma separated list of device
				 * names with wild cards
				 */
	int	level;
	unsigned int raid_disks;
	unsigned int spare_disks;
	struct supertype *st;
	int	autof;		/* 1 for normal, 2 for partitioned */
	char	*spare_group;
	int	bitmap_fd;

	struct mddev_ident_s *next;
} *mddev_ident_t;

/* List of device names - wildcards expanded */
typedef struct mddev_dev_s {
	char *devname;
	char disposition;	/* 'a' for add, 'r' for remove, 'f' for fail.
				 * Not set for names read from .config
				 */
	char writemostly;
	char re_add;
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

extern struct mdstat_ent *mdstat_read(int hold, int start);
extern void free_mdstat(struct mdstat_ent *ms);
extern void mdstat_wait(int seconds);

/* Data structure for holding info read from sysfs */
struct sysdev {
	char	name[20];
	int	role;
	int	major, minor;
	unsigned long long offset, size;
	int	state;
	int	errors;
	struct sysdev *next;
};
struct sysarray {
	char	name[20];
	struct sysdev *devs;
	int	chunk;
	unsigned long long component_size;
	int	layout;
	int	level;
	int	spares;
};
/* various details can be requested */
#define	GET_LEVEL	1
#define	GET_LAYOUT	2
#define	GET_COMPONENT	4
#define	GET_CHUNK	8

#define	GET_DEVS	1024 /* gets role, major, minor */
#define	GET_OFFSET	2048
#define	GET_SIZE	4096
#define	GET_STATE	8192
#define	GET_ERROR	16384

/* If fd >= 0, get the array it is open on,
 * else use devnum. >=0 -> major9. <0.....
 */
extern struct sysarray *sysfs_read(int fd, int devnum, unsigned long options);
extern int sysfs_set_str(struct sysarray *sra, struct sysdev *dev,
			 char *name, char *val);
extern int sysfs_set_num(struct sysarray *sra, struct sysdev *dev,
			 char *name, unsigned long long val);
extern int sysfs_get_ll(struct sysarray *sra, struct sysdev *dev,
			char *name, unsigned long long *val);


extern int save_stripes(int *source, unsigned long long *offsets,
			int raid_disks, int chunk_size, int level, int layout,
			int nwrites, int *dest,
			unsigned long long start, unsigned long long length);
extern int restore_stripes(int *dest, unsigned long long *offsets,
			   int raid_disks, int chunk_size, int level, int layout,
			   int source, unsigned long long read_offset,
			   unsigned long long start, unsigned long long length);

#ifndef Sendmail
#define Sendmail "/usr/lib/sendmail -t"
#endif

#define SYSLOG_FACILITY LOG_DAEMON

extern char *map_num(mapping_t *map, int num);
extern int map_name(mapping_t *map, char *name);
extern mapping_t r5layout[], pers[], modes[], faultylayout[];

extern char *map_dev(int major, int minor, int create);


extern struct superswitch {
	void (*examine_super)(void *sbv);
	void (*brief_examine_super)(void *sbv);
	void (*detail_super)(void *sbv);
	void (*brief_detail_super)(void *sbv);
	void (*uuid_from_super)(int uuid[4], void *sbv);
	void (*getinfo_super)(struct mdinfo *info, void *sbv);
	int (*update_super)(struct mdinfo *info, void *sbv, char *update, char *devname, int verbose);
	__u64 (*event_super)(void *sbv);
	int (*init_super)(struct supertype *st, void **sbp, mdu_array_info_t *info, unsigned long long size, char *name);
	void (*add_to_super)(void *sbv, mdu_disk_info_t *dinfo);
	int (*store_super)(struct supertype *st, int fd, void *sbv);
	int (*write_init_super)(struct supertype *st, void *sbv, mdu_disk_info_t *dinfo, char *devname);
	int (*compare_super)(void **firstp, void *secondv);
	int (*load_super)(struct supertype *st, int fd, void **sbp, char *devname);
	struct supertype * (*match_metadata_desc)(char *arg);
	__u64 (*avail_size)(struct supertype *st, __u64 size);
	int (*add_internal_bitmap)(struct supertype *st, void *sbv, int chunk, int delay, int write_behind,
				   unsigned long long size, int may_change, int major);
	void (*locate_bitmap)(struct supertype *st, int fd, void *sbv);
	int (*write_bitmap)(struct supertype *st, int fd, void *sbv);
	int major;
	int swapuuid; /* true if uuid is bigending rather than hostendian */
} super0, super1, *superlist[];

struct supertype {
	struct superswitch *ss;
	int minor_version;
	int max_devs;
};

extern struct supertype *super_by_version(int vers, int minor);
extern struct supertype *guess_super(int fd);

#if __GNUC__ < 3
struct stat64;
#endif

#ifdef UCLIBC
  struct FTW {};
# define FTW_PHYS 1
#else
# define  __USE_XOPEN_EXTENDED
# include <ftw.h>
# ifdef __dietlibc__
#  define FTW_PHYS 1
# endif
#endif

extern int add_dev(const char *name, const struct stat *stb, int flag, struct FTW *s);


extern int Manage_ro(char *devname, int fd, int readonly);
extern int Manage_runstop(char *devname, int fd, int runstop, int quiet);
extern int Manage_resize(char *devname, int fd, long long size, int raid_disks);
extern int Manage_reconfig(char *devname, int fd, int layout);
extern int Manage_subdevs(char *devname, int fd,
			  mddev_dev_t devlist, int verbose);
extern int Grow_Add_device(char *devname, int fd, char *newdev);
extern int Grow_addbitmap(char *devname, int fd, char *file, int chunk, int delay, int write_behind, int force);
extern int Grow_reshape(char *devname, int fd, int quiet, char *backup_file,
			long long size,
			int level, int layout, int chunksize, int raid_disks);
extern int Grow_restart(struct supertype *st, struct mdinfo *info,
			int *fdlist, int cnt, char *backup_file);


extern int Assemble(struct supertype *st, char *mddev, int mdfd,
		    mddev_ident_t ident,
		    char *conffile,
		    mddev_dev_t devlist, char *backup_file,
		    int readonly, int runstop,
		    char *update,
		    int verbose, int force);

extern int Build(char *mddev, int mdfd, int chunk, int level, int layout,
		 int raiddisks,
		 mddev_dev_t devlist, int assume_clean,
		 char *bitmap_file, int bitmap_chunk, int write_behind, int delay, int verbose);


extern int Create(struct supertype *st, char *mddev, int mdfd,
		  int chunk, int level, int layout, unsigned long long size, int raiddisks, int sparedisks,
		  char *name,
		  int subdevs, mddev_dev_t devlist,
		  int runstop, int verbose, int force, int assume_clean,
		  char *bitmap_file, int bitmap_chunk, int write_behind, int delay);

extern int Detail(char *dev, int brief, int test);
extern int Query(char *dev);
extern int Examine(mddev_dev_t devlist, int brief, int scan, int SparcAdjust,
		   struct supertype *forcest);
extern int Monitor(mddev_dev_t devlist,
		   char *mailaddr, char *alert_cmd,
		   int period, int daemonise, int scan, int oneshot,
		   int dosyslog, char *config, int test, char *pidfile);

extern int Kill(char *dev, int force);

extern int CreateBitmap(char *filename, int force, char uuid[16],
			unsigned long chunksize, unsigned long daemon_sleep,
			unsigned long write_behind,
			unsigned long long array_size,
			int major);
extern int ExamineBitmap(char *filename, int brief, struct supertype *st);

extern int md_get_version(int fd);
extern int get_linux_version(void);
extern int parse_uuid(char *str, int uuid[4]);
extern int check_ext2(int fd, char *name);
extern int check_reiser(int fd, char *name);
extern int check_raid(int fd, char *name);

extern int get_mdp_major(void);
extern int dev_open(char *dev, int flags);
extern int is_standard(char *dev, int *nump);


extern mddev_ident_t conf_get_ident(char *conffile, char *dev);
extern mddev_dev_t conf_get_devs(char *conffile);
extern char *conf_get_mailaddr(char *conffile);
extern char *conf_get_program(char *conffile);
extern char *conf_line(FILE *file);
extern char *conf_word(FILE *file, int allow_key);
extern void free_line(char *line);
extern int match_oneof(char *devices, char *devname);
extern void uuid_from_super(int uuid[4], mdp_super_t *super);
extern int same_uuid(int a[4], int b[4], int swapuuid);
/* extern int compare_super(mdp_super_t *first, mdp_super_t *second);*/
extern unsigned long calc_csum(void *super, int bytes);
extern int enough(int level, int raid_disks, int layout,
		   char *avail, int avail_disks);
extern int ask(char *mesg);
extern unsigned long long get_component_size(int fd);


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


#ifdef __TINYC__
#undef minor
#undef major
#undef makedev
#define minor(x) ((x)&0xff)
#define major(x) (((x)>>8)&0xff)
#define makedev(M,m) (((M)<<8) | (m))
#endif

/* for raid5 */
#define ALGORITHM_LEFT_ASYMMETRIC	0
#define ALGORITHM_RIGHT_ASYMMETRIC	1
#define ALGORITHM_LEFT_SYMMETRIC	2
#define ALGORITHM_RIGHT_SYMMETRIC	3
