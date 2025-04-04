// SPDX-License-Identifier: GPL-2.0-only

/*
 * Copyright (C) Peter T. Breuer (ptb@ot.uc3m.es) 2003
 * Copyright (C) 2003-2004, Paul Clements, SteelEye Technology, Inc.
 * Copyright (C) 2005 Neil Brown <neilb@suse.com>
 */

/* See documentation/bitmap.md */

#ifndef BITMAP_H
#define BITMAP_H 1

#define BITMAP_MAJOR_LO 3
#define BITMAP_MAJOR_HI 4
#define	BITMAP_MAJOR_CLUSTERED 5
#define BITMAP_MAGIC 0x6d746962

/* use these for bitmap->flags and bitmap->sb->state bit-fields */
enum bitmap_state {
	BITMAP_ACTIVE = 0x001, /* the bitmap is in use */
	BITMAP_STALE  = 0x002  /* the bitmap file is out of date or had -EIO */
};

/* the superblock at the front of the bitmap file -- little endian */
typedef struct bitmap_super_s {
	__u32 magic;        /*  0  BITMAP_MAGIC */
	__u32 version;      /*  4  the bitmap major for now, could change... */
	__u8  uuid[16];     /*  8  128 bit uuid - must match md device uuid */
	__u64 events;       /* 24  event counter for the bitmap (1)*/
	__u64 events_cleared;/*32  event counter when last bit cleared (2) */
	__u64 sync_size;    /* 40  the size of the md device's sync range(3) */
	__u32 state;        /* 48  bitmap state information */
	__u32 chunksize;    /* 52  the bitmap chunk size in bytes */
	__u32 daemon_sleep; /* 56  seconds between disk flushes */
	__u32 write_behind; /* 60  number of outstanding write-behind writes */
	__u32 sectors_reserved; /* 64 number of 512-byte sectors that are
				 * reserved for the bitmap. */
	__u32 nodes;        /* 68 the maximum number of nodes in cluster. */
	__u8 cluster_name[64]; /* 72 cluster name to which this md belongs */
	__u8  pad[256 - 136]; /* set to zero */
} bitmap_super_t;

#endif
