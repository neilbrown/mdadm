/*
 * mdadm - manage Linux "md" devices aka RAID arrays.
 *
 * Copyright (C) 2004 Paul Clements, SteelEye Technology, Inc.
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
 */

#include <sys/types.h>
#include <sys/stat.h>
#include "mdadm.h"
#include <asm/byteorder.h>

#define min(a,b) (((a) < (b)) ? (a) : (b))

inline void sb_le_to_cpu(bitmap_super_t *sb)
{
	sb->magic = __le32_to_cpu(sb->magic);
	sb->version = __le32_to_cpu(sb->version);
	/* uuid gets no translation */
	sb->events = __le64_to_cpu(sb->events);
	sb->events_cleared = __le64_to_cpu(sb->events_cleared);
	sb->state = __le32_to_cpu(sb->state);
	sb->chunksize = __le32_to_cpu(sb->chunksize);
	sb->daemon_sleep = __le32_to_cpu(sb->daemon_sleep);
	sb->sync_size = __le64_to_cpu(sb->sync_size);
}

inline void sb_cpu_to_le(bitmap_super_t *sb)
{
	sb_le_to_cpu(sb); /* these are really the same thing */
}

mapping_t bitmap_states[] = {
	{ "OK", 0 },
	{ "Out of date", 2 },
	{ NULL, -1 }
};

const char *bitmap_state(int state_num)
{
	char *state = map_num(bitmap_states, state_num);
	return state ? state : "Unknown";
}

const char *human_chunksize(unsigned long bytes)
{
	static char buf[16];
	char *suffixes[] = { "B", "KB", "MB", "GB", "TB", NULL };
	int i = 0;

	while (bytes >> 10) {
		bytes >>= 10;
		i++;
	}

	snprintf(buf, sizeof(buf), "%lu %s", bytes, suffixes[i]);

	return buf;
}

typedef struct bitmap_info_s {
	bitmap_super_t sb;
	unsigned long long total_bits;
	unsigned long long dirty_bits;
} bitmap_info_t;

/* count the dirty bits in the first num_bits of byte */
inline int count_dirty_bits_byte(char byte, int num_bits)
{
	int num = 0;

	switch (num_bits) { /* fall through... */
		case 8:	if (byte & 128) num++;
		case 7:	if (byte &  64) num++;
		case 6:	if (byte &  32) num++;
		case 5:	if (byte &  16) num++;
		case 4:	if (byte &   8) num++;
		case 3: if (byte &   4) num++;
		case 2:	if (byte &   2) num++;
		case 1:	if (byte &   1) num++;
		default: break;
	}

	return num;
}

int count_dirty_bits(char *buf, int num_bits)
{
	int i, num = 0;

	for (i=0; i < num_bits / 8; i++)
		num += count_dirty_bits_byte(buf[i], 8);

	if (num_bits % 8) /* not an even byte boundary */
		num += count_dirty_bits_byte(buf[i], num_bits % 8);

	return num;
}

/* calculate the size of the bitmap given the array size and bitmap chunksize */
unsigned long long bitmap_bits(unsigned long long array_size,
				unsigned long chunksize)
{
	return (array_size * 512 + chunksize - 1) / chunksize;
}

bitmap_info_t *bitmap_fd_read(int fd, int brief)
{
	unsigned long long total_bits = 0, read_bits = 0, dirty_bits = 0;
	bitmap_info_t *info;
	char buf[512];
	int n;

	info = malloc(sizeof(*info));
	if (info == NULL) {
		fprintf(stderr, Name ": failed to allocate %d bytes\n",
				sizeof(*info));
		return NULL;
	}

	if (read(fd, &info->sb, sizeof(info->sb)) != sizeof(info->sb)) {
		fprintf(stderr, Name ": failed to read superblock of bitmap "
			"file: %s\n", strerror(errno));
		free(info);
		return NULL;
	}

	sb_le_to_cpu(&info->sb); /* convert superblock to CPU byte ordering */
	
	if (brief || info->sb.sync_size == 0)
		goto out;

	/* read the rest of the file counting total bits and dirty bits --
	 * we stop when either:
	 * 1) we hit EOF, in which case we assume the rest of the bits (if any)
	 *    are dirty
	 * 2) we've read the full bitmap, in which case we ignore any trailing
	 *    data in the file
	 */
	total_bits = bitmap_bits(info->sb.sync_size, info->sb.chunksize);

	while ((n = read(fd, buf, sizeof(buf))) > 0) {
		unsigned long long remaining = total_bits - read_bits;

		if (remaining > sizeof(buf) * 8) /* we want the full buffer */
			remaining = sizeof(buf) * 8;
		if (remaining > n * 8) /* the file is truncated */
			remaining = n * 8;
		dirty_bits += count_dirty_bits(buf, remaining);

		read_bits += remaining;
		if (read_bits >= total_bits) /* we've got what we want */
			break;
	}

	if (read_bits < total_bits) { /* file truncated... */
		fprintf(stderr, Name ": WARNING: bitmap file is not large "
			"enough for array size %llu!\n\n", info->sb.sync_size);
		total_bits = read_bits;
	}
out:
	info->total_bits = total_bits;
	info->dirty_bits = dirty_bits;
	return info;
}

bitmap_info_t *bitmap_file_read(char *filename, int brief, struct supertype **stp)
{
	int fd;
	bitmap_info_t *info;
	struct stat stb;
	struct supertype *st = *stp;

	fd = open(filename, O_RDONLY);
	if (fd < 0) {
		fprintf(stderr, Name ": failed to open bitmap file %s: %s\n",
				filename, strerror(errno));
		return NULL;
	}
	fstat(fd, &stb);
	if ((S_IFMT & stb.st_mode) == S_IFBLK) {
		/* block device, so we are probably after an internal bitmap */
		if (!st) st = guess_super(fd);
		if (!st) {
			/* just look at device... */
			lseek(fd, 0, 0);
		} else {	
			st->ss->locate_bitmap(st, fd);
		}
		ioctl(fd, BLKFLSBUF, 0); /* make sure we read current data */
		*stp = st;
	}

	info = bitmap_fd_read(fd, brief);
	close(fd);
	return info;
}

__u32 swapl(__u32 l)
{
	char *c = (char*)&l;
	char t= c[0];
	c[0] = c[3];
	c[3] = t;

	t = c[1];
	c[1] = c[2];
	c[2] = t;
	return l;
}
int ExamineBitmap(char *filename, int brief, struct supertype *st)
{
	/*
	 * Read the bitmap file and display its contents
	 */

	bitmap_super_t *sb;
	bitmap_info_t *info;
	int rv = 1;
	char buf[64];

	info = bitmap_file_read(filename, brief, &st);
	if (!info)
		return rv;

	sb = &info->sb;
	printf("        Filename : %s\n", filename);
	printf("           Magic : %08x\n", sb->magic);
	if (sb->magic != BITMAP_MAGIC) {
		fprintf(stderr, Name ": invalid bitmap magic 0x%x, the bitmap file appears to be corrupted\n", sb->magic);
	}
	printf("         Version : %d\n", sb->version);
	if (sb->version != BITMAP_MAJOR) {
		fprintf(stderr, Name ": unknown bitmap version %d, either the bitmap file is corrupted or you need to upgrade your tools\n", sb->version);
		goto free_info;
	}

	rv = 0;
	if (st && st->ss->swapuuid) {
	printf("            UUID : %08x.%08x.%08x.%08x\n",
					swapl(*(__u32 *)(sb->uuid+0)),
					swapl(*(__u32 *)(sb->uuid+4)),
					swapl(*(__u32 *)(sb->uuid+8)),
					swapl(*(__u32 *)(sb->uuid+12)));
	} else {
	printf("            UUID : %08x.%08x.%08x.%08x\n",
					*(__u32 *)(sb->uuid+0),
					*(__u32 *)(sb->uuid+4),
					*(__u32 *)(sb->uuid+8),
					*(__u32 *)(sb->uuid+12));
	}
	printf("          Events : %llu\n", sb->events);
	printf("  Events Cleared : %llu\n", sb->events_cleared);
	printf("           State : %s\n", bitmap_state(sb->state));
	printf("       Chunksize : %s\n", human_chunksize(sb->chunksize));
	printf("          Daemon : %ds flush period\n", sb->daemon_sleep);
	if (sb->write_behind)
		sprintf(buf, "Allow write behind, max %d", sb->write_behind);
	else
		sprintf(buf, "Normal");
	printf("      Write Mode : %s\n", buf);
	printf("       Sync Size : %llu%s\n", sb->sync_size/2,
					human_size(sb->sync_size * 512));
	if (brief)
		goto free_info;
	printf("          Bitmap : %llu bits (chunks), %llu dirty (%2.1f%%)\n",
			info->total_bits, info->dirty_bits,
			100.0 * info->dirty_bits / (info->total_bits + 1));
free_info:
	free(info);
	return rv;
}

int CreateBitmap(char *filename, int force, char uuid[16],
		unsigned long chunksize, unsigned long daemon_sleep,
		unsigned long write_behind,
		unsigned long long array_size)
{
	/*
	 * Create a bitmap file with a superblock and (optionally) a full bitmap
	 */

	FILE *fp;
	int rv = 1;
	char block[512];
	bitmap_super_t sb;
	long long bytes, filesize;

	if (!force && access(filename, F_OK) == 0) {
		fprintf(stderr, Name ": bitmap file %s already exists, use --force to overwrite\n", filename);
		return rv;
	}

	fp = fopen(filename, "w");
	if (fp == NULL) {
		fprintf(stderr, Name ": failed to open bitmap file %s: %s\n",
			filename, strerror(errno));
		return rv;
	}

	memset(&sb, 0, sizeof(sb));
	sb.magic = BITMAP_MAGIC;
	sb.version = BITMAP_MAJOR;
	if (uuid != NULL)
		memcpy(sb.uuid, uuid, 16);
	sb.chunksize = chunksize;
	sb.daemon_sleep = daemon_sleep;
	sb.write_behind = write_behind;
	sb.sync_size = array_size;

	sb_cpu_to_le(&sb); /* convert to on-disk byte ordering */

	if (fwrite(&sb, sizeof(sb), 1, fp) != 1) {
		fprintf(stderr, Name ": failed to write superblock to bitmap file %s: %s\n", filename, strerror(errno));
		goto out;
	}

	/* calculate the size of the bitmap and write it to disk */
	bytes = (bitmap_bits(array_size, chunksize) + 7) / 8;
	if (!bytes) {
		rv = 0;
		goto out;
	}

	filesize = bytes + sizeof(sb);

	memset(block, 0xff, sizeof(block));

	while (bytes > 0) {
		if (fwrite(block, sizeof(block), 1, fp) != 1) {
			fprintf(stderr, Name ": failed to write bitmap file %s: %s\n", filename, strerror(errno));
			goto out;
		}
		bytes -= sizeof(block);
	}
	
	rv = 0;
	/* make the file be the right size (well, to the nearest byte) */
	ftruncate(fileno(fp), filesize);
out:
	fclose(fp);
	if (rv)
		unlink(filename); /* possibly corrupted, better get rid of it */
	return rv;
}
