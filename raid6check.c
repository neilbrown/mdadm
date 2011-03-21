/*
 * raid6check - extended consistency check for RAID-6
 *
 * Copyright (C) 2011 Piergiorgio Sartor
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
 *    Author: Piergiorgio Sartor
 *    Based on "restripe.c" from "mdadm" codebase
 */

#include "mdadm.h"
#include <stdint.h>

int geo_map(int block, unsigned long long stripe, int raid_disks,
	    int level, int layout);
void qsyndrome(uint8_t *p, uint8_t *q, uint8_t **sources, int disks, int size);
void make_tables(void);

/* Collect per stripe consistency information */
void raid6_collect(int chunk_size, uint8_t *p, uint8_t *q,
		   char *chunkP, char *chunkQ, int *results)
{
	int i;
	int data_id;
	uint8_t Px, Qx;
	extern uint8_t raid6_gflog[];

	for(i = 0; i < chunk_size; i++) {
		Px = (uint8_t)chunkP[i] ^ (uint8_t)p[i];
		Qx = (uint8_t)chunkQ[i] ^ (uint8_t)q[i];

		if((Px != 0) && (Qx == 0))
			results[i] = -1;

		if((Px == 0) && (Qx != 0))
			results[i] = -2;

		if((Px != 0) && (Qx != 0)) {
			data_id = (raid6_gflog[Qx] - raid6_gflog[Px]);
			if(data_id < 0) data_id += 255;
			results[i] = data_id;
		}

		if((Px == 0) && (Qx == 0))
			results[i] = -255;
	}
}

/* Try to find out if a specific disk has problems */
int raid6_stats(int *results, int raid_disks, int chunk_size)
{
	int i;
	int curr_broken_disk = -255;
	int prev_broken_disk = -255;
	int broken_status = 0;

	for(i = 0; i < chunk_size; i++) {

		if(results[i] != -255)
			curr_broken_disk = results[i];

		if(curr_broken_disk >= raid_disks)
			broken_status = 2;

		switch(broken_status) {
		case 0:
			if(curr_broken_disk != -255) {
				prev_broken_disk = curr_broken_disk;
				broken_status = 1;
			}
			break;

		case 1:
			if(curr_broken_disk != prev_broken_disk)
				broken_status = 2;
			break;

		case 2:
		default:
			curr_broken_disk = prev_broken_disk = -65535;
			break;
		}
	}

	return curr_broken_disk;
}

int check_stripes(int *source, unsigned long long *offsets,
		  int raid_disks, int chunk_size, int level, int layout,
		  unsigned long long start, unsigned long long length, char *name[])
{
	/* read the data and p and q blocks, and check we got them right */
	char *stripe_buf = malloc(raid_disks * chunk_size);
	char **stripes = malloc(raid_disks * sizeof(char*));
	char **blocks = malloc(raid_disks * sizeof(char*));
	uint8_t *p = malloc(chunk_size);
	uint8_t *q = malloc(chunk_size);
	int *results = malloc(chunk_size * sizeof(int));

	int i;
	int diskP, diskQ;
	int data_disks = raid_disks - 2;

	extern int tables_ready;

	if (!tables_ready)
		make_tables();

	for ( i = 0 ; i < raid_disks ; i++)
		stripes[i] = stripe_buf + i * chunk_size;

	while (length > 0) {
		int disk;

		for (i = 0 ; i < raid_disks ; i++) {
			lseek64(source[i], offsets[i]+start, 0);
			read(source[i], stripes[i], chunk_size);
		}
		for (i = 0 ; i < data_disks ; i++) {
			int disk = geo_map(i, start/chunk_size, raid_disks,
					   level, layout);
			blocks[i] = stripes[disk];
			printf("%d->%d\n", i, disk);
		}

		qsyndrome(p, q, (uint8_t**)blocks, data_disks, chunk_size);
		diskP = geo_map(-1, start/chunk_size, raid_disks,
				level, layout);
		if (memcmp(p, stripes[diskP], chunk_size) != 0) {
			printf("P(%d) wrong at %llu\n", diskP,
			       start / chunk_size);
		}
		diskQ = geo_map(-2, start/chunk_size, raid_disks,
				level, layout);
		if (memcmp(q, stripes[diskQ], chunk_size) != 0) {
			printf("Q(%d) wrong at %llu\n", diskQ,
			       start / chunk_size);
		}
		raid6_collect(chunk_size, p, q,
			      stripes[diskP], stripes[diskQ], results);
		disk = raid6_stats(results, raid_disks, chunk_size);

		if(disk >= -2) {
			disk = geo_map(disk, start/chunk_size, raid_disks,
				       level, layout);
		}
		if(disk >= 0) {
			printf("Possible failed disk: %d --> %s\n", disk, name[disk]);
		}
		if(disk == -65535) {
			printf("Failure detected, but disk unknown\n");
		}

		length -= chunk_size;
		start += chunk_size;
	}

	free(stripe_buf);
	free(stripes);
	free(blocks);
	free(p);
	free(q);
	free(results);

	return 0;
}

unsigned long long getnum(char *str, char **err)
{
	char *e;
	unsigned long long rv = strtoull(str, &e, 10);
	if (e==str || *e) {
		*err = str;
		return 0;
	}
	return rv;
}

int main(int argc, char *argv[])
{
	/* raid_disks chunk_size layout start length devices...
	 */
	int *fds;
	char *buf;
	unsigned long long *offsets;
	int raid_disks, chunk_size, layout;
	int level = 6;
	unsigned long long start, length;
	int i;

	char *err = NULL;
	if (argc < 8) {
		fprintf(stderr, "Usage: raid6check raid_disks"
			" chunk_size layout start length devices...\n");
		exit(1);
	}

	raid_disks = getnum(argv[1], &err);
	chunk_size = getnum(argv[2], &err);
	layout = getnum(argv[3], &err);
	start = getnum(argv[4], &err);
	length = getnum(argv[5], &err);
	if (err) {
		fprintf(stderr, "test_stripe: Bad number: %s\n", err);
		exit(2);
	}
	if (argc != raid_disks + 6) {
		fprintf(stderr, "test_stripe: wrong number of devices: want %d found %d\n",
			raid_disks, argc-6);
		exit(2);
	}
	fds = malloc(raid_disks * sizeof(*fds));
	offsets = malloc(raid_disks * sizeof(*offsets));
	memset(offsets, 0, raid_disks * sizeof(*offsets));

	for (i=0; i<raid_disks; i++) {
		char *p;
		p = strchr(argv[6+i], ':');

		if(p != NULL) {
			*p++ = '\0';
			offsets[i] = atoll(p) * 512;
		}
		fds[i] = open(argv[6+i], O_RDWR);
		if (fds[i] < 0) {
			perror(argv[6+i]);
			fprintf(stderr,"test_stripe: cannot open %s.\n", argv[6+i]);
			exit(3);
		}
	}

	buf = malloc(raid_disks * chunk_size);

	int rv = check_stripes(fds, offsets,
			       raid_disks, chunk_size, level, layout,
			       start, length, &argv[6]);
	if (rv != 0) {
		fprintf(stderr,
			"test_stripe: test_stripes returned %d\n", rv);
		exit(1);
	}

	free(fds);
	free(offsets);
	free(buf);

	exit(0);
}
