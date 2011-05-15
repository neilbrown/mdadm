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
#include <signal.h>
#include <sys/mman.h>

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

int check_stripes(struct mdinfo *info, int *source, unsigned long long *offsets,
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
	int err = 0;
	sighandler_t sig[3];
	int rv;

	extern int tables_ready;

	if((stripe_buf == NULL) ||
	   (stripes == NULL) ||
	   (blocks == NULL) ||
	   (p == NULL) ||
	   (q == NULL) ||
	   (results == NULL)) {
		err = 1;
		goto exitCheck;
	}

	if (!tables_ready)
		make_tables();

	for ( i = 0 ; i < raid_disks ; i++)
		stripes[i] = stripe_buf + i * chunk_size;

	while (length > 0) {
		int disk;

		printf("pos --> %llu\n", start);

		if(mlockall(MCL_CURRENT | MCL_FUTURE) != 0) {
			err = 2;
			goto exitCheck;
		}
		sig[0] = signal(SIGTERM, SIG_IGN);
		sig[1] = signal(SIGINT, SIG_IGN);
		sig[2] = signal(SIGQUIT, SIG_IGN);
		rv = sysfs_set_num(info, NULL, "suspend_lo", start * chunk_size * data_disks);
		rv |= sysfs_set_num(info, NULL, "suspend_hi", (start + 1) * chunk_size * data_disks);
		for (i = 0 ; i < raid_disks ; i++) {
			lseek64(source[i], offsets[i] + start * chunk_size, 0);
			read(source[i], stripes[i], chunk_size);
		}
		rv |= sysfs_set_num(info, NULL, "suspend_lo", 0x7FFFFFFFFFFFFFFFULL);
		rv |= sysfs_set_num(info, NULL, "suspend_hi", 0);
		rv |= sysfs_set_num(info, NULL, "suspend_lo", 0);
		signal(SIGQUIT, sig[2]);
		signal(SIGINT, sig[1]);
		signal(SIGTERM, sig[0]);
		if(munlockall() != 0) {
			err = 3;
			goto exitCheck;
		}

		if(rv != 0) {
			err = rv * 256;
			goto exitCheck;
		}

		for (i = 0 ; i < data_disks ; i++) {
			int disk = geo_map(i, start, raid_disks, level, layout);
			blocks[i] = stripes[disk];
			printf("%d->%d\n", i, disk);
		}

		qsyndrome(p, q, (uint8_t**)blocks, data_disks, chunk_size);
		diskP = geo_map(-1, start, raid_disks, level, layout);
		if (memcmp(p, stripes[diskP], chunk_size) != 0) {
			printf("P(%d) wrong at %llu\n", diskP, start);
		}
		diskQ = geo_map(-2, start, raid_disks, level, layout);
		if (memcmp(q, stripes[diskQ], chunk_size) != 0) {
			printf("Q(%d) wrong at %llu\n", diskQ, start);
		}
		raid6_collect(chunk_size, p, q, stripes[diskP], stripes[diskQ], results);
		disk = raid6_stats(results, raid_disks, chunk_size);

		if(disk >= -2) {
			disk = geo_map(disk, start, raid_disks, level, layout);
		}
		if(disk >= 0) {
			printf("Error detected at %llu: possible failed disk slot: %d --> %s\n",
				start, disk, name[disk]);
		}
		if(disk == -65535) {
			printf("Error detected at %llu: disk slot unknown\n", start);
		}

		length--;
		start++;
	}

exitCheck:

	free(stripe_buf);
	free(stripes);
	free(blocks);
	free(p);
	free(q);
	free(results);

	return err;
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
	/* md_device start length */
	int *fds = NULL;
	char *buf = NULL;
	char **disk_name = NULL;
	unsigned long long *offsets = NULL;
	int raid_disks = 0;
	int active_disks;
	int chunk_size = 0;
	int layout = -1;
	int level = 6;
	unsigned long long start, length;
	int i;
	int mdfd;
	struct mdinfo *info = NULL, *comp = NULL;
	char *err = NULL;
	int exit_err = 0;
	int close_flag = 0;
	char *prg = strrchr(argv[0], '/');

	if (prg == NULL)
		prg = argv[0];
	else
		prg++;

	if (argc < 4) {
		fprintf(stderr, "Usage: %s md_device start_stripe length_stripes\n", prg);
		exit_err = 1;
		goto exitHere;
	}

	mdfd = open(argv[1], O_RDONLY);
	if(mdfd < 0) {
		perror(argv[1]);
		fprintf(stderr,"%s: cannot open %s\n", prg, argv[1]);
		exit_err = 2;
		goto exitHere;
	}

	info = sysfs_read(mdfd, -1,
			  GET_LEVEL|
			  GET_LAYOUT|
			  GET_DISKS|
			  GET_DEGRADED |
			  GET_COMPONENT|
			  GET_CHUNK|
			  GET_DEVS|
			  GET_OFFSET|
			  GET_SIZE);

	if(info == NULL) {
		fprintf(stderr, "%s: Error reading sysfs information of %s\n", prg, argv[1]);
		exit_err = 9;
		goto exitHere;
	}

	if(info->array.level != level) {
		fprintf(stderr, "%s: %s not a RAID-6\n", prg, argv[1]);
		exit_err = 3;
		goto exitHere;
	}

	if(info->array.failed_disks > 0) {
		fprintf(stderr, "%s: %s degraded array\n", prg, argv[1]);
		exit_err = 8;
		goto exitHere;
	}

	printf("layout: %d\n", info->array.layout);
	printf("disks: %d\n", info->array.raid_disks);
	printf("component size: %llu\n", info->component_size * 512);
	printf("total stripes: %llu\n", (info->component_size * 512) / info->array.chunk_size);
	printf("chunk size: %d\n", info->array.chunk_size);
	printf("\n");

	comp = info->devs;
	for(i = 0, active_disks = 0; active_disks < info->array.raid_disks; i++) {
		printf("disk: %d - offset: %llu - size: %llu - name: %s - slot: %d\n",
			i, comp->data_offset * 512, comp->component_size * 512,
			map_dev(comp->disk.major, comp->disk.minor, 0),
			comp->disk.raid_disk);
		if(comp->disk.raid_disk >= 0)
			active_disks++;
		comp = comp->next;
	}
	printf("\n");

	close(mdfd);

	raid_disks = info->array.raid_disks;
	chunk_size = info->array.chunk_size;
	layout = info->array.layout;
	start = getnum(argv[2], &err);
	length = getnum(argv[3], &err);

	if (err) {
		fprintf(stderr, "%s: Bad number: %s\n", prg, err);
		exit_err = 4;
		goto exitHere;
	}

	if(start > ((info->component_size * 512) / chunk_size)) {
		start = (info->component_size * 512) / chunk_size;
		fprintf(stderr, "%s: start beyond disks size\n", prg);
	}

	if((length == 0) ||
	   ((length + start) > ((info->component_size * 512) / chunk_size))) {
		length = (info->component_size * 512) / chunk_size - start;
	}

	disk_name = malloc(raid_disks * sizeof(*disk_name));
	fds = malloc(raid_disks * sizeof(*fds));
	offsets = malloc(raid_disks * sizeof(*offsets));
	buf = malloc(raid_disks * chunk_size);

	if((disk_name == NULL) ||
	   (fds == NULL) ||
	   (offsets == NULL) ||
	   (buf == NULL)) {
		fprintf(stderr, "%s: allocation fail\n", prg);
		exit_err = 5;
		goto exitHere;
	}

	memset(offsets, 0, raid_disks * sizeof(*offsets));
	for(i=0; i<raid_disks; i++) {
		fds[i] = -1;
	}
	close_flag = 1;

	comp = info->devs;
	for (i=0, active_disks=0; active_disks<raid_disks; i++) {
		int disk_slot = comp->disk.raid_disk;
		if(disk_slot >= 0) {
			disk_name[disk_slot] = map_dev(comp->disk.major, comp->disk.minor, 0);
			offsets[disk_slot] = comp->data_offset * 512;
			fds[disk_slot] = open(disk_name[disk_slot], O_RDWR);
			if (fds[disk_slot] < 0) {
				perror(disk_name[disk_slot]);
				fprintf(stderr,"%s: cannot open %s\n", prg, disk_name[disk_slot]);
				exit_err = 6;
				goto exitHere;
			}
			active_disks++;
		}
		comp = comp->next;
	}

	int rv = check_stripes(info, fds, offsets,
			       raid_disks, chunk_size, level, layout,
			       start, length, disk_name);
	if (rv != 0) {
		fprintf(stderr,
			"%s: check_stripes returned %d\n", prg, rv);
		exit_err = 7;
		goto exitHere;
	}

exitHere:

	if (close_flag)
		for(i = 0; i < raid_disks; i++)
			close(fds[i]);

	free(disk_name);
	free(fds);
	free(offsets);
	free(buf);

	exit(exit_err);
}
