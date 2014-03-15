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

#define CHECK_PAGE_BITS (12)
#define CHECK_PAGE_SIZE (1 << CHECK_PAGE_BITS)

enum repair {
	NO_REPAIR = 0,
	MANUAL_REPAIR,
	AUTO_REPAIR
};

int geo_map(int block, unsigned long long stripe, int raid_disks,
	    int level, int layout);
void qsyndrome(uint8_t *p, uint8_t *q, uint8_t **sources, int disks, int size);
void make_tables(void);
void ensure_zero_has_size(int chunk_size);
void raid6_datap_recov(int disks, size_t bytes, int faila, uint8_t **ptrs);
void raid6_2data_recov(int disks, size_t bytes, int faila, int failb,
		       uint8_t **ptrs);
void xor_blocks(char *target, char **sources, int disks, int size);

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

/* Try to find out if a specific disk has problems in a CHECK_PAGE_SIZE page size */
int raid6_stats_blk(int *results, int raid_disks)
{
	int i;
	int curr_broken_disk = -255;
	int prev_broken_disk = -255;
	int broken_status = 0;

	for(i = 0; i < CHECK_PAGE_SIZE; i++) {

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

/* Collect disks status for a strip in CHECK_PAGE_SIZE page size blocks */
void raid6_stats(int *disk, int *results, int raid_disks, int chunk_size)
{
	int i, j;

	for(i = 0, j = 0; i < chunk_size; i += CHECK_PAGE_SIZE, j++) {
		disk[j] = raid6_stats_blk(&results[i], raid_disks);
	}
}

int lock_stripe(struct mdinfo *info, unsigned long long start,
		int chunk_size, int data_disks, sighandler_t *sig) {
	int rv;
	if(mlockall(MCL_CURRENT | MCL_FUTURE) != 0) {
		return 2;
	}

	sig[0] = signal(SIGTERM, SIG_IGN);
	sig[1] = signal(SIGINT, SIG_IGN);
	sig[2] = signal(SIGQUIT, SIG_IGN);

	rv = sysfs_set_num(info, NULL, "suspend_lo", start * chunk_size * data_disks);
	rv |= sysfs_set_num(info, NULL, "suspend_hi", (start + 1) * chunk_size * data_disks);
	return rv * 256;
}

int unlock_all_stripes(struct mdinfo *info, sighandler_t *sig) {
	int rv;
	rv = sysfs_set_num(info, NULL, "suspend_lo", 0x7FFFFFFFFFFFFFFFULL);
	rv |= sysfs_set_num(info, NULL, "suspend_hi", 0);
	rv |= sysfs_set_num(info, NULL, "suspend_lo", 0);

	signal(SIGQUIT, sig[2]);
	signal(SIGINT, sig[1]);
	signal(SIGTERM, sig[0]);

	if(munlockall() != 0)
		return 3;
	return rv * 256;
}

/* Autorepair */
int autorepair(int *disk, int diskP, int diskQ, unsigned long long start, int chunk_size,
		char *name[], int raid_disks, int data_disks, char **blocks_page,
		char **blocks, uint8_t *p, char **stripes, int *block_index_for_slot,
		int *source, unsigned long long *offsets)
{
	int i, j;
	int pages_to_write_count = 0;
	int page_to_write[chunk_size >> CHECK_PAGE_BITS];
	for(j = 0; j < (chunk_size >> CHECK_PAGE_BITS); j++) {
		if (disk[j] >= 0) {
			printf("Auto-repairing slot %d (%s)\n", disk[j], name[disk[j]]);
			pages_to_write_count++;
			page_to_write[j] = 1;
			for(i = 0; i < raid_disks; i++) {
				blocks_page[i] = blocks[i] + j * CHECK_PAGE_SIZE;
			}
			if (disk[j] == diskQ) {
				qsyndrome(p, (uint8_t*)stripes[diskQ] + j * CHECK_PAGE_SIZE, (uint8_t**)blocks_page, data_disks, CHECK_PAGE_SIZE);
			}
			else {
				char *all_but_failed_blocks[data_disks];
				int failed_block_index = block_index_for_slot[disk[j]];
				for(i = 0; i < data_disks; i++) {
					if (failed_block_index == i) {
						all_but_failed_blocks[i] = stripes[diskP] + j * CHECK_PAGE_SIZE;
					}
					else {
						all_but_failed_blocks[i] = blocks_page[i];
					}
				}
				xor_blocks(stripes[disk[j]] + j * CHECK_PAGE_SIZE,
				all_but_failed_blocks, data_disks, CHECK_PAGE_SIZE);
			}
		}
		else {
			page_to_write[j] = 0;
		}
	}

	if(pages_to_write_count > 0) {
		int write_res = 0;
		for(j = 0; j < (chunk_size >> CHECK_PAGE_BITS); j++) {
			if(page_to_write[j] == 1) {
				lseek64(source[disk[j]], offsets[disk[j]] + start * chunk_size + j * CHECK_PAGE_SIZE, SEEK_SET);
				write_res += write(source[disk[j]], stripes[disk[j]] + j * CHECK_PAGE_SIZE, CHECK_PAGE_SIZE);
			}
		}

		if (write_res != (CHECK_PAGE_SIZE * pages_to_write_count)) {
			fprintf(stderr, "Failed to write a full chunk.\n");
			return -1;
		}
	}

	return 0;
}

/* Manual repair */
int manual_repair(int diskP, int diskQ, int chunk_size, int raid_disks, int data_disks,
		  int failed_disk1, int failed_disk2, unsigned long long start, int *block_index_for_slot,
		  char *name[], char **stripes, char **blocks, uint8_t *p, struct mdinfo *info, sighandler_t *sig,
		  int *source, unsigned long long *offsets)
{
	int err = 0;
	int i;
	printf("Repairing stripe %llu\n", start);
	printf("Assuming slots %d (%s) and %d (%s) are incorrect\n",
	       failed_disk1, name[failed_disk1],
	       failed_disk2, name[failed_disk2]);

	if (failed_disk1 == diskQ || failed_disk2 == diskQ) {
		char *all_but_failed_blocks[data_disks];
		int failed_data_or_p;
		int failed_block_index;

		if (failed_disk1 == diskQ) {
			failed_data_or_p = failed_disk2;
		}
		else {
			failed_data_or_p = failed_disk1;
		}
		printf("Repairing D/P(%d) and Q\n", failed_data_or_p);
		failed_block_index = block_index_for_slot[failed_data_or_p];
		for (i = 0; i < data_disks; i++) {
			if (failed_block_index == i) {
				all_but_failed_blocks[i] = stripes[diskP];
			}
			else {
				all_but_failed_blocks[i] = blocks[i];
			}
		}
		xor_blocks(stripes[failed_data_or_p],
			   all_but_failed_blocks, data_disks, chunk_size);
		qsyndrome(p, (uint8_t*)stripes[diskQ], (uint8_t**)blocks, data_disks, chunk_size);
	}
	else {
		ensure_zero_has_size(chunk_size);
		if (failed_disk1 == diskP || failed_disk2 == diskP) {
			int failed_data, failed_block_index;
			if (failed_disk1 == diskP) {
				failed_data = failed_disk2;
			}
			else {
				failed_data = failed_disk1;
			}
			failed_block_index = block_index_for_slot[failed_data];
			printf("Repairing D(%d) and P\n", failed_data);
			raid6_datap_recov(raid_disks, chunk_size, failed_block_index, (uint8_t**)blocks);
		}
		else {
			printf("Repairing D and D\n");
			int failed_block_index1 = block_index_for_slot[failed_disk1];
			int failed_block_index2 = block_index_for_slot[failed_disk2];
			if (failed_block_index1 > failed_block_index2) {
				int t = failed_block_index1;
				failed_block_index1 = failed_block_index2;
				failed_block_index2 = t;
			}
			raid6_2data_recov(raid_disks, chunk_size, failed_block_index1, failed_block_index2, (uint8_t**)blocks);
		}
	}

	err = lock_stripe(info, start, chunk_size, data_disks, sig);
	if(err != 0) {
		if (err != 2) {
			return -1;
		}
		return -2;;
	}

	int write_res1, write_res2;
	off64_t seek_res;

	seek_res = lseek64(source[failed_disk1],
			   offsets[failed_disk1] + start * chunk_size, SEEK_SET);
	if (seek_res < 0) {
		fprintf(stderr, "lseek failed for failed_disk1\n");
		return -1;
	}
	write_res1 = write(source[failed_disk1], stripes[failed_disk1], chunk_size);

	seek_res = lseek64(source[failed_disk2],
			   offsets[failed_disk2] + start * chunk_size, SEEK_SET);
	if (seek_res < 0) {
		fprintf(stderr, "lseek failed for failed_disk1\n");
		return -1;
	}
	write_res2 = write(source[failed_disk2], stripes[failed_disk2], chunk_size);

	err = unlock_all_stripes(info, sig);
	if(err != 0) {
		return -2;
	}

	if (write_res1 != chunk_size || write_res2 != chunk_size) {
		fprintf(stderr, "Failed to write a complete chunk.\n");
		return -2;
	}

	return 0;
}

int check_stripes(struct mdinfo *info, int *source, unsigned long long *offsets,
		  int raid_disks, int chunk_size, int level, int layout,
		  unsigned long long start, unsigned long long length, char *name[],
		  enum repair repair, int failed_disk1, int failed_disk2)
{
	/* read the data and p and q blocks, and check we got them right */
	char *stripe_buf = xmalloc(raid_disks * chunk_size);
	char **stripes = xmalloc(raid_disks * sizeof(char*));
	char **blocks = xmalloc(raid_disks * sizeof(char*));
	char **blocks_page = xmalloc(raid_disks * sizeof(char*));
	int *block_index_for_slot = xmalloc(raid_disks * sizeof(int));
	uint8_t *p = xmalloc(chunk_size);
	uint8_t *q = xmalloc(chunk_size);
	int *results = xmalloc(chunk_size * sizeof(int));
	sighandler_t *sig = xmalloc(3 * sizeof(sighandler_t));

	int i, j;
	int diskP, diskQ;
	int data_disks = raid_disks - 2;
	int err = 0;

	extern int tables_ready;

	if (!tables_ready)
		make_tables();

	for ( i = 0 ; i < raid_disks ; i++)
		stripes[i] = stripe_buf + i * chunk_size;

	while (length > 0) {
		int disk[chunk_size >> CHECK_PAGE_BITS];

		err = lock_stripe(info, start, chunk_size, data_disks, sig);
		if(err != 0) {
			if (err != 2)
				unlock_all_stripes(info, sig);
			goto exitCheck;
		}
		for (i = 0 ; i < raid_disks ; i++) {
			off64_t seek_res = lseek64(source[i], offsets[i] + start * chunk_size,
						   SEEK_SET);
			if (seek_res < 0) {
				fprintf(stderr, "lseek to source %d failed\n", i);
				unlock_all_stripes(info, sig);
				err = -1;
				goto exitCheck;
			}
			int read_res = read(source[i], stripes[i], chunk_size);
			if (read_res < chunk_size) {
				fprintf(stderr, "Failed to read complete chunk disk %d, aborting\n", i);
				unlock_all_stripes(info, sig);
				err = -1;
				goto exitCheck;
			}
		}

		for (i = 0 ; i < data_disks ; i++) {
			int disk = geo_map(i, start, raid_disks, level, layout);
			blocks[i] = stripes[disk];
			block_index_for_slot[disk] = i;
		}

		qsyndrome(p, q, (uint8_t**)blocks, data_disks, chunk_size);
		diskP = geo_map(-1, start, raid_disks, level, layout);
		diskQ = geo_map(-2, start, raid_disks, level, layout);
		blocks[data_disks] = stripes[diskP];
		block_index_for_slot[diskP] = data_disks;
		blocks[data_disks+1] = stripes[diskQ];
		block_index_for_slot[diskQ] = data_disks+1;

		raid6_collect(chunk_size, p, q, stripes[diskP], stripes[diskQ], results);
		raid6_stats(disk, results, raid_disks, chunk_size);

		for(j = 0; j < (chunk_size >> CHECK_PAGE_BITS); j++) {
			if(disk[j] >= -2) {
				disk[j] = geo_map(disk[j], start, raid_disks, level, layout);
			}
			if(disk[j] >= 0) {
				printf("Error detected at stripe %llu, page %d: possible failed disk slot: %d --> %s\n",
					start, j, disk[j], name[disk[j]]);
			}
			if(disk[j] == -65535) {
				printf("Error detected at stripe %llu, page %d: disk slot unknown\n", start, j);
			}
		}

		if(repair == AUTO_REPAIR) {
			err = autorepair(disk, diskP, diskQ, start, chunk_size,
					name, raid_disks, data_disks, blocks_page,
					blocks, p, stripes, block_index_for_slot,
					source, offsets);
			if(err != 0) {
				unlock_all_stripes(info, sig);
				goto exitCheck;
			}
		}

		err = unlock_all_stripes(info, sig);
		if(err != 0) {
			goto exitCheck;
		}

		if(repair == MANUAL_REPAIR) {
			err = manual_repair(diskP, diskQ, chunk_size, raid_disks, data_disks,
					failed_disk1, failed_disk2, start, block_index_for_slot,
					name, stripes, blocks, p, info, sig,
					source, offsets);
			if(err == -1) {
				unlock_all_stripes(info, sig);
				goto exitCheck;
			}
		}

		length--;
		start++;
	}

exitCheck:

	free(stripe_buf);
	free(stripes);
	free(blocks);
	free(blocks_page);
	free(block_index_for_slot);
	free(p);
	free(q);
	free(results);
	free(sig);

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
	enum repair repair = NO_REPAIR;
	int failed_disk1 = -1;
	int failed_disk2 = -1;
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
		fprintf(stderr, "Usage: %s md_device start_stripe length_stripes [autorepair]\n", prg);
		fprintf(stderr, "   or: %s md_device repair stripe failed_slot_1 failed_slot_2\n", prg);
		exit_err = 1;
		goto exitHere;
	}

	mdfd = open(argv[1], O_RDONLY);
	if(mdfd < 0) {
		perror(argv[1]);
		fprintf(stderr, "%s: cannot open %s\n", prg, argv[1]);
		exit_err = 2;
		goto exitHere;
	}

	info = sysfs_read(mdfd, NULL,
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
	if (strcmp(argv[2], "repair")==0) {
		if (argc < 6) {
			fprintf(stderr, "For repair mode, call %s md_device repair stripe failed_slot_1 failed_slot_2\n", prg);
			exit_err = 1;
			goto exitHere;
		}
		repair = MANUAL_REPAIR;
		start = getnum(argv[3], &err);
		length = 1;
		failed_disk1 = getnum(argv[4], &err);
		failed_disk2 = getnum(argv[5], &err);

		if(failed_disk1 >= info->array.raid_disks) {
			fprintf(stderr, "%s: failed_slot_1 index is higher than number of devices in raid\n", prg);
			exit_err = 4;
			goto exitHere;
		}
		if(failed_disk2 >= info->array.raid_disks) {
			fprintf(stderr, "%s: failed_slot_2 index is higher than number of devices in raid\n", prg);
			exit_err = 4;
			goto exitHere;
		}
		if(failed_disk1 == failed_disk2) {
			fprintf(stderr, "%s: failed_slot_1 and failed_slot_2 are the same\n", prg);
			exit_err = 4;
			goto exitHere;
		}
	}
	else {
		start = getnum(argv[2], &err);
		length = getnum(argv[3], &err);
		if (argc >= 5 && strcmp(argv[4], "autorepair")==0)
			repair = AUTO_REPAIR;
	}

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

	disk_name = xmalloc(raid_disks * sizeof(*disk_name));
	fds = xmalloc(raid_disks * sizeof(*fds));
	offsets = xcalloc(raid_disks, sizeof(*offsets));
	buf = xmalloc(raid_disks * chunk_size);

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
			fds[disk_slot] = open(disk_name[disk_slot], O_RDWR | O_SYNC);
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
			       start, length, disk_name, repair, failed_disk1, failed_disk2);
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
