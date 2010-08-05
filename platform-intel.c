/*
 * Intel(R) Matrix Storage Manager hardware and firmware support routines
 *
 * Copyright (C) 2008 Intel Corporation
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms and conditions of the GNU General Public License,
 * version 2, as published by the Free Software Foundation.
 *
 * This program is distributed in the hope it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St - Fifth Floor, Boston, MA 02110-1301 USA.
 */
#include "mdadm.h"
#include "platform-intel.h"
#include "probe_roms.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <dirent.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>

void free_sys_dev(struct sys_dev **list)
{
	while (*list) {
		struct sys_dev *next = (*list)->next;

		if ((*list)->path)
			free((*list)->path);
		free(*list);
		*list = next;
	}
}

struct sys_dev *find_driver_devices(const char *bus, const char *driver)
{
	/* search sysfs for devices driven by 'driver' */
	char path[292];
	char link[256];
	char *c;
	DIR *driver_dir;
	struct dirent *de;
	struct sys_dev *head = NULL;
	struct sys_dev *list = NULL;

	sprintf(path, "/sys/bus/%s/drivers/%s", bus, driver);
	driver_dir = opendir(path);
	if (!driver_dir)
		return NULL;
	for (de = readdir(driver_dir); de; de = readdir(driver_dir)) {
		int n;

		/* is 'de' a device? check that the 'subsystem' link exists and
		 * that its target matches 'bus'
		 */
		sprintf(path, "/sys/bus/%s/drivers/%s/%s/subsystem",
			bus, driver, de->d_name);
		n = readlink(path, link, sizeof(link));
		if (n < 0 || n >= (int)sizeof(link))
			continue;
		link[n] = '\0';
		c = strrchr(link, '/');
		if (!c)
			continue;
		if (strncmp(bus, c+1, strlen(bus)) != 0)
			continue;

		/* start / add list entry */
		if (!head) {
			head = malloc(sizeof(*head));
			list = head;
		} else {
			list->next = malloc(sizeof(*head));
			list = list->next;
		}

		if (!list) {
			free_sys_dev(&head);
			break;
		}

		/* generate canonical path name for the device */
		sprintf(path, "/sys/bus/%s/drivers/%s/%s",
			bus, driver, de->d_name);
		list->path = canonicalize_file_name(path);
		list->next = NULL;
	}
	closedir(driver_dir);
	return head;
}

__u16 devpath_to_vendor(const char *dev_path)
{
	char path[strlen(dev_path) + strlen("/vendor") + 1];
	char vendor[7];
	int fd;
	__u16 id = 0xffff;
	int n;

	sprintf(path, "%s/vendor", dev_path);

	fd = open(path, O_RDONLY);
	if (fd < 0)
		return 0xffff;

	n = read(fd, vendor, sizeof(vendor));
	if (n == sizeof(vendor)) {
		vendor[n - 1] = '\0';
		id = strtoul(vendor, NULL, 16);
	}
	close(fd);

	return id;
}

static int platform_has_intel_ahci(void)
{
	struct sys_dev *devices = find_driver_devices("pci", "ahci");
	struct sys_dev *dev;
	int ret = 0;

	for (dev = devices; dev; dev = dev->next)
		if (devpath_to_vendor(dev->path) == 0x8086) {
			ret = 1;
			break;
		}

	free_sys_dev(&devices);

	return ret;
}


static struct imsm_orom imsm_orom;
static int scan(const void *start, const void *end)
{
	int offset;
	const struct imsm_orom *imsm_mem;
	int len = (end - start);

	for (offset = 0; offset < len; offset += 4) {
		imsm_mem = start + offset;
		if (memcmp(imsm_mem->signature, "$VER", 4) == 0) {
			imsm_orom = *imsm_mem;
			return 1;
		}
	}

	return 0;
}

const struct imsm_orom *find_imsm_orom(void)
{
	static int populated = 0;
	unsigned long align;

	/* it's static data so we only need to read it once */
	if (populated)
		return &imsm_orom;

	if (check_env("IMSM_TEST_OROM")) {
		memset(&imsm_orom, 0, sizeof(imsm_orom));
		imsm_orom.rlc = IMSM_OROM_RLC_RAID0 | IMSM_OROM_RLC_RAID1 |
				IMSM_OROM_RLC_RAID10 | IMSM_OROM_RLC_RAID5;
		imsm_orom.sss = IMSM_OROM_SSS_4kB | IMSM_OROM_SSS_8kB |
				IMSM_OROM_SSS_16kB | IMSM_OROM_SSS_32kB |
				IMSM_OROM_SSS_64kB | IMSM_OROM_SSS_128kB |
				IMSM_OROM_SSS_256kB | IMSM_OROM_SSS_512kB |
				IMSM_OROM_SSS_1MB | IMSM_OROM_SSS_2MB;
		imsm_orom.dpa = 6;
		imsm_orom.tds = 6;
		imsm_orom.vpa = 2;
		imsm_orom.vphba = 4;
		imsm_orom.attr = imsm_orom.rlc | IMSM_OROM_ATTR_ChecksumVerify;
		populated = 1;
		return &imsm_orom;
	}

	if (!platform_has_intel_ahci())
		return NULL;

	/* scan option-rom memory looking for an imsm signature */
	if (check_env("IMSM_SAFE_OROM_SCAN"))
		align = 2048;
	else
		align = 512;
	if (probe_roms_init(align) != 0)
		return NULL;
	probe_roms();
	populated = scan_adapter_roms(scan);
	probe_roms_exit();

	if (populated)
		return &imsm_orom;
	return NULL;
}

char *devt_to_devpath(dev_t dev)
{
	char device[46];

	sprintf(device, "/sys/dev/block/%d:%d/device", major(dev), minor(dev));
	return canonicalize_file_name(device);
}

static char *diskfd_to_devpath(int fd)
{
	/* return the device path for a disk, return NULL on error or fd
	 * refers to a partition
	 */
	struct stat st;

	if (fstat(fd, &st) != 0)
		return NULL;
	if (!S_ISBLK(st.st_mode))
		return NULL;

	return devt_to_devpath(st.st_rdev);
}

int path_attached_to_hba(const char *disk_path, const char *hba_path)
{
	int rc;

	if (!disk_path || !hba_path)
		return 0;

	if (strncmp(disk_path, hba_path, strlen(hba_path)) == 0)
		rc = 1;
	else
		rc = 0;

	return rc;
}

int devt_attached_to_hba(dev_t dev, const char *hba_path)
{
	char *disk_path = devt_to_devpath(dev);
	int rc = path_attached_to_hba(disk_path, hba_path);

	if (disk_path)
		free(disk_path);

	return rc;
}

int disk_attached_to_hba(int fd, const char *hba_path)
{
	char *disk_path = diskfd_to_devpath(fd);
	int rc = path_attached_to_hba(disk_path, hba_path);

	if (disk_path)
		free(disk_path);

	return rc;
}

