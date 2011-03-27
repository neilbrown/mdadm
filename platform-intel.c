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
#include <limits.h>


static int devpath_to_ll(const char *dev_path, const char *entry,
			 unsigned long long *val);

static __u16 devpath_to_vendor(const char *dev_path);

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
	enum sys_dev_type type;
	unsigned long long dev_id;

	if (strcmp(driver, "isci") == 0)
		type = SYS_DEV_SAS;
	else if (strcmp(driver, "ahci") == 0)
		type = SYS_DEV_SATA;
	else
		type = SYS_DEV_UNKNOWN;

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

		sprintf(path, "/sys/bus/%s/drivers/%s/%s",
			bus, driver, de->d_name);

		/* if it's not Intel device skip it. */
		if (devpath_to_vendor(path) != 0x8086)
			continue;

		if (devpath_to_ll(path, "device", &dev_id) != 0)
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

		list->dev_id = (__u16) dev_id;
		list->type = type;
		list->path = canonicalize_file_name(path);
		list->next = NULL;
		if ((list->pci_id = strrchr(list->path, '/')) != NULL)
			list->pci_id++;
	}
	closedir(driver_dir);
	return head;
}


static struct sys_dev *intel_devices=NULL;

static enum sys_dev_type device_type_by_id(__u16 device_id)
{
	struct sys_dev *iter;

	for(iter = intel_devices; iter != NULL; iter = iter->next)
		if (iter->dev_id == device_id)
			return iter->type;
	return SYS_DEV_UNKNOWN;
}

static int devpath_to_ll(const char *dev_path, const char *entry, unsigned long long *val)
{
	char path[strlen(dev_path) + strlen(entry) + 2];
	int fd;
	int n;

	sprintf(path, "%s/%s", dev_path, entry);

	fd = open(path, O_RDONLY);
	if (fd < 0)
		return -1;
	n = sysfs_fd_get_ll(fd, val);
	close(fd);
	return n;
}


static __u16 devpath_to_vendor(const char *dev_path)
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

struct sys_dev *find_intel_devices(void)
{
	struct sys_dev *ahci, *isci;

	isci = find_driver_devices("pci", "isci");
	ahci = find_driver_devices("pci", "ahci");

	if (!ahci) {
		ahci = isci;
	} else {
		struct sys_dev *elem = ahci;
		while (elem->next)
			elem = elem->next;
		elem->next = isci;
	}
	return ahci;
}

/*
 * PCI Expansion ROM Data Structure Format */
struct pciExpDataStructFormat {
	__u8  ver[4];
	__u16 vendorID;
	__u16 deviceID;
} __attribute__ ((packed));

static struct imsm_orom imsm_orom[SYS_DEV_MAX];
static int populated_orom[SYS_DEV_MAX];

static int scan(const void *start, const void *end, const void *data)
{
	int offset;
	const struct imsm_orom *imsm_mem;
	int dev;
	int len = (end - start);
	struct pciExpDataStructFormat *ptr= (struct pciExpDataStructFormat *)data;

	if (data + 0x18 > end) {
		dprintf("cannot find pciExpDataStruct \n");
		return 0;
	}

	dprintf("ptr->vendorID: %lx __le16_to_cpu(ptr->deviceID): %lx \n",
		(ulong) __le16_to_cpu(ptr->vendorID),
		(ulong) __le16_to_cpu(ptr->deviceID));

	if (__le16_to_cpu(ptr->vendorID) == 0x8086) {
		/* serach  attached intel devices by device id from OROM */
		dev = device_type_by_id(__le16_to_cpu(ptr->deviceID));
		if (dev == SYS_DEV_UNKNOWN)
			return 0;
	}
	else
		return 0;

	for (offset = 0; offset < len; offset += 4) {
		imsm_mem = start + offset;
		if ((memcmp(imsm_mem->signature, "$VER", 4) == 0)) {
			imsm_orom[dev] = *imsm_mem;
			populated_orom[dev] = 1;
			return populated_orom[SYS_DEV_SATA] && populated_orom[SYS_DEV_SAS];
		}
	}
	return 0;
}


const struct imsm_orom *imsm_platform_test(enum sys_dev_type hba_id, int *populated,
					   struct imsm_orom *imsm_orom)
{
	memset(imsm_orom, 0, sizeof(*imsm_orom));
	imsm_orom->rlc = IMSM_OROM_RLC_RAID0 | IMSM_OROM_RLC_RAID1 |
				IMSM_OROM_RLC_RAID10 | IMSM_OROM_RLC_RAID5;
	imsm_orom->sss = IMSM_OROM_SSS_4kB | IMSM_OROM_SSS_8kB |
				IMSM_OROM_SSS_16kB | IMSM_OROM_SSS_32kB |
				IMSM_OROM_SSS_64kB | IMSM_OROM_SSS_128kB |
				IMSM_OROM_SSS_256kB | IMSM_OROM_SSS_512kB |
				IMSM_OROM_SSS_1MB | IMSM_OROM_SSS_2MB;
	imsm_orom->dpa = IMSM_OROM_DISKS_PER_ARRAY;
	imsm_orom->tds = IMSM_OROM_TOTAL_DISKS;
	imsm_orom->vpa = IMSM_OROM_VOLUMES_PER_ARRAY;
	imsm_orom->vphba = IMSM_OROM_VOLUMES_PER_HBA;
	imsm_orom->attr = imsm_orom->rlc | IMSM_OROM_ATTR_ChecksumVerify;
	*populated = 1;

	if (check_env("IMSM_TEST_OROM_NORAID5")) {
		imsm_orom->rlc = IMSM_OROM_RLC_RAID0 | IMSM_OROM_RLC_RAID1 |
				IMSM_OROM_RLC_RAID10;
	}
	if (check_env("IMSM_TEST_AHCI_EFI_NORAID5") && (hba_id == SYS_DEV_SAS)) {
		imsm_orom->rlc = IMSM_OROM_RLC_RAID0 | IMSM_OROM_RLC_RAID1 |
				IMSM_OROM_RLC_RAID10;
	}
	if (check_env("IMSM_TEST_SCU_EFI_NORAID5") && (hba_id == SYS_DEV_SATA)) {
		imsm_orom->rlc = IMSM_OROM_RLC_RAID0 | IMSM_OROM_RLC_RAID1 |
				IMSM_OROM_RLC_RAID10;
	}

	return imsm_orom;
}



static const struct imsm_orom *find_imsm_hba_orom(enum sys_dev_type hba_id)
{
	unsigned long align;

	if (hba_id >= SYS_DEV_MAX)
		return NULL;

	/* it's static data so we only need to read it once */
	if (populated_orom[hba_id]) {
		dprintf("OROM CAP: %p, pid: %d pop: %d\n",
			&imsm_orom[hba_id], (int) getpid(), populated_orom[hba_id]);
		return &imsm_orom[hba_id];
	}
	if (check_env("IMSM_TEST_OROM")) {
		dprintf("OROM CAP: %p,  pid: %d pop: %d\n",
                     &imsm_orom[hba_id], (int) getpid(), populated_orom[hba_id]);
		return imsm_platform_test(hba_id, &populated_orom[hba_id], &imsm_orom[hba_id]);
	}
	/* return empty OROM capabilities in EFI test mode */
	if (check_env("IMSM_TEST_AHCI_EFI") ||
	    check_env("IMSM_TEST_SCU_EFI"))
		return NULL;


	if (intel_devices != NULL)
		free_sys_dev(&intel_devices);

	intel_devices = find_intel_devices();

	if (intel_devices == NULL)
		return NULL;

	/* scan option-rom memory looking for an imsm signature */
	if (check_env("IMSM_SAFE_OROM_SCAN"))
		align = 2048;
	else
		align = 512;
	if (probe_roms_init(align) != 0)
		return NULL;
	probe_roms();
	/* ignore return value - True is returned if both adapater roms are found */
	scan_adapter_roms(scan);
	probe_roms_exit();

	if (intel_devices != NULL)
		free_sys_dev(&intel_devices);
	intel_devices = NULL;

	if (populated_orom[hba_id])
		return &imsm_orom[hba_id];
	return NULL;
}

#define GUID_STR_MAX	37  /* according to GUID format:
			     * xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" */

#define EFI_GUID(a, b, c, d0, d1, d2, d3, d4, d5, d6, d7) \
((struct efi_guid) \
{{ (a) & 0xff, ((a) >> 8) & 0xff, ((a) >> 16) & 0xff, ((a) >> 24) & 0xff, \
  (b) & 0xff, ((b) >> 8) & 0xff, \
  (c) & 0xff, ((c) >> 8) & 0xff, \
  (d0), (d1), (d2), (d3), (d4), (d5), (d6), (d7) }})


#define SYS_EFI_VAR_PATH "/sys/firmware/efi/vars"
#define SCU_PROP "RstScuV"
#define AHCI_PROP "RstSataV"

#define VENDOR_GUID \
	EFI_GUID(0x193dfefa, 0xa445, 0x4302, 0x99, 0xd8, 0xef, 0x3a, 0xad, 0x1a, 0x04, 0xc6)

int populated_efi[SYS_DEV_MAX] = { 0, 0 };

static struct imsm_orom imsm_efi[SYS_DEV_MAX];

int read_efi_variable(void *buffer, ssize_t buf_size, char *variable_name, struct efi_guid guid)
{
	char path[PATH_MAX];
	char buf[GUID_STR_MAX];
	int dfd;
	ssize_t n, var_data_len;

	snprintf(path, PATH_MAX, "%s/%s-%s/size", SYS_EFI_VAR_PATH, variable_name, guid_str(buf, guid));

	dprintf("EFI VAR: path=%s\n", path);
	/* get size of variable data */
	dfd = open(path, O_RDONLY);
	if (dfd < 0)
		return 1;

	n = read(dfd, &buf, sizeof(buf));
	close(dfd);
	if (n < 0)
		return 1;
	buf[n] = '\0';

	errno = 0;
	var_data_len = strtoul(buf, NULL, 16);
	if ((errno == ERANGE && (var_data_len == LONG_MAX))
            || (errno != 0 && var_data_len == 0))
		return 1;

	/* get data */
	snprintf(path, PATH_MAX, "%s/%s-%s/data", SYS_EFI_VAR_PATH, variable_name, guid_str(buf, guid));

	dprintf("EFI VAR: path=%s\n", path);
	dfd = open(path, O_RDONLY);
	if (dfd < 0)
		return 1;

	n = read(dfd, buffer, buf_size);
	close(dfd);
	if (n != var_data_len || n < buf_size) {
		return 1;
	}

	return 0;
}

const struct imsm_orom *find_imsm_efi(enum sys_dev_type hba_id)
{
	if (hba_id >= SYS_DEV_MAX)
		return NULL;

	dprintf("EFI CAP: %p,  pid: %d pop: %d\n",
		&imsm_efi[hba_id], (int) getpid(), populated_efi[hba_id]);

	/* it's static data so we only need to read it once */
	if (populated_efi[hba_id]) {
		dprintf("EFI CAP: %p, pid: %d pop: %d\n",
			&imsm_efi[hba_id], (int) getpid(), populated_efi[hba_id]);
		return &imsm_efi[hba_id];
	}
	if (check_env("IMSM_TEST_AHCI_EFI") ||
	    check_env("IMSM_TEST_SCU_EFI")) {
		dprintf("OROM CAP: %p,  pid: %d pop: %d\n",
			&imsm_efi[hba_id], (int) getpid(), populated_efi[hba_id]);
		return imsm_platform_test(hba_id, &populated_efi[hba_id], &imsm_efi[hba_id]);
	}
	/* OROM test is set, return that there is no EFI capabilities */
	if (check_env("IMSM_TEST_OROM"))
		return NULL;

	if (read_efi_variable(&imsm_efi[hba_id], sizeof(imsm_efi[0]), hba_id == SYS_DEV_SAS ? SCU_PROP : AHCI_PROP, VENDOR_GUID)) {
		populated_efi[hba_id] = 0;
		return NULL;
	}

	populated_efi[hba_id] = 1;
	return &imsm_efi[hba_id];
}

/*
 * backward interface compatibility
 */
const struct imsm_orom *find_imsm_orom(void)
{
	return find_imsm_hba_orom(SYS_DEV_SATA);
}

const struct imsm_orom *find_imsm_capability(enum sys_dev_type hba_id)
{
	const struct imsm_orom *cap=NULL;


	if ((cap = find_imsm_efi(hba_id)) != NULL)
		return cap;
	if ((cap = find_imsm_hba_orom(hba_id)) != NULL)
		return cap;
	return NULL;
}

char *devt_to_devpath(dev_t dev)
{
	char device[46];

	sprintf(device, "/sys/dev/block/%d:%d/device", major(dev), minor(dev));
	return canonicalize_file_name(device);
}

char *diskfd_to_devpath(int fd)
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

	if (check_env("IMSM_TEST_AHCI_DEV") ||
	    check_env("IMSM_TEST_SCU_DEV")) {
		return 1;
	}

	if (!disk_path || !hba_path)
		return 0;
	dprintf("hba: %s - disk: %s\n", hba_path, disk_path);
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
