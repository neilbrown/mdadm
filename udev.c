/*
 * mdadm - manage Linux "md" devices aka RAID arrays.
 *
 * Copyright (C) 2022 Mateusz Grzonka <mateusz.grzonka@intel.com>
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

#include	"mdadm.h"
#include	"udev.h"
#include	"md_p.h"
#include	"md_u.h"
#include	"xmalloc.h"

#include	<sys/wait.h>
#include	<signal.h>
#include	<limits.h>
#include	<syslog.h>

#ifndef NO_LIBUDEV
#include	<libudev.h>
#endif

static char *unblock_path;

/*
 * udev_is_available() - Checks for udev in the system.
 *
 * Function looks whether udev directories are available and MDADM_NO_UDEV env defined.
 *
 * Return:
 * true if udev is available,
 * false if not
 */
bool udev_is_available(void)
{
	struct stat stb;

	if (stat("/dev/.udev", &stb) != 0 &&
	    stat("/run/udev", &stb) != 0)
		return false;
	if (check_env("MDADM_NO_UDEV") == 1)
		return false;
	return true;
}

#ifndef NO_LIBUDEV

static struct udev *udev;
static struct udev_monitor *udev_monitor;

/*
 * udev_release() - Drops references of udev and udev_monitor.
 */
static void udev_release(void)
{
	udev_monitor_unref(udev_monitor);
	udev_unref(udev);
}

/*
 * udev_initialize() - Initializes udev and udev_monitor structures.
 *
 * Function initializes udev, udev_monitor, and sets udev_monitor filter for block devices.
 *
 * Return:
 * UDEV_STATUS_SUCCESS on success
 * UDEV_STATUS_ERROR on error
 * UDEV_STATUS_ERROR_NO_UDEV when udev not available
 */
static enum udev_status udev_initialize(void)
{
	if (!udev_is_available()) {
		pr_err("No udev.\n");
		return UDEV_STATUS_ERROR_NO_UDEV;
	}

	udev = udev_new();
	if (!udev) {
		pr_err("Cannot initialize udev.\n");
		return UDEV_STATUS_ERROR;
	}

	udev_monitor = udev_monitor_new_from_netlink(udev, "udev");
	if (!udev_monitor) {
		pr_err("Cannot initialize udev monitor.\n");
		udev = udev_unref(udev);
		return UDEV_STATUS_ERROR;
	}

	if (udev_monitor_filter_add_match_subsystem_devtype(udev_monitor, "block", 0) < 0) {
		pr_err("Cannot add udev monitor event filter for md devices.\n");
		udev_release();
		return UDEV_STATUS_ERROR;
	}
	if (udev_monitor_enable_receiving(udev_monitor) < 0) {
		pr_err("Cannot enable receiving udev events through udev monitor.\n");
		udev_release();
		return UDEV_STATUS_ERROR;
	}
	atexit(udev_release);
	return UDEV_STATUS_SUCCESS;
}

/*
 * udev_wait_for_events() - Waits for events from udev.
 * @seconds: Timeout in seconds.
 *
 * Function waits udev events, wakes up on event or timeout.
 *
 * Return:
 * UDEV_STATUS_SUCCESS on detected event
 * UDEV_STATUS_TIMEOUT on timeout
 * UDEV_STATUS_ERROR on error
 */
enum udev_status udev_wait_for_events(int seconds)
{
	int fd;
	fd_set readfds;
	struct timeval tv;
	int ret;

	if (!udev || !udev_monitor) {
		ret = udev_initialize();
		if (ret != UDEV_STATUS_SUCCESS)
			return ret;
	}

	fd = udev_monitor_get_fd(udev_monitor);
	if (fd < 0) {
		pr_err("Cannot access file descriptor associated with udev monitor.\n");
		return UDEV_STATUS_ERROR;
	}

	FD_ZERO(&readfds);
	FD_SET(fd, &readfds);
	tv.tv_sec = seconds;
	tv.tv_usec = 0;

	if (select(fd + 1, &readfds, NULL, NULL, &tv) > 0 && FD_ISSET(fd, &readfds))
		if (udev_monitor_receive_device(udev_monitor))
			return UDEV_STATUS_SUCCESS; /* event detected */
	return UDEV_STATUS_TIMEOUT;
}
#endif

/*
 * udev_block() - Block udev from examining newly created arrays.
 *
 * When array is created, we don't want udev to examine it immediately.
 * Function creates /run/mdadm/creating-mdXXX and expects that udev rule
 * will notice it and act accordingly.
 *
 * Return:
 * UDEV_STATUS_SUCCESS when successfully blocked udev
 * UDEV_STATUS_ERROR on error
 */
enum udev_status udev_block(char *devnm)
{
	int fd;
	char *path = xcalloc(1, BUFSIZ);

	snprintf(path, BUFSIZ, "/run/mdadm/creating-%s", devnm);

	fd = open(path, O_CREAT | O_RDWR, 0600);
	if (!is_fd_valid(fd)) {
		pr_err("Cannot block udev, error creating blocking file.\n");
		pr_err("%s: %s\n", strerror(errno), path);
		free(path);
		return UDEV_STATUS_ERROR;
	}

	close(fd);
	unblock_path = path;
	return UDEV_STATUS_SUCCESS;
}

/*
 * udev_unblock() - Unblock udev.
 */
void udev_unblock(void)
{
	if (unblock_path)
		unlink(unblock_path);
	free(unblock_path);
	unblock_path = NULL;
}
