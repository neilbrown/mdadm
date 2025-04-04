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

#ifndef MONITOR_UDEV_H
#define MONITOR_UDEV_H

enum udev_status {
	UDEV_STATUS_ERROR_NO_UDEV = -2,
	UDEV_STATUS_ERROR,
	UDEV_STATUS_SUCCESS = 0,
	UDEV_STATUS_TIMEOUT
};

bool udev_is_available(void);

#ifndef NO_LIBUDEV
enum udev_status udev_wait_for_events(int seconds);
#endif

enum udev_status udev_block(char *devnm);
void udev_unblock(void);

#endif
