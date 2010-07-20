/*
 * Copyright (C) 2008 Intel Corporation
 *
 * 	mdmon socket / message handling
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
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include "mdadm.h"
#include "mdmon.h"

static const __u32 start_magic = 0x5a5aa5a5;
static const __u32 end_magic = 0xa5a55a5a;

static int send_buf(int fd, const void* buf, int len, int tmo)
{
	fd_set set;
	int rv;
	struct timeval timeout = {tmo, 0};
	struct timeval *ptmo = tmo ? &timeout : NULL;

	while (len) {
		FD_ZERO(&set);
		FD_SET(fd, &set);
		rv = select(fd+1, NULL, &set, NULL, ptmo);
		if (rv <= 0)
			return -1;
		rv = write(fd, buf, len);
		if (rv <= 0)
			return -1;
		len -= rv;
		buf += rv;
	}
	return 0;
}

static int recv_buf(int fd, void* buf, int len, int tmo)
{
	fd_set set;
	int rv;
	struct timeval timeout = {tmo, 0};
	struct timeval *ptmo = tmo ? &timeout : NULL;

	while (len) {
		FD_ZERO(&set);
		FD_SET(fd, &set);
		rv = select(fd+1, &set, NULL, NULL, ptmo);
		if (rv <= 0)
			return -1;
		rv = read(fd, buf, len);
		if (rv <= 0)
			return -1;
		len -= rv;
		buf += rv;
	}
	return 0;
}


int send_message(int fd, struct metadata_update *msg, int tmo)
{
	__s32 len = msg->len;
	int rv;

	rv = send_buf(fd, &start_magic, 4, tmo);
	rv = rv ?: send_buf(fd, &len, 4, tmo);
	if (len > 0)
		rv = rv ?: send_buf(fd, msg->buf, msg->len, tmo);
	rv = send_buf(fd, &end_magic, 4, tmo);

	return rv;
}

int receive_message(int fd, struct metadata_update *msg, int tmo)
{
	__u32 magic;
	__s32 len;
	int rv;

	rv = recv_buf(fd, &magic, 4, tmo);
	if (rv < 0 || magic != start_magic)
		return -1;
	rv = recv_buf(fd, &len, 4, tmo);
	if (rv < 0 || len > MSG_MAX_LEN)
		return -1;
	if (len > 0) {
		msg->buf = malloc(len);
		if (msg->buf == NULL)
			return -1;
		rv = recv_buf(fd, msg->buf, len, tmo);
		if (rv < 0) {
			free(msg->buf);
			return -1;
		}
	} else
		msg->buf = NULL;
	rv = recv_buf(fd, &magic, 4, tmo);
	if (rv < 0 || magic != end_magic) {
		free(msg->buf);
		return -1;
	}
	msg->len = len;
	return 0;
}

int ack(int fd, int tmo)
{
	struct metadata_update msg = { .len = 0 };

	return send_message(fd, &msg, tmo);
}

int wait_reply(int fd, int tmo)
{
	struct metadata_update msg;
	return receive_message(fd, &msg, tmo);
}

int connect_monitor(char *devname)
{
	char path[100];
	int sfd;
	long fl;
	struct sockaddr_un addr;
	int pos;
	char *c;

	pos = sprintf(path, "%s/", MDMON_DIR);
	if (is_subarray(devname)) {
		devname++;
		c = strchr(devname, '/');
		if (!c)
			return -1;
		snprintf(&path[pos], c - devname + 1, "%s", devname);
		pos += c - devname;
	} else
		pos += sprintf(&path[pos], "%s", devname);
	sprintf(&path[pos], ".sock");

	sfd = socket(PF_LOCAL, SOCK_STREAM, 0);
	if (sfd < 0)
		return -1;

	addr.sun_family = PF_LOCAL;
	strcpy(addr.sun_path, path);
	if (connect(sfd, &addr, sizeof(addr)) < 0) {
		close(sfd);
		return -1;
	}

	fl = fcntl(sfd, F_GETFL, 0);
	fl |= O_NONBLOCK;
	fcntl(sfd, F_SETFL, fl);

	return sfd;
}

int fping_monitor(int sfd)
{
	int err = 0;

	if (sfd < 0)
		return sfd;

	/* try to ping existing socket */
	if (ack(sfd, 20) != 0)
		err = -1;

	/* check the reply */
	if (!err && wait_reply(sfd, 20) != 0)
		err = -1;

	return err;
}


/* give the monitor a chance to update the metadata */
int ping_monitor(char *devname)
{
	int sfd = connect_monitor(devname);
	int err = fping_monitor(sfd);

	close(sfd);
	return err;
}

/* give the manager a chance to view the updated container state.  This
 * would naturally happen due to the manager noticing a change in
 * /proc/mdstat; however, pinging encourages this detection to happen
 * while an exclusive open() on the container is active
 */
int ping_manager(char *devname)
{
	int sfd = connect_monitor(devname);
	struct metadata_update msg = { .len = -1 };
	int err = 0;

	if (sfd < 0)
		return sfd;

	err = send_message(sfd, &msg, 20);

	/* check the reply */
	if (!err && wait_reply(sfd, 20) != 0)
		err = -1;

	close(sfd);
	return err;
}
