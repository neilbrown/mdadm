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

enum tx_rx_state {
	TX_RX_START,
	TX_RX_SEQ,
	TX_RX_NUM_BYTES,
	TX_RX_BUF,
	TX_RX_END,
	TX_RX_SUCCESS,
	TX_RX_ERR,
};

const int start_magic = 0x5a5aa5a5;
const int end_magic = 0xa5a55a5a;

#define txrx(fd, buf, size, flags) (recv_send ? \
	recv(fd, buf, size, flags) : \
	send(fd, buf, size, flags))

/* non-blocking send/receive with n second timeout */
static enum tx_rx_state
tx_rx_message(int fd, struct md_message *msg, int recv_send, int tmo)
{
	int d = recv_send ? 0 : start_magic;
	int flags = recv_send ? 0 : MSG_NOSIGNAL;
	enum tx_rx_state state = TX_RX_START;
	void *buf = &d;
	size_t size = sizeof(d);
	off_t n = 0;
	int rc;
	int again;

	do {
		again = 0;
		rc = txrx(fd, buf + n, size - n, flags);
		if (rc <= 0) { /* error */
			if (rc == -1 && errno == EAGAIN)
				again = 1;
			else
				state = TX_RX_ERR;
		} else if (rc + n == size) /* done */
			switch (state) {
			case TX_RX_START:
				if (recv_send && d != start_magic)
					state = TX_RX_ERR;
				else {
					state = TX_RX_SEQ;
					buf = &msg->seq;
					size = sizeof(msg->seq);
					n = 0;
				}
				break;
			case TX_RX_SEQ:
				state = TX_RX_NUM_BYTES;
				buf = &msg->num_bytes;
				size = sizeof(msg->num_bytes);
				n = 0;
				break;
			case TX_RX_NUM_BYTES:
				if (msg->num_bytes >
				    sizeof(union md_message_commands))
					state = TX_RX_ERR;
				else if (recv_send && msg->num_bytes) {
					msg->buf = malloc(msg->num_bytes);
					if (!msg->buf)
						state = TX_RX_ERR;
					else {
						state = TX_RX_BUF;
						buf = msg->buf;
						size = msg->num_bytes;
						n = 0;
					}
				} else if (!recv_send && msg->num_bytes) {
					state = TX_RX_BUF;
					buf = msg->buf;
					size = msg->num_bytes;
					n = 0;
				} else {
					d = recv_send ? 0 : end_magic;
					state = TX_RX_END;
					buf = &d;
					size = sizeof(d);
					n = 0;
				}
				break;
			case TX_RX_BUF:
				d = recv_send ? 0 : end_magic;
				state = TX_RX_END;
				buf = &d;
				size = sizeof(d);
				n = 0;
				break;
			case TX_RX_END:
				if (recv_send && d != end_magic)
					state = TX_RX_ERR;
				else
					state = TX_RX_SUCCESS;
				break;
			case TX_RX_ERR:
			case TX_RX_SUCCESS:
				break;
			}
		else /* continue */
			n += rc;

		if (again) {
			fd_set set;
			struct timeval timeout = { tmo, 0 };
			struct timeval *ptmo = tmo ? &timeout : NULL;

			FD_ZERO(&set);
			FD_SET(fd, &set);

			if (recv_send)
				rc = select(fd + 1, &set, NULL, NULL, ptmo);
			else
				rc = select(fd + 1, NULL, &set, NULL, ptmo);

			if (rc <= 0)
				state = TX_RX_ERR;
		}
	} while (state < TX_RX_SUCCESS);

	return state;
}


int receive_message(int fd, struct md_message *msg, int tmo)
{
	if (tx_rx_message(fd, msg, 1, tmo) == TX_RX_SUCCESS)
		return 0;
	else
		return -1;
}

int send_message(int fd, struct md_message *msg, int tmo)
{
	if (tx_rx_message(fd, msg, 0, tmo) == TX_RX_SUCCESS)
		return 0;
	else
		return -1;
}

int ack(int fd, int seq, int tmo)
{
	struct md_message msg = { .seq = seq, .num_bytes = 0 };

	return send_message(fd, &msg, tmo);
}

int nack(int fd, int err, int tmo)
{
	struct md_message msg = { .seq = err, .num_bytes = 0 };

	return send_message(fd, &msg, tmo);
}

int connect_monitor(char *devname)
{
	char path[100];
	int sfd;
	long fl;
	struct sockaddr_un addr;

	sprintf(path, "/var/run/mdadm/%s.sock", devname);
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

int ping_monitor(char *devname)
{
	int sfd = connect_monitor(devname);
	struct md_message msg;
	int err = 0;

	if (sfd < 0)
		return sfd;

	/* try to ping existing socket */
	if (ack(sfd, 0, 0) != 0)
		err = -1;

	/* check the reply */
	if (!err && receive_message(sfd, &msg, 0) != 0)
		err = -1;

	if (msg.seq != 0)
		err = -1;

	close(sfd);
	return err;
}
