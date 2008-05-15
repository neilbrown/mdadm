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

struct mdinfo;
struct md_message {
	int seq;
	int num_bytes;
	void *buf;
};

enum md_message_action {
	md_action_ping_monitor,
	md_action_remove_device,
};

struct md_generic_cmd {
	enum md_message_action action;
};

struct md_remove_device_cmd {
	enum md_message_action action;
	dev_t rdev;
};

/* union of all known command types, used to sanity check ->num_bytes
 * on the receive path
 */
union md_message_commands {
	struct md_generic_cmd generic;
	struct md_remove_device_cmd remove;
};

extern const int start_magic;
extern const int end_magic;

extern int receive_message(int fd, struct md_message *msg, int tmo);
extern int send_message(int fd, struct md_message *msg, int tmo);
extern int ack(int fd, int seq, int tmo);
extern int nack(int fd, int err, int tmo);
extern int connect_monitor(char *devname);
extern int ping_monitor(char *devname);
extern int send_remove_device(int fd, dev_t rdev, int seq, int tmo);

