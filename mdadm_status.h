// SPDX-License-Identifier: GPL-2.0-only

#ifndef MDADM_STATUS_H
#define MDADM_STATUS_H

typedef enum mdadm_status {
	MDADM_STATUS_SUCCESS = 0,
	MDADM_STATUS_ERROR,
	MDADM_STATUS_UNDEF,
	MDADM_STATUS_MEM_FAIL,
	MDADM_STATUS_FORKED
} mdadm_status_t;

#endif
