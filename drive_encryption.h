/* SPDX-License-Identifier: GPL-2.0-only */
/*
 * Read encryption information for Opal and ATA devices.
 *
 * Copyright (C) 2024 Intel Corporation
 *	Author: Blazej Kucman <blazej.kucman@intel.com>
 */

typedef enum encryption_status {
	/* The drive is not currently encrypted. */
	ENC_STATUS_UNENCRYPTED = 0,
	/* The drive is encrypted and the data is not accessible. */
	ENC_STATUS_LOCKED,
	/* The drive is encrypted but the data is accessible in unencrypted form. */
	ENC_STATUS_UNLOCKED
} encryption_status_t;

typedef enum encryption_ability {
	ENC_ABILITY_NONE = 0,
	ENC_ABILITY_OTHER,
	/* Self encrypted drive */
	ENC_ABILITY_SED
} encryption_ability_t;

typedef struct encryption_information {
	encryption_ability_t ability;
	encryption_status_t status;
} encryption_information_t;

mdadm_status_t
get_nvme_opal_encryption_information(int disk_fd, struct encryption_information *information,
				     const int verbose);
mdadm_status_t
get_ata_encryption_information(int disk_fd, struct encryption_information *information,
			       const int verbose);
const char *get_encryption_ability_string(enum encryption_ability ability);
const char *get_encryption_status_string(enum encryption_status status);
