// SPDX-License-Identifier: GPL-2.0-only
/*
 * Read encryption information for Opal and ATA devices.
 *
 * Copyright (C) 2024 Intel Corporation
 *	Author: Blazej Kucman <blazej.kucman@intel.com>
 */

#include "mdadm.h"

#include <asm/types.h>
#include <linux/nvme_ioctl.h>
#include <scsi/sg.h>
#include <scsi/scsi.h>
#include "drive_encryption.h"

#define DEFAULT_SECTOR_SIZE (512)

/*
 * Opal defines
 * TCG Storage Opal SSC 2.01 chapter 3.3.3
 * NVM ExpressTM Revision 1.4c, chapter 5
 */
#define TCG_SECP_01 (0x01)
#define TCG_SECP_00 (0x00)
#define OPAL_DISCOVERY_COMID (0x0001)
#define OPAL_LOCKING_FEATURE (0x0002)
#define OPAL_IO_BUFFER_LEN 2048
#define OPAL_DISCOVERY_FEATURE_HEADER_LEN (4)

/*
 * NVMe defines
 * NVM ExpressTM Revision 1.4c, chapter 5
 */
#define NVME_SECURITY_RECV (0x82)
#define NVME_IDENTIFY (0x06)
#define NVME_IDENTIFY_RESPONSE_LEN 4096
#define NVME_OACS_BYTE_POSITION (256)
#define NVME_IDENTIFY_CONTROLLER_DATA (1)

/*
 * ATA defines
 * ATA/ATAPI Command Set ATA8-ACS
 * SCSI / ATA Translation - 3 (SAT-3)
 * SCSI Primary Commands - 4 (SPC-4)
 * AT Attachment-8 - ATA Serial Transport (ATA8-AST)
 * ATA Command Pass-Through
 */
#define ATA_IDENTIFY (0xec)
#define ATA_TRUSTED_RECEIVE (0x5c)
#define ATA_SECURITY_WORD_POSITION (128)
#define HDIO_DRIVE_CMD (0x031f)
#define ATA_TRUSTED_COMPUTING_POS (48)
#define ATA_PASS_THROUGH_12 (0xa1)
#define ATA_IDENTIFY_RESPONSE_LEN (512)
#define ATA_PIO_DATA_IN (4)
#define SG_CHECK_CONDITION (0x02)
#define ATA_STATUS_RETURN_DESCRIPTOR (0x09)
#define ATA_PT_INFORMATION_AVAILABLE_ASCQ (0x1d)
#define ATA_PT_INFORMATION_AVAILABLE_ASC (0x00)
#define ATA_INQUIRY_LENGTH (0x0c)
#define SG_INTERFACE_ID 'S'
#define SG_IO_TIMEOUT (60000)
#define SG_SENSE_SIZE (32)
#define SENSE_DATA_CURRENT_FIXED (0x70)
#define SENSE_DATA_CURRENT_DESC (0x72)
#define SENSE_CURRENT_RES_DESC_POS (8)
#define SENSE_RESPONSE_CODE_MASK (0x7f)
#define SG_DRIVER_SENSE	(0x08)

typedef enum drive_feature_support_status {
	/* Drive feature is supported. */
	DRIVE_FEAT_SUP_ST = 0,
	/* Drive feature is not supported. */
	DRIVE_FEAT_NOT_SUP_ST,
	/* Drive feature support check failed. */
	DRIVE_FEAT_CHECK_FAILED_ST
} drive_feat_sup_st;

/* TCG Storage Opal SSC 2.01 chapter 3.1.1.3 */
typedef struct opal_locking_feature {
	/* feature header */
	__u16 feature_code;
	__u8 reserved : 4;
	__u8 version : 4;
	__u8 description_length;
	/* feature description */
	__u8 locking_supported : 1;
	__u8 locking_enabled : 1;
	__u8 locked : 1;
	__u8 media_encryption : 1;
	__u8 mbr_enabled : 1;
	__u8 mbr_done : 1;
	__u8 mbr_shadowing_not_supported : 1;
	__u8 hw_reset_for_dor_supported : 1;
	__u8 reserved1[11];
} __attribute__((__packed__)) opal_locking_feature_t;

/* TCG Storage Opal SSC 2.01 chapter 3.1.1.1 */
typedef struct opal_level0_header {
	__u32 length;
	__u32 version;
	__u64 reserved;
	__u8 vendor_specific[32];
} opal_level0_header_t;

/**
 * NVM ExpressTM Revision 1.4c, Figure 249
 * Structure specifies only OACS filed, which is needed in the current use case.
 */
typedef struct nvme_identify_ctrl {
	__u8 reserved[255];
	__u16 oacs;
	__u8 reserved2[3839];
} nvme_identify_ctrl_t;

/* SCSI Primary Commands - 4 (SPC-4), Table 512 */
typedef struct supported_security_protocols {
	__u8  reserved[6];
	__u16 list_length;
	__u8  list[504];
} supported_security_protocols_t;

/* ATA/ATAPI Command Set - 3 (ACS-3), Table 45 */
typedef struct ata_security_status {
	__u16 security_supported : 1;
	__u16 security_enabled : 1;
	__u16 security_locked : 1;
	__u16 security_frozen : 1;
	__u16 security_count_expired : 1;
	__u16 enhanced_security_erase_supported : 1;
	__u16 reserved1 : 2;
	__u16 security_level : 1;
	__u16 reserved2 : 7;
} __attribute__((__packed__)) ata_security_status_t;

/* ATA/ATAPI Command Set - 3 (ACS-3), Table 45 */
typedef struct ata_trusted_computing {
	__u16 tc_feature :1;
	__u16 reserved : 13;
	__u16 var1 : 1;
	__u16 var2 : 1;
} __attribute__((__packed__)) ata_trusted_computing_t;

mapping_t encryption_ability_map[] = {
	{ "None", ENC_ABILITY_NONE },
	{ "Other", ENC_ABILITY_OTHER },
	{ "SED", ENC_ABILITY_SED },
	{ NULL, UnSet }
};

mapping_t encryption_status_map[] = {
	{ "Unencrypted", ENC_STATUS_UNENCRYPTED },
	{ "Locked", ENC_STATUS_LOCKED },
	{ "Unlocked", ENC_STATUS_UNLOCKED },
	{ NULL, UnSet }
};

/**
 * get_encryption_ability_string() - get encryption ability name string.
 * @ability: encryption ability enum.
 *
 * Return: encryption ability string.
 */
const char *get_encryption_ability_string(enum encryption_ability ability)
{
	return map_num_s(encryption_ability_map, ability);
}

/**
 * get_encryption_status_string() - get encryption status name string.
 * @ability: encryption status enum.
 *
 * Return: encryption status string.
 */
const char *get_encryption_status_string(enum encryption_status status)
{
	return map_num_s(encryption_status_map, status);
}

/**
 * get_opal_locking_feature_description() - get opal locking feature description.
 * @response: response from Opal Discovery Level 0.
 *
 * Based on the documentation TCG Storage Opal SSC 2.01 chapter 3.1.1,
 * a Locking feature is searched for in Opal Level 0 Discovery response.
 *
 * Return: if locking feature is found, pointer to struct %opal_locking_feature_t, NULL otherwise.
 */
static opal_locking_feature_t *get_opal_locking_feature_description(__u8 *response)
{
	opal_level0_header_t *response_header = (opal_level0_header_t *)response;
	int features_length = __be32_to_cpu(response_header->length);
	int current_position = sizeof(*response_header);

	while (current_position < features_length) {
		opal_locking_feature_t *feature;

		feature = (opal_locking_feature_t *)(response + current_position);

		if (__be16_to_cpu(feature->feature_code) == OPAL_LOCKING_FEATURE)
			return feature;

		current_position += feature->description_length + OPAL_DISCOVERY_FEATURE_HEADER_LEN;
	}

	return NULL;
}

/**
 * nvme_security_recv_ioctl() - nvme security receive ioctl.
 * @disk_fd: a disk file descriptor.
 * @sec_protocol: security protocol.
 * @comm_id: command id.
 * @response_buffer: response buffer to fill out.
 * @buf_size: response buffer size.
 * @verbose: verbose flag.
 *
 * Based on the documentations TCG Storage Opal SSC 2.01 chapter 3.3.3 and
 * NVM ExpressTM Revision 1.4c, chapter 5.25,
 * read security receive command via ioctl().
 * On success, @response_buffer is completed.
 *
 * Return: %MDADM_STATUS_SUCCESS on success, %MDADM_STATUS_ERROR otherwise.
 */
static mdadm_status_t
nvme_security_recv_ioctl(int disk_fd, __u8 sec_protocol, __u16 comm_id, void *response_buffer,
			 size_t buf_size, const int verbose)
{
	struct nvme_admin_cmd nvme_cmd = {0};
	int status;

	nvme_cmd.opcode = NVME_SECURITY_RECV;
	nvme_cmd.cdw10 = sec_protocol << 24 | comm_id << 8;
	nvme_cmd.cdw11 = buf_size;
	nvme_cmd.data_len = buf_size;
	nvme_cmd.addr = (__u64)(uintptr_t)response_buffer;

	status = ioctl(disk_fd, NVME_IOCTL_ADMIN_CMD, &nvme_cmd);
	if (status != 0) {
		pr_vrb("Failed to read NVMe security receive ioctl() for device /dev/%s, status: %d\n",
		       fd2kname(disk_fd), status);
		return MDADM_STATUS_ERROR;
	}

	return MDADM_STATUS_SUCCESS;
}

/**
 * nvme_identify_ioctl() - NVMe identify ioctl.
 * @disk_fd: a disk file descriptor.
 * @response_buffer: response buffer to fill out.
 * @buf_size: response buffer size.
 * @verbose: verbose flag.
 *
 * Based on the documentations TCG Storage Opal SSC 2.01 chapter 3.3.3 and
 * NVM ExpressTM Revision 1.4c, chapter 5.25,
 * read NVMe identify via ioctl().
 * On success, @response_buffer will be completed.
 *
 * Return: %MDADM_STATUS_SUCCESS on success, %MDADM_STATUS_ERROR otherwise.
 */
static mdadm_status_t
nvme_identify_ioctl(int disk_fd, void *response_buffer, size_t buf_size, const int verbose)
{
	struct nvme_admin_cmd nvme_cmd = {0};
	int status;

	nvme_cmd.opcode = NVME_IDENTIFY;
	nvme_cmd.cdw10 = NVME_IDENTIFY_CONTROLLER_DATA;
	nvme_cmd.data_len = buf_size;
	nvme_cmd.addr = (__u64)(uintptr_t)response_buffer;

	status = ioctl(disk_fd, NVME_IOCTL_ADMIN_CMD, &nvme_cmd);
	if (status != 0) {
		pr_vrb("Failed to read NVMe identify ioctl() for device /dev/%s, status: %d\n",
		       fd2kname(disk_fd), status);
		return MDADM_STATUS_ERROR;
	}

	return MDADM_STATUS_SUCCESS;
}

/**
 * is_sec_prot_01h_supported() - check if security protocol 01h supported.
 * @security_protocols: struct with response from disk (NVMe, SATA) describing supported
 * security protocols.
 *
 * Return: true if TCG_SECP_01 found, false otherwise.
 */
static bool is_sec_prot_01h_supported(supported_security_protocols_t *security_protocols)
{
	int list_length = be16toh(security_protocols->list_length);
	int index;

	for (index = 0 ; index < list_length; index++) {
		if (security_protocols->list[index] == TCG_SECP_01)
			return true;
	}

	return false;
}

/**
 * is_sec_prot_01h_supported_nvme() - check if security protocol 01h supported for given NVMe disk.
 * @disk_fd: a disk file descriptor.
 * @verbose: verbose flag.
 *
 * Return: %DRIVE_FEAT_SUP_ST if TCG_SECP_01 supported, %DRIVE_FEAT_NOT_SUP_ST if not supported,
 * %DRIVE_FEAT_CHECK_FAILED_ST if failed to check.
 */
static drive_feat_sup_st is_sec_prot_01h_supported_nvme(int disk_fd, const int verbose)
{
	supported_security_protocols_t security_protocols = {0};

	/* security_protocol: TCG_SECP_00, comm_id: not applicable */
	if (nvme_security_recv_ioctl(disk_fd, TCG_SECP_00, 0x0, &security_protocols,
				     sizeof(security_protocols), verbose))
		return DRIVE_FEAT_CHECK_FAILED_ST;

	if (is_sec_prot_01h_supported(&security_protocols))
		return DRIVE_FEAT_SUP_ST;

	return DRIVE_FEAT_NOT_SUP_ST;
}

/**
 * is_nvme_sec_send_recv_supported() - check if Security Send and Security Receive is supported.
 * @disk_fd: a disk file descriptor.
 * @verbose: verbose flag.
 *
 * Check if "Optional Admin Command Support" bit 0 is set in NVMe identify.
 * Bit 0 set to 1 means controller supports the Security Send and Security Receive commands.
 *
 * Return: %DRIVE_FEAT_SUP_ST if security send/receive supported,
 * %DRIVE_FEAT_NOT_SUP_ST if not supported, %DRIVE_FEAT_CHECK_FAILED_ST if check failed.
 */
static drive_feat_sup_st is_nvme_sec_send_recv_supported(int disk_fd, const int verbose)
{
	nvme_identify_ctrl_t nvme_identify = {0};
	int status = 0;

	status = nvme_identify_ioctl(disk_fd, &nvme_identify, sizeof(nvme_identify), verbose);
	if (status)
		return DRIVE_FEAT_CHECK_FAILED_ST;

	if ((__le16_to_cpu(nvme_identify.oacs) & 0x1) == 0x1)
		return DRIVE_FEAT_SUP_ST;

	return DRIVE_FEAT_NOT_SUP_ST;
}

/**
 * get_opal_encryption_information() - get Opal encryption information.
 * @buffer: buffer with Opal Level 0 Discovery response.
 * @information: struct to fill out, describing encryption status of disk.
 *
 * If Locking feature frame is in response from Opal Level 0 discovery, &encryption_information_t
 * structure is completed with status and ability otherwise the status is set to &None.
 * For possible encryption statuses and abilities,
 * please refer to enums &encryption_status and &encryption_ability.
 *
 * Return: %MDADM_STATUS_SUCCESS on success, %MDADM_STATUS_ERROR otherwise.
 */
static mdadm_status_t get_opal_encryption_information(__u8 *buffer,
						      encryption_information_t *information)
{
	opal_locking_feature_t *opal_locking_feature =
					get_opal_locking_feature_description(buffer);

	if (!opal_locking_feature)
		return MDADM_STATUS_ERROR;

	if (opal_locking_feature->locking_supported == 1) {
		information->ability = ENC_ABILITY_SED;

		if (opal_locking_feature->locking_enabled == 0)
			information->status = ENC_STATUS_UNENCRYPTED;
		else if (opal_locking_feature->locked == 1)
			information->status = ENC_STATUS_LOCKED;
		else
			information->status = ENC_STATUS_UNLOCKED;
	} else {
		information->ability = ENC_ABILITY_NONE;
		information->status = ENC_STATUS_UNENCRYPTED;
	}

	return MDADM_STATUS_SUCCESS;
}

/**
 * get_nvme_opal_encryption_information() - get NVMe Opal encryption information.
 * @disk_fd: a disk file descriptor.
 * @information: struct to fill out, describing encryption status of disk.
 * @verbose: verbose flag.
 *
 * In case the disk supports Opal Level 0 discovery, &encryption_information_t structure
 * is completed with status and ability based on ioctl response,
 * otherwise the ability is set to %ENC_ABILITY_NONE and &status to %ENC_STATUS_UNENCRYPTED.
 * As the current use case does not need the knowledge of Opal support, if there is no support,
 * %MDADM_STATUS_SUCCESS will be returned, with the values described above.
 * For possible encryption statuses and abilities,
 * please refer to enums &encryption_status and &encryption_ability.
 *
 * %MDADM_STATUS_SUCCESS on success, %MDADM_STATUS_ERROR otherwise.
 */
mdadm_status_t
get_nvme_opal_encryption_information(int disk_fd, encryption_information_t *information,
				     const int verbose)
{
	__u8 buffer[OPAL_IO_BUFFER_LEN];
	int sec_send_recv_supported = 0;
	int protocol_01h_supported = 0;
	mdadm_status_t status;

	information->ability = ENC_ABILITY_NONE;
	information->status = ENC_STATUS_UNENCRYPTED;

	sec_send_recv_supported = is_nvme_sec_send_recv_supported(disk_fd, verbose);
	if (sec_send_recv_supported == DRIVE_FEAT_CHECK_FAILED_ST)
		return MDADM_STATUS_ERROR;

	/* Opal not supported */
	if (sec_send_recv_supported == DRIVE_FEAT_NOT_SUP_ST)
		return MDADM_STATUS_SUCCESS;

	/**
	 * sec_send_recv_supported determine that it should be possible to read
	 * supported sec protocols
	 */
	protocol_01h_supported = is_sec_prot_01h_supported_nvme(disk_fd, verbose);
	if (protocol_01h_supported == DRIVE_FEAT_CHECK_FAILED_ST)
		return MDADM_STATUS_ERROR;

	/* Opal not supported */
	if (sec_send_recv_supported == DRIVE_FEAT_SUP_ST &&
	    protocol_01h_supported == DRIVE_FEAT_NOT_SUP_ST)
		return MDADM_STATUS_SUCCESS;

	if (nvme_security_recv_ioctl(disk_fd, TCG_SECP_01, OPAL_DISCOVERY_COMID, (void *)&buffer,
				     OPAL_IO_BUFFER_LEN, verbose))
		return MDADM_STATUS_ERROR;

	status = get_opal_encryption_information((__u8 *)&buffer, information);
	if (status)
		pr_vrb("Locking feature description not found in Level 0 discovery response. Device /dev/%s.\n",
		       fd2kname(disk_fd));

	if (information->ability == ENC_ABILITY_NONE)
		assert(information->status == ENC_STATUS_UNENCRYPTED);

	return status;
}

/**
 * ata_pass_through12_ioctl() - ata pass through12 ioctl.
 * @disk_fd: a disk file descriptor.
 * @ata_command: ata command.
 * @sec_protocol: security protocol.
 * @comm_id: additional command id.
 * @response_buffer: response buffer to fill out.
 * @buf_size: response buffer size.
 * @verbose: verbose flag.
 *
 * Based on the documentations ATA Command Pass-Through, chapter 13.2.2 and
 * ATA Translation - 3 (SAT-3), send read ata pass through 12 command via ioctl().
 * On success, @response_buffer will be completed.
 *
 * Return: %MDADM_STATUS_SUCCESS on success, %MDADM_STATUS_ERROR on fail.
 */
static mdadm_status_t
ata_pass_through12_ioctl(int disk_fd, __u8 ata_command,  __u8 sec_protocol, __u16 comm_id,
			 void *response_buffer, size_t buf_size, const int verbose)
{
	__u8 cdb[ATA_INQUIRY_LENGTH] = {0};
	__u8 sense[SG_SENSE_SIZE] = {0};
	__u8 sense_response_code;
	__u8 *sense_desc = NULL;
	sg_io_hdr_t sg = {0};

	/*
	 * ATA Command Pass-Through, chapter 13.2.2
	 * SCSI Primary Commands - 4 (SPC-4)
	 * ATA Translation - 3 (SAT-3)
	 */
	cdb[0] = ATA_PASS_THROUGH_12;
	/* protocol, bits 1-4 */
	cdb[1] = ATA_PIO_DATA_IN << 1;
	/* Bytes: CK_COND=1, T_DIR = 1, BYTE_BLOCK = 1, Length in Sector Count = 2 */
	cdb[2] = 0x2E;
	cdb[3] = sec_protocol;
	/* Sector count */
	cdb[4] = buf_size / DEFAULT_SECTOR_SIZE;
	cdb[6] = (comm_id) & 0xFF;
	cdb[7] = (comm_id >> 8) & 0xFF;
	cdb[9] = ata_command;

	sg.interface_id = SG_INTERFACE_ID;
	sg.cmd_len = sizeof(cdb);
	sg.mx_sb_len = sizeof(sense);
	sg.dxfer_direction = SG_DXFER_FROM_DEV;
	sg.dxfer_len = buf_size;
	sg.dxferp = response_buffer;
	sg.cmdp = cdb;
	sg.sbp = sense;
	sg.timeout = SG_IO_TIMEOUT;
	sg.usr_ptr = NULL;

	if (ioctl(disk_fd, SG_IO, &sg) < 0) {
		pr_vrb("Failed ata passthrough12 ioctl. Device: /dev/%s.\n", fd2kname(disk_fd));
		return MDADM_STATUS_ERROR;
	}

	if ((sg.status && sg.status != SG_CHECK_CONDITION) || sg.host_status ||
	    (sg.driver_status && sg.driver_status != SG_DRIVER_SENSE)) {
		pr_vrb("Failed ata passthrough12 ioctl. Device: /dev/%s.\n", fd2kname(disk_fd));
		pr_vrb("SG_IO error: ATA_12 Status: %d Host Status: %d, Driver Status: %d\n",
		       sg.status, sg.host_status, sg.driver_status);
		return MDADM_STATUS_ERROR;
	}

	sense_response_code = sense[0] & SENSE_RESPONSE_CODE_MASK;
	/* verify expected sense response code */
	if (!(sense_response_code == SENSE_DATA_CURRENT_DESC ||
	      sense_response_code == SENSE_DATA_CURRENT_FIXED)) {
		pr_vrb("Failed ata passthrough12 ioctl. Device: /dev/%s.\n", fd2kname(disk_fd));
		return MDADM_STATUS_ERROR;
	}

	sense_desc = sense + SENSE_CURRENT_RES_DESC_POS;
	/* verify sense data current response with descriptor format */
	if (sense_response_code == SENSE_DATA_CURRENT_DESC &&
	    !(sense_desc[0] == ATA_STATUS_RETURN_DESCRIPTOR &&
	    sense_desc[1] == ATA_INQUIRY_LENGTH)) {
		pr_vrb("Failed ata passthrough12 ioctl. Device: /dev/%s. Sense data ASC: %d, ASCQ: %d.\n",
		       fd2kname(disk_fd), sense[2], sense[3]);
		return MDADM_STATUS_ERROR;
	}

	/* verify sense data current response with fixed format */
	if (sense_response_code == SENSE_DATA_CURRENT_FIXED &&
	    !(sense[12] == ATA_PT_INFORMATION_AVAILABLE_ASC &&
	    sense[13] == ATA_PT_INFORMATION_AVAILABLE_ASCQ)) {
		pr_vrb("Failed ata passthrough12 ioctl. Device: /dev/%s. Sense data ASC: %d, ASCQ: %d.\n",
		       fd2kname(disk_fd), sense[12], sense[13]);
		return MDADM_STATUS_ERROR;
	}

	return MDADM_STATUS_SUCCESS;
}

/**
 * is_sec_prot_01h_supported_ata() - check if security protocol 01h supported for given SATA disk.
 * @disk_fd: a disk file descriptor.
 * @verbose: verbose flag.
 *
 * Return: %DRIVE_FEAT_SUP_ST if TCG_SECP_01 supported, %DRIVE_FEAT_NOT_SUP_ST if not supported,
 * %DRIVE_FEAT_CHECK_FAILED_ST if failed.
 */
static drive_feat_sup_st is_sec_prot_01h_supported_ata(int disk_fd, const int verbose)
{
	supported_security_protocols_t security_protocols;

	mdadm_status_t result = ata_pass_through12_ioctl(disk_fd, ATA_TRUSTED_RECEIVE, TCG_SECP_00,
							 0x0, &security_protocols,
							 sizeof(security_protocols), verbose);
	if (result)
		return DRIVE_FEAT_CHECK_FAILED_ST;

	if (is_sec_prot_01h_supported(&security_protocols))
		return DRIVE_FEAT_SUP_ST;

	return DRIVE_FEAT_NOT_SUP_ST;
}

/**
 * is_ata_trusted_computing_supported() - check if ata trusted computing supported.
 * @buffer: buffer with ATA identify response, not NULL.
 *
 * Return: true if trusted computing bit set, false otherwise.
 */
bool is_ata_trusted_computing_supported(__u16 *buffer)
{
	/* Added due to warnings from the compiler about a possible uninitialized variable below. */
	assert(buffer);

	__u16 security_tc_frame = __le16_to_cpu(buffer[ATA_TRUSTED_COMPUTING_POS]);
	ata_trusted_computing_t *security_tc = (ata_trusted_computing_t *)&security_tc_frame;

	if (security_tc->tc_feature == 1)
		return true;

	return false;
}

/**
 * get_ata_standard_security_status() - get ATA disk encryption information from ATA identify.
 * @buffer: buffer with response from ATA identify, not NULL.
 * @information: struct to fill out, describing encryption status of disk.
 *
 * The function based on the Security status frame from ATA identify,
 * completed encryption information.
 * For possible encryption statuses and abilities,
 * please refer to enums &encryption_status and &encryption_ability.
 *
 * Return: %MDADM_STATUS_SUCCESS on success, %MDADM_STATUS_ERROR on fail.
 */
static mdadm_status_t get_ata_standard_security_status(__u16 *buffer,
						       struct encryption_information *information)
{
	/* Added due to warnings from the compiler about a possible uninitialized variable below. */
	assert(buffer);

	__u16 security_status_frame = __le16_to_cpu(buffer[ATA_SECURITY_WORD_POSITION]);
	ata_security_status_t *security_status = (ata_security_status_t *)&security_status_frame;

	if (!security_status->security_supported) {
		information->ability = ENC_ABILITY_NONE;
		information->status = ENC_STATUS_UNENCRYPTED;

		return MDADM_STATUS_SUCCESS;
	}

	information->ability = ENC_ABILITY_OTHER;

	if (security_status->security_enabled == 0)
		information->status = ENC_STATUS_UNENCRYPTED;
	else if (security_status->security_locked == 1)
		information->status = ENC_STATUS_LOCKED;
	else
		information->status = ENC_STATUS_UNLOCKED;

	return MDADM_STATUS_SUCCESS;
}

/**
 * is_ata_opal() - check if SATA disk support Opal.
 * @disk_fd: a disk file descriptor.
 * @buffer: buffer with ATA identify response.
 * @verbose: verbose flag.
 *
 * Return: %DRIVE_FEAT_SUP_ST if TCG_SECP_01 supported, %DRIVE_FEAT_NOT_SUP_ST if not supported,
 * %DRIVE_FEAT_CHECK_FAILED_ST if failed to check.
 */
static drive_feat_sup_st is_ata_opal(int disk_fd, __u16 *buffer_identify, const int verbose)
{
	bool tc_status = is_ata_trusted_computing_supported(buffer_identify);
	drive_feat_sup_st tcg_sec_prot_status;

	if (!tc_status)
		return DRIVE_FEAT_NOT_SUP_ST;

	tcg_sec_prot_status = is_sec_prot_01h_supported_ata(disk_fd, verbose);

	if (tcg_sec_prot_status == DRIVE_FEAT_CHECK_FAILED_ST) {
		pr_vrb("Failed to verify if security protocol 01h supported. Device /dev/%s.\n",
		       fd2kname(disk_fd));
		return DRIVE_FEAT_CHECK_FAILED_ST;
	}

	if (tc_status && tcg_sec_prot_status == DRIVE_FEAT_SUP_ST)
		return DRIVE_FEAT_SUP_ST;

	return DRIVE_FEAT_NOT_SUP_ST;
}

/**
 * get_ata_encryption_information() - get ATA disk encryption information.
 * @disk_fd: a disk file descriptor.
 * @information: struct to fill out, describing encryption status of disk.
 * @verbose: verbose flag.
 *
 * The function reads information about encryption, if the disk supports Opal,
 * the information is completed based on Opal Level 0 discovery, otherwise,
 * based on ATA security status frame from ATA identification response.
 * For possible encryption statuses and abilities,
 * please refer to enums &encryption_status and &encryption_ability.
 *
 * Based on the documentations ATA/ATAPI Command Set ATA8-ACS and
 * AT Attachment-8 - ATA Serial Transport (ATA8-AST).
 *
 * Return: %MDADM_STATUS_SUCCESS on success, %MDADM_STATUS_ERROR on fail.
 */
mdadm_status_t
get_ata_encryption_information(int disk_fd, struct encryption_information *information,
			       const int verbose)
{
	__u8 buffer_opal_level0_discovery[OPAL_IO_BUFFER_LEN] = {0};
	__u16 buffer_identify[ATA_IDENTIFY_RESPONSE_LEN] = {0};
	drive_feat_sup_st ata_opal_status;
	mdadm_status_t status;

	/* Get disk ATA identification */
	status = ata_pass_through12_ioctl(disk_fd, ATA_IDENTIFY, 0x0, 0x0, buffer_identify,
					  sizeof(buffer_identify), verbose);
	if (status == MDADM_STATUS_ERROR)
		return MDADM_STATUS_ERROR;

	/* Possible OPAL support, further checks require tpm_enabled.*/
	if (is_ata_trusted_computing_supported(buffer_identify)) {
		/* OPAL SATA encryption checking disabled. */
		if (conf_get_sata_opal_encryption_no_verify())
			return MDADM_STATUS_SUCCESS;

		if (!sysfs_is_libata_allow_tpm_enabled(verbose)) {
			pr_vrb("Detected SATA drive /dev/%s with Trusted Computing support.\n",
			       fd2kname(disk_fd));
			pr_vrb("Cannot verify encryption state. Requires libata.tpm_enabled=1.\n");
			return MDADM_STATUS_ERROR;
		}
	}

	ata_opal_status = is_ata_opal(disk_fd, buffer_identify, verbose);
	if (ata_opal_status == DRIVE_FEAT_CHECK_FAILED_ST)
		return MDADM_STATUS_ERROR;

	if (ata_opal_status == DRIVE_FEAT_NOT_SUP_ST)
		return get_ata_standard_security_status(buffer_identify, information);

	/* SATA Opal */
	status = ata_pass_through12_ioctl(disk_fd, ATA_TRUSTED_RECEIVE, TCG_SECP_01,
					  OPAL_DISCOVERY_COMID, buffer_opal_level0_discovery,
					  OPAL_IO_BUFFER_LEN, verbose);
	if (status != MDADM_STATUS_SUCCESS)
		return MDADM_STATUS_ERROR;

	return get_opal_encryption_information(buffer_opal_level0_discovery, information);
}
