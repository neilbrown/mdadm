/*
 * mdadm - manage Linux "md" devices aka RAID arrays.
 *
 * Copyright (C) 2001-2009 Neil Brown <neilb@suse.de>
 *
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
 *
 *    Author: Neil Brown
 *    Email: <neilb@suse.de>
 */

#include	"mdadm.h"
#include	"dlink.h"
#include	"xmalloc.h"

#include	<dirent.h>
#include	<glob.h>
#include	<fnmatch.h>
#include	<ctype.h>
#include	<pwd.h>
#include	<grp.h>

/*
 * Read the config file
 *
 * conf_get_uuids gets a list of devicename+uuid pairs
 * conf_get_devs gets device names after expanding wildcards
 *
 * Each keeps the returned list and frees it when asked to make
 * a new list.
 *
 * The format of the config file needs to be fairly extensible.
 * Now, arrays only have names and uuids and devices merely are.
 * But later arrays might want names, and devices might want superblock
 * versions, and who knows what else.
 * I like free format, abhore backslash line continuation, adore
 *   indentation for structure and am ok about # comments.
 *
 * So, each line that isn't blank or a #comment must either start
 *  with a key word, and not be indented, or must start with a
 *  non-key-word and must be indented.
 *
 * Keywords are DEVICE and ARRAY ... and several others.
 * DEV{ICE} introduces some devices that might contain raid components.
 * e.g.
 *   DEV style=0 /dev/sda* /dev/hd*
 *   DEV style=1 /dev/sd[b-f]*
 * ARR{AY} describes an array giving md device and attributes like uuid=whatever
 * e.g.
 *   ARRAY /dev/md0 uuid=whatever name=something
 * Spaces separate words on each line.  Quoting, with "" or '' protects them,
 * but may not wrap over lines
 *
 */
#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#endif

#ifndef CONFFILE
#define CONFFILE "/etc/mdadm.conf"
#endif
#ifndef CONFFILE2
/* for Debian compatibility .... */
#define CONFFILE2 "/etc/mdadm/mdadm.conf"
#endif
char DefaultConfFile[] = CONFFILE;
char DefaultConfDir[] = CONFFILE ".d";
char DefaultAltConfFile[] = CONFFILE2;
char DefaultAltConfDir[] = CONFFILE2 ".d";

enum linetype { Devices, Array, Mailaddr, Mailfrom, Program, CreateDev,
		Homehost, HomeCluster, AutoMode, Policy, PartPolicy, Sysfs,
		MonitorDelay, EncryptionNoVerify, LTEnd };
char *keywords[] = {
	[Devices]  = "devices",
	[Array]    = "array",
	[Mailaddr] = "mailaddr",
	[Mailfrom] = "mailfrom",
	[Program]  = "program",
	[CreateDev]= "create",
	[Homehost] = "homehost",
	[HomeCluster] = "homecluster",
	[AutoMode] = "auto",
	[Policy]   = "policy",
	[PartPolicy]="part-policy",
	[Sysfs]    = "sysfs",
	[MonitorDelay] = "monitordelay",
	[EncryptionNoVerify] = "ENCRYPTION_NO_VERIFY",
	[LTEnd]    = NULL
};

/*
 * match_keyword returns an index into the keywords array, or -1 for no match
 * case is ignored, and at least three characters must be given
 */

int match_keyword(char *word)
{
	int len = strlen(word);
	int n;

	if (len < 3)
		return -1;
	for (n = 0; keywords[n]; n++) {
		if (strncasecmp(word, keywords[n], len) == 0)
			return n;
	}

	return -1;
}

/**
 * is_devname_ignore() - check if &devname is a special "<ignore>" keyword.
 */
bool is_devname_ignore(const char *devname)
{
	static const char keyword[] = "<ignore>";

	if (strcasecmp(devname, keyword) == 0)
		return true;
	return false;
}

/**
 * ident_log() - generate and write message to the user.
 * @param_name: name of the property.
 * @value: value of the property.
 * @reason: meaningful description.
 * @cmdline: context dependent actions, see below.
 *
 * The function is made to provide similar error handling for both config and cmdline. The behavior
 * is configurable via @cmdline. Message has following format:
 * "Value "@value" cannot be set for @param_name. Reason: @reason."
 *
 * If cmdline is on:
 * - message is written to stderr.
 * otherwise:
 * - message is written to stdout.
 * - "Value ignored" is added at the end of the message.
 */
static void ident_log(const char *param_name, const char *value, const char *reason,
		      const bool cmdline)
{
	if (cmdline == true)
		pr_err("Value \"%s\" cannot be set as %s. Reason: %s.\n", value, param_name,
		       reason);
	else
		pr_info("Value \"%s\" cannot be set as %s. Reason: %s. Value ignored.\n", value,
			param_name, reason);
}

/**
 * ident_init() - Set defaults.
 * @ident: ident pointer, not NULL.
 */
inline void ident_init(struct mddev_ident *ident)
{
	assert(ident);

	ident->assembled = false;
	ident->btype = BitmapUnknown;
	ident->container = NULL;
	ident->devices = NULL;
	ident->devname = NULL;
	ident->level = UnSet;
	ident->member = NULL;
	ident->name[0] = 0;
	ident->next = NULL;
	ident->raid_disks = UnSet;
	ident->spare_group = NULL;
	ident->spare_disks = 0;
	ident->st = NULL;
	ident->super_minor = UnSet;
	ident->uuid[0] = 0;
	ident->uuid_set = 0;
}

/** ident_check_name() - helper function to verify name.
 * @name: name to check.
 * @prop_name: the name of the property it is validated against, used for logging.
 * @cmdline: context dependent actions.
 *
 * @name must follow name's criteria, be POSIX compatible and does not have leading dot.
 */
static mdadm_status_t ident_check_name(const char *name, const char *prop_name, const bool cmdline)
{
	if (!is_string_lq(name, MD_NAME_MAX + 1)) {
		ident_log(prop_name, name, "Too long or empty", cmdline);
		return MDADM_STATUS_ERROR;
	}

	if (*name == '.') {
		/* MD device should not be considered as hidden. */
		ident_log(prop_name, name, "Leading dot forbidden", cmdline);
		return MDADM_STATUS_ERROR;
	}

	if (!is_name_posix_compatible(name)) {
		ident_log(prop_name, name, "Not POSIX compatible", cmdline);
		return MDADM_STATUS_ERROR;
	}

	return MDADM_STATUS_SUCCESS;
}

/**
 * _ident_set_devname() - verify devname and set it in &mddev_ident.
 * @ident: pointer to &mddev_ident.
 * @devname: devname to be set.
 * @cmdline: context dependent actions. If set, ignore keyword is not allowed.
 *
 * @devname can have following forms:
 *	'<ignore>' keyword (if allowed)
 *	/dev/md{number}
 *	/dev/md_d{number} (legacy)
 *	/dev/md_{name}
 *	/dev/md/{name}
 *	{name}
 *
 * If verification passed, duplicate memory and set devname in @ident.
 *
 * Return: %MDADM_STATUS_SUCCESS or %MDADM_STATUS_ERROR.
 */
mdadm_status_t _ident_set_devname(struct mddev_ident *ident, const char *devname,
				  const bool cmdline)
{
	assert(ident);
	assert(devname);

	static const char named_dev_pref[] = DEV_NUM_PREF "_";
	static const int named_dev_pref_size = sizeof(named_dev_pref) - 1;
	const char *prop_name = "devname";
	mdadm_status_t ret;
	const char *name;

	if (ident->devname) {
		ident_log(prop_name, devname, "Already defined", cmdline);
		return MDADM_STATUS_ERROR;
	}

	if (is_devname_ignore(devname) == true) {
		if (!cmdline)
			goto pass;

		ident_log(prop_name, devname, "Special keyword is invalid in this context",
			  cmdline);
		return MDADM_STATUS_ERROR;
	}

	if (is_devname_md_numbered(devname) == true || is_devname_md_d_numbered(devname) == true)
		goto pass;

	if (strncmp(devname, DEV_MD_DIR, DEV_MD_DIR_LEN) == 0)
		name = devname + DEV_MD_DIR_LEN;
	else if (strncmp(devname, named_dev_pref, named_dev_pref_size) == 0)
		name = devname + named_dev_pref_size;
	else
		name = devname;

	ret = ident_check_name(name, prop_name, cmdline);
	if (ret)
		return ret;
pass:
	ident->devname = xstrdup(devname);
	return MDADM_STATUS_SUCCESS;
}

/**
 * _ident_set_name() - set name in &mddev_ident.
 * @ident: pointer to &mddev_ident.
 * @name: name to be set.
 *
 * If criteria passed, set name in @ident.
 * Note: name is not used by config file, it for cmdline only.
 *
 * Return: %MDADM_STATUS_SUCCESS or %MDADM_STATUS_ERROR.
 */
mdadm_status_t ident_set_name(struct mddev_ident *ident, const char *name)
{
	assert(name);
	assert(ident);

	const char *prop_name = "name";
	mdadm_status_t ret;

	if (ident->name[0]) {
		ident_log(prop_name, name, "Already defined", true);
		return MDADM_STATUS_ERROR;
	}

	ret = ident_check_name(name, prop_name, true);
	if (ret)
		return ret;

	snprintf(ident->name, MD_NAME_MAX + 1, "%s", name);
	return MDADM_STATUS_SUCCESS;
}

/**
 * ident_set_devname()- exported, for cmdline.
 */
mdadm_status_t ident_set_devname(struct mddev_ident *ident, const char *name)
{
	return _ident_set_devname(ident, name, true);
}

struct conf_dev {
	struct conf_dev *next;
	char *name;
} *cdevlist = NULL;

struct mddev_dev *load_partitions(void)
{
	FILE *f = fopen("/proc/partitions", "r");
	char buf[1024];
	struct mddev_dev *rv = NULL;

	if (f == NULL) {
		pr_err("cannot open /proc/partitions\n");
		return NULL;
	}
	while (fgets(buf, 1024, f)) {
		int major, minor;
		char *name, *mp;
		struct mddev_dev *d;

		buf[1023] = '\0';
		if (buf[0] != ' ')
			continue;
		major = strtoul(buf, &mp, 10);
		if (mp == buf || *mp != ' ')
			continue;
		minor = strtoul(mp, NULL, 10);

		name = map_dev(major, minor, 1);
		if (!name)
			continue;
		d = xcalloc(1, sizeof(*d));
		d->devname = xstrdup(name);
		d->next = rv;
		rv = d;
	}
	fclose(f);
	return rv;
}

struct mddev_dev *load_containers(void)
{
	struct mdstat_ent *mdstat = mdstat_read(0, 0);
	struct mddev_dev *dev_list = NULL;
	struct map_ent *map_list = NULL;
	struct mdstat_ent *ent;

	for (ent = mdstat; ent; ent = ent->next) {
		struct mddev_dev *d;
		struct map_ent *map;

		if (!is_mdstat_ent_external(ent))
			continue;

		if (is_mdstat_ent_subarray(ent))
			continue;

		d = xcalloc(1, sizeof(*d));

		map = map_by_devnm(&map_list, ent->devnm);
		if (map) {
			d->devname = xstrdup(map->path);
		} else if (asprintf(&d->devname, "/dev/%s", ent->devnm) < 0) {
			free(d);
			continue;
		}

		d->next = dev_list;
		dev_list = d;
	}

	free_mdstat(mdstat);
	map_free(map_list);

	return dev_list;
}

struct createinfo createinfo = {
	.names = 0, /* By default, stick with numbered md devices. */
	.bblist = 1, /* Use a bad block list by default */
#ifdef DEBIAN
	.gid = 6, /* disk */
	.mode = 0660,
#else
	.mode = 0600,
#endif
};

static void createline(char *line)
{
	char *w;
	char *ep;

	for (w = dl_next(line); w != line; w = dl_next(w)) {
		if (strncasecmp(w, "auto=", 5) == 0)
			/* auto is no supported now, ignore it silently */
			continue;
		else if (strncasecmp(w, "owner=", 6) == 0) {
			if (w[6] == 0) {
				pr_err("missing owner name\n");
				continue;
			}
			createinfo.uid = strtoul(w + 6, &ep, 10);
			if (*ep != 0) {
				struct passwd *pw;
				/* must be a name */
				pw = getpwnam(w + 6);
				if (pw)
					createinfo.uid = pw->pw_uid;
				else
					pr_err("CREATE user %s not found\n",
					       w + 6);
			}
		} else if (strncasecmp(w, "group=", 6) == 0) {
			if (w[6] == 0) {
				pr_err("missing group name\n");
				continue;
			}
			createinfo.gid = strtoul(w + 6, &ep, 10);
			if (*ep != 0) {
				struct group *gr;
				/* must be a name */
				gr = getgrnam(w + 6);
				if (gr)
					createinfo.gid = gr->gr_gid;
				else
					pr_err("CREATE group %s not found\n",
					       w + 6);
			}
		} else if (strncasecmp(w, "mode=", 5) == 0) {
			if (w[5] == 0) {
				pr_err("missing CREATE mode\n");
				continue;
			}
			createinfo.mode = strtoul(w + 5, &ep, 8);
			if (*ep != 0) {
				createinfo.mode = 0600;
				pr_err("unrecognised CREATE mode %s\n",
					w + 5);
			}
		} else if (strncasecmp(w, "metadata=", 9) == 0) {
			/* style of metadata to use by default */
			int i;
			for (i = 0; superlist[i] && !createinfo.supertype; i++)
				createinfo.supertype = superlist[i]->match_metadata_desc(w + 9);
			if (!createinfo.supertype)
				pr_err("metadata format %s unknown, ignoring\n",
					w+9);
		} else if (strncasecmp(w, "names=yes", 12) == 0)
			createinfo.names = 1;
		else if  (strncasecmp(w, "names=no", 11) == 0)
			createinfo.names = 0;
		else if  (strncasecmp(w, "bbl=no", 11) == 0)
			createinfo.bblist = 0;
		else if  (strncasecmp(w, "bbl=yes", 11) == 0)
			createinfo.bblist = 1;
		else {
			pr_err("unrecognised word on CREATE line: %s\n",
				w);
		}
	}
}

void devline(char *line)
{
	char *w;
	struct conf_dev *cd;

	for (w = dl_next(line); w != line; w = dl_next(w)) {
		if (w[0] == '/' || strcasecmp(w, "partitions") == 0 ||
		    strcasecmp(w, "containers") == 0) {
			cd = xmalloc(sizeof(*cd));
			cd->name = xstrdup(w);
			cd->next = cdevlist;
			cdevlist = cd;
		} else {
			pr_err("unreconised word on DEVICE line: %s\n", w);
		}
	}
}

struct mddev_ident *mddevlist = NULL;
struct mddev_ident **mddevlp = &mddevlist;

void arrayline(char *line)
{
	char *w;

	struct mddev_ident mis;
	struct mddev_ident *mi;

	ident_init(&mis);

	for (w = dl_next(line); w != line; w = dl_next(w)) {
		if (w[0] == '/' || strchr(w, '=') == NULL) {
			_ident_set_devname(&mis, w, false);
		} else if (strncasecmp(w, "uuid=", 5) == 0) {
			if (mis.uuid_set)
				pr_err("only specify uuid once, %s ignored.\n",
				       w);
			else {
				if (parse_uuid(w + 5, mis.uuid))
					mis.uuid_set = 1;
				else
					pr_err("bad uuid: %s\n", w);
			}
		} else if (strncasecmp(w, "super-minor=", 12) == 0) {
			if (mis.super_minor != UnSet)
				pr_err("only specify super-minor once, %s ignored.\n",
					w);
			else {
				char *endptr;
				int minor = strtol(w + 12, &endptr, 10);

				if (w[12] == 0 || endptr[0] != 0 || minor < 0)
					pr_err("invalid super-minor number: %s\n",
					       w);
				else
					mis.super_minor = minor;
			}
		} else if (strncasecmp(w, "name=", 5) == 0) {
			/* Ignore name in confile */
			continue;
		} else if (strncasecmp(w, "bitmap=", 7) == 0) {
			if (mis.btype != BitmapUnknown)
				pr_err("only specify bitmap file once. %s ignored\n",
					w);
			else {
				char *bname = xstrdup(w + 7);

				if (strcmp(bname, STR_COMMON_NONE) == 0)
					mis.btype = BitmapNone;
				else if (strcmp(bname, "internal") == 0)
					mis.btype = BitmapInternal;
				else if (strcmp(bname, "clustered") == 0)
					mis.btype = BitmapCluster;
			}

		} else if (strncasecmp(w, "devices=", 8 ) == 0) {
			if (mis.devices)
				pr_err("only specify devices once (use a comma separated list). %s ignored\n",
					w);
			else
				mis.devices = xstrdup(w + 8);
		} else if (strncasecmp(w, "spare-group=", 12) == 0) {
			if (mis.spare_group)
				pr_err("only specify one spare group per array. %s ignored.\n",
					w);
			else
				mis.spare_group = xstrdup(w + 12);
		} else if (strncasecmp(w, "level=", 6) == 0 ) {
			/* this is mainly for compatability with --brief output */
			mis.level = map_name(pers, w + 6);
		} else if (strncasecmp(w, "disks=", 6) == 0) {
			/* again, for compat */
			mis.raid_disks = atoi(w + 6);
		} else if (strncasecmp(w, "num-devices=", 12) == 0) {
			/* again, for compat */
			mis.raid_disks = atoi(w + 12);
		} else if (strncasecmp(w, "spares=", 7) == 0) {
			/* for warning if not all spares present */
			mis.spare_disks = atoi(w + 7);
		} else if (strncasecmp(w, "metadata=", 9) == 0) {
			/* style of metadata on the devices. */
			int i;

			for(i=0; superlist[i] && !mis.st; i++)
				mis.st = superlist[i]->
					match_metadata_desc(w + 9);

			if (!mis.st)
				pr_err("metadata format %s unknown, ignored.\n",
				       w + 9);
		} else if (strncasecmp(w, "auto=", 5) == 0) {
			/* Ignore for backward compatibility */
			continue;
		} else if (strncasecmp(w, "member=", 7) == 0) {
			/* subarray within a container */
			mis.member = xstrdup(w + 7);
		} else if (strncasecmp(w, "container=", 10) == 0) {
			/* The container holding this subarray.
			 * Either a device name or a uuid */
			mis.container = xstrdup(w + 10);
		} else {
			pr_err("unrecognised word on ARRAY line: %s\n",
				w);
		}
	}
	if (mis.uuid_set == 0 && mis.devices == NULL &&
	    mis.super_minor == UnSet && mis.name[0] == 0 &&
	    (mis.container == NULL || mis.member == NULL))
		pr_err("ARRAY line %s has no identity information.\n",
		       mis.devname);
	else {
		mi = xmalloc(sizeof(*mi));
		*mi = mis;
		mi->devname = mis.devname ? xstrdup(mis.devname) : NULL;
		mi->next = NULL;
		*mddevlp = mi;
		mddevlp = &mi->next;
	}
}

static char *alert_email = NULL;
void mailline(char *line)
{
	char *w;

	for (w = dl_next(line); w != line; w = dl_next(w))
		if (alert_email == NULL)
			alert_email = xstrdup(w);
}

static char *alert_mail_from = NULL;
void mailfromline(char *line)
{
	char *w;

	for (w = dl_next(line); w != line; w = dl_next(w)) {
		if (alert_mail_from == NULL)
			alert_mail_from = xstrdup(w);
		else {
			char *t = NULL;

			if (xasprintf(&t, "%s %s", alert_mail_from, w) > 0) {
				free(alert_mail_from);
				alert_mail_from = t;
			}
		}
	}
}

static char *alert_program = NULL;
void programline(char *line)
{
	char *w;

	for (w = dl_next(line); w != line; w = dl_next(w))
		if (alert_program == NULL)
			alert_program = xstrdup(w);
}

static char *home_host = NULL;
static int require_homehost = 1;
void homehostline(char *line)
{
	char *w;

	for (w = dl_next(line); w != line; w = dl_next(w)) {
		if (is_devname_ignore(w) == true)
			require_homehost = 0;
		else if (home_host == NULL) {
			if (strcasecmp(w, "<none>") == 0)
				home_host = xstrdup("");
			else
				home_host = xstrdup(w);
		}
	}
}

static char *home_cluster = NULL;
void homeclusterline(char *line)
{
	char *w;

	for (w = dl_next(line); w != line; w = dl_next(w)) {
		if (home_cluster == NULL) {
			if (strcasecmp(w, "<none>") == 0)
				home_cluster = xstrdup("");
			else
				home_cluster = xstrdup(w);
		}
	}
}

static int monitor_delay;
void monitordelayline(char *line)
{
	char *w;

	for (w = dl_next(line); w != line; w = dl_next(w)) {
		if (monitor_delay == 0)
			monitor_delay = strtol(w, NULL, 10);
	}
}

static bool sata_opal_encryption_no_verify;
void encryption_no_verify_line(char *line)
{
	char *word;

	for (word = dl_next(line); word != line; word = dl_next(word)) {
		if (strcasecmp(word, "sata_opal") == 0)
			sata_opal_encryption_no_verify = true;
		else
			pr_err("unrecognised word on ENCRYPTION_NO_VERIFY line: %s\n", word);
	}
}

char auto_yes[] = "yes";
char auto_no[] = "no";
char auto_homehost[] = "homehost";

static int auto_seen = 0;
void autoline(char *line)
{
	char *w;
	char *seen;
	int super_cnt;
	char *dflt = auto_yes;
	int homehost = 0;
	int i;

	if (auto_seen)
		return;
	auto_seen = 1;

	/*
	 * Parse the 'auto' line creating policy statements for the 'auto'
	 * policy.
	 *
	 * The default is 'yes' but the 'auto' line might over-ride that.
	 * Words in the line are processed in order with the first
	 * match winning.
	 * word can be:
	 *   +version   - that version can be assembled
	 *   -version   - that version cannot be auto-assembled
	 *   yes or +all - any other version can be assembled
	 *   no or -all  - no other version can be assembled.
	 *   homehost   - any array associated by 'homehost' to this
	 *                host can be assembled.
	 *
	 * Thus:
	 *   +ddf -0.90 homehost -all
	 * will auto-assemble any ddf array, no 0.90 array, and
	 * any other array (imsm, 1.x) if and only if it is identified
	 * as belonging to this host.
	 *
	 * We translate that to policy by creating 'auto=yes' when we see
	 * a '+version' line, 'auto=no' if we see '-version' before 'homehost',
	 * or 'auto=homehost' if we see '-version' after 'homehost'.
	 * When we see yes, no, +all or -all we stop and any version that hasn't
	 * been seen gets an appropriate auto= entry.
	 */

	/*
	 * If environment variable MDADM_CONF_AUTO is defined, then
	 * it is prepended to the auto line.  This allow a script
	 * to easily disable some metadata types.
	 */
	w = getenv("MDADM_CONF_AUTO");
	if (w && *w) {
		char *l = xstrdup(w);
		char *head = line;
		w = strtok(l, " \t");
		while (w) {
			char *nw = dl_strdup(w);
			dl_insert(head, nw);
			head = nw;
			w = strtok(NULL, " \t");
		}
		free(l);
	}

	for (super_cnt = 0; superlist[super_cnt]; super_cnt++)
		;
	seen = xcalloc(super_cnt, 1);

	for (w = dl_next(line); w != line; w = dl_next(w)) {
		char *val;

		if (strcasecmp(w, "yes") == 0) {
			dflt = auto_yes;
			break;
		}
		if (strcasecmp(w, "no") == 0) {
			if (homehost)
				dflt = auto_homehost;
			else
				dflt = auto_no;
			break;
		}
		if (strcasecmp(w, "homehost") == 0) {
			homehost = 1;
			continue;
		}
		if (w[0] == '+')
			val = auto_yes;
		else if (w[0] == '-') {
			if (homehost)
				val = auto_homehost;
			else
				val = auto_no;
		} else
			continue;

		if (strcasecmp(w + 1, "all") == 0) {
			dflt = val;
			break;
		}
		for (i = 0; superlist[i]; i++) {
			const char *version = superlist[i]->name;
			if (strcasecmp(w + 1, version) == 0)
				break;
			/* 1 matches 1.x, 0 matches 0.90 */
			if (version[1] == '.' && strlen(w + 1) == 1 &&
			    w[1] == version[0])
				break;
			/* 1.anything matches 1.x */
			if (strcmp(version, "1.x") == 0 &&
			    strncmp(w + 1, "1.", 2) == 0)
				break;
		}
		if (superlist[i] == NULL)
			/* ignore this word */
			continue;
		if (seen[i])
			/* already know about this metadata */
			continue;
		policy_add(rule_policy, pol_auto, val, pol_metadata,
			   superlist[i]->name, NULL);
		seen[i] = 1;
	}
	for (i = 0; i < super_cnt; i++)
		if (!seen[i])
			policy_add(rule_policy, pol_auto, dflt, pol_metadata,
				   superlist[i]->name, NULL);

	free(seen);
}

int loaded = 0;

static char *conffile = NULL;
void set_conffile(char *file)
{
	conffile = file;
}

void conf_file(FILE *f)
{
	char *line;
	while ((line = conf_line(f))) {
		switch(match_keyword(line)) {
		case Devices:
			devline(line);
			break;
		case Array:
			arrayline(line);
			break;
		case Mailaddr:
			mailline(line);
			break;
		case Mailfrom:
			mailfromline(line);
			break;
		case Program:
			programline(line);
			break;
		case CreateDev:
			createline(line);
			break;
		case Homehost:
			homehostline(line);
			break;
		case HomeCluster:
			homeclusterline(line);
			break;
		case AutoMode:
			autoline(line);
			break;
		case Policy:
			policyline(line, rule_policy);
			break;
		case PartPolicy:
			policyline(line, rule_part);
			break;
		case Sysfs:
			sysfsline(line);
			break;
		case MonitorDelay:
			monitordelayline(line);
			break;
		case EncryptionNoVerify:
			encryption_no_verify_line(line);
			break;
		default:
			pr_err("Unknown keyword %s\n", line);
		}
		free_line(line);
	}
}

struct fname {
	struct fname *next;
	char name[];
};

void conf_file_or_dir(FILE *f)
{
	struct stat st;
	DIR *dir;
	struct dirent *dp;
	struct fname *list = NULL;

	if (fstat(fileno(f), &st) != 0)
		return;
	if (S_ISREG(st.st_mode))
		conf_file(f);
	else if (!S_ISDIR(st.st_mode))
		return;
#if _XOPEN_SOURCE >= 700 || _POSIX_C_SOURCE >= 200809L
	dir = fdopendir(fileno(f));
	if (!dir)
		return;
	while ((dp = readdir(dir)) != NULL) {
		int l;
		struct fname *fn, **p;
		if (dp->d_ino == 0)
			continue;
		if (dp->d_name[0] == '.')
			continue;
		l = strlen(dp->d_name);
		if (l < 6 || strcmp(dp->d_name + l - 5, ".conf") != 0)
			continue;
		fn = xmalloc(sizeof(*fn) + l + 1);
		strcpy(fn->name, dp->d_name);
		for (p = &list;
		     *p && strcmp((*p)->name, fn->name) < 0;
		     p = & (*p)->next)
			;
		fn->next = *p;
		*p = fn;
	}
	while (list) {
		int fd;
		FILE *f2;
		struct fname *fn = list;
		list = list->next;
		fd = openat(fileno(f), fn->name, O_RDONLY);
		free(fn);
		if (fd < 0)
			continue;
		f2 = fdopen(fd, "r");
		if (!f2) {
			close(fd);
			continue;
		}
		conf_file(f2);
		fclose(f2);
	}
	closedir(dir);
#endif
}

void load_conffile(void)
{
	FILE *f;
	char *confdir = NULL;
	char *head;

	if (loaded)
		return;
	if (conffile == NULL) {
		conffile = DefaultConfFile;
		confdir = DefaultConfDir;
	}

	if (strcmp(conffile, "partitions") == 0) {
		char *list = dl_strdup("DEV");
		dl_init(list);
		dl_add(list, dl_strdup("partitions"));
		devline(list);
		free_line(list);
	} else if (str_is_none(conffile) == false) {
		f = fopen(conffile, "r");
		/* Debian chose to relocate mdadm.conf into /etc/mdadm/.
		 * To allow Debian users to compile from clean source and still
		 * have a working mdadm, we read /etc/mdadm/mdadm.conf
		 * if /etc/mdadm.conf doesn't exist
		 */
		if (f == NULL && conffile == DefaultConfFile) {
			f = fopen(DefaultAltConfFile, "r");
			if (f) {
				conffile = DefaultAltConfFile;
				confdir = DefaultAltConfDir;
			}
		}
		if (f) {
			conf_file_or_dir(f);
			fclose(f);
		}
		if (confdir) {
			f = fopen(confdir, "r");
			if (f) {
				conf_file_or_dir(f);
				fclose(f);
			}
		}
	}
	/* If there was no AUTO line, process an empty line
	 * now so that the MDADM_CONF_AUTO env var gets processed.
	 */
	head = dl_strdup("AUTO");
	dl_init(head);
	autoline(head);
	free_line(head);

	loaded = 1;
}

char *conf_get_mailaddr(void)
{
	load_conffile();
	return alert_email;
}

char *conf_get_mailfrom(void)
{
	load_conffile();
	return alert_mail_from;
}

char *conf_get_program(void)
{
	load_conffile();
	return alert_program;
}

char *conf_get_homehost(int *require_homehostp)
{
	load_conffile();
	if (require_homehostp)
		*require_homehostp = require_homehost;
	return home_host;
}

char *conf_get_homecluster(void)
{
	load_conffile();
	return home_cluster;
}

int conf_get_monitor_delay(void)
{
	load_conffile();
	return monitor_delay;
}

bool conf_get_sata_opal_encryption_no_verify(void)
{
	load_conffile();
	return sata_opal_encryption_no_verify;
}

struct createinfo *conf_get_create_info(void)
{
	load_conffile();
	return &createinfo;
}

struct mddev_ident *conf_get_ident(char *dev)
{
	struct mddev_ident *rv;
	load_conffile();
	rv = mddevlist;
	while (dev && rv && (rv->devname == NULL ||
			     !devname_matches(dev, rv->devname)))
		rv = rv->next;
	return rv;
}

static void append_dlist(struct mddev_dev **dlp, struct mddev_dev *list)
{
	while (*dlp)
		dlp = &(*dlp)->next;
	*dlp = list;
}

struct mddev_dev *conf_get_devs()
{
	glob_t globbuf;
	struct conf_dev *cd;
	int flags = 0;
	static struct mddev_dev *dlist = NULL;
	unsigned int i;

	while (dlist) {
		struct mddev_dev *t = dlist;
		dlist = dlist->next;
		free(t->devname);
		free(t);
	}

	load_conffile();

	if (cdevlist == NULL) {
		/* default to 'partitions' and 'containers' */
		dlist = load_partitions();
		append_dlist(&dlist, load_containers());
	}

	for (cd = cdevlist; cd; cd = cd->next) {
		if (strcasecmp(cd->name, "partitions") == 0)
			append_dlist(&dlist, load_partitions());
		else if (strcasecmp(cd->name, "containers") == 0)
			append_dlist(&dlist, load_containers());
		else {
			glob(cd->name, flags, NULL, &globbuf);
			flags |= GLOB_APPEND;
		}
	}
	if (flags & GLOB_APPEND) {
		for (i = 0; i < globbuf.gl_pathc; i++) {
			struct mddev_dev *t;
			t = xcalloc(1, sizeof(*t));
			t->devname = xstrdup(globbuf.gl_pathv[i]);
			t->next = dlist;
			dlist = t;
/*	printf("one dev is %s\n", t->devname);*/
		}
		globfree(&globbuf);
	}

	return dlist;
}

int conf_test_dev(char *devname)
{
	struct conf_dev *cd;
	if (cdevlist == NULL)
		/* allow anything by default */
		return 1;
	for (cd = cdevlist; cd; cd = cd->next) {
		if (strcasecmp(cd->name, "partitions") == 0)
			return 1;
		if (fnmatch(cd->name, devname, FNM_PATHNAME) == 0)
			return 1;
	}
	return 0;
}

int conf_test_metadata(const char *version, struct dev_policy *pol, int is_homehost)
{
	/* If anyone said 'yes', that sticks.
	 * else if homehost applies, use that
	 * else if there is a 'no', say 'no'.
	 * else 'yes'.
	 */
	struct dev_policy *p;
	int no = 0, found_homehost = 0;
	load_conffile();

	pol = pol_find(pol, pol_auto);
	pol_for_each(p, pol, version) {
		if (strcmp(p->value, "yes") == 0)
			return 1;
		if (strcmp(p->value, "homehost") == 0)
			found_homehost = 1;
		if (strcmp(p->value, "no") == 0)
			no = 1;
	}
	if (is_homehost && found_homehost)
		return 1;
	if (no)
		return 0;
	return 1;
}

int match_oneof(char *devices, char *devname)
{
	/* check if one of the comma separated patterns in devices
	 * matches devname
	 */

	while (devices && *devices) {
		char patn[1024];
		char *p = devices;
		devices = strchr(devices, ',');
		if (!devices)
			devices = p + strlen(p);
		if (devices-p < 1024) {
			strncpy(patn, p, devices - p);
			patn[devices-p] = 0;
			if (fnmatch(patn, devname, FNM_PATHNAME) == 0)
				return 1;
		}
		if (*devices == ',')
			devices++;
	}
	return 0;
}

int devname_matches(char *name, char *match)
{
	/* See if the given array name matches the
	 * given match from config file.
	 *
	 * First strip and /dev/md/ or /dev/, then
	 * see if there might be a numeric match of
	 *  mdNN with NN
	 * then just strcmp
	 */
	if (strncmp(name, DEV_MD_DIR, DEV_MD_DIR_LEN) == 0)
		name += DEV_MD_DIR_LEN;
	else if (strncmp(name, "/dev/", 5) == 0)
		name += 5;

	if (strncmp(match, DEV_MD_DIR, DEV_MD_DIR_LEN) == 0)
		match += DEV_MD_DIR_LEN;
	else if (strncmp(match, "/dev/", 5) == 0)
		match += 5;

	if (strncmp(name, "md", 2) == 0 && isdigit(name[2]))
		name += 2;
	if (strncmp(match, "md", 2) == 0 && isdigit(match[2]))
		match += 2;

	return (strcmp(name, match) == 0);
}

int conf_name_is_free(char *name)
{
	/* Check if this name is already taken by an ARRAY entry in
	 * the config file.
	 * It can be taken either by a match on devname, name, or
	 * even super-minor.
	 */
	struct mddev_ident *dev;

	load_conffile();
	for (dev = mddevlist; dev; dev = dev->next) {
		char nbuf[100];
		if (dev->devname && devname_matches(name, dev->devname))
			return 0;
		if (dev->name[0] && devname_matches(name, dev->name))
			return 0;
		sprintf(nbuf, "%d", dev->super_minor);
		if (dev->super_minor != UnSet && devname_matches(name, nbuf))
			return 0;
	}
	return 1;
}

struct mddev_ident *conf_match(struct supertype *st,
			       struct mdinfo *info,
			       char *devname,
			       int verbose, int *rvp)
{
	struct mddev_ident *array_list, *match;
	array_list = conf_get_ident(NULL);
	match = NULL;
	for (; array_list; array_list = array_list->next) {
		if (array_list->uuid_set &&
		    same_uuid(array_list->uuid, info->uuid,
			      st->ss->swapuuid) == 0) {
			if (verbose >= 2 && array_list->devname)
				pr_err("UUID differs from %s.\n",
				       array_list->devname);
			continue;
		}

		if (array_list->devices && devname &&
		    !match_oneof(array_list->devices, devname)) {
			if (verbose >= 2 && array_list->devname)
				pr_err("Not a listed device for %s.\n",
				       array_list->devname);
			continue;
		}
		if (array_list->super_minor != UnSet &&
		    array_list->super_minor != info->array.md_minor) {
			if (verbose >= 2 && array_list->devname)
				pr_err("Different super-minor to %s.\n",
				       array_list->devname);
			continue;
		}
		if (!array_list->uuid_set && !array_list->name[0] &&
		    !array_list->devices && array_list->super_minor == UnSet) {
			if (verbose >= 2 && array_list->devname)
				pr_err("%s doesn't have any identifying information.\n",
				       array_list->devname);
			continue;
		}
		/* FIXME, should I check raid_disks and level too?? */

		if (match) {
			if (verbose >= 0) {
				if (match->devname && array_list->devname)
					pr_err("we match both %s and %s - cannot decide which to use.\n",
					       match->devname,
					       array_list->devname);
				else
					pr_err("multiple lines in mdadm.conf match\n");
			}
			if (rvp)
				*rvp = 2;
			match = NULL;
			break;
		}
		match = array_list;
	}
	return match;
}

int conf_verify_devnames(struct mddev_ident *array_list)
{
	struct mddev_ident *a1, *a2;

	for (a1 = array_list; a1; a1 = a1->next) {
		if (!a1->devname)
			continue;
		if (strcmp(a1->devname, "<ignore>") == 0)
			continue;
		for (a2 = a1->next; a2; a2 = a2->next) {
			if (!a2->devname)
				continue;
			if (strcmp(a1->devname, a2->devname) != 0)
				continue;

			if (a1->uuid_set && a2->uuid_set) {
				char nbuf[64];
				__fname_from_uuid(a1->uuid, 0, nbuf, ':');
				pr_err("Devices %s and ",
				       nbuf);
				__fname_from_uuid(a2->uuid, 0, nbuf, ':');
				fprintf(stderr,
					"%s have the same name: %s\n",
					nbuf, a1->devname);
			} else
				pr_err("Device %s given twice in config file\n", a1->devname);
			return 1;
		}
	}

	return 0;
}
