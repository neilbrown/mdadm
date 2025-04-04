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
#include	"md_p.h"
#include	"md_u.h"
#include	"udev.h"
#include	"xmalloc.h"

#include	<sys/wait.h>
#include	<limits.h>
#include	<syslog.h>

#define TASK_COMM_LEN 16
#define EVENT_NAME_MAX 32
#define AUTOREBUILD_PID_PATH MDMON_DIR "/autorebuild.pid"
#define FALLBACK_DELAY 5

/**
 * struct state - external array or container properties.
 * @devname: has length of %DEV_MD_DIR + device name + terminating byte
 * @devnm: to sync with mdstat info
 * @parent_devnm: or subarray, devnm of parent, for others, ""
 * @subarray: for a container it is a link to first subarray, for a subarray it is a link to next
 *	      subarray in the same container
 * @parent: for a subarray it is a link to its container
 */
struct state {
	char devname[MD_NAME_MAX + sizeof(DEV_MD_DIR)];
	char devnm[MD_NAME_MAX];
	unsigned int utime;
	int err;
	char *spare_group;
	int active, working, failed, spare, raid;
	int from_config;
	int from_auto;
	int expected_spares;
	int devstate[MAX_DISKS];
	dev_t devid[MAX_DISKS];
	int percent;
	char parent_devnm[MD_NAME_MAX];
	struct supertype *metadata;
	struct state *subarray;
	struct state *parent;
	struct state *next;
};

struct alert_info {
	char hostname[HOST_NAME_MAX];
	char *mailaddr;
	char *mailfrom;
	char *alert_cmd;
	int dosyslog;
	int test;
} info;

enum event {
	EVENT_SPARE_ACTIVE = 0,
	EVENT_NEW_ARRAY,
	EVENT_MOVE_SPARE,
	EVENT_TEST_MESSAGE,
	__SYSLOG_PRIORITY_WARNING,
	EVENT_REBUILD_STARTED,
	EVENT_REBUILD,
	EVENT_REBUILD_FINISHED,
	EVENT_SPARES_MISSING,
	__SYSLOG_PRIORITY_CRITICAL,
	EVENT_DEVICE_DISAPPEARED,
	EVENT_FAIL,
	EVENT_FAIL_SPARE,
	EVENT_DEGRADED_ARRAY,
	EVENT_UNKNOWN
};

mapping_t events_map[] = {
	{"SpareActive", EVENT_SPARE_ACTIVE},
	{"NewArray", EVENT_NEW_ARRAY},
	{"MoveSpare", EVENT_MOVE_SPARE},
	{"TestMessage", EVENT_TEST_MESSAGE},
	{"RebuildStarted", EVENT_REBUILD_STARTED},
	{"Rebuild", EVENT_REBUILD},
	{"RebuildFinished", EVENT_REBUILD_FINISHED},
	{"SparesMissing", EVENT_SPARES_MISSING},
	{"DeviceDisappeared", EVENT_DEVICE_DISAPPEARED},
	{"Fail", EVENT_FAIL},
	{"FailSpare", EVENT_FAIL_SPARE},
	{"DegradedArray", EVENT_DEGRADED_ARRAY},
	{NULL, EVENT_UNKNOWN}
};

struct event_data {
	enum event event_enum;
	/*
	 * @event_name: Rebuild event name must be in form "RebuildXX", where XX is rebuild progress.
	 */
	char event_name[EVENT_NAME_MAX];
	char message[BUFSIZ];
	const char *description;
	const char *dev;
	const char *disc;
};

static int add_new_arrays(struct mdstat_ent *mdstat, struct state **statelist);
static void try_spare_migration(struct state *statelist);
static void link_containers_with_subarrays(struct state *list);
static void free_statelist(struct state *statelist);
static int check_array(struct state *st, struct mdstat_ent *mdstat, int increments, char *prefer);
static int check_one_sharer(int scan);
static void link_containers_with_subarrays(struct state *list);
static int make_daemon(char *pidfile);
static void try_spare_migration(struct state *statelist);
static void wait_for_events(int *delay_for_event, int c_delay);
static void wait_for_events_mdstat(int *delay_for_event, int c_delay);
static int write_autorebuild_pid(void);

int Monitor(struct mddev_dev *devlist,
	    char *mailaddr, char *alert_cmd,
	    struct context *c,
	    int daemonise, int oneshot,
	    int dosyslog, char *pidfile, int increments,
	    int share)
{
	/*
	 * Every few seconds, scan every md device looking for changes
	 * When a change is found, log it, possibly run the alert command,
	 * and possibly send Email
	 *
	 * For each array, we record:
	 *   Update time
	 *   active/working/failed/spare drives
	 *   State of each device.
	 *   %rebuilt if rebuilding
	 *
	 * If the update time changes, check out all the data again
	 * It is possible that we cannot get the state of each device
	 * due to bugs in the md kernel module.
	 * We also read /proc/mdstat to get rebuild percent,
	 * and to get state on all active devices incase of kernel bug.
	 *
	 * Events are:
	 *    Fail
	 *	An active device had Faulty set or Active/Sync removed
	 *    FailSpare
	 *      A spare device had Faulty set
	 *    SpareActive
	 *      An active device had a reverse transition
	 *    RebuildStarted
	 *      percent went from -1 to +ve
	 *    RebuildNN
	 *      percent went from below to not-below NN%
	 *    DeviceDisappeared
	 *      Couldn't access a device which was previously visible
	 *
	 * if we detect an array with active<raid and spare==0
	 * we look at other arrays that have same spare-group
	 * If we find one with active==raid and spare>0,
	 *  and if we can get_disk_info and find a name
	 *  Then we hot-remove and hot-add to the other array
	 *
	 * If devlist is NULL, then we can monitor everything if --scan
	 * was given.  We get an initial list from config file and add anything
	 * that appears in /proc/mdstat
	 */

	struct state *statelist = NULL;
	int finished = 0;
	struct mdstat_ent *mdstat = NULL;
	char *mailfrom;
	struct mddev_ident *mdlist;
	int delay_for_event = c->delay;

	if (devlist && c->scan) {
		pr_err("Devices list and --scan option cannot be combined - not monitoring.\n");
		return 1;
	}

	if (!mailaddr)
		mailaddr = conf_get_mailaddr();

	if (!alert_cmd)
		alert_cmd = conf_get_program();

	mailfrom = conf_get_mailfrom();

	if (c->scan && !mailaddr && !alert_cmd && !dosyslog) {
		pr_err("No mail address or alert command - not monitoring.\n");
		return 1;
	}

	if (c->verbose) {
		pr_err("Monitor is started with delay %ds\n", c->delay);
		if (mailaddr)
			pr_err("Monitor using email address %s\n", mailaddr);
		if (alert_cmd)
			pr_err("Monitor using program %s\n", alert_cmd);
	}

	info.alert_cmd = alert_cmd;
	info.mailaddr = mailaddr;
	info.mailfrom = mailfrom;
	info.dosyslog = dosyslog;
	info.test = c->test;

	if (s_gethostname(info.hostname, sizeof(info.hostname)) != 0) {
		pr_err("Cannot get hostname.\n");
		return 1;
	}

	if (mkdir(MDMON_DIR, 0700) < 0 && errno != EEXIST) {
		pr_err("Failed to create directory " MDMON_DIR ": %s\n", strerror(errno));
		return 1;
	}

	if (share){
		if (check_one_sharer(c->scan) == 2)
			return 1;
	}

	if (daemonise) {
		int rv = make_daemon(pidfile);
		if (rv >= 0)
			return rv;
	}

	if (share)
		if (write_autorebuild_pid() != 0)
			return 1;

	if (devlist == NULL) {
		mdlist = conf_get_ident(NULL);
		for (; mdlist; mdlist = mdlist->next) {
			struct state *st;

			if (mdlist->devname == NULL)
				continue;
			if (is_devname_ignore(mdlist->devname) == true)
				continue;
			if (!is_mddev(mdlist->devname))
				continue;

			st = xcalloc(1, sizeof *st);
			snprintf(st->devname, MD_NAME_MAX + sizeof(DEV_MD_DIR), DEV_MD_DIR "%s",
				 basename(mdlist->devname));
			st->next = statelist;
			st->devnm[0] = 0;
			st->percent = RESYNC_UNKNOWN;
			st->from_config = 1;
			st->expected_spares = mdlist->spare_disks;
			if (mdlist->spare_group)
				st->spare_group = xstrdup(mdlist->spare_group);
			statelist = st;
		}
	} else {
		struct mddev_dev *dv;

		for (dv = devlist; dv; dv = dv->next) {
			struct state *st;

			if (!is_mddev(dv->devname))
				continue;

			st = xcalloc(1, sizeof *st);
			mdlist = conf_get_ident(dv->devname);
			snprintf(st->devname, MD_NAME_MAX + sizeof(DEV_MD_DIR), "%s", dv->devname);
			st->next = statelist;
			st->devnm[0] = 0;
			st->percent = RESYNC_UNKNOWN;
			st->expected_spares = -1;
			if (mdlist) {
				st->expected_spares = mdlist->spare_disks;
				if (mdlist->spare_group)
					st->spare_group = xstrdup(mdlist->spare_group);
			}
			statelist = st;
		}
	}

	while (!finished) {
		int new_found = 0;
		struct state *st, **stp;
		int anydegraded = 0;
		int anyredundant = 0;

		if (mdstat)
			free_mdstat(mdstat);
		mdstat = mdstat_read(oneshot ? 0 : 1, 0);

		for (st = statelist; st; st = st->next) {
			if (check_array(st, mdstat, increments, c->prefer))
				anydegraded = 1;
			/* for external arrays, metadata is filled for
			 * containers only
			 */
			if (st->metadata && st->metadata->ss->external)
				continue;
			if (st->err == 0 && !anyredundant)
				anyredundant = 1;
		}

		/* now check if there are any new devices found in mdstat */
		if (c->scan)
			new_found = add_new_arrays(mdstat, &statelist);

		/* If an array has active < raid && spare == 0 && spare_group != NULL
		 * Look for another array with spare > 0 and active == raid and same spare_group
		 * if found, choose a device and hotremove/hotadd
		 */
		if (share && anydegraded)
			try_spare_migration(statelist);
		if (!new_found) {
			if (oneshot)
				break;
			if (!anyredundant) {
				pr_err("No array with redundancy detected, stopping\n");
				break;
			}

			wait_for_events(&delay_for_event, c->delay);
		}
		info.test = 0;

		for (stp = &statelist; (st = *stp) != NULL; ) {
			if (st->from_auto && st->err > 5) {
				*stp = st->next;
				if (st->spare_group)
					free(st->spare_group);

				free(st);
			} else
				stp = &st->next;
		}
	}

	free_statelist(statelist);

	if (pidfile)
		unlink(pidfile);
	return 0;
}

/*
 * wait_for_events() - Waits for events on md devices.
 * @delay_for_event: pointer to current event delay
 * @c_delay: delay from config
 */
static void wait_for_events(int *delay_for_event, int c_delay)
{
#ifndef NO_LIBUDEV
	if (udev_is_available()) {
		if (udev_wait_for_events(*delay_for_event) == UDEV_STATUS_ERROR)
			pr_err("Error while waiting for udev events.\n");
		return;
	}
#endif
	wait_for_events_mdstat(delay_for_event, c_delay);
}

/*
 * wait_for_events_mdstat() - Waits for events on mdstat.
 * @delay_for_event: pointer to current event delay
 * @c_delay: delay from config
 */
static void wait_for_events_mdstat(int *delay_for_event, int c_delay)
{
	int wait_result = mdstat_wait(*delay_for_event);

	if (wait_result < 0) {
		pr_err("Error while waiting for events on mdstat.\n");
		return;
	}

	/*
	 * Give chance to process new device
	 */
	if (wait_result != 0) {
		if (c_delay > FALLBACK_DELAY)
			*delay_for_event = FALLBACK_DELAY;
	} else {
		*delay_for_event = c_delay;
	}
	mdstat_close();
}

static int make_daemon(char *pidfile)
{
	/* Return:
	 * -1 in the forked daemon
	 *  0 in the parent
	 *  1 on error
	 * so a none-negative becomes the exit code.
	 */
	int pid = fork();
	if (pid > 0) {
		if (!pidfile)
			printf("%d\n", pid);
		else {
			FILE *pid_file = NULL;
			int fd = open(pidfile, O_WRONLY | O_CREAT | O_TRUNC,
				      0644);
			if (fd >= 0)
				pid_file = fdopen(fd, "w");
			if (!pid_file)
				perror("cannot create pid file");
			else {
				fprintf(pid_file,"%d\n", pid);
				fclose(pid_file);
			}
		}
		return 0;
	}
	if (pid < 0) {
		perror("daemonise");
		return 1;
	}
	manage_fork_fds(0);
	setsid();
	return -1;
}

/*
 * check_one_sharer() - Checks for other mdmonitor processes running.
 *
 * Return:
 * 0 - no other processes running,
 * 1 - warning,
 * 2 - error, or when scan mode is enabled, and one mdmonitor process already exists
 */
static int check_one_sharer(int scan)
{
	int pid;
	FILE *fp, *comm_fp;
	char comm_path[PATH_MAX];
	char comm[TASK_COMM_LEN];

	if (!is_directory(MDMON_DIR)) {
		pr_err("%s is not a regular directory.\n", MDMON_DIR);
		return 2;
	}

	fp = fopen(AUTOREBUILD_PID_PATH, "r");
	if (!fp) {
		/* PID file does not exist */
		if (errno == ENOENT)
			return 0;

		pr_err("Cannot open %s file.\n", AUTOREBUILD_PID_PATH);
		return 2;
	}

	if (!is_file(AUTOREBUILD_PID_PATH)) {
		pr_err("%s is not a regular file.\n", AUTOREBUILD_PID_PATH);
		fclose(fp);
		return 2;
	}

	if (fscanf(fp, "%d", &pid) != 1) {
		pr_err("Cannot read pid from %s file.\n", AUTOREBUILD_PID_PATH);
		fclose(fp);
		return 2;
	}

	snprintf(comm_path, sizeof(comm_path), "/proc/%d/comm", pid);

	comm_fp = fopen(comm_path, "r");
	if (!comm_fp) {
		dprintf("Warning: Cannot open %s, continuing\n", comm_path);
		fclose(fp);
		return 1;
	}

	if (fscanf(comm_fp, "%15s", comm) == 0) {
		dprintf("Warning: Cannot read comm from %s, continuing\n", comm_path);
		fclose(comm_fp);
		fclose(fp);
		return 1;
	}

	if (strncmp(basename(comm), Name, strlen(Name)) == 0) {
		if (scan) {
			pr_err("Only one autorebuild process allowed in scan mode, aborting\n");
			fclose(comm_fp);
			fclose(fp);
			return 2;
		}
		pr_err("Warning: One autorebuild process already running.\n");
	}
	fclose(comm_fp);
	fclose(fp);
	return 0;
}

/*
 * write_autorebuild_pid() - Writes pid to autorebuild.pid file.
 *
 * Return: 0 on success, 1 on error
 */
static int write_autorebuild_pid(void)
{
	FILE *fp;
	int fd;

	if (!is_directory(MDMON_DIR)) {
		pr_err("%s is not a regular directory.\n", MDMON_DIR);
		return 1;
	}

	fd = open(AUTOREBUILD_PID_PATH, O_WRONLY | O_CREAT | O_TRUNC, 0700);

	if (fd < 0) {
		pr_err("Error opening %s file.\n", AUTOREBUILD_PID_PATH);
		return 1;
	}

	fp = fdopen(fd, "w");

	if (!fp) {
		pr_err("Error opening fd for %s file.\n", AUTOREBUILD_PID_PATH);
		return 1;
	}

	fprintf(fp, "%d\n", getpid());

	fclose(fp);
	return 0;
}

#define BASE_MESSAGE "%s event detected on md device %s"
#define COMPONENT_DEVICE_MESSAGE ", component device %s"
#define DESCRIPTION_MESSAGE ": %s"
/*
 * sprint_event_message() - Writes basic message about detected event to destination ptr.
 * @dest: message destination, should be at least the size of BUFSIZ
 * @data: event data
 *
 * Return: 0 on success, 1 on error
 */
static int sprint_event_message(char *dest, const struct event_data *data)
{
	if (!dest || !data)
		return 1;

	if (data->disc && data->description)
		snprintf(dest, BUFSIZ, BASE_MESSAGE COMPONENT_DEVICE_MESSAGE DESCRIPTION_MESSAGE,
			 data->event_name, data->dev, data->disc, data->description);
	else if (data->disc)
		snprintf(dest, BUFSIZ, BASE_MESSAGE COMPONENT_DEVICE_MESSAGE,
			 data->event_name, data->dev, data->disc);
	else if (data->description)
		snprintf(dest, BUFSIZ, BASE_MESSAGE DESCRIPTION_MESSAGE,
			 data->event_name, data->dev, data->description);
	else
		snprintf(dest, BUFSIZ, BASE_MESSAGE, data->event_name, data->dev);

	return 0;
}

/*
 * get_syslog_event_priority() - Determines event priority.
 * @event_enum: event to be checked
 *
 * Return: LOG_CRIT, LOG_WARNING or LOG_INFO
 */
static int get_syslog_event_priority(const enum event event_enum)
{
	if (event_enum > __SYSLOG_PRIORITY_CRITICAL)
		return LOG_CRIT;
	if (event_enum > __SYSLOG_PRIORITY_WARNING)
		return LOG_WARNING;
	return LOG_INFO;
}

/*
 * is_email_event() - Determines whether email for event should be sent or not.
 * @event_enum: event to be checked
 *
 * Return: true if email should be sent, false otherwise
 */
static bool is_email_event(const enum event event_enum)
{
	static const enum event email_events[] = {
	EVENT_FAIL,
	EVENT_FAIL_SPARE,
	EVENT_DEGRADED_ARRAY,
	EVENT_SPARES_MISSING,
	EVENT_TEST_MESSAGE
	};
	unsigned int i;

	for (i = 0; i < ARRAY_SIZE(email_events); ++i) {
		if (event_enum == email_events[i])
			return true;
	}
	return false;
}

/*
 * execute_alert_cmd() - Forks and executes command provided as alert_cmd.
 * @data: event data
 */
static void execute_alert_cmd(const struct event_data *data)
{
	int pid = fork();

	switch (pid) {
	default:
		waitpid(pid, NULL, 0);
		break;
	case -1:
		pr_err("Cannot fork to execute alert command");
		break;
	case 0:
		execl(info.alert_cmd, info.alert_cmd, data->event_name, data->dev, data->disc, NULL);
		exit(2);
	}
}

/*
 * send_event_email() - Sends an email about event detected by monitor.
 * @data: event data
 */
static void send_event_email(const struct event_data *data)
{
	FILE *mp, *mdstat;
	char buf[BUFSIZ];
	int n;

	mp = popen(Sendmail, "w");
	if (!mp) {
		pr_err("Cannot open pipe stream for sendmail.\n");
		return;
	}

	signal(SIGPIPE, SIG_IGN);
	if (info.mailfrom)
		fprintf(mp, "From: %s\n", info.mailfrom);
	else
		fprintf(mp, "From: %s monitoring <root>\n", Name);
	fprintf(mp, "To: %s\n", info.mailaddr);
	fprintf(mp, "Subject: %s event on %s:%s\n\n", data->event_name, data->dev, info.hostname);
	fprintf(mp, "This is an automatically generated mail message.\n");
	fprintf(mp, "%s\n", data->message);

	mdstat = fopen("/proc/mdstat", "r");
	if (!mdstat) {
		pr_err("Cannot open /proc/mdstat\n");
		pclose(mp);
		return;
	}

	fprintf(mp, "The /proc/mdstat file currently contains the following:\n\n");
	while ((n = fread(buf, 1, sizeof(buf), mdstat)) > 0)
		n = fwrite(buf, 1, n, mp);
	fclose(mdstat);
	pclose(mp);
}

/*
 * log_event_to_syslog() - Logs an event into syslog.
 * @data: event data
 */
static void log_event_to_syslog(const struct event_data *data)
{
	int priority;

	priority = get_syslog_event_priority(data->event_enum);

	syslog(priority, "%s\n", data->message);
}

/*
 * alert() - Alerts about the monitor event.
 * @event_enum: event to be sent
 * @description: event description
 * @progress: rebuild progress
 * @dev: md device name
 * @disc: component device
 *
 * If needed function executes alert command, sends an email or logs event to syslog.
 */
static void alert(const enum event event_enum, const char *description, const uint8_t progress,
		  const char *dev, const char *disc)
{
	struct event_data data = {.dev = dev, .disc = disc, .description = description};

	if (!dev)
		return;

	if (event_enum == EVENT_REBUILD) {
		snprintf(data.event_name, sizeof(data.event_name), "%s%02d",
			 map_num_s(events_map, EVENT_REBUILD), progress);
	} else {
		snprintf(data.event_name, sizeof(data.event_name), "%s", map_num_s(events_map, event_enum));
	}

	data.event_enum = event_enum;

	if (sprint_event_message(data.message, &data) != 0) {
		pr_err("Cannot create event message.\n");
		return;
	}
	pr_err("%s\n", data.message);

	if (info.alert_cmd)
		execute_alert_cmd(&data);

	if (info.mailaddr && is_email_event(event_enum))
		send_event_email(&data);

	if (info.dosyslog)
		log_event_to_syslog(&data);
}

static int check_array(struct state *st, struct mdstat_ent *mdstat,
		       int increments, char *prefer)
{
	/* Update the state 'st' to reflect any changes shown in mdstat,
	 * or found by directly examining the array, and return
	 * '1' if the array is degraded, or '0' if it is optimal (or dead).
	 */
	struct { int state, major, minor; } disks_info[MAX_DISKS];
	struct mdinfo *sra = NULL;
	mdu_array_info_t array;
	struct mdstat_ent *mse = NULL, *mse2;
	char *dev = st->devname;
	int fd;
	int i;
	int remaining_disks;
	int last_disk;
	int new_array = 0;
	int retval;
	int is_container = 0;
	unsigned long redundancy_only_flags = 0;

	if (info.test)
		alert(EVENT_TEST_MESSAGE, NULL, 0, dev, NULL);

	retval = 0;

	fd = open(dev, O_RDONLY);
	if (fd < 0)
		goto disappeared;

	if (st->devnm[0] == 0)
		snprintf(st->devnm, MD_NAME_MAX, "%s", fd2devnm(fd));

	for (mse2 = mdstat; mse2; mse2 = mse2->next)
		if (strcmp(mse2->devnm, st->devnm) == 0) {
			mse2->devnm[0] = 0; /* flag it as "used" */
			mse = mse2;
		}

	if (!mse) {
		/* duplicated array in statelist
		 * or re-created after reading mdstat
		 */
		st->err++;
		goto out;
	}

	if (mse->level == NULL)
		is_container = 1;

	if (!is_container && !md_array_active(fd))
		goto disappeared;

	if (fcntl(fd, F_SETFD, FD_CLOEXEC) < 0)
		goto out;

	if (md_get_array_info(fd, &array) < 0)
		goto disappeared;

	if (!is_container && map_name(pers, mse->level) > 0)
		redundancy_only_flags |= GET_MISMATCH;

	sra = sysfs_read(-1, st->devnm, GET_LEVEL | GET_DISKS | GET_DEVS |
			GET_STATE | redundancy_only_flags);

	if (!sra)
		goto disappeared;

	/* It's much easier to list what array levels can't
	 * have a device disappear than all of them that can
	 */
	if (sra->array.level == 0 || sra->array.level == -1) {
		if (!st->err && !st->from_config)
			alert(EVENT_DEVICE_DISAPPEARED, "Wrong-Level", 0, dev, NULL);
		st->err++;
		goto out;
	}

	/* this array is in /proc/mdstat */
	if (array.utime == 0)
		/* external arrays don't update utime, so
		 * just make sure it is always different. */
		array.utime = st->utime + 1;;

	if (st->err) {
		/* New array appeared where previously had an error */
		st->err = 0;
		st->percent = RESYNC_NONE;
		new_array = 1;
		if (!is_container)
			alert(EVENT_NEW_ARRAY, NULL, 0, st->devname, NULL);
	}

	if (st->utime == array.utime && st->failed == sra->array.failed_disks &&
	    st->working == sra->array.working_disks &&
	    st->spare == sra->array.spare_disks &&
	    (mse == NULL || (mse->percent == st->percent))) {
		if ((st->active < st->raid) && st->spare == 0)
			retval = 1;
		goto out;
	}
	if (st->utime == 0 && /* new array */
	    mse->pattern && strchr(mse->pattern, '_') /* degraded */)
		alert(EVENT_DEGRADED_ARRAY, NULL, 0, dev, NULL);

	if (st->utime == 0 && /* new array */ st->expected_spares > 0 &&
	    sra->array.spare_disks < st->expected_spares)
		alert(EVENT_SPARES_MISSING, NULL, 0, dev, NULL);
	if (st->percent < 0 && st->percent != RESYNC_UNKNOWN &&
	    mse->percent >= 0)
		alert(EVENT_REBUILD_STARTED, NULL, 0, dev, NULL);
	if (st->percent >= 0 && mse->percent >= 0 &&
	    (mse->percent / increments) > (st->percent / increments)) {
		if((mse->percent / increments) == 0)
			alert(EVENT_REBUILD_STARTED, NULL, 0, dev, NULL);
		else
			alert(EVENT_REBUILD, NULL, mse->percent, dev, NULL);
	}

	if (mse->percent == RESYNC_NONE && st->percent >= 0) {
		/* Rebuild/sync/whatever just finished.
		 * If there is a number in /mismatch_cnt,
		 * we should report that.
		 */
		if (sra && sra->mismatch_cnt > 0) {
			char cnt[80];
			snprintf(cnt, sizeof(cnt),
				 " mismatches found: %d (on raid level %d)",
				 sra->mismatch_cnt, sra->array.level);
			alert(EVENT_REBUILD_FINISHED, NULL, 0, dev, cnt);
		} else
			alert(EVENT_REBUILD_FINISHED, NULL, 0, dev, NULL);
	}
	st->percent = mse->percent;

	remaining_disks = sra->array.nr_disks;
	for (i = 0; i < MAX_DISKS && remaining_disks > 0; i++) {
		mdu_disk_info_t disc;
		disc.number = i;
		if (md_get_disk_info(fd, &disc) >= 0) {
			disks_info[i].state = disc.state;
			disks_info[i].major = disc.major;
			disks_info[i].minor = disc.minor;
			if (disc.major || disc.minor)
				remaining_disks --;
		} else
			disks_info[i].major = disks_info[i].minor = 0;
	}
	last_disk = i;

	if (is_mdstat_ent_subarray(mse)) {
		char *sl;
		snprintf(st->parent_devnm, MD_NAME_MAX, "%s", mse->metadata_version + 10);
		sl = strchr(st->parent_devnm, '/');
		if (sl)
			*sl = 0;
	} else
		st->parent_devnm[0] = 0;
	if (st->metadata == NULL && st->parent_devnm[0] == 0)
		st->metadata = super_by_fd(fd, NULL);

	for (i = 0; i < MAX_DISKS; i++) {
		mdu_disk_info_t disc = {0, 0, 0, 0, 0};
		int newstate = 0;
		int change;
		char *dv = NULL;
		disc.number = i;
		if (i < last_disk && (disks_info[i].major || disks_info[i].minor)) {
			newstate = disks_info[i].state;
			dv = map_dev_preferred(disks_info[i].major, disks_info[i].minor, 1,
					       prefer);
			disc.state = newstate;
			disc.major = disks_info[i].major;
			disc.minor = disks_info[i].minor;
		} else
			newstate = (1 << MD_DISK_REMOVED);

		if (dv == NULL && st->devid[i])
			dv = map_dev_preferred(major(st->devid[i]),
					       minor(st->devid[i]), 1, prefer);
		change = newstate ^ st->devstate[i];
		if (st->utime && change && !st->err && !new_array) {
			if ((st->devstate[i]&change) & (1 << MD_DISK_SYNC))
				alert(EVENT_FAIL, NULL, 0, dev, dv);
			else if ((newstate & (1 << MD_DISK_FAULTY)) &&
				 (disc.major || disc.minor) &&
				 st->devid[i] == makedev(disc.major,
							 disc.minor))
				alert(EVENT_FAIL_SPARE, NULL, 0, dev, dv);
			else if ((newstate&change) & (1 << MD_DISK_SYNC))
				alert(EVENT_SPARE_ACTIVE, NULL, 0, dev, dv);
		}
		st->devstate[i] = newstate;
		st->devid[i] = makedev(disc.major, disc.minor);
	}
	st->active = sra->array.active_disks;
	st->working = sra->array.working_disks;
	st->spare = sra->array.spare_disks;
	st->failed = sra->array.failed_disks;
	st->utime = array.utime;
	st->raid = sra->array.raid_disks;
	st->err = 0;
	if ((st->active < st->raid) && st->spare == 0)
		retval = 1;

 out:
	if (sra)
		sysfs_free(sra);
	if (fd >= 0)
		close(fd);
	return retval;

 disappeared:
	if (!st->err && !is_container)
		alert(EVENT_DEVICE_DISAPPEARED, NULL, 0, dev, NULL);
	st->err++;
	goto out;
}

static int add_new_arrays(struct mdstat_ent *mdstat, struct state **statelist)
{
	struct mdstat_ent *mse;
	int new_found = 0;
	char *name;

	for (mse = mdstat; mse; mse = mse->next)
		if (mse->devnm[0] && (!mse->level || /* retrieve containers */
				      (strcmp(mse->level, "raid0") != 0 &&
				       strcmp(mse->level, "linear") != 0))) {
			struct state *st = xcalloc(1, sizeof *st);
			mdu_array_info_t array;
			int fd;

			name = get_md_name(mse->devnm);
			if (!name) {
				free(st);
				continue;
			}

			snprintf(st->devname, MD_NAME_MAX + sizeof(DEV_MD_DIR), "%s", name);
			if ((fd = open(st->devname, O_RDONLY)) < 0 ||
			    md_get_array_info(fd, &array) < 0) {
				/* no such array */
				if (fd >= 0)
					close(fd);
				put_md_name(st->devname);
				if (st->metadata) {
					st->metadata->ss->free_super(st->metadata);
					free(st->metadata);
				}
				free(st);
				continue;
			}
			close(fd);
			st->next = *statelist;
			st->err = 1;
			st->from_auto = 1;
			snprintf(st->devnm, MD_NAME_MAX, "%s", mse->devnm);
			st->percent = RESYNC_UNKNOWN;
			st->expected_spares = -1;

			if (is_mdstat_ent_subarray(mse)) {
				char *sl;

				snprintf(st->parent_devnm, MD_NAME_MAX, "%s",
					 mse->metadata_version + 10);
				sl = strchr(st->parent_devnm, '/');
				if (sl)
					*sl = 0;
			} else
				st->parent_devnm[0] = 0;
			*statelist = st;
			if (info.test)
				alert(EVENT_TEST_MESSAGE, NULL, 0, st->devname, NULL);
			new_found = 1;
		}
	return new_found;
}

static int check_donor(struct state *from, struct state *to)
{
	struct state *sub;

	if (from == to)
		return 0;
	if (from->parent)
		/* Cannot move from a member */
		return 0;
	if (from->err)
		return 0;
	for (sub = from->subarray; sub; sub = sub->subarray)
		/* If source array has degraded subarrays, don't
		 * remove anything
		 */
		if (sub->active < sub->raid)
			return 0;
	if (from->metadata->ss->external == 0)
		if (from->active < from->raid)
			return 0;
	if (from->spare <= 0)
		return 0;
	return 1;
}

static dev_t choose_spare(struct state *from, struct state *to,
			  struct domainlist *domlist, struct spare_criteria *sc)
{
	int d;
	dev_t dev = 0;

	for (d = from->raid; !dev && d < MAX_DISKS; d++) {
		if (from->devid[d] > 0 && from->devstate[d] == 0) {
			struct dev_policy *pol;

			if (to->metadata->ss->external &&
			    test_partition_from_id(from->devid[d]))
				continue;

			if (devid_matches_criteria(to->metadata, from->devid[d], sc) == false)
				continue;

			pol = devid_policy(from->devid[d]);
			if (from->spare_group)
				pol_add(&pol, pol_domain,
					from->spare_group, NULL);
			if (domain_test(domlist, pol,
					to->metadata->ss->name) == 1)
			    dev = from->devid[d];
			dev_policy_free(pol);
		}
	}
	return dev;
}

static dev_t container_choose_spare(struct state *from, struct state *to,
				    struct domainlist *domlist,
				    struct spare_criteria *sc, int active)
{
	/* This is similar to choose_spare, but we cannot trust devstate,
	 * so we need to read the metadata instead
	 */
	struct mdinfo *list;
	struct supertype *st = from->metadata;
	int fd = open(from->devname, O_RDONLY);
	int err;
	dev_t dev = 0;

	if (fd < 0)
		return 0;
	if (!st->ss->getinfo_super_disks) {
		close(fd);
		return 0;
	}

	err = st->ss->load_container(st, fd, NULL);
	close(fd);
	if (err)
		return 0;

	if (from == to) {
		/* We must check if number of active disks has not increased
		 * since ioctl in main loop. mdmon may have added spare
		 * to subarray. If so we do not need to look for more spares
		 * so return non zero value */
		int active_cnt = 0;
		struct mdinfo *dp;
		list = st->ss->getinfo_super_disks(st);
		if (!list) {
			st->ss->free_super(st);
			return 1;
		}
		dp = list->devs;
		while (dp) {
			if (dp->disk.state & (1 << MD_DISK_SYNC) &&
			    !(dp->disk.state & (1 << MD_DISK_FAULTY)))
				active_cnt++;
			dp = dp->next;
		}
		sysfs_free(list);
		if (active < active_cnt) {
			/* Spare just activated.*/
			st->ss->free_super(st);
			return 1;
		}
	}

	/* We only need one spare so full list not needed */
	list = container_choose_spares(st, sc, domlist, from->spare_group,
				       to->metadata->ss->name, 1);
	if (list) {
		struct mdinfo *disks = list->devs;
		if (disks)
			dev = makedev(disks->disk.major, disks->disk.minor);
		sysfs_free(list);
	}
	st->ss->free_super(st);
	return dev;
}

static void try_spare_migration(struct state *statelist)
{
	struct state *from;
	struct state *st;

	link_containers_with_subarrays(statelist);
	for (st = statelist; st; st = st->next)
		if (st->active < st->raid && st->spare == 0 && !st->err) {
			struct domainlist *domlist = NULL;
			struct spare_criteria sc = {0};
			int d;
			struct state *to = st;

			if (to->parent_devnm[0] && !to->parent)
				/* subarray monitored without parent container
				 * we can't move spares here */
				continue;

			if (to->parent)
				/* member of a container */
				to = to->parent;

			if (to->metadata->ss->get_spare_criteria)
				if (to->metadata->ss->get_spare_criteria(to->metadata, to->devname,
									 &sc))
					continue;

			if (to->metadata->ss->external) {
				/* We must make sure there is
				 * no suitable spare in container already.
				 * If there is we don't add more */
				dev_t devid = container_choose_spare(
					to, to, NULL, &sc, st->active);
				if (devid > 0)
					continue;
			}
			for (d = 0; d < MAX_DISKS; d++)
				if (to->devid[d])
					domainlist_add_dev(&domlist,
							   to->devid[d],
							   to->metadata->ss->name);
			if (to->spare_group)
				domain_add(&domlist, to->spare_group);
			/*
			 * No spare migration if the destination
			 * has no domain. Skip this array.
			 */
			if (!domlist)
				continue;
			for (from=statelist ; from ; from=from->next) {
				dev_t devid;
				if (!check_donor(from, to))
					continue;
				if (from->metadata->ss->external)
					devid = container_choose_spare(
						from, to, domlist, &sc, 0);
				else
					devid = choose_spare(from, to, domlist,
							     &sc);
				if (devid > 0 &&
				    move_spare(from->devname, to->devname,
					       devid)) {
					alert(EVENT_MOVE_SPARE, NULL, 0, to->devname, from->devname);
					break;
				}
			}
			domain_free(domlist);
			dev_policy_free(sc.pols);
		}
}

/* search the statelist to connect external
 * metadata subarrays with their containers
 * We always completely rebuild the tree from scratch as
 * that is safest considering the possibility of entries
 * disappearing or changing.
 */
static void link_containers_with_subarrays(struct state *list)
{
	struct state *st;
	struct state *cont;
	for (st = list; st; st = st->next) {
		st->parent = NULL;
		st->subarray = NULL;
	}
	for (st = list; st; st = st->next)
		if (st->parent_devnm[0])
			for (cont = list; cont; cont = cont->next)
				if (!cont->err && cont->parent_devnm[0] == 0 &&
				    strcmp(cont->devnm, st->parent_devnm) == 0) {
					st->parent = cont;
					st->subarray = cont->subarray;
					cont->subarray = st;
					break;
				}
}

/**
 * free_statelist() - Frees statelist.
 * @statelist: statelist to free
 */
static void free_statelist(struct state *statelist)
{
	struct state *tmp = NULL;

	while (statelist) {
		if (statelist->spare_group)
			free(statelist->spare_group);

		tmp = statelist;
		statelist = statelist->next;
		free(tmp);
	}
}

/* Not really Monitor but ... */
int Wait(char *dev)
{
	char devnm[32];
	dev_t rdev;
	char *tmp;
	int rv = 1;
	int frozen_remaining = 3;

	if (!stat_is_blkdev(dev, &rdev))
		return 2;

	tmp = devid2devnm(rdev);
	if (!tmp) {
		pr_err("Cannot get md device name.\n");
		return 2;
	}

	snprintf(devnm, sizeof(devnm), "%s", tmp);

	while(1) {
		struct mdstat_ent *ms = mdstat_read(1, 0);
		struct mdstat_ent *e;

		for (e = ms; e; e = e->next)
			if (strcmp(e->devnm, devnm) == 0)
				break;

		if (e && e->percent == RESYNC_NONE) {
			/* We could be in the brief pause before something
			 * starts. /proc/mdstat doesn't show that, but
			 * sync_action does.
			 */
			struct mdinfo mdi;
			char buf[SYSFS_MAX_BUF_SIZE];

			if (sysfs_init(&mdi, -1, devnm))
				return 2;
			if (sysfs_get_str(&mdi, NULL, "sync_action",
					  buf, sizeof(buf)) > 0 &&
			    strcmp(buf,"idle\n") != 0) {
				e->percent = RESYNC_UNKNOWN;
				if (strcmp(buf, "frozen\n") == 0) {
					if (frozen_remaining == 0)
						e->percent = RESYNC_NONE;
					else
						frozen_remaining -= 1;
				}
			}
		}
		if (!e || e->percent == RESYNC_NONE) {
			if (e && is_mdstat_ent_external(e)) {
				if (is_subarray(&e->metadata_version[9]))
					ping_monitor(&e->metadata_version[9]);
				else
					ping_monitor(devnm);
			}
			free_mdstat(ms);
			return rv;
		}
		free_mdstat(ms);
		rv = 0;
		mdstat_wait(5);
	}
}

/* The state "broken" is used only for RAID0/LINEAR - it's the same as
 * "clean", but used in case the array has one or more members missing.
 */
static char *clean_states[] = {
	"clear", "inactive", "readonly", "read-auto", "clean", "broken", NULL };

int WaitClean(char *dev, int verbose)
{
	int fd;
	struct mdinfo *mdi;
	int rv = 1;
	char devnm[32];

	if (!stat_is_blkdev(dev, NULL))
		return 2;
	fd = open(dev, O_RDONLY);
	if (fd < 0) {
		if (verbose)
			pr_err("Couldn't open %s: %s\n", dev, strerror(errno));
		return 1;
	}

	snprintf(devnm, sizeof(devnm), "%s", fd2devnm(fd));

	mdi = sysfs_read(fd, devnm, GET_VERSION|GET_LEVEL|GET_SAFEMODE);
	if (!mdi) {
		if (verbose)
			pr_err("Failed to read sysfs attributes for %s\n", dev);
		close(fd);
		return 0;
	}

	switch(mdi->array.level) {
	case LEVEL_LINEAR:
	case LEVEL_MULTIPATH:
	case 0:
		/* safemode delay is irrelevant for these levels */
		rv = 0;
	}

	/* for internal metadata the kernel handles the final clean
	 * transition, containers can never be dirty
	 */
	if (!is_subarray(mdi->text_version))
		rv = 0;

	/* safemode disabled ? */
	if (mdi->safe_mode_delay == 0)
		rv = 0;

	if (rv) {
		int state_fd = sysfs_open(fd2devnm(fd), NULL, "array_state");
		char buf[SYSFS_MAX_BUF_SIZE];
		int delay = 5000;

		/* minimize the safe_mode_delay and prepare to wait up to 5s
		 * for writes to quiesce
		 */
		sysfs_set_safemode(mdi, 1);

		/* wait for array_state to be clean */
		while (1) {
			rv = read(state_fd, buf, sizeof(buf));
			if (rv < 0)
				break;
			if (sysfs_match_word(buf, clean_states) <
			    (int)ARRAY_SIZE(clean_states) - 1)
				break;
			rv = sysfs_wait(state_fd, &delay);
			if (rv < 0 && errno != EINTR)
				break;
			lseek(state_fd, 0, SEEK_SET);
		}
		if (rv < 0)
			rv = 1;
		else if (ping_monitor(mdi->text_version) == 0) {
			/* we need to ping to close the window between array
			 * state transitioning to clean and the metadata being
			 * marked clean
			 */
			rv = 0;
		} else {
			rv = 1;
			pr_err("Error connecting monitor with %s\n", dev);
		}
		if (rv && verbose)
			pr_err("Error waiting for %s to be clean\n", dev);

		/* restore the original safe_mode_delay */
		sysfs_set_safemode(mdi, mdi->safe_mode_delay);
		close(state_fd);
	}

	sysfs_free(mdi);
	close(fd);

	return rv;
}
