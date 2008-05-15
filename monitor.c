
#include "mdadm.h"
#include "mdmon.h"

#include <sys/select.h>


static char *array_states[] = {
	"clear", "inactive", "suspended", "readonly", "read-auto",
	"clean", "active", "write-pending", "active-idle", NULL };
static char *sync_actions[] = {
	"idle", "reshape", "resync", "recover", "check", "repair", NULL
};

static int write_attr(char *attr, int fd)
{
	return write(fd, attr, strlen(attr));
}

static void add_fd(fd_set *fds, int *maxfd, int fd)
{
	if (fd < 0)
		return;
	if (fd > *maxfd)
		*maxfd = fd;
	FD_SET(fd, fds);
}

static int read_attr(char *buf, int len, int fd)
{
	int n;

	if (fd < 0) {
		buf[0] = 0;
		return 0;
	}
	lseek(fd, 0, 0);
	n = read(fd, buf, len - 1);

	if (n <= 0) {
		buf[0] = 0;
		return 0;
	}
	buf[n] = 0;
	if (buf[n-1] == '\n')
		buf[n-1] = 0;
	return n;
}

static int get_sync_pos(struct active_array *a)
{
	char buf[30];
	int n;

	n = read_attr(buf, 30, a->sync_pos_fd);
	if (n <= 0)
		return n;

	if (strncmp(buf, "max", 3) == 0) {
		a->sync_pos = ~(unsigned long long)0;
		return 1;
	}
	a->sync_pos = strtoull(buf, NULL, 10);
	return 1;
}

static int get_resync_start(struct active_array *a)
{
	char buf[30];
	int n;

	n = read_attr(buf, 30, a->resync_start_fd);
	if (n <= 0)
		return n;

	a->resync_start = strtoull(buf, NULL, 10);

	return 1;
}

static int attr_match(const char *attr, const char *str)
{
	/* See if attr, read from a sysfs file, matches
	 * str.  They must either be the same, or attr can
	 * have a trailing newline or comma
	 */
	while (*attr && *str && *attr == *str) {
		attr++;
		str++;
	}

	if (*str || (*attr && *attr != ',' && *attr != '\n'))
		return 0;
	return 1;
}

static int match_word(const char *word, char **list)
{
	int n;
	for (n=0; list[n]; n++)
		if (attr_match(word, list[n]))
			break;
	return n;
}

static enum array_state read_state(int fd)
{
	char buf[20];
	int n = read_attr(buf, 20, fd);

	if (n <= 0)
		return bad_word;
	return (enum array_state) match_word(buf, array_states);
}

static enum sync_action read_action( int fd)
{
	char buf[20];
	int n = read_attr(buf, 20, fd);

	if (n <= 0)
		return bad_action;
	return (enum sync_action) match_word(buf, sync_actions);
}

#define DS_FAULTY	1
#define	DS_INSYNC	2
#define	DS_WRITE_MOSTLY	4
#define	DS_SPARE	8
#define	DS_REMOVE	1024

int read_dev_state(int fd)
{
	char buf[60];
	int n = read_attr(buf, 60, fd);
	char *cp;
	int rv = 0;

	if (n <= 0)
		return 0;

	cp = buf;
	while (cp) {
		if (attr_match("faulty", cp))
			rv |= DS_FAULTY;
		if (attr_match("in_sync", cp))
			rv |= DS_INSYNC;
		if (attr_match("write_mostly", cp))
			rv |= DS_WRITE_MOSTLY;
		if (attr_match("spare", cp))
			rv |= DS_SPARE;
		cp = strchr(cp, ',');
		if (cp)
			cp++;
	}
	return rv;
}


/* Monitor a set of active md arrays - all of which share the
 * same metadata - and respond to events that require
 * metadata update.
 *
 * New arrays are detected by another thread which allocates
 * required memory and attaches the data structure to our list.
 *
 * Events:
 *  Array stops.
 *    This is detected by array_state going to 'clear' or 'inactive'.
 *    while we thought it was active.
 *    Response is to mark metadata as clean and 'clear' the array(??)
 *  write-pending
 *    array_state if 'write-pending'
 *    We mark metadata as 'dirty' then set array to 'active'.
 *  active_idle
 *    Either ignore, or mark clean, then mark metadata as clean.
 *
 *  device fails
 *    detected by rd-N/state reporting "faulty"
 *    mark device as 'failed' in metadata, the remove device
 *    by writing 'remove' to rd/state.
 *
 *  sync completes
 *    sync_action was 'resync' and becomes 'idle' and resync_start becomes
 *    MaxSector
 *    Notify metadata that sync is complete.
 *    "Deal with Degraded"
 *
 *  recovery completes
 *    sync_action changes from 'recover' to 'idle'
 *    Check each device state and mark metadata if 'faulty' or 'in_sync'.
 *    "Deal with Degraded"
 *
 *  deal with degraded array
 *    We only do this when first noticing the array is degraded.
 *    This can be when we first see the array, when sync completes or
 *    when recovery completes.
 *
 *    Check if number of failed devices suggests recovery is needed, and
 *    skip if not.
 *    Ask metadata for a spare device
 *    Add device as not in_sync and give a role
 *    Update metadata.
 *    Start recovery.
 *
 *  deal with resync
 *    This only happens on finding a new array... mdadm will have set
 *    'resync_start' to the correct value.  If 'resync_start' indicates that an
 *    resync needs to occur set the array to the 'active' state rather than the
 *    initial read-auto state.
 *
 *
 *
 * We wait for a change (poll/select) on array_state, sync_action, and
 * each rd-X/state file.
 * When we get any change, we check everything.  So read each state file,
 * then decide what to do.
 *
 * The core action is to write new metadata to all devices in the array.
 * This is done at most once on any wakeup.
 * After that we might:
 *   - update the array_state
 *   - set the role of some devices.
 *   - request a sync_action
 *
 */

static int read_and_act(struct active_array *a)
{
	int check_degraded;
	struct mdinfo *mdi;

	a->next_state = bad_word;
	a->next_action = bad_action;

	a->curr_state = read_state(a->info.state_fd);
	a->curr_action = read_action(a->action_fd);
	for (mdi = a->info.devs; mdi ; mdi = mdi->next) {
		mdi->next_state = 0;
		mdi->curr_state = read_dev_state(mdi->state_fd);
	}

	if (a->curr_state <= inactive &&
	    a->prev_state > inactive) {
		/* array has been stopped */
		get_sync_pos(a);
		a->container->ss->mark_clean(a, a->sync_pos);
		a->next_state = clear;
	}
	if (a->curr_state == write_pending) {
		a->container->ss->mark_dirty(a);
		a->next_state = active;
	}
	if (a->curr_state == active_idle) {
		/* Set array to 'clean' FIRST, then
		 * a->ss->mark_clean(a);
		 * just ignore for now.
		 */
	}

	if (a->curr_state == readonly) {
		/* Well, I'm ready to handle things, so
		 * read-auto is OK. FIXME what if we really want
		 * readonly ???
		 */
		get_resync_start(a);
		if (a->resync_start == ~0ULL)
			a->next_state = read_auto; /* array is clean */
		else {
			a->container->ss->mark_dirty(a);
			a->next_state = active;
		}
	}

	if (a->curr_action == idle &&
	    a->prev_action == resync) {
		/* check resync_start to see if it is 'max' */
		get_resync_start(a);
		a->container->ss->mark_sync(a, a->resync_start);
		check_degraded = 1;
	}

	if (a->curr_action == idle &&
	    a->prev_action == recover) {
		for (mdi = a->info.devs ; mdi ; mdi = mdi->next) {
			a->container->ss->set_disk(a, mdi->disk.raid_disk);
			if (! (mdi->curr_state & DS_INSYNC))
				check_degraded = 1;
		}
	}


	for (mdi = a->info.devs ; mdi ; mdi = mdi->next) {
		if (mdi->curr_state & DS_FAULTY) {
			a->container->ss->set_disk(a, mdi->disk.raid_disk);
			check_degraded = 1;
			mdi->next_state = DS_REMOVE;
		}
	}

	if (check_degraded) {
		// FIXME;
	}

	a->container->ss->sync_metadata(a);

	/* Effect state changes in the array */
	if (a->next_state != bad_word)
		write_attr(array_states[a->next_state], a->info.state_fd);
	if (a->next_action != bad_action)
		write_attr(sync_actions[a->next_action], a->action_fd);
	for (mdi = a->info.devs; mdi ; mdi = mdi->next) {
		if (mdi->next_state == DS_REMOVE)
			write_attr("remove", mdi->state_fd);
		if (mdi->next_state & DS_INSYNC)
			write_attr("+in_sync", mdi->state_fd);
	}

	/* move curr_ to prev_ */
	a->prev_state = a->curr_state;

	a->prev_action = a->curr_action;

	for (mdi = a->info.devs; mdi ; mdi = mdi->next) {
		mdi->prev_state = mdi->curr_state;
		mdi->next_state = 0;
	}

	return 1;
}

static int wait_and_act(struct active_array *aa, int pfd, int nowait)
{
	fd_set rfds;
	int maxfd = 0;
	struct active_array *a;
	int rv;

	FD_ZERO(&rfds);

	add_fd(&rfds, &maxfd, pfd);
	for (a = aa ; a ; a = a->next) {
		struct mdinfo *mdi;

		add_fd(&rfds, &maxfd, a->info.state_fd);
		add_fd(&rfds, &maxfd, a->action_fd);
		for (mdi = a->info.devs ; mdi ; mdi = mdi->next)
			add_fd(&rfds, &maxfd, mdi->state_fd);
	}

	if (!nowait) {
		rv = select(maxfd+1, &rfds, NULL, NULL, NULL);

		if (rv <= 0)
			return rv;

		if (FD_ISSET(pfd, &rfds)) {
			char buf[4];
			read(pfd, buf, 4);
			; // FIXME read from the pipe
		}
	}

	for (a = aa; a ; a = a->next) {
		if (a->replaces) {
			struct active_array **ap;
			for (ap = &a->next; *ap && *ap != a->replaces;
			     ap = & (*ap)->next)
				;
			if (*ap)
				*ap = (*ap)->next;
			discard_this = a->replaces;
			a->replaces = NULL;
		}
		rv += read_and_act(a);
	}
	return rv;
}

void do_monitor(struct supertype *container)
{
	int rv;
	int first = 1;
	do {
		rv = wait_and_act(container->arrays, container->pipe[0], first);
		first = 0;
	} while (rv >= 0);
}
