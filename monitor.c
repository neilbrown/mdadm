
#include "mdadm.h"
#include "mdmon.h"

#include <sys/select.h>
#include <signal.h>

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
		if (attr_match(cp, "faulty"))
			rv |= DS_FAULTY;
		if (attr_match(cp, "in_sync"))
			rv |= DS_INSYNC;
		if (attr_match(cp, "write_mostly"))
			rv |= DS_WRITE_MOSTLY;
		if (attr_match(cp, "spare"))
			rv |= DS_SPARE;
		if (attr_match(cp, "blocked"))
			rv |= DS_BLOCKED;
		cp = strchr(cp, ',');
		if (cp)
			cp++;
	}
	return rv;
}

static void signal_manager(void)
{
	kill(getpid(), SIGUSR1);
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
 *    mark device as 'failed' in metadata, let the kernel release the
 *    device by writing '-blocked' to rd/state, and finally write 'remove' to
 *    rd/state.  Before a disk can be replaced it must be failed and removed
 *    from all container members, this will be preemptive for the other
 *    arrays... safe?
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
	int deactivate = 0;
	struct mdinfo *mdi;

	a->next_state = bad_word;
	a->next_action = bad_action;

	a->curr_state = read_state(a->info.state_fd);
	a->curr_action = read_action(a->action_fd);
	for (mdi = a->info.devs; mdi ; mdi = mdi->next) {
		mdi->next_state = 0;
		if (mdi->state_fd > 0)
			mdi->curr_state = read_dev_state(mdi->state_fd);
	}

	if (a->curr_state <= inactive &&
	    a->prev_state > inactive) {
		/* array has been stopped */
		a->container->ss->set_array_state(a, 1);
		a->next_state = clear;
		deactivate = 1;
	}
	if (a->curr_state == write_pending) {
		get_resync_start(a);
		a->container->ss->set_array_state(a, 0);
		a->next_state = active;
	}
	if (a->curr_state == active_idle) {
		/* Set array to 'clean' FIRST, then
		 * a->ss->mark_clean(a, ~0ULL);
		 * just ignore for now.
		 */
	}

	if (a->curr_state == readonly) {
		/* Well, I'm ready to handle things, so
		 * read-auto is OK. FIXME what if we really want
		 * readonly ???
		 */
		get_resync_start(a);
		printf("Found a readonly array at %llu\n", a->resync_start);
		if (a->resync_start == ~0ULL)
			a->next_state = read_auto; /* array is clean */
		else {
			a->container->ss->set_array_state(a, 0);
			a->next_state = active;
		}
	}

	if (a->curr_action == idle &&
	    a->prev_action == resync) {
		/* A resync has finished.  The endpoint is recorded in
		 * 'sync_start'.  We don't update the metadata
		 * until the array goes inactive or readonly though.
		 * Just check if we need to fiddle spares.
		 */
		get_resync_start(a);
		a->container->ss->set_array_state(a, 0);
		check_degraded = 1;
	}

	if (a->curr_action == idle &&
	    a->prev_action == recover) {
		for (mdi = a->info.devs ; mdi ; mdi = mdi->next) {
			a->container->ss->set_disk(a, mdi->disk.raid_disk,
						   mdi->curr_state);
			if (! (mdi->curr_state & DS_INSYNC))
				check_degraded = 1;
		}
	}


	for (mdi = a->info.devs ; mdi ; mdi = mdi->next) {
		if (mdi->curr_state & DS_FAULTY) {
			a->container->ss->set_disk(a, mdi->disk.raid_disk,
						   mdi->curr_state);
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
		if (mdi->next_state == DS_REMOVE && mdi->state_fd > 0) {
			int remove_err;

			write_attr("-blocked", mdi->state_fd);
			/* the kernel may not be able to immediately remove the
			 * disk, we can simply wait until the next event to try
			 * again.
			 */
			remove_err = write_attr("remove", mdi->state_fd);
			if (!remove_err) {
				close(mdi->state_fd);
				mdi->state_fd = -1;
			}
		}
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

	if (deactivate)
		a->container = NULL;

	return 1;
}

static struct mdinfo *
find_device(struct active_array *a, int major, int minor)
{
	struct mdinfo *mdi;

	for (mdi = a->info.devs ; mdi ; mdi = mdi->next)
		if (mdi->disk.major == major && mdi->disk.minor == minor)
			return mdi;

	return NULL;
}

static void reconcile_failed(struct active_array *aa, struct mdinfo *failed)
{
	struct active_array *a;
	struct mdinfo *victim;

	for (a = aa; a; a = a->next) {
		if (!a->container)
			continue;
		victim = find_device(a, failed->disk.major, failed->disk.minor);
		if (!victim)
			continue;

		if (!(victim->curr_state & DS_FAULTY))
			write_attr("faulty", victim->state_fd);
	}
}

static int handle_remove_device(struct md_remove_device_cmd *cmd, struct active_array *aa)
{
	struct active_array *a;
	struct mdinfo *victim;
	int rv;

	/* scan all arrays for the given device, if ->state_fd is closed (-1)
	 * in all cases then mark the disk as removed in the metadata.
	 * Otherwise reply that it is busy.
	 */

	/* pass1 check that it is not in use anywhere */
	/* note: we are safe from re-adds as long as the device exists in the
	 * container
	 */
	for (a = aa; a; a = a->next) {
		if (!a->container)
			continue;
		victim = find_device(a, major(cmd->rdev), minor(cmd->rdev));
		if (!victim)
			continue;
		if (victim->state_fd > 0)
			return -EBUSY;
	}

	/* pass2 schedule and process removal per array */
	for (a = aa; a; a = a->next) {
		if (!a->container)
			continue;
		victim = find_device(a, major(cmd->rdev), minor(cmd->rdev));
		if (!victim)
			continue;
		victim->curr_state |= DS_REMOVE;
		rv = read_and_act(a);
		if (rv < 0)
			return rv;
	}

	return 0;
}

static int handle_pipe(struct md_generic_cmd *cmd, struct active_array *aa)
{
	switch (cmd->action) {
	case md_action_ping_monitor:
		return 0;
	case md_action_remove_device:
		return handle_remove_device((void *) cmd, aa);
	}

	return -1;
}

static int wait_and_act(struct supertype *container, int pfd,
			int monfd, int nowait)
{
	fd_set rfds;
	int maxfd = 0;
	struct active_array **aap = &container->arrays;
	struct active_array *a, **ap;
	int rv;
	struct mdinfo *mdi;

	FD_ZERO(&rfds);

	add_fd(&rfds, &maxfd, pfd);
	for (ap = aap ; *ap ;) {
		a = *ap;
		/* once an array has been deactivated we want to
		 * ask the manager to discard it.
		 */
		if (!a->container) {
			if (discard_this) {
				ap = &(*ap)->next;
				continue;
			}
			*ap = a->next;
			a->next = NULL;
			discard_this = a;
			signal_manager();
			continue;
		}

		add_fd(&rfds, &maxfd, a->info.state_fd);
		add_fd(&rfds, &maxfd, a->action_fd);
		for (mdi = a->info.devs ; mdi ; mdi = mdi->next)
			add_fd(&rfds, &maxfd, mdi->state_fd);

		ap = &(*ap)->next;
	}

	if (manager_ready && *aap == NULL) {
		/* No interesting arrays. Lets see about exiting.
		 * Note that blocking at this point is not a problem
		 * as there are no active arrays, there is nothing that
		 * we need to be ready to do.
		 */
		int fd = open(container->device_name, O_RDONLY|O_EXCL);
		if (fd >= 0 || errno != EBUSY) {
			/* OK, we are safe to leave */
			exit_now = 1;
			signal_manager();
			remove_pidfile(container->devname);
			exit(0);
		}
	}

	if (!nowait) {
		rv = select(maxfd+1, &rfds, NULL, NULL, NULL);

		if (rv <= 0)
			return rv;

		if (FD_ISSET(pfd, &rfds)) {
			int err = -1;

			if (read(pfd, &err, 1) > 0)
				err = handle_pipe(active_cmd, *aap);
			write(monfd, &err, 1);
		}
	}

	for (a = *aap; a ; a = a->next) {
		if (a->replaces && !discard_this) {
			struct active_array **ap;
			for (ap = &a->next; *ap && *ap != a->replaces;
			     ap = & (*ap)->next)
				;
			if (*ap)
				*ap = (*ap)->next;
			discard_this = a->replaces;
			a->replaces = NULL;
			signal_manager();
		}
		if (a->container)
			rv += read_and_act(a);
	}

	/* propagate failures across container members */
	for (a = *aap; a ; a = a->next) {
		if (!a->container)
			continue;
		for (mdi = a->info.devs ; mdi ; mdi = mdi->next)
			if (mdi->curr_state & DS_FAULTY)
				reconcile_failed(*aap, mdi);
	}

	return rv;
}

void do_monitor(struct supertype *container)
{
	int rv;
	int first = 1;
	do {
		rv = wait_and_act(container, container->mgr_pipe[0],
				  container->mon_pipe[1], first);
		first = 0;
	} while (rv >= 0);
}
