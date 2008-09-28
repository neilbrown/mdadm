enum array_state { clear, inactive, suspended, readonly, read_auto,
		   clean, active, write_pending, active_idle, bad_word};

enum sync_action { idle, reshape, resync, recover, check, repair, bad_action };


struct active_array {
	struct mdinfo info;
	struct supertype *container;
	struct active_array *next, *replaces;

	int action_fd;
	int resync_start_fd;
	int metadata_fd; /* for monitoring rw/ro status */

	enum array_state prev_state, curr_state, next_state;
	enum sync_action prev_action, curr_action, next_action;

	int check_degraded; /* flag set by mon, read by manage */

	int devnum;

	unsigned long long resync_start;
};

/*
 * Metadata updates are handled by the monitor thread,
 * as it has exclusive access to the metadata.
 * When the manager want to updates metadata, either
 * for it's own reason (e.g. committing a spare) or
 * on behalf of mdadm, it creates a metadata_update
 * structure and queues it to the monitor.
 * Updates are created and processed by code under the
 * superswitch.  All common code sees them as opaque
 * blobs.
 */
extern struct metadata_update *update_queue, *update_queue_handled;

#define MD_MAJOR 9

extern struct active_array *container;
extern struct active_array *discard_this;
extern struct active_array *pending_discard;
extern struct md_generic_cmd *active_cmd;


void remove_pidfile(char *devname);
void do_monitor(struct supertype *container);
void do_manager(struct supertype *container);
int make_control_sock(char *devname);
int make_pidfile(char *devname, int o_excl);
extern int socket_hup_requested;

int read_dev_state(int fd);
int get_resync_start(struct active_array *a);

struct mdstat_ent *mdstat_read(int hold, int start);

extern int exit_now, manager_ready;
extern int mon_tid, mgr_tid;
extern int monitor_loop_cnt;

/* helper routine to determine resync completion since MaxSector is a
 * moving target
 */
static inline int is_resync_complete(struct active_array *a)
{
	if (a->resync_start >= a->info.component_size)
		return 1;
	return 0;
}

