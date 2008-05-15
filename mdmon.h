
enum array_state { clear, inactive, suspended, readonly, read_auto,
		   clean, active, write_pending, active_idle, bad_word};

enum sync_action { idle, reshape, resync, recover, check, repair, bad_action };


struct active_array {
	struct mdinfo info;
	struct supertype *container;
	struct active_array *next, *replaces;

	int action_fd;
	int sync_pos_fd;
	int resync_start_fd;

	enum array_state prev_state, curr_state, next_state;
	enum sync_action prev_action, curr_action, next_action;

	int devnum;

	unsigned long long sync_pos;
	unsigned long long resync_start;
};



#define MD_MAJOR 9

extern struct active_array *container;
extern struct active_array *array_list;
extern struct active_array *discard_this;
extern struct active_array *pending_discard;


void do_monitor(struct supertype *container);
void do_manager(struct supertype *container);

int read_dev_state(int fd);

struct mdstat_ent *mdstat_read(int hold, int start);

extern struct superswitch super_ddf, super_ddf_bvd, super_ddf_svd;
