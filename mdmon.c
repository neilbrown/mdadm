
/*
 * md array manager.
 * When md arrays have user-space managed metadata, this is the program
 * that does the managing.
 *
 * Given one argument: the name of the array (e.g. /dev/md0) that is
 * the container.
 * We fork off a helper that runs high priority and mlocked.  It responds to
 * device failures and other events that might stop writeout, or that are
 * trivial to deal with.
 * The main thread then watches for new arrays being created in the container
 * and starts monitoring them too ... along with a few other tasks.
 *
 * The main thread communicates with the priority thread by writing over
 * a pipe.
 * Separate programs can communicate with the main thread via Unix-domain
 * socket.
 * The two threads share address space and open file table.
 *
 */

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include	<unistd.h>
#include	<stdlib.h>
#include	<sys/types.h>
#include	<sys/stat.h>
#include	<sys/socket.h>
#include	<sys/un.h>
#include	<sys/mman.h>
#include	<sys/syscall.h>
#include	<sys/wait.h>
#include	<stdio.h>
#include	<errno.h>
#include	<string.h>
#include	<fcntl.h>
#include	<signal.h>

#include	<sched.h>

#include	"mdadm.h"
#include	"mdmon.h"

struct active_array *discard_this;
struct active_array *pending_discard;

int mon_tid, mgr_tid;

int sigterm;

int run_child(void *v)
{
	struct supertype *c = v;

	do_monitor(c);
	return 0;
}

int clone_monitor(struct supertype *container)
{
	static char stack[4096];

	mon_tid = clone(run_child, stack+4096-64,
		   CLONE_FS|CLONE_FILES|CLONE_VM|CLONE_SIGHAND|CLONE_THREAD,
		   container);

	mgr_tid = syscall(SYS_gettid);

	return mon_tid;
}

static struct superswitch *find_metadata_methods(char *vers)
{
	if (strcmp(vers, "ddf") == 0)
		return &super_ddf;
	if (strcmp(vers, "imsm") == 0)
		return &super_imsm;
	return NULL;
}


int make_pidfile(char *devname, int o_excl)
{
	char path[100];
	char pid[10];
	int fd;
	int n;

	if (sigterm)
		return -1;

	sprintf(path, "/var/run/mdadm/%s.pid", devname);

	fd = open(path, O_RDWR|O_CREAT|o_excl, 0600);
	if (fd < 0)
		return -errno;
	sprintf(pid, "%d\n", getpid());
	n = write(fd, pid, strlen(pid));
	close(fd);
	if (n < 0)
		return -errno;
	return 0;
}

int is_container_member(struct mdstat_ent *mdstat, char *container)
{
	if (mdstat->metadata_version == NULL ||
	    strncmp(mdstat->metadata_version, "external:", 9) != 0 ||
	    !is_subarray(mdstat->metadata_version+9) ||
	    strncmp(mdstat->metadata_version+10, container, strlen(container)) != 0 ||
	    mdstat->metadata_version[10+strlen(container)] != '/')
		return 0;

	return 1;
}

void remove_pidfile(char *devname);
static void try_kill_monitor(char *devname)
{
	char buf[100];
	int fd;
	pid_t pid;
	struct mdstat_ent *mdstat;

	sprintf(buf, "/var/run/mdadm/%s.pid", devname);
	fd = open(buf, O_RDONLY);
	if (fd < 0)
		return;

	if (read(fd, buf, sizeof(buf)) < 0) {
		close(fd);
		return;
	}

	close(fd);
	pid = strtoul(buf, NULL, 10);

	/* kill this process if it is mdmon */
	sprintf(buf, "/proc/%lu/cmdline", (unsigned long) pid);
	fd = open(buf, O_RDONLY);
	if (fd < 0)
		return;

	if (read(fd, buf, sizeof(buf)) < 0) {
		close(fd);
		return;
	}

	if (!strstr(buf, "mdmon"))
		return;

	kill(pid, SIGTERM);

	mdstat = mdstat_read(0, 0);
	for ( ; mdstat; mdstat = mdstat->next)
		if (is_container_member(mdstat, devname)) {
			sprintf(buf, "/dev/%s", mdstat->dev);
			WaitClean(buf);
		}
	free_mdstat(mdstat);
	remove_pidfile(devname);
}

void remove_pidfile(char *devname)
{
	char buf[100];

	if (sigterm)
		return;

	sprintf(buf, "/var/run/mdadm/%s.pid", devname);
	unlink(buf);
	sprintf(buf, "/var/run/mdadm/%s.sock", devname);
	unlink(buf);
}

int make_control_sock(char *devname)
{
	char path[100];
	int sfd;
	long fl;
	struct sockaddr_un addr;

	if (sigterm)
		return -1;

	sprintf(path, "/var/run/mdadm/%s.sock", devname);
	unlink(path);
	sfd = socket(PF_LOCAL, SOCK_STREAM, 0);
	if (sfd < 0)
		return -1;

	addr.sun_family = PF_LOCAL;
	strcpy(addr.sun_path, path);
	if (bind(sfd, &addr, sizeof(addr)) < 0) {
		close(sfd);
		return -1;
	}
	listen(sfd, 10);
	fl = fcntl(sfd, F_GETFL, 0);
	fl |= O_NONBLOCK;
	fcntl(sfd, F_SETFL, fl);
	return sfd;
}

int socket_hup_requested;
static void hup(int sig)
{
	socket_hup_requested = 1;
}

static void term(int sig)
{
	sigterm = 1;
}

static void wake_me(int sig)
{

}

/* if we are debugging and starting mdmon by hand then don't fork */
static int do_fork(void)
{
	#ifdef DEBUG
	if (env_no_mdmon())
		return 0;
	#endif

	return 1;
}



int main(int argc, char *argv[])
{
	int mdfd;
	struct mdinfo *mdi, *di;
	struct supertype *container;
	sigset_t set;
	struct sigaction act;
	int pfd[2];
	int status;
	int ignore;

	if (argc != 2) {
		fprintf(stderr, "Usage: md-manage /device/name/for/container\n");
		exit(2);
	}
	mdfd = open(argv[1], O_RDWR);
	if (mdfd < 0) {
		fprintf(stderr, "md-manage: %s: %s\n", argv[1],
			strerror(errno));
		exit(1);
	}
	if (md_get_version(mdfd) < 0) {
		fprintf(stderr, "md-manage: %s: Not an md device\n",
			argv[1]);
		exit(1);
	}

	/* Fork, and have the child tell us when they are ready */
	if (do_fork()) {
		if (pipe(pfd) != 0) {
			fprintf(stderr, "mdmon: failed to create pipe\n");
			exit(1);
		}
		switch(fork()) {
		case -1:
			fprintf(stderr, "mdmon: failed to fork: %s\n",
				strerror(errno));
			exit(1);
		case 0: /* child */
			close(pfd[0]);
			break;
		default: /* parent */
			close(pfd[1]);
			if (read(pfd[0], &status, sizeof(status)) != sizeof(status)) {
				wait(&status);
				status = WEXITSTATUS(status);
			}
			exit(status);
		}
	} else
		pfd[0] = pfd[1] = -1;
	/* hopefully it is a container - we'll check later */

	container = malloc(sizeof(*container));
	container->devnum = fd2devnum(mdfd);
	container->devname = devnum2devname(container->devnum);
	container->device_name = argv[1];

	/* SIGUSR is sent between parent and child.  So both block it
	 * and enable it only with pselect.
	 */
	sigemptyset(&set);
	sigaddset(&set, SIGUSR1);
	sigaddset(&set, SIGHUP);
	sigaddset(&set, SIGALRM);
	sigaddset(&set, SIGTERM);
	sigprocmask(SIG_BLOCK, &set, NULL);
	act.sa_handler = wake_me;
	act.sa_flags = 0;
	sigaction(SIGUSR1, &act, NULL);
	sigaction(SIGALRM, &act, NULL);
	act.sa_handler = hup;
	sigaction(SIGHUP, &act, NULL);
	act.sa_handler = term;
	sigaction(SIGTERM, &act, NULL);
	act.sa_handler = SIG_IGN;
	sigaction(SIGPIPE, &act, NULL);

	/* If this fails, we hope it already exists */
	mkdir("/var/run/mdadm", 0600);
	/* pid file lives in /var/run/mdadm/mdXX.pid */
	if (make_pidfile(container->devname, O_EXCL) < 0) {
		if (ping_monitor(container->devname) == 0) {
			fprintf(stderr, "mdmon: %s already managed\n",
				container->devname);
			exit(3);
		} else {
			int err;

			/* cleanup the old monitor, this one is taking over */
			try_kill_monitor(container->devname);
			err = make_pidfile(container->devname, 0);
			if (err < 0) {
				fprintf(stderr, "mdmon: %s Cannot create pidfile\n",
					container->devname);
				if (err == -EROFS) {
					/* FIXME implement a mechanism to
					 * prevent duplicate monitor instances
					 */
					fprintf(stderr,
						"mdmon: continuing on read-only file system\n");
				} else
					exit(3);
			}
		}
	}

	container->sock = make_control_sock(container->devname);
	container->arrays = NULL;

	mdi = sysfs_read(mdfd, container->devnum,
			 GET_VERSION|GET_LEVEL|GET_DEVS);

	if (!mdi) {
		fprintf(stderr, "mdmon: failed to load sysfs info for %s\n",
			container->devname);
		exit(3);
	}
	if (mdi->array.level != UnSet) {
		fprintf(stderr, "mdmon: %s is not a container - cannot monitor\n",
			argv[1]);
		exit(3);
	}
	if (mdi->array.major_version != -1 ||
	    mdi->array.minor_version != -2) {
		fprintf(stderr, "mdmon: %s does not use external metadata - cannot monitor\n",
			argv[1]);
		exit(3);
	}

	container->ss = find_metadata_methods(mdi->text_version);
	if (container->ss == NULL) {
		fprintf(stderr, "mdmon: %s uses unknown metadata: %s\n",
			argv[1], mdi->text_version);
		exit(3);
	}

	container->devs = NULL;
	for (di = mdi->devs; di; di = di->next) {
		struct mdinfo *cd = malloc(sizeof(*cd));
		*cd = *di;
		cd->next = container->devs;
		container->devs = cd;
	}
	sysfs_free(mdi);


	if (container->ss->load_super(container, mdfd, argv[1])) {
		fprintf(stderr, "mdmon: Cannot load metadata for %s\n",
			argv[1]);
		exit(3);
	}

	/* Ok, this is close enough.  We can say goodbye to our parent now.
	 */
	status = 0;
	if (write(pfd[1], &status, sizeof(status)) < 0)
		fprintf(stderr, "mdmon: failed to notify our parent: %d\n",
			getppid());
	close(pfd[1]);

	ignore = chdir("/");
	setsid();
	close(0);
	open("/dev/null", O_RDWR);
	close(1);
	ignore = dup(0);
#ifndef DEBUG
	close(2);
	ignore = dup(0);
#endif

	mlockall(MCL_FUTURE);

	if (clone_monitor(container) < 0) {
		fprintf(stderr, "mdmon: failed to start monitor process: %s\n",
			strerror(errno));
		exit(2);
	}

	do_manager(container);

	exit(0);
}
