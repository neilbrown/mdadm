/*
 * mdadm - manage Linux "md" devices aka RAID arrays.
 *
 * Copyright (C) 2001-2002 Neil Brown <neilb@cse.unsw.edu.au>
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
 *    Email: <neilb@cse.unsw.edu.au>
 *    Paper: Neil Brown
 *           School of Computer Science and Engineering
 *           The University of New South Wales
 *           Sydney, 2052
 *           Australia
 */

#include	"mdadm.h"
#include	"md_p.h"
#include	"md_u.h"
#include	<sys/wait.h>
#include	<sys/signal.h>
#include	<values.h>

static void alert(char *event, char *dev, char *disc, char *mailaddr, char *cmd);

static char *percentalerts[] = { 
	"RebuildStarted",
	"Rebuild20",
	"Rebuild40",
	"Rebuild60",
	"Rebuild80",
};

int Monitor(mddev_dev_t devlist,
	    char *mailaddr, char *alert_cmd,
	    int period, int daemonise, int scan, int oneshot,
	    char *config, int test)
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
	 *    Rebuild20 Rebuild40 Rebuild60 Rebuild80
	 *      percent went from below to not-below that number
	 *    DeviceDisappeared
	 *      Couldn't access a device which was previously visible
	 *
	 * if we detect an array with active<raid and spare==0
	 * we look at other arrays that have same spare-group
	 * If we find one with active==raid and spare>0,
	 *  and if we can get_disk_info and find a name
	 *  Then we hot-remove and hot-add to the other array
	 *
	 * If devlist is NULL, then we can monitor everything because --scan
	 * was given.  We get an initial list from config file and add anything
	 * that appears in /proc/mdstat
	 */

	struct state {
		char *devname;
		int devnum;	/* to sync with mdstat info */
		long utime;
		int err;
		char *spare_group;
		int active, working, failed, spare, raid;
		int expected_spares;
		int devstate[MD_SB_DISKS];
		int devid[MD_SB_DISKS];
		int percent;
		struct state *next;
	} *statelist = NULL;
	int finished = 0;
	struct mdstat_ent *mdstat = NULL;

	if (!mailaddr) {
		mailaddr = conf_get_mailaddr(config);
		if (mailaddr && ! scan)
			fprintf(stderr, Name ": Monitor using email address \"%s\" from config file\n",
			       mailaddr);
	}
	if (!alert_cmd) {
		alert_cmd = conf_get_program(config);
		if (alert_cmd && ! scan)
			fprintf(stderr, Name ": Monitor using program \"%s\" from config file\n",
			       alert_cmd);
	}
	if (scan && !mailaddr && !alert_cmd) {
		fprintf(stderr, Name ": No mail address or alert command - not monitoring.\n");
		return 1;
	}

	if (daemonise) {
		int pid = fork();
		if (pid > 0) {
			printf("%d\n", pid);
			return 0;
		}
		if (pid < 0) {
			perror("daemonise");
			return 1;
		}
		close(0);
		open("/dev/null", 3);
		dup2(0,1);
		dup2(0,2);
		setsid();
	}

	if (devlist == NULL) {
		mddev_ident_t mdlist = conf_get_ident(config, NULL);
		for (; mdlist; mdlist=mdlist->next) {
			struct state *st = malloc(sizeof *st);
			if (st == NULL)
				continue;
			st->devname = strdup(mdlist->devname);
			st->utime = 0;
			st->next = statelist;
			st->err = 0;
			st->devnum = MAXINT;
			st->percent = -2;
			st->expected_spares = mdlist->spare_disks;
			if (mdlist->spare_group)
				st->spare_group = strdup(mdlist->spare_group);
			else
				st->spare_group = NULL;
			statelist = st;
		}
	} else {
		mddev_dev_t dv;
		for (dv=devlist ; dv; dv=dv->next) {
			struct state *st = malloc(sizeof *st);
			if (st == NULL)
				continue;
			st->devname = strdup(dv->devname);
			st->utime = 0;
			st->next = statelist;
			st->err = 0;
			st->devnum = MAXINT;
			st->percent = -2;
			st->expected_spares = -1;
			st->spare_group = NULL;
			statelist = st;
		}
	}


	while (! finished) {
		int new_found = 0;
		struct state *st;

		if (mdstat)
			free_mdstat(mdstat);
		mdstat = mdstat_read();

		for (st=statelist; st; st=st->next) {
			mdu_array_info_t array;
			struct mdstat_ent *mse;
			char *dev = st->devname;
			int fd;
			unsigned int i;

			if (test)
				alert("TestMessage", dev, NULL, mailaddr, alert_cmd);
			fd = open(dev, O_RDONLY);
			if (fd < 0) {
				if (!st->err)
					alert("DeviceDisappeared", dev, NULL,
					      mailaddr, alert_cmd);
/*					fprintf(stderr, Name ": cannot open %s: %s\n",
						dev, strerror(errno));
*/				st->err=1;
				continue;
			}
			if (ioctl(fd, GET_ARRAY_INFO, &array)<0) {
				if (!st->err)
					alert("DeviceDisappeared", dev, NULL,
					      mailaddr, alert_cmd);
/*					fprintf(stderr, Name ": cannot get array info for %s: %s\n",
						dev, strerror(errno));
*/				st->err=1;
				close(fd);
				continue;
			}
			if (array.level != 1 && array.level != 5 && array.level != -4) {
				if (!st->err)
					alert("DeviceDisappeared", dev, "Wrong-Level",
					      mailaddr, alert_cmd);
				st->err = 1;
				close(fd);
				continue;
			}
			if (st->devnum == MAXINT) {
				struct stat stb;
				if (fstat(fd, &stb) == 0 &&
				    (S_IFMT&stb.st_mode)==S_IFBLK) {
					if (MINOR(stb.st_rdev) == 9)
						st->devnum = MINOR(stb.st_rdev);
					else
						st->devnum = -1- (MINOR(stb.st_rdev)>>6);
				}
			}

			for (mse = mdstat ; mse ; mse=mse->next)
				if (mse->devnum == st->devnum)
					mse->devnum = MAXINT; /* flag it as "used" */

			if (st->utime == array.utime &&
			    st->failed == array.failed_disks &&
			    st->working == array.working_disks &&
			    st->spare == array.spare_disks &&
			    (mse == NULL  || (
				    mse->percent == st->percent
				    ))) {
				close(fd);
				st->err = 0;
				continue;
			}
			if (st->utime == 0 && /* new array */
			    mse &&	/* is in /proc/mdstat */
			    mse->pattern && strchr(mse->pattern, '_') /* degraded */
				)
				alert("DegradedArray", dev, NULL, mailaddr, alert_cmd);

			if (st->utime == 0 && /* new array */
			    st->expected_spares > 0 && 
			    array.spare_disks < st->expected_spares) 
				alert("SparesMissing", dev, NULL, mailaddr, alert_cmd);
			if (mse &&
			    st->percent == -1 && 
			    mse->percent >= 0)
				alert("RebuildStarted", dev, NULL, mailaddr, alert_cmd);
			if (mse &&
			    st->percent >= 0 &&
			    mse->percent >= 0 &&
			    (mse->percent / 20) > (st->percent / 20))
				alert(percentalerts[mse->percent/20],
				      dev, NULL, mailaddr, alert_cmd);

			if (mse &&
			    mse->percent == -1 &&
			    st->percent >= 0)
				alert("RebuildFinished", dev, NULL, mailaddr, alert_cmd);

			if (mse)
				st->percent = mse->percent;
					
			for (i=0; i<MD_SB_DISKS; i++) {
				mdu_disk_info_t disc;
				int newstate=0;
				int change;
				char *dv = NULL;
				disc.number = i;
				if (ioctl(fd, GET_DISK_INFO, &disc)>= 0) {
					newstate = disc.state;
					dv = map_dev(disc.major, disc.minor);
				} else if (mse &&  mse->pattern && i < strlen(mse->pattern))
					switch(mse->pattern[i]) {
					case 'U': newstate = 6 /* ACTIVE/SYNC */; break;
					case '_': newstate = 0; break;
					}
				change = newstate ^ st->devstate[i];
				if (st->utime && change && !st->err) {
					if (i < (unsigned)array.raid_disks &&
					    (((newstate&change)&(1<<MD_DISK_FAULTY)) ||
					     ((st->devstate[i]&change)&(1<<MD_DISK_ACTIVE)) ||
					     ((st->devstate[i]&change)&(1<<MD_DISK_SYNC)))
						)
						alert("Fail", dev, dv, mailaddr, alert_cmd);
					else if (i >= (unsigned)array.raid_disks &&
						 (disc.major || disc.minor) &&
						 st->devid[i] == MKDEV(disc.major, disc.minor) &&
						 ((newstate&change)&(1<<MD_DISK_FAULTY))
						)
						alert("FailSpare", dev, dv, mailaddr, alert_cmd);
					else if (i < (unsigned)array.raid_disks &&
						 (((st->devstate[i]&change)&(1<<MD_DISK_FAULTY)) ||
						  ((newstate&change)&(1<<MD_DISK_ACTIVE)) ||
						  ((newstate&change)&(1<<MD_DISK_SYNC)))
						)
						alert("SpareActive", dev, dv, mailaddr, alert_cmd);
				}
				st->devstate[i] = disc.state;
				st->devid[i] = MKDEV(disc.major, disc.minor);
			}
			close(fd);
			st->active = array.active_disks;
			st->working = array.working_disks;
			st->spare = array.spare_disks;
			st->failed = array.failed_disks;
			st->utime = array.utime;
			st->raid = array.raid_disks;
			st->err = 0;
		}
		/* now check if there are any new devices found in mdstat */
		if (scan) {
			struct mdstat_ent *mse;
			for (mse=mdstat; mse; mse=mse->next) 
				if (mse->devnum != MAXINT &&
				    (strcmp(mse->level, "raid1")==0 ||
				     strcmp(mse->level, "raid5")==0 ||
				     strcmp(mse->level, "multipath")==0)
					) {
					struct state *st = malloc(sizeof *st);
					mdu_array_info_t array;
					int fd;
					if (st == NULL)
						continue;
					st->devname = strdup(get_md_name(mse->devnum));
					if ((fd = open(st->devname, O_RDONLY)) < 0 ||
					    ioctl(fd, GET_ARRAY_INFO, &array)< 0) {
						/* no such array */
						if (fd >=0) close(fd);
						free(st->devname);
						free(st);
						continue;
					}
					st->utime = 0;
					st->next = statelist;
					st->err = 1;
					st->devnum = mse->devnum;
					st->percent = -2;
					st->spare_group = NULL;
					st->expected_spares = -1;
					statelist = st;
					alert("NewArray", st->devname, NULL, mailaddr, alert_cmd);
					new_found = 1;
				}
		}
		/* If an array has active < raid && spare == 0 && spare_group != NULL
		 * Look for another array with spare > 0 and active == raid and same spare_group
		 *  if found, choose a device and hotremove/hotadd
		 */
		for (st = statelist; st; st=st->next)
			if (st->active < st->raid &&
			    st->spare == 0 &&
			    st->spare_group != NULL) {
				struct state *st2;
				for (st2=statelist ; st2 ; st2=st2->next)
					if (st2 != st &&
					    st2->spare > 0 &&
					    st2->active == st2->raid &&
					    st2->spare_group != NULL &&
					    strcmp(st->spare_group, st2->spare_group) == 0) {
						/* try to remove and add */
						int fd1 = open(st->devname, O_RDONLY);
						int fd2 = open(st2->devname, O_RDONLY);
						int dev = -1;
						int d;
						if (fd1 < 0 || fd2 < 0) {
							if (fd1>=0) close(fd1);
							if (fd2>=0) close(fd2);
							continue;
						}
						for (d=st2->raid; d<MD_SB_DISKS; d++) {
							if (st2->devid[d] > 0 &&
							    st2->devstate[d] == 0) {
								dev = st2->devid[d];
								break;
							}
						}
						if (dev > 0) {
							if (ioctl(fd2, HOT_REMOVE_DISK, 
								  (unsigned long)dev) == 0) {
								if (ioctl(fd1, HOT_ADD_DISK,
									  (unsigned long)dev) == 0) {
									alert("MoveSpare", st->devname, st2->devname, mailaddr, alert_cmd);
									close(fd1);
									close(fd2);
									break;
								}
								else ioctl(fd2, HOT_ADD_DISK, (unsigned long) dev);
							}
						}
						close(fd1);
						close(fd2);
					}
			}
		if (!new_found) {
			if (oneshot)
				break;
			else
				sleep(period);
		}
		test = 0;
	}
	return 0;
}


static void alert(char *event, char *dev, char *disc, char *mailaddr, char *cmd)
{
	if (!cmd && !mailaddr) {
		time_t now = time(0);
	       
		printf("%1.15s: %s on %s %s\n", ctime(&now)+4, event, dev, disc?disc:"unknown device");
	}
	if (cmd) {
		int pid = fork();
		switch(pid) {
		default:
			waitpid(pid, NULL, 0);
			break;
		case -1:
			break;
		case 0:
			execl(cmd, cmd, event, dev, disc, NULL);
			exit(2);
		}
	}
	if (mailaddr && 
	    (strncmp(event, "Fail", 4)==0 || 
	     strncmp(event, "Test", 4)==0 ||
	     strncmp(event, "Degrade", 7)==0)) {
		FILE *mp = popen(Sendmail, "w");
		if (mp) {
			char hname[256];
			gethostname(hname, sizeof(hname));
			signal(SIGPIPE, SIG_IGN);
			fprintf(mp, "From: " Name " monitoring <root>\n");
			fprintf(mp, "To: %s\n", mailaddr);
			fprintf(mp, "Subject: %s event on %s:%s\n\n", event, dev, hname);

			fprintf(mp, "This is an automatically generated mail message from " Name "\n");
			fprintf(mp, "running on %s\n\n", hname);

			fprintf(mp, "A %s event had been detected on md device %s.\n\n", event, dev);

			if (disc)
				fprintf(mp, "It could be related to component device %s.\n\n", disc);

			fprintf(mp, "Faithfully yours, etc.\n");
			fclose(mp);
		}

	}
	/* FIXME log the event to syslog maybe */
}
