/*
 * mdctl - manage Linux "md" devices aka RAID arrays.
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

#include	"mdctl.h"
#include	"md_p.h"
#include	"md_u.h"
#include	<sys/signal.h>

static void alert(char *event, char *dev, char *disc, char *mailaddr, char *cmd);

int Monitor(mddev_dev_t devlist,
	    char *mailaddr, char *alert_cmd,
	    int period,
	    char *config)
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
	 *
	 * If the update time changes, check out all the data again
	 * It is possible that we cannot get the state of each device
	 * due to bugs in the md kernel module.
	 *
	 * if active_drives decreases, generate a "Fail" event
	 * if active_drives increases, generate a "SpareActive" event
	 *
	 * if we detect an array with active<raid and spare==0
	 * we look at other arrays that have same spare-group
	 * If we find one with active==raid and spare>0,
	 *  and if we can get_disk_info and find a name
	 *  Then we hot-remove and hot-add to the other array
	 *
	 */

	struct state {
		char *devname;
		long utime;
		int err;
		int active, working, failed, spare;
		int devstate[MD_SB_DISKS];
		struct state *next;
	} *statelist = NULL;
	int finished = 0;
	while (! finished) {
		mddev_ident_t mdlist = NULL;
		mddev_dev_t dv;
		int dnum=0;
		if (devlist== NULL)
			mdlist = conf_get_ident(config, NULL);
		dv = devlist;
		while (dv || mdlist) {
			mddev_ident_t mdident;
			struct state *st;
			mdu_array_info_t array;
			char *dev;
			int fd;
			char *event = NULL;
			int i;
			char *event_disc = NULL;
			if (dv) {
				dev = dv->devname;
				mdident = conf_get_ident(config, dev);
				dv = dv->next;
			} else {
				mdident = mdlist;
				dev = mdident->devname;
				mdlist = mdlist->next;
			}
			for (st=statelist; st ; st=st->next)
				if (strcmp(st->devname, dev)==0)
					break;
			if (!st) {
				st =malloc(sizeof *st);
				if (st == NULL)
					continue;
				st->devname = strdup(dev);
				st->utime = 0;
				st->next = statelist;
				st->err = 0;
				statelist = st;
			}
			fd = open(dev, O_RDONLY);
			if (fd < 0) {
				if (!st->err)
					fprintf(stderr, Name ": cannot open %s: %s\n",
						dev, strerror(errno));
				st->err=1;
				continue;
			}
			if (ioctl(fd, GET_ARRAY_INFO, &array)<0) {
				if (!st->err)
					fprintf(stderr, Name ": cannot get array info for %s: %s\n",
						dev, strerror(errno));
				st->err=1;
				close(fd);
				continue;
			}
			st->err = 0;
		    
			if (st->utime == array.utime &&
			    st->failed == array.failed_disks) {
				close(fd);
				continue;
			}
			event = NULL;
			if (st->utime) {
				int i;
				if (st->active > array.active_disks)
					event = "Fail";
				else if (st->working > array.working_disks)
					event = "FailSpare";
				else if (st->active < array.active_disks)
					event = "ActiveSpare";
			}
			for (i=0; i<array.raid_disks+array.spare_disks; i++) {
				mdu_disk_info_t disc;
				disc.number = i;
				if (ioctl(fd, GET_DISK_INFO, &disc)>= 0) {
					if (event && event_disc == NULL &&
					    st->devstate[i] != disc.state) {
						char * dv = map_dev(disc.major, disc.minor);
						if (dv)
							event_disc = strdup(dv);
					}
					st->devstate[i] = disc.state;
				}
			}
			close(fd);
			st->active = array.active_disks;
			st->working = array.working_disks;
			st->spare = array.spare_disks;
			st->failed = array.failed_disks;
			st->utime = array.utime;
			if (event)
				alert(event, dev, event_disc, mailaddr, alert_cmd);
		}
		sleep(period);
	}
	return 0;
}


static void alert(char *event, char *dev, char *disc, char *mailaddr, char *cmd)
{
	if (!cmd && !mailaddr) {
		time_t now = time(0);
	       
		printf("%0.15s: %s on %s %s\n", ctime(&now)+4, event, dev, disc?disc:"unknown device");
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
	if (mailaddr && strncmp(event, "Fail", 4)==0) {
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
				fprintf(mp, "It could be related to sub-device %s.\n\n", disc);

			fprintf(mp, "Faithfully yours, etc.\n");
			fclose(mp);
		}

	}
	/* FIXME log the event to syslog maybe */
}
