/*
 * mdstat - parse /proc/mdstat file. Part of:
 * mdadm - manage Linux "md" devices aka RAID arrays.
 *
 * Copyright (C) 2002 Neil Brown <neilb@cse.unsw.edu.au>
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

/*
 * The /proc/mdstat file comes in at least 3 flavours:
 * In an unpatched 2.2 kernel (md 0.36.6):
 *  Personalities : [n raidx] ...
 *  read_ahead {not set|%d sectors}
 *  md0 : {in}active{ raidX /dev/hda...  %d blocks{ maxfault=%d}}
 *  md1 : .....
 *
 * Normally only 4 md lines, but all are listed.
 *
 * In a patched 2.2 kernel (md 0.90.0)
 *  Personalities : [raidx] ...
 *  read_ahead {not set|%d sectors}
 *  mdN : {in}active {(readonly)} raidX dev[%d]{(F)} ... %d blocks STATUS RESYNC
 *  ... Only initialised arrays listed
 *  unused: dev dev dev | <none>
 *
 * STATUS is personality dependant:
 *    linear:  %dk rounding
 *    raid0:   %dk chunks
 *    raid1:   [%d/%d] [U_U]   ( raid/working.  operational or not)
 *    raid5:   level 4/5, %dk chunk, algorithm %d [%d/%d] [U_U]
 *
 * RESYNC is  empty or:
 *    {resync|recovery}=%u%% finish=%u.%umin
 *  or
 *    resync=DELAYED
 *
 * In a 2.4 kernel (md 0.90.0/2.4)
 *  Personalities : [raidX] ...
 *  read_ahead {not set|%d sectors}
 *  mdN : {in}active {(read-only)} raidX dev[%d]{(F)} ...
 *       %d blocks STATUS
 *       RESYNC
 *  unused: dev dev .. | <none>
 *
 *  STATUS matches 0.90.0/2.2
 *  RESYNC includes [===>....],
 *          adds a space after {resync|recovery} and before and after '='
 *          adds a decimal to the recovery percent.
 *	    adds (%d/%d) resync amount  and max_blocks, before finish.
 *	    adds speed=%dK/sec after finish
 *
 *
 *
 * Out of this we want to extract:
 *   list of devices, active or not
 *   pattern of failed drives (so need number of drives)
 *   percent resync complete
 *
 * As continuation is indicated by leading space, we use
 *  conf_line from config.c to read logical lines
 *
 */

#include	"mdadm.h"
#include	"dlink.h"

void free_mdstat(struct mdstat_ent *ms)
{
	while (ms) {
		struct mdstat_ent *t;
		if (ms->dev) free(ms->dev);
		if (ms->level) free(ms->level);
		if (ms->pattern) free(ms->pattern);
		t = ms;
		ms = ms->next;
		free(t);
	}
}

struct mdstat_ent *mdstat_read()
{
	FILE *f;
	struct mdstat_ent *all, **end;
	char *line;

	f = fopen("/proc/mdstat", "r");
	if (f == NULL)
		return NULL;

	all = NULL;
	end = &all;
	for (; (line = conf_line(f)) ; free_line(line)) {
		struct mdstat_ent *ent;
		char *w;
		int devnum;
		char *ep;

		if (strcmp(line, "Personalities")==0)
			continue;
		if (strcmp(line, "read_ahead")==0)
			continue;
		if (strcmp(line, "unused")==0)
			continue;
		/* Better be an md line.. */
		if (strncmp(line, "md", 2)!= 0)
			continue;
		if (strncmp(line, "md_d", 4) == 0)
			devnum = -1-strtoul(line+4, &ep, 10);
		else if (strncmp(line, "md", 2) == 0)
			devnum = strtoul(line+2, &ep, 10);
		else
			continue;
		if (ep == NULL || *ep ) {
			/* fprintf(stderr, Name ": bad /proc/mdstat line starts: %s\n", line); */
			continue;
		}

		ent = malloc(sizeof(*ent));
		if (!ent) {
			fprintf(stderr, Name ": malloc failed reading /proc/mdstat.\n");
			free_line(line);
			fclose(f);
			return all;
		}
		ent->dev = ent->level = ent->pattern= NULL;
		ent->next = NULL;
		ent->percent = -1;
		ent->active = -1;

		ent->dev = strdup(line);
		ent->devnum = devnum;
		
		for (w=dl_next(line); w!= line ; w=dl_next(w)) {
			int l = strlen(w);
			char *eq;
			if (strcmp(w, "active")==0)
				ent->active = 1;
			else if (strcmp(w, "inactive")==0)
				ent->active = 0;
			else if (ent->active >=0 &&
				 ent->level == NULL &&
				 w[0] != '(' /*readonly*/)
				ent->level = strdup(w);
			else if (!ent->pattern &&
				 w[0] == '[' &&
				 (w[1] == 'U' || w[1] == '_')) {
				ent->pattern = strdup(w+1);
				if (ent->pattern[l-2]==']')
					ent->pattern[l-2] = '\0';
			} else if (ent->percent == -1 &&
				   strncmp(w, "re", 2)== 0 &&
				   w[l-1] == '%' &&
				   (eq=strchr(w, '=')) != NULL ) {
				ent->percent = atoi(eq+1);
			} else if (ent->percent == -1 &&
				   w[0] >= '0' && 
				   w[0] <= '9' &&
				   w[l-1] == '%') {
				ent->percent = atoi(w);
			}
		}
		*end = ent;
		end = &ent->next;
	}
	fclose(f);
	return all;
}
