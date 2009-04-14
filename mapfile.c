/*
 * mapfile - manage /var/run/mdadm.map. Part of:
 * mdadm - manage Linux "md" devices aka RAID arrays.
 *
 * Copyright (C) 2006-2009 Neil Brown <neilb@suse.de>
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
 *    Paper: Neil Brown
 *           Novell Inc
 *           GPO Box Q1283
 *           QVB Post Office, NSW 1230
 *           Australia
 */

/* /var/run/mdadm.map is used to track arrays being created in --incremental
 * more.  It particularly allows lookup from UUID to array device, but
 * also allows the array device name to be easily found.
 *
 * The map file is line based with space separated fields.  The fields are:
 *  Device id  -  mdX or mdpX  where X is a number.
 *  metadata   -  0.90 1.0 1.1 1.2 ddf ...
 *  UUID       -  uuid of the array
 *  path       -  path where device created: /dev/md/home
 *
 * The preferred location for the map file is /var/run/mdadm.map.
 * However /var/run may not exist or be writable in early boot.  And if
 * no-one has created /var/run/mdadm, we still want to survive.
 * So possible locations are:
 *   /var/run/mdadm/map  /var/run/mdadm.map  /dev/.mdadm.map
 * the last, because udev requires a writable /dev very early.
 * We read from the first one that exists and write to the first
 * one that we can.
 */
#include "mdadm.h"

#define mapnames(base) { #base, #base ".new", #base ".lock"}
char *mapname[3][3] = {
	mapnames(/var/run/mdadm/map),
	mapnames(/var/run/mdadm.map),
	mapnames(/dev/.mdadm.map)
};

int mapmode[3] = { O_RDONLY, O_RDWR|O_CREAT, O_RDWR|O_CREAT | O_TRUNC };
char *mapsmode[3] = { "r", "w", "w"};

FILE *open_map(int modenum, int *choice)
{
	int i;
	for (i = 0 ; i < 3 ; i++) {
		int fd = open(mapname[i][modenum], mapmode[modenum], 0600);
		if (fd >= 0) {
			*choice = i;
			return fdopen(fd, mapsmode[modenum]);
		}
	}
	return NULL;
}

int map_write(struct map_ent *mel)
{
	FILE *f;
	int err;
	int which;

	f = open_map(1, &which);

	if (!f)
		return 0;
	for (; mel; mel = mel->next) {
		if (mel->bad)
			continue;
		if (mel->devnum < 0)
			fprintf(f, "mdp%d ", -1-mel->devnum);
		else
			fprintf(f, "md%d ", mel->devnum);
		fprintf(f, "%s ", mel->metadata);
		fprintf(f, "%08x:%08x:%08x:%08x ", mel->uuid[0],
			mel->uuid[1], mel->uuid[2], mel->uuid[3]);
		fprintf(f, "%s\n", mel->path);
	}
	fflush(f);
	err = ferror(f);
	fclose(f);
	if (err) {
		unlink(mapname[which][1]);
		return 0;
	}
	return rename(mapname[which][1],
		      mapname[which][0]) == 0;
}


static FILE *lf = NULL;
static int lwhich = 0;
int map_lock(struct map_ent **melp)
{
	if (lf == NULL) {
		lf = open_map(2, &lwhich);
		if (lf == NULL)
			return -1;
		if (lockf(fileno(lf), F_LOCK, 0) != 0) {
			fclose(lf);
			lf = NULL;
			return -1;
		}
	}
	if (*melp)
		map_free(*melp);
	map_read(melp);
	return 0;
}

void map_unlock(struct map_ent **melp)
{
	if (lf)
		fclose(lf);
	unlink(mapname[lwhich][2]);
	lf = NULL;
}

void map_add(struct map_ent **melp,
	    int devnum, char *metadata, int uuid[4], char *path)
{
	struct map_ent *me = malloc(sizeof(*me));

	me->devnum = devnum;
	strcpy(me->metadata, metadata);
	memcpy(me->uuid, uuid, 16);
	me->path = strdup(path);
	me->next = *melp;
	me->bad = 0;
	*melp = me;
}

void map_read(struct map_ent **melp)
{
	FILE *f;
	char buf[8192];
	char path[200];
	int devnum, uuid[4];
	char metadata[30];
	char nam[4];
	int which;

	*melp = NULL;

	f = open_map(0, &which);
	if (!f) {
		RebuildMap();
		f = open_map(0, &which);
	}
	if (!f)
		return;

	while (fgets(buf, sizeof(buf), f)) {
		if (sscanf(buf, " %3[mdp]%d %s %x:%x:%x:%x %200s",
			   nam, &devnum, metadata, uuid, uuid+1,
			   uuid+2, uuid+3, path) == 8) {
			if (strncmp(nam, "md", 2) != 0)
				continue;
			if (nam[2] == 'p')
				devnum = -1 - devnum;
			map_add(melp, devnum, metadata, uuid, path);
		}
	}
	fclose(f);
}

void map_free(struct map_ent *map)
{
	while (map) {
		struct map_ent *mp = map;
		map = mp->next;
		free(mp->path);
		free(mp);
	}
}

int map_update(struct map_ent **mpp, int devnum, char *metadata,
	       int *uuid, char *path)
{
	struct map_ent *map, *mp;
	int rv;

	if (mpp && *mpp)
		map = *mpp;
	else
		map_read(&map);

	for (mp = map ; mp ; mp=mp->next)
		if (mp->devnum == devnum) {
			strcpy(mp->metadata, metadata);
			memcpy(mp->uuid, uuid, 16);
			free(mp->path);
			mp->path = strdup(path);
			break;
		}
	if (!mp)
		map_add(&map, devnum, metadata, uuid, path);
	if (mpp)
		*mpp = NULL;
	rv = map_write(map);
	map_free(map);
	return rv;
}

void map_delete(struct map_ent **mapp, int devnum)
{
	struct map_ent *mp;

	if (*mapp == NULL)
		map_read(mapp);

	for (mp = *mapp; mp; mp = *mapp) {
		if (mp->devnum == devnum) {
			*mapp = mp->next;
			free(mp->path);
			free(mp);
		} else
			mapp = & mp->next;
	}
}

struct map_ent *map_by_uuid(struct map_ent **map, int uuid[4])
{
	struct map_ent *mp;
	if (!*map)
		map_read(map);

	for (mp = *map ; mp ; mp = mp->next) {
		if (memcmp(uuid, mp->uuid, 16) != 0)
			continue;
		if (!mddev_busy(mp->devnum)) {
			mp->bad = 1;
			continue;
		}
		return mp;
	}
	return NULL;
}

struct map_ent *map_by_devnum(struct map_ent **map, int devnum)
{
	struct map_ent *mp;
	if (!*map)
		map_read(map);

	for (mp = *map ; mp ; mp = mp->next) {
		if (mp->devnum != devnum)
			continue;
		if (!mddev_busy(mp->devnum)) {
			mp->bad = 1;
			continue;
		}
		return mp;
	}
	return NULL;
}

struct map_ent *map_by_name(struct map_ent **map, char *name)
{
	struct map_ent *mp;
	if (!*map)
		map_read(map);

	for (mp = *map ; mp ; mp = mp->next) {
		if (strncmp(mp->path, "/dev/md/", 8) != 0)
			continue;
		if (strcmp(mp->path+8, name) != 0)
			continue;
		if (!mddev_busy(mp->devnum)) {
			mp->bad = 1;
			continue;
		}
		return mp;
	}
	return NULL;
}

void RebuildMap(void)
{
	struct mdstat_ent *mdstat = mdstat_read(0, 0);
	struct mdstat_ent *md;
	struct map_ent *map = NULL;
	int mdp = get_mdp_major();

	for (md = mdstat ; md ; md = md->next) {
		struct mdinfo *sra = sysfs_read(-1, md->devnum, GET_DEVS|SKIP_GONE_DEVS);
		struct mdinfo *sd;

		if (!sra)
			continue;

		for (sd = sra->devs ; sd ; sd = sd->next) {
			char dn[30];
			int dfd;
			int ok;
			struct supertype *st;
			char *path;
			struct mdinfo info;

			sprintf(dn, "%d:%d", sd->disk.major, sd->disk.minor);
			dfd = dev_open(dn, O_RDONLY);
			if (dfd < 0)
				continue;
			st = guess_super(dfd);
			if ( st == NULL)
				ok = -1;
			else
				ok = st->ss->load_super(st, dfd, NULL);
			close(dfd);
			if (ok != 0)
				continue;
			st->ss->getinfo_super(st, &info);
			if (md->devnum > 0)
				path = map_dev(MD_MAJOR, md->devnum, 0);
			else
				path = map_dev(mdp, (-1-md->devnum)<< 6, 0);
			map_add(&map, md->devnum,
				info.text_version,
				info.uuid, path ? : "/unknown");
			st->ss->free_super(st);
			break;
		}
		sysfs_free(sra);
	}
	map_write(map);
	map_free(map);
	for (md = mdstat ; md ; md = md->next) {
		struct mdinfo *sra = sysfs_read(-1, md->devnum, GET_VERSION);
		sysfs_uevent(sra, "change");
		sysfs_free(sra);
	}
	free_mdstat(mdstat);
}
