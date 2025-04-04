/*
 * mdadm - manage Linux "md" devices aka RAID arrays.
 *
 * Copyright (C) 2001-2013 Neil Brown <neilb@suse.de>
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

#include "mdadm.h"
#include "udev.h"
#include "md_p.h"
#include "xmalloc.h"

#include <ctype.h>

int create_named_array(char *devnm)
{
	int fd;
	int n = -1;
	static const char new_array_file[] = {
		"/sys/module/md_mod/parameters/new_array"
	};

	fd = open(new_array_file, O_WRONLY);
	if (fd < 0 && errno == ENOENT) {
		char buf[PATH_MAX] = {0};
		char *env_ptr;

		env_ptr = getenv("PATH");
		/*
		 * When called by udev worker context, path of modprobe
		 * might not be in env PATH. Set sbin paths into PATH
		 * env to avoid potential failure when run modprobe here.
		 */
		if (env_ptr)
			snprintf(buf, PATH_MAX - 1, "%s:%s", env_ptr,
				 "/sbin:/usr/sbin:/usr/local/sbin");
		else
			snprintf(buf, PATH_MAX - 1, "%s",
				 "/sbin:/usr/sbin:/usr/local/sbin");

		setenv("PATH", buf, 1);

		if (system("modprobe md_mod") == 0)
			fd = open(new_array_file, O_WRONLY);
	}
	if (fd >= 0) {
		n = write(fd, devnm, strlen(devnm));
		close(fd);
	}
	if (fd < 0 || n != (int)strlen(devnm)) {
		pr_err("Fail to create %s when using %s, fallback to creation via node\n",
			devnm, new_array_file);
		return 0;
	}

	return 1;
}

char *find_free_devnm(void)
{
	static char devnm[MD_NAME_MAX];
	int devnum;

	for (devnum = 127; devnum != 128; devnum = devnum ? devnum - 1 : 511) {
		sprintf(devnm, "md%d", devnum);

		if (mddev_busy(devnm))
			continue;

		if (!conf_name_is_free(devnm))
			continue;

		if (!udev_is_available()) {
			/* make sure it is new to /dev too*/
			dev_t devid = devnm2devid(devnm);

			if (devid && map_dev(major(devid), minor(devid), 0))
				continue;
		}

		break;
	}
	if (devnum == 128)
		return NULL;
	return devnm;
}

/*
 * We need a new md device to assemble/build/create an array.
 * 'dev' is a name given us by the user (command line or mdadm.conf)
 * It might start with /dev or /dev/md any might end with a digit
 * string.
 * If it starts with just /dev, it must be /dev/mdX or /dev/md_dX
 * If it ends with a digit string, then it must be as above, or
 * 'trustworthy' must be 'METADATA' and the 'dev' must be
 *  /dev/md/'name'NN or 'name'NN
 * If it doesn't end with a digit string, it must be /dev/md/'name'
 * or 'name' or must be NULL.
 * If the digit string is present, it gives the minor number to use
 * If not, we choose a high, unused minor number.
 * If the 'dev' is a standard name, it devices whether 'md' or 'mdp'.
 * else if the name is 'd[0-9]+' then we use mdp
 * else if trustworthy is 'METADATA' we use md
 * else the choice depends on 'autof'.
 * If name is NULL it is assumed to match whatever dev provides.
 * If both name and dev are NULL, we choose a name 'mdXX' or 'mdpXX'
 *
 * If 'name' is given, and 'trustworthy' is 'foreign' and name is not
 * supported by 'dev', we add a "_%d" suffix based on the minor number
 * use that.
 *
 * If udev is configured, we create a temporary device, open it, and
 * unlink it.
 * If not, we create the /dev/mdXX device, and if name is usable,
 * /dev/md/name
 * In any case we return /dev/md/name or (if that isn't available)
 * /dev/mdXX in 'chosen'.
 *
 * When we create devices, we use uid/gid/umask from config file.
 */

int create_mddev(char *dev, char *name, int trustworthy,
		 char *chosen, int block_udev)
{
	int mdfd;
	struct stat stb;
	int num = -1;
	struct createinfo *ci = conf_get_create_info();
	char *cname;
	char devname[37];
	char devnm[32];
	char cbuf[400];

	if (!udev_is_available())
		block_udev = 0;

	if (chosen == NULL)
		chosen = cbuf;

	strcpy(chosen, DEV_MD_DIR);
	cname = chosen + strlen(chosen);

	if (dev) {
		if (strncmp(dev, DEV_MD_DIR, DEV_MD_DIR_LEN) == 0) {
			snprintf(cname, MD_NAME_MAX, "%s", dev + DEV_MD_DIR_LEN);
		} else if (strncmp(dev, "/dev/", 5) == 0) {
			char *e = dev + strlen(dev);
			while (e > dev && isdigit(e[-1]))
				e--;
			if (e[0])
				num = strtoul(e, NULL, 10);
			snprintf(cname, MD_NAME_MAX, "%s", dev + 5);
			cname[e-(dev+5)] = 0;
			/* name *must* be mdXX or md_dXX in this context */
			if (num < 0 ||
			    (strcmp(cname, "md") != 0 && strcmp(cname, "md_d") != 0)) {
				pr_err("%s is an invalid name for an md device.  Try /dev/md/%s\n",
					dev, dev+5);
				return -1;
			}

			/* recreate name: /dev/md/0 or /dev/md/d0 */
			sprintf(cname, "%d", num);
		} else
			strcpy(cname, dev);

		/* 'cname' must not contain a slash, and may not be
		 * empty.
		 */
		if (strchr(cname, '/') != NULL) {
			pr_err("%s is an invalid name for an md device.\n", dev);
			return -1;
		}
		if (cname[0] == 0) {
			pr_err("%s is an invalid name for an md device (empty!).\n", dev);
			return -1;
		}
		if (num < 0) {
			/* If cname  is 'N' or 'dN', we get dev number
			 * from there.
			 */
			char *sp = cname;
			char *ep;
			if (cname[0] == 'd')
				sp++;
			if (isdigit(sp[0]))
				num = strtoul(sp, &ep, 10);
			else
				ep = sp;
			if (ep == sp || *ep || num < 0)
				num = -1;
		}
	}

	/* Now determine device number */
	if (name && name[0] == 0)
		name = NULL;

	if (num < 0 && trustworthy == LOCAL && name) {
		/* if name is numeric, possibly prefixed by
		 * 'md' or '/dev/md', use that for num
		 * if it is not already in use */
		char *ep;
		char *n2 = name;
		if (strncmp(n2, "/dev/", 5) == 0)
			n2 += 5;
		if (strncmp(n2, "md", 2) == 0)
			n2 += 2;
		if (*n2 == '/')
			n2++;
		num = strtoul(n2, &ep, 10);
		if (ep == n2 || *ep)
			num = -1;
		else {
			sprintf(devnm, "md%d", num);
			if (mddev_busy(devnm))
				num = -1;
		}
	}

	if (cname[0] == 0 && name) {
		/* Need to find a name if we can
		 * We don't completely trust 'name'.  Truncate to
		 * reasonable length and remove '/'
		 */
		char *cp;
		struct map_ent *map = NULL;
		int conflict = 1;
		int unum = 0;
		int cnlen;
		strncpy(cname, name, 200);
		cname[200] = 0;
		for (cp = cname; *cp ; cp++)
			switch (*cp) {
			case '/':
				*cp = '-';
				break;
			case ' ':
			case '\t':
				*cp = '_';
				break;
			}

		if (trustworthy == LOCAL ||
		    (trustworthy == FOREIGN && strchr(cname, ':') != NULL)) {
			/* Only need suffix if there is a conflict */
			if (map_by_name(&map, cname) == NULL)
				conflict = 0;
		}
		cnlen = strlen(cname);
		while (conflict) {
			if (trustworthy == METADATA && !isdigit(cname[cnlen-1]))
				sprintf(cname+cnlen, "%d", unum);
			else
				/* add _%d to FOREIGN array that don't
				 * a 'host:' prefix
				 */
				sprintf(cname+cnlen, "_%d", unum);
			unum++;
			if (map_by_name(&map, cname) == NULL)
				conflict = 0;
		}
	}

	devnm[0] = 0;
	if (num < 0 && cname && ci->names) {
		sprintf(devnm, "md_%s", cname);
		if (block_udev && udev_block(devnm) != UDEV_STATUS_SUCCESS)
			return -1;
		if (!create_named_array(devnm)) {
			devnm[0] = 0;
			udev_unblock();
		}
	}
	if (num >= 0) {
		sprintf(devnm, "md%d", num);
		if (block_udev && udev_block(devnm) != UDEV_STATUS_SUCCESS)
			return -1;
		if (!create_named_array(devnm)) {
			devnm[0] = 0;
			udev_unblock();
		}
	}
	if (devnm[0] == 0) {
		if (num < 0) {
			/* need to choose a free number. */
			char *_devnm = find_free_devnm();

			if (!_devnm) {
				pr_err("No avail md devices - aborting\n");
				return -1;
			}
			strcpy(devnm, _devnm);
		} else {
			sprintf(devnm, "md%d", num);
			if (mddev_busy(devnm)) {
				pr_err("%s is already in use.\n",
				       dev);
				return -1;
			}
		}
		if (block_udev && udev_block(devnm) != UDEV_STATUS_SUCCESS)
			return -1;
		create_named_array(devnm);
	}

	sprintf(devname, "/dev/%s", devnm);

	if (dev && dev[0] == '/' && strlen(dev) < 400)
		strcpy(chosen, dev);
	else if (cname[0] == 0)
		strcpy(chosen, devname);

	/* We have a device number and name.
	 * If we cannot detect udev, we need to make
	 * devices and links ourselves.
	 */
	if (!udev_is_available()) {
		/* Make sure 'devname' exists and 'chosen' is a symlink to it */
		if (lstat(devname, &stb) == 0) {
			/* Must be the correct device, else error */
			if ((stb.st_mode&S_IFMT) != S_IFBLK ||
			    stb.st_rdev != devnm2devid(devnm)) {
				pr_err("%s exists but looks wrong, please fix\n",
					devname);
				return -1;
			}
		} else {
			if (mknod(devname, S_IFBLK|0600,
				  devnm2devid(devnm)) != 0) {
				pr_err("failed to create %s\n",
					devname);
				return -1;
			}
			if (chown(devname, ci->uid, ci->gid))
				perror("chown");
			if (chmod(devname, ci->mode))
				perror("chmod");
			if (stat(devname, &stb) < 0) {
				pr_err("failed to stat %s\n",
						devname);
				return -1;
			}
			add_dev(devname, &stb, 0, NULL);
		}

		if (strcmp(chosen, devname) != 0) {
			if (mkdir(DEV_NUM_PREF, 0700) == 0) {
				if (chown(DEV_NUM_PREF, ci->uid, ci->gid))
					perror("chown " DEV_NUM_PREF);
				if (chmod(DEV_NUM_PREF, ci->mode | ((ci->mode >> 2) & 0111)))
					perror("chmod " DEV_NUM_PREF);
			}

			if (dev && strcmp(chosen, dev) == 0)
				/* We know we are allowed to use this name */
				unlink(chosen);

			if (lstat(chosen, &stb) == 0) {
				char buf[300];
				ssize_t link_len = readlink(chosen, buf, sizeof(buf)-1);
				if (link_len >= 0)
					buf[link_len] = '\0';

				if ((stb.st_mode & S_IFMT) != S_IFLNK ||
				    link_len < 0 ||
				    strcmp(buf, devname) != 0) {
					pr_err("%s exists - ignoring\n",
						chosen);
					strcpy(chosen, devname);
				}
			} else if (symlink(devname, chosen) != 0)
				pr_err("failed to create %s: %s\n",
					chosen, strerror(errno));
		}
	}
	mdfd = open_dev_excl(devnm);
	if (mdfd < 0)
		pr_err("unexpected failure opening %s\n",
			devname);
	return mdfd;
}

/* Open this and check that it is an md device.
 * On success, return filedescriptor.
 * On failure, return -1 if it doesn't exist,
 * or -2 if it exists but is not an md device.
 */
int open_mddev(char *dev, int report_errors)
{
	int mdfd = open(dev, O_RDONLY);

	if (mdfd < 0) {
		if (report_errors)
			pr_err("error opening %s: %s\n",
				dev, strerror(errno));
		return -1;
	}

	if (md_array_valid(mdfd) == 0) {
		close(mdfd);
		if (report_errors)
			pr_err("%s does not appear to be an md device\n", dev);
		return -2;
	}

	return mdfd;
}

/**
 * is_mddev() - check that file name passed is an md device.
 * @dev: file name that has to be checked.
 * Return: 1 if file passed is an md device, 0 if not.
 */
int is_mddev(char *dev)
{
	int fd = open_mddev(dev, 1);

	if (fd >= 0) {
		close(fd);
		return 1;
	}

	return 0;
}
