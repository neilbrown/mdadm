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

#include "mdctl.h"
#include "md_p.h"

int open_mddev(char *dev)
{
	int mdfd = open(dev, O_RDWR, 0);
	if (mdfd < 0)
		fprintf(stderr,Name ": error opening %s: %s\n",
			dev, strerror(errno));
	else if (md_get_version(mdfd) <= 0) {
		fprintf(stderr, Name ": %s does not appear to be an md device\n",
			dev);
		close(mdfd);
		mdfd = -1;
	}
	return mdfd;
}



int main(int argc, char *argv[])
{
	char mode = '\0';
	int opt;
	char *help_text;
	char *c;
	int rv;
	int i;

	int chunk = 0;
	int size = 0;
	int level = -10;
	int layout = -1;
	int raiddisks = 0;
	int sparedisks = 0;
	struct mddev_ident_s ident;
	char *configfile = NULL;
	char *cp;
	int scan = 0;
	char devmode = 0;
	int runstop = 0;
	int readonly = 0;
	mddev_dev_t devlist = NULL;
	mddev_dev_t *devlistend = & devlist;
	mddev_dev_t dv;
	int devs_found = 0;
	int verbose = 0;
	int brief = 0;
	int force = 0;

	char *mailaddr = NULL;
	char *program = NULL;
	int delay = 0;

	int mdfd = -1;

	ident.uuid_set=0;
	ident.level = -10;
	ident.raid_disks = -1;
	ident.super_minor= -1;
	ident.devices=0;

	while ((opt=getopt_long(argc, argv,
				short_options, long_options,
				NULL)) != -1) {

		switch(opt) {
		case '@': /* just incase they say --manage */
		case 'A':
		case 'B':
		case 'C':
		case 'D':
		case 'E':
		case 'F':
			/* setting mode - only once */
			if (mode) {
				fprintf(stderr, Name ": --%s/-%c not allowed, mode already set to %s\n",
					long_options[opt-'A'+1].name,
					long_options[opt-'A'+1].val,
					long_options[mode-'A'+1].name);
				exit(2);
			}
			mode = opt;
			continue;

		case 'h':
			help_text = Help;
			switch (mode) {
			case 'C': help_text = Help_create; break;
			case 'B': help_text = Help_build; break;
			case 'A': help_text = Help_assemble; break;
			}
			fputs(help_text,stderr);
			exit(0);

		case 'V':
			fputs(Version, stderr);
			exit(0);

		case 'v': verbose = 1;
			continue;

		case 'b': brief = 1;
			continue;

		case 1: /* an undecorated option - must be a device name.
			 * Depending on mode, it could be that:
			 *    All devices listed are "md" devices : --Detail, -As
			 *    No devices are "md" devices : --Examine
			 *    First device is "md", others are component: -A,-B,-C
			 * Only accept on device before mode is determined.
			 *  If mode is @, then require devmode for other devices.
			 */
			if (devs_found > 0 && !mode ) {
				fprintf(stderr, Name ": Must give mode flag before second device name at %s\n", optarg);
				exit(2);
			}
			if (devs_found > 0 && mode == '@' && !devmode) {
				fprintf(stderr, Name ": Must give on of -a/-r/-f for subsequent devices at %s\n", optarg);
				exit(2);
			}
			dv = malloc(sizeof(*dv));
			if (dv == NULL) {
				fprintf(stderr, Name ": malloc failed\n");
				exit(3);
			}
			dv->devname = optarg;
			dv->disposition = devmode;
			dv->next = NULL;
			*devlistend = dv;
			devlistend = &dv->next;
			
			devs_found++;
			continue;

		case ':':
		case '?':
			fputs(Usage, stderr);
			exit(2);
		default:
			/* force mode setting - @==manage if nothing else */
			if (!mode) mode = '@';
		}

		/* We've got a mode, and opt is now something else which
		 * could depend on the mode */
#define O(a,b) ((a<<8)|b)
		switch (O(mode,opt)) {
		case O('C','c'):
		case O('B','c'): /* chunk or rounding */
			if (chunk) {
				fprintf(stderr, Name ": chunk/rounding may only be specified once. "
					"Second value is %s.\n", optarg);
				exit(2);
			}
			chunk = strtol(optarg, &c, 10);
			if (!optarg[0] || *c || chunk<4 || ((chunk-1)&chunk)) {
				fprintf(stderr, Name ": invalid chunk/rounding value: %s\n",
					optarg);
				exit(2);
			}
			continue;

		case O('C','z'): /* size */
			if (size) {
				fprintf(stderr, Name ": size may only be specified once. "
					"Second value is %s.\n", optarg);
				exit(2);
			}
			size = strtol(optarg, &c, 10);
			if (!optarg[0] || *c || size < 4) {
				fprintf(stderr, Name ": invalid size: %s\n",
					optarg);
				exit(2);
			}
			continue;

		case O('C','l'):
		case O('B','l'): /* set raid level*/
			if (level != -10) {
				fprintf(stderr, Name ": raid level may only be set once.  "
					"Second value is %s.\n", optarg);
				exit(2);
			}
			level = map_name(pers, optarg);
			if (level == -10) {
				fprintf(stderr, Name ": invalid raid level: %s\n",
					optarg);
				exit(2);
			}
			if (level > 0 && mode == 'B') {
				fprintf(stderr, Name ": Raid level %s not permitted with --build.\n",
					optarg);
				exit(2);
			}
			if (sparedisks > 0 && level < 1) {
				fprintf(stderr, Name ": raid level %s is incompatible with spare-disks setting.\n",
					optarg);
				exit(2);
			}
			ident.level = level;
			continue;

		case O('C','p'): /* raid5 layout */
			if (layout >= 0) {
				fprintf(stderr,Name ": layout may only be sent once.  "
					"Second value was %s\n", optarg);
				exit(2);
			}
			switch(level) {
			default:
				fprintf(stderr, Name ": layout now meaningful for %s arrays.\n",
					map_num(pers, level));
				exit(2);
			case -10:
				fprintf(stderr, Name ": raid level must be given before layout.\n");
				exit(2);

			case 5:
				layout = map_name(r5layout, optarg);
				if (layout==-10) {
					fprintf(stderr, Name ": layout %s not understood for raid5.\n",
						optarg);
					exit(2);
				}
				break;
			}
			continue;

		case O('C','n'):
		case O('B','n'): /* number of raid disks */
			if (raiddisks) {
				fprintf(stderr, Name ": raid-disks set twice: %d and %s\n",
					raiddisks, optarg);
				exit(2);
			}
			raiddisks = strtol(optarg, &c, 10);
			if (!optarg[0] || *c || raiddisks<=0 || raiddisks > MD_SB_DISKS) {
				fprintf(stderr, Name ": invalid number of raid disks: %s\n",
					optarg);
				exit(2);
			}
			ident.raid_disks = raiddisks;
			continue;

		case O('C','x'): /* number of spare (eXtra) discs */
			if (sparedisks) {
				fprintf(stderr,Name ": spare-disks set twice: %d and %s\n",
					sparedisks, optarg);
				exit(2);
			}
			if (level > -10 && level < 1) {
				fprintf(stderr, Name ": spare-disks setting is incompatible with raid level %d\n",
					level);
				exit(2);
			}
			sparedisks = strtol(optarg, &c, 10);
			if (!optarg[0] || *c || sparedisks < 0 || sparedisks > MD_SB_DISKS - raiddisks) {
				fprintf(stderr, Name ": invalid number of spare disks: %s\n",
					optarg);
				exit(2);
			}
			continue;
		case O('C','f'): /* force honouring of device list */
			force=1;
			continue;

			/* now for the Assemble options */
		case O('A','f'): /* force assembly */
			force = 1;
			continue;
		case O('A','u'): /* uuid of array */
			if (ident.uuid_set) {
				fprintf(stderr, Name ": uuid cannot be set twice.  "
					"Second value %s.\n", optarg);
				exit(2);
			}
			if (parse_uuid(optarg, ident.uuid))
				ident.uuid_set = 1;
			else {
				fprintf(stderr,Name ": Bad uuid: %s\n", optarg);
				exit(2);
			}
			continue;

		case O('A','m'): /* super-minor for array */
			if (ident.super_minor >= 0) {
				fprintf(stderr, Name ": super-minor cannot be set twice.  "
					"Second value: %s.\n", optarg);
				exit(2);
			}
			ident.super_minor = strtoul(optarg, &cp, 10);
			if (!optarg[0] || *cp) {
				fprintf(stderr, Name ": Bad super-minor number: %s.\n", optarg);
				exit(2);
			}
			continue;

		case O('A','c'): /* config file */
		case O('F','c'):
			if (configfile) {
				fprintf(stderr, Name ": configfile cannot be set twice.  "
					"Second value is %s.\n", optarg);
				exit(2);
			}
			configfile = optarg;
			/* FIXME possibly check that config file exists.  Even parse it */
			continue;
		case O('A','s'): /* scan */
		case O('E','s'):
			scan = 1;
			continue;

		case O('F','m'): /* mail address */
			if (mailaddr)
				fprintf(stderr, Name ": only specify one mailaddress. %s ignored.\n",
					optarg);
			else
				mailaddr = optarg;
			continue;

		case O('F','p'): /* alert program */
			if (program)
				fprintf(stderr, Name ": only specify one alter program. %s ignored.\n",
					optarg);
			else
				program = optarg;
			continue;

		case O('F','d'): /* delay in seconds */
			if (delay)
				fprintf(stderr, Name ": only specify delay once. %s ignored.\n",
					optarg);
			else {
				delay = strtol(optarg, &c, 10);
				if (!optarg[0] || *c || delay<1) {
					fprintf(stderr, Name ": invalid delay: %s\n",
						optarg);
					exit(2);
				}
			}
			continue;
			

			/* now the general management options.  Some are applicable
			 * to other modes. None have arguments.
			 */
		case O('@','a'):
		case O('C','a'):
		case O('B','a'):
		case O('A','a'): /* add a drive */
			devmode = 'a';
			continue;
		case O('@','r'): /* remove a drive */
			devmode = 'r';
			continue;
		case O('@','f'): /* set faulty */
			devmode = 'f';
			continue;
		case O('@','R'):
		case O('A','R'):
		case O('B','R'):
		case O('C','R'): /* Run the array */
			if (runstop < 0) {
				fprintf(stderr, Name ": Cannot both Stop and Run an array\n");
				exit(2);
			}
			runstop = 1;
			continue;
		case O('@','S'):
			if (runstop > 0) {
				fprintf(stderr, Name ": Cannot both Run and Stop an array\n");
				exit(2);
			}
			runstop = -1;
			continue;

		case O('@','o'):
			if (readonly < 0) {
				fprintf(stderr, Name ": Cannot have both readonly and readwrite\n");
				exit(2);
			}
			readonly = 1;
			continue;
		case O('@','w'):
			if (readonly > 0) {
				fprintf(stderr, "mkdctl: Cannot have both readwrite and readonly.\n");
				exit(2);
			}
			readonly = -1;
			continue;
		}
		/* We have now processed all the valid options. Anything else is
		 * an error
		 */
		fprintf(stderr, Name ": option %c not valid in mode %c\n",
			opt, mode);
		exit(2);

	}

	if (!mode) {
		fputs(Usage, stderr);
		exit(2);
	}
	/* Ok, got the option parsing out of the way
	 * hopefully it's mostly right but there might be some stuff
	 * missing
	 *
	 * That is mosty checked in ther per-mode stuff but...
	 *
	 * For @,B,C  and A without -s, the first device listed must be an md device
	 * we check that here and open it.
	 */

	if (mode=='@' || mode == 'B' || mode == 'C' || (mode == 'A' && ! scan)) {
		if (devs_found < 1) {
			fprintf(stderr, Name ": an md device must be given in this mode\n");
			exit(2);
		}
		mdfd = open_mddev(devlist->devname);
		if (mdfd < 0)
			exit(1);
	}


	rv = 0;
	switch(mode) {
	case '@':/* Management */
		/* readonly, add/remove, readwrite, runstop */
		if (readonly>0)
			rv = Manage_ro(devlist->devname, mdfd, readonly);
		if (!rv && devs_found>1)
			rv = Manage_subdevs(devlist->devname, mdfd,
					    devlist->next);
		if (!rv && readonly < 0)
			rv = Manage_ro(devlist->devname, mdfd, readonly);
		if (!rv && runstop)
			rv = Manage_runstop(devlist->devname, mdfd, runstop);
		break;
	case 'A': /* Assemble */
		if (!scan)
			rv = Assemble(devlist->devname, mdfd, &ident, configfile,
				      devlist->next,
				      readonly, runstop, verbose, force);
		else if (devs_found>0)
			for (dv = devlist ; dv ; dv=dv->next) {
				mddev_ident_t array_ident = conf_get_ident(configfile, dv->devname);
				mdfd = open_mddev(dv->devname);
				if (mdfd < 0) {
					rv |= 1;
					continue;
				}
				if (array_ident == NULL) {
					fprintf(stderr, Name ": %s not identified in config file.\n",
						dv->devname);
					rv |= 1;
					continue;
				}
				rv |= Assemble(dv->devname, mdfd, array_ident, configfile,
					       NULL,
					       readonly, runstop, verbose, force);
			}
		else {
			mddev_ident_t array_list =  conf_get_ident(configfile, NULL);
			if (!array_list) {
				fprintf(stderr, Name ": No arrays found in config file\n");
				rv = 1;
			} else
				for (; array_list; array_list = array_list->next) {
					mdu_array_info_t array;
					mdfd = open_mddev(array_list->devname);
					if (mdfd < 0) {
						rv |= 1;
						continue;
					}
					if (ioctl(mdfd, GET_ARRAY_INFO, &array)>=0)
						/* already assembled, skip */
						continue;
					rv |= Assemble(array_list->devname, mdfd,
						       array_list, configfile,
						       NULL,
						       readonly, runstop, verbose, force);
				}
		}
		break;
	case 'B': /* Build */
		rv = Build(devlist->devname, mdfd, chunk, level, raiddisks, devlist->next);
		break;
	case 'C': /* Create */
		rv = Create(devlist->devname, mdfd, chunk, level, layout, size,
			    raiddisks, sparedisks,
			    devs_found-1, devlist->next, runstop, verbose, force);
		break;
	case 'D': /* Detail */
		for (dv=devlist ; dv; dv=dv->next)
			rv |= Detail(dv->devname, brief);
		break;
	case 'E': /* Examine */
		if (devlist == NULL && scan==0) {
			fprintf(stderr, Name ": No devices to examine\n");
			exit(2);
		}
		rv = Examine(devlist, devlist?brief:!verbose, configfile);
		break;
	case 'F': /* Follow */
		rv= Monitor(devlist, mailaddr, program,
			    delay?delay:60, configfile);
	}
	exit(rv);
}
