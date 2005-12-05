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
 *
 *    Additions for bitmap and write-behind RAID options, Copyright (C) 2003-2004, 
 *    Paul Clements, SteelEye Technology, Inc.
 */

#include "mdadm.h"
#include "md_p.h"
#include <ctype.h>


int main(int argc, char *argv[])
{
	int mode = 0;
	int opt;
	int option_index;
	char *help_text;
	char *c;
	int rv;
	int i;

	int chunk = 0;
	int size = -1;
	int level = UnSet;
	int layout = UnSet;
	int raiddisks = 0;
	int max_disks = MD_SB_DISKS;
	int sparedisks = 0;
	struct mddev_ident_s ident;
	char *configfile = NULL;
	char *cp;
	char *update = NULL;
	int scan = 0;
	char devmode = 0;
	int runstop = 0;
	int readonly = 0;
	int write_behind = 0;
	int bitmap_fd = -1;
	char *bitmap_file = NULL;
	int bitmap_chunk = UnSet;
	int SparcAdjust = 0;
	mddev_dev_t devlist = NULL;
	mddev_dev_t *devlistend = & devlist;
	mddev_dev_t dv;
	int devs_found = 0;
	int verbose = 0;
	int quiet = 0;
	int brief = 0;
	int force = 0;
	int test = 0;
	int assume_clean = 0;
	int autof = 0; /* -2 means create device based on name:
			*    if it ends mdN, then non-partitioned array N
			*    if it ends dN, then partitions array N
			* -1 means create non-partitioned, choose N
			*  1 or more to create partitioned
			* If -1 or 1 and name is a 'standard' name, then
			* insist on a match of type and number.
			*/

	char *mailaddr = NULL;
	char *program = NULL;
	int delay = 0;
	int daemonise = 0;
	char *pidfile = NULL;
	int oneshot = 0;
	struct supertype *ss = NULL;
	int writemostly = 0;
	int re_add = 0;
	char *shortopt = short_options;
	int dosyslog = 0;

	int copies;

	int mdfd = -1;

	srandom(time(0) ^ getpid());

	ident.uuid_set=0;
	ident.level = UnSet;
	ident.raid_disks = UnSet;
	ident.super_minor= UnSet;
	ident.devices=0;
	ident.spare_group = NULL;
	ident.autof = 0;
	ident.st = NULL;
	ident.bitmap_fd = -1;
	ident.name[0] = 0;

	while ((option_index = -1) ,
	       (opt=getopt_long(argc, argv,
				shortopt, long_options,
				&option_index)) != -1) {
		int newmode = mode;
		/* firstly, some mode-independant options */
		switch(opt) {
		case 'h':
			help_text = Help;
			if (option_index > 0 && 
			    strcmp(long_options[option_index].name, "help-options")==0)
				help_text = OptionHelp;
			else
				switch (mode) {
				case ASSEMBLE : help_text = Help_assemble; break;
				case BUILD    : help_text = Help_build; break;
				case CREATE   : help_text = Help_create; break;
				case MANAGE   : help_text = Help_manage; break;
				case MISC     : help_text = Help_misc; break;
				case MONITOR  : help_text = Help_monitor; break;
				case GROW     : help_text = Help_grow; break;
				}
			fputs(help_text,stderr);
			exit(0);

		case 'V':
			fputs(Version, stderr);
			exit(0);

		case 'v': verbose++;
			continue;

		case 'q': quiet++;
			continue;

		case 'b':
			if (mode == ASSEMBLE || mode == BUILD || mode == CREATE || mode == GROW)
				break; /* b means bitmap */
			brief = 1;
			if (optarg) {
				fprintf(stderr, Name ": -b cannot have any extra immediately after it, sorry.\n");
				exit(2);
			}
			continue;

		case ':':
		case '?':
			fputs(Usage, stderr);
			exit(2);
		}
		/* second, figure out the mode.
		 * Some options force the mode.  Others
		 * set the mode if it isn't already 
		 */

		switch(opt) {
		case '@': /* just incase they say --manage */
			newmode = MANAGE;
			shortopt = short_bitmap_auto_options;
			break;
		case 'a':
		case 'r':
		case 'f':
		case 6: /* re-add */
			if (!mode) {
				newmode = MANAGE;
				shortopt = short_bitmap_auto_options;
			}
			break;

		case 'A': newmode = ASSEMBLE; shortopt = short_bitmap_auto_options; break;
		case 'B': newmode = BUILD; shortopt = short_bitmap_auto_options; break;
		case 'C': newmode = CREATE; shortopt = short_bitmap_auto_options; break;
		case 'F': newmode = MONITOR;break;
		case 'G': newmode = GROW; shortopt = short_bitmap_auto_options; break;

		case '#':
		case 'D':
		case 'E':
		case 'X':
		case 'Q': newmode = MISC; break;
		case 'R':
		case 'S':
		case 'o':
		case 'w':
		case 'K': if (!mode) newmode = MISC; break;
		}
		if (mode && newmode == mode) {
			/* everybody happy ! */
		} else if (mode && newmode != mode) {
			/* not allowed.. */
			fprintf(stderr, Name ": ");
			if (option_index >= 0)
				fprintf(stderr, "--%s", long_options[option_index].name);
			else
				fprintf(stderr, "-%c", opt);
			fprintf(stderr, " would set mdadm mode to \"%s\", but it is already set to \"%s\".\n",
				map_num(modes, newmode),
				map_num(modes, mode));
			exit(2);
		} else if (!mode && newmode) {
			mode = newmode;
		} else {
			/* special case of -c --help */
			if (opt == 'c' && 
			    ( strncmp(optarg, "--h", 3)==0 ||
			      strncmp(optarg, "-h", 2)==0)) {
				fputs(Help_config, stderr);
				exit(0);
			}

			/* If first option is a device, don't force the mode yet */
			if (opt == 1) {
				if (devs_found == 0) {
					dv = malloc(sizeof(*dv));
					if (dv == NULL) {
						fprintf(stderr, Name ": malloc failed\n");
						exit(3);
					}
					dv->devname = optarg;
					dv->disposition = devmode;
					dv->writemostly = writemostly;
					dv->re_add = re_add;
					dv->next = NULL;
					*devlistend = dv;
					devlistend = &dv->next;
			
					devs_found++;
					continue;
				}
				/* No mode yet, and this is the second device ... */
				fprintf(stderr, Name ": An option must be given to set the mode before a second device is listed\n");
				exit(2);
			}
			if (option_index >= 0)
				fprintf(stderr, Name ": --%s", long_options[option_index].name);
			else
				fprintf(stderr, Name ": -%c", opt);
			fprintf(stderr, " does not set the mode, and so cannot be the first option.\n");
			exit(2);
		}

		/* if we just set the mode, then done */
		switch(opt) {
		case '@':
		case '#':
		case 'A':
		case 'B':
		case 'C':
		case 'F':
		case 'G':
			continue;
		}
		if (opt == 1) {
		        /* an undecorated option - must be a device name.
			 */
			if (devs_found > 0 && mode == '@' && !devmode) {
				fprintf(stderr, Name ": Must give one of -a/-r/-f for subsequent devices at %s\n", optarg);
				exit(2);
			}
			if (devs_found > 0 && mode == 'G' && !devmode) {
				fprintf(stderr, Name ": Must give one of -a for devices do add: %s\n", optarg);
				exit(2);
			}
			dv = malloc(sizeof(*dv));
			if (dv == NULL) {
				fprintf(stderr, Name ": malloc failed\n");
				exit(3);
			}
			dv->devname = optarg;
			dv->disposition = devmode;
			dv->writemostly = writemostly;
			dv->re_add = re_add;
			dv->next = NULL;
			*devlistend = dv;
			devlistend = &dv->next;
			
			devs_found++;
			continue;
		}

		/* We've got a mode, and opt is now something else which
		 * could depend on the mode */
#define O(a,b) ((a<<8)|b)
		switch (O(mode,opt)) {
		case O(CREATE,'c'):
		case O(BUILD,'c'): /* chunk or rounding */
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

		case O(CREATE,'e'):
		case O(ASSEMBLE,'e'):
		case O(MISC,'e'): /* set metadata (superblock) information */
			if (ss) {
				fprintf(stderr, Name ": metadata information already given\n");
				exit(2);
			}
			for(i=0; !ss && superlist[i]; i++) 
				ss = superlist[i]->match_metadata_desc(optarg);

			if (!ss) {
				fprintf(stderr, Name ": unrecognised metadata identifier: %s\n", optarg);
				exit(2);
			}
			max_disks = ss->max_devs;
			continue;

		case O(MANAGE,'W'):
		case O(BUILD,'W'):
		case O(CREATE,'W'):
			/* set write-mostly for following devices */
			writemostly = 1;
			continue;

		case O(GROW,'z'):
		case O(CREATE,'z'): /* size */
			if (size >= 0) {
				fprintf(stderr, Name ": size may only be specified once. "
					"Second value is %s.\n", optarg);
				exit(2);
			}
			if (strcmp(optarg, "max")==0)
				size = 0;
			else {
				size = strtol(optarg, &c, 10);
				if (!optarg[0] || *c || size < 4) {
					fprintf(stderr, Name ": invalid size: %s\n",
						optarg);
					exit(2);
				}
			}
			continue;

		case O(GROW,'l'): /* hack - needed to understand layout */
		case O(CREATE,'l'):
		case O(BUILD,'l'): /* set raid level*/
			if (level != UnSet) {
				fprintf(stderr, Name ": raid level may only be set once.  "
					"Second value is %s.\n", optarg);
				exit(2);
			}
			level = map_name(pers, optarg);
			if (level == UnSet) {
				fprintf(stderr, Name ": invalid raid level: %s\n",
					optarg);
				exit(2);
			}
			if (level != 0 && level != -1 && level != 1 && level != -4 && level != -5 && mode == BUILD) {
				fprintf(stderr, Name ": Raid level %s not permitted with --build.\n",
					optarg);
				exit(2);
			}
			if (sparedisks > 0 && level < 1 && level >= -1) {
				fprintf(stderr, Name ": raid level %s is incompatible with spare-devices setting.\n",
					optarg);
				exit(2);
			}
			ident.level = level;
			continue;

		case O(CREATE,'p'): /* raid5 layout */
		case O(BUILD,'p'): /* faulty layout */
		case O(GROW, 'p'): /* faulty reconfig */
			if (layout != UnSet) {
				fprintf(stderr,Name ": layout may only be sent once.  "
					"Second value was %s\n", optarg);
				exit(2);
			}
			switch(level) {
			default:
				fprintf(stderr, Name ": layout not meaningful for %s arrays.\n",
					map_num(pers, level));
				exit(2);
			case UnSet:
				fprintf(stderr, Name ": raid level must be given before layout.\n");
				exit(2);

			case 5:
			case 6:
				layout = map_name(r5layout, optarg);
				if (layout==UnSet) {
					fprintf(stderr, Name ": layout %s not understood for raid5.\n",
						optarg);
					exit(2);
				}
				break;

			case 10:
				/* 'f' or 'n' followed by a number <= raid_disks */
				if ((optarg[0] !=  'n' && optarg[0] != 'f') ||
				    (copies = strtoul(optarg+1, &cp, 10)) < 1 ||
				    copies > 200 ||
				    *cp) {
					fprintf(stderr, Name ": layout for raid10 must be 'nNN' or 'fNN' where NN is a number, not %s\n", optarg);
					exit(2);
				}
				if (optarg[0] == 'n')
					layout = 256 + copies;
				else
					layout = 1 + (copies<<8);
				break;
			case -5: /* Faulty
				  * modeNNN
				  */
				    
			{
				int ln = strcspn(optarg, "0123456789");
				char *m = strdup(optarg);
				int mode;
				m[ln] = 0;
				mode = map_name(faultylayout, m);
				if (mode == UnSet) {
					fprintf(stderr, Name ": layout %s not understood for faulty.\n",
						optarg);
					exit(2);
				}
				layout = mode | (atoi(optarg+ln)<< ModeShift);
			}
			}
			continue;

		case O(CREATE,3):
		case O(BUILD,3): /* assume clean */
			assume_clean = 1;
			continue;

		case O(GROW,'n'):
		case O(CREATE,'n'):
		case O(BUILD,'n'): /* number of raid disks */
			if (raiddisks) {
				fprintf(stderr, Name ": raid-devices set twice: %d and %s\n",
					raiddisks, optarg);
				exit(2);
			}
			raiddisks = strtol(optarg, &c, 10);
			if (!optarg[0] || *c || raiddisks<=0) {
				fprintf(stderr, Name ": invalid number of raid devices: %s\n",
					optarg);
				exit(2);
			}
			ident.raid_disks = raiddisks;
			continue;

		case O(CREATE,'x'): /* number of spare (eXtra) discs */
			if (sparedisks) {
				fprintf(stderr,Name ": spare-devices set twice: %d and %s\n",
					sparedisks, optarg);
				exit(2);
			}
			if (level != UnSet && level <= 0 && level >= -1) {
				fprintf(stderr, Name ": spare-devices setting is incompatible with raid level %d\n",
					level);
				exit(2);
			}
			sparedisks = strtol(optarg, &c, 10);
			if (!optarg[0] || *c || sparedisks < 0) {
				fprintf(stderr, Name ": invalid number of spare-devices: %s\n",
					optarg);
				exit(2);
			}
			continue;

		case O(CREATE,'a'):
		case O(BUILD,'a'):
		case O(ASSEMBLE,'a'): /* auto-creation of device node */
			if (optarg == NULL)
				autof = -2;
			else if (strcasecmp(optarg,"no")==0)
				autof = 0;
			else if (strcasecmp(optarg,"yes")==0)
				autof = -2;
			else if (strcasecmp(optarg,"md")==0)
				autof = -1;
			else {
				/* There might be digits, and maybe a hypen, at the end */
				char *e = optarg + strlen(optarg);
				int num = 4;
				int len;
				while (e > optarg && isdigit(e[-1]))
					e--;
				if (*e) {
					num = atoi(e);
					if (num <= 0) num = 1;
				}
				if (e > optarg && e[-1] == '-')
					e--;
				len = e - optarg;
				if ((len == 3 && strncasecmp(optarg,"mdp",3)==0) ||
				    (len == 1 && strncasecmp(optarg,"p",1)==0) ||
				    (len >= 4 && strncasecmp(optarg,"part",4)==0))
					autof = num;
				else {
					fprintf(stderr, Name ": --auto flag arg of \"%s\" unrecognised: use no,yes,md,mdp,part\n"
						"        optionally followed by a number.\n",
						optarg);
					exit(2);
				}
			}
			continue;

		case O(BUILD,'f'): /* force honouring '-n 1' */
		case O(GROW,'f'): /* ditto */
		case O(CREATE,'f'): /* force honouring of device list */
		case O(ASSEMBLE,'f'): /* force assembly */
		case O(MISC,'f'): /* force zero */
			force=1;
			continue;

			/* now for the Assemble options */
		case O(ASSEMBLE,'u'): /* uuid of array */
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

		case O(CREATE,'N'):
		case O(ASSEMBLE,'N'):
			if (ident.name[0]) {
				fprintf(stderr, Name ": name cannot be set twice.   "
					"Second value %s.\n", optarg);
				exit(2);
			}
			if (strlen(optarg) > 32) {
				fprintf(stderr, Name ": name '%s' is too long, 32 chars max.\n",
					optarg);
				exit(2);
			}
			strcpy(ident.name, optarg);
			continue;

		case O(ASSEMBLE,'m'): /* super-minor for array */
			if (ident.super_minor != UnSet) {
				fprintf(stderr, Name ": super-minor cannot be set twice.  "
					"Second value: %s.\n", optarg);
				exit(2);
			}
			if (strcmp(optarg, "dev")==0)
				ident.super_minor = -2;
			else {
				ident.super_minor = strtoul(optarg, &cp, 10);
				if (!optarg[0] || *cp) {
					fprintf(stderr, Name ": Bad super-minor number: %s.\n", optarg);
					exit(2);
				}
			}
			continue;

		case O(ASSEMBLE,'U'): /* update the superblock */
			if (update) {
				fprintf(stderr, Name ": Can only update one aspect of superblock, both %s and %s given.\n",
					update, optarg);
				exit(2);
			}
			update = optarg;
			if (strcmp(update, "sparc2.2")==0) 
				continue;
			if (strcmp(update, "super-minor") == 0)
				continue;
			if (strcmp(update, "summaries")==0)
				continue;
			if (strcmp(update, "resync")==0)
				continue;
			if (strcmp(update, "byteorder")==0) {
				if (ss) {
					fprintf(stderr, Name ": must not set metadata type with --update=byteorder.\n");
					exit(2);
				}
				for(i=0; !ss && superlist[i]; i++) 
					ss = superlist[i]->match_metadata_desc("0.swap");
				if (!ss) {
					fprintf(stderr, Name ": INTERNAL ERROR cannot find 0.swap\n");
					exit(2);
				}

				continue;
			}
			fprintf(stderr, Name ": '--update %s' invalid.  Only 'sparc2.2', 'super-minor', 'resync' or 'summaries' supported\n",update);
			exit(2);

		case O(ASSEMBLE,'c'): /* config file */
		case O(MISC, 'c'):
		case O(MONITOR,'c'):
			if (configfile) {
				fprintf(stderr, Name ": configfile cannot be set twice.  "
					"Second value is %s.\n", optarg);
				exit(2);
			}
			configfile = optarg;
			/* FIXME possibly check that config file exists.  Even parse it */
			continue;
		case O(ASSEMBLE,'s'): /* scan */
		case O(MISC,'s'):
		case O(MONITOR,'s'):
			scan = 1;
			continue;

		case O(MONITOR,'m'): /* mail address */
			if (mailaddr)
				fprintf(stderr, Name ": only specify one mailaddress. %s ignored.\n",
					optarg);
			else
				mailaddr = optarg;
			continue;

		case O(MONITOR,'p'): /* alert program */
			if (program)
				fprintf(stderr, Name ": only specify one alter program. %s ignored.\n",
					optarg);
			else
				program = optarg;
			continue;

		case O(MONITOR,'d'): /* delay in seconds */
		case O(GROW, 'd'):
		case O(BUILD,'d'): /* delay for bitmap updates */
		case O(CREATE,'d'):
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
		case O(MONITOR,'f'): /* daemonise */
			daemonise = 1;
			continue;
		case O(MONITOR,'i'): /* pid */
			if (pidfile)
				fprintf(stderr, Name ": only specify one pid file. %s ignored.\n",
					optarg);
			else
				pidfile = optarg;
			continue;
		case O(MONITOR,'1'): /* oneshot */
			oneshot = 1;
			continue;
		case O(MONITOR,'t'): /* test */
			test = 1;
			continue;
		case O(MONITOR,'y'): /* log messages to syslog */
			openlog("mdadm", 0, SYSLOG_FACILITY);
			dosyslog = 1;
			continue;

			/* now the general management options.  Some are applicable
			 * to other modes. None have arguments.
			 */
		case O(GROW,'a'):
		case O(MANAGE,'a'): /* add a drive */
			devmode = 'a';
			re_add = 0;
			continue;
		case O(MANAGE,6):
			devmode = 'a';
			re_add = 1;
			continue;
		case O(MANAGE,'r'): /* remove a drive */
			devmode = 'r';
			continue;
		case O(MANAGE,'f'): /* set faulty */
			devmode = 'f';
			continue;
		case O(MANAGE,'R'):
		case O(ASSEMBLE,'R'):
		case O(BUILD,'R'):
		case O(CREATE,'R'): /* Run the array */
			if (runstop < 0) {
				fprintf(stderr, Name ": Cannot both Stop and Run an array\n");
				exit(2);
			}
			runstop = 1;
			continue;
		case O(MANAGE,'S'):
			if (runstop > 0) {
				fprintf(stderr, Name ": Cannot both Run and Stop an array\n");
				exit(2);
			}
			runstop = -1;
			continue;

		case O(MANAGE,'o'):
			if (readonly < 0) {
				fprintf(stderr, Name ": Cannot have both readonly and readwrite\n");
				exit(2);
			}
			readonly = 1;
			continue;
		case O(MANAGE,'w'):
			if (readonly > 0) {
				fprintf(stderr, Name ": Cannot have both readwrite and readonly.\n");
				exit(2);
			}
			readonly = -1;
			continue;

		case O(MISC,'Q'):
		case O(MISC,'D'):
		case O(MISC,'E'):
		case O(MISC,'K'):
		case O(MISC,'R'):
		case O(MISC,'S'):
		case O(MISC,'X'):
		case O(MISC,'o'):
		case O(MISC,'w'):
			if (devmode && devmode != opt &&
			    (devmode == 'E' || (opt == 'E' && devmode != 'Q'))) {
				fprintf(stderr, Name ": --examine/-E cannot be given with -%c\n",
					devmode =='E'?opt:devmode);
				exit(2);
			}
			devmode = opt;
			continue;
		case O(MISC,'t'):
			test = 1;
			continue;

		case O(MISC, 22):
			if (devmode != 'E') {
				fprintf(stderr, Name ": --sparc2.2 only allowed with --examine\n");
				exit(2);
			}
			SparcAdjust = 1;
			continue;

		case O(ASSEMBLE,'b'): /* here we simply set the bitmap file */
			if (!optarg) {
				fprintf(stderr, Name ": bitmap file needed with -b in --assemble mode\n");
				exit(2);
			}
			if (strcmp(optarg, "internal")==0) {
				fprintf(stderr, Name ": there is no need to specify --bitmap when assembling arrays with internal bitmaps\n");
				continue;
			}
			bitmap_fd = open(optarg, O_RDWR);
			if (!*optarg || bitmap_fd < 0) {
				fprintf(stderr, Name ": cannot open bitmap file %s: %s\n", optarg, strerror(errno));
				exit(2);
			}
			ident.bitmap_fd = bitmap_fd; /* for Assemble */
			continue;

		case O(GROW,'b'):
		case O(BUILD,'b'):
		case O(CREATE,'b'): /* here we create the bitmap */
			bitmap_file = optarg;
			continue;

		case O(GROW,4):
		case O(BUILD,4):
		case O(CREATE,4): /* bitmap chunksize */
			bitmap_chunk = strtol(optarg, &c, 10);
			if (!optarg[0] || *c || bitmap_chunk < 0 ||
					bitmap_chunk & (bitmap_chunk - 1)) {
				fprintf(stderr, Name ": invalid bitmap chunksize: %s\n",
						optarg);
				exit(2);
			}
			/* convert K to B, chunk of 0K means 512B */
			bitmap_chunk = bitmap_chunk ? bitmap_chunk * 1024 : 512;
			continue;

		case O(BUILD, 5):
		case O(CREATE, 5): /* write-behind mode */
			write_behind = DEFAULT_MAX_WRITE_BEHIND;
			if (optarg) {
				write_behind = strtol(optarg, &c, 10);
				if (write_behind < 0 || *c ||
				    write_behind > 16383) {
					fprintf(stderr, Name ": Invalid value for maximum outstanding write-behind writes: %s.\n\tMust be between 0 and 16383.\n", optarg);
					exit(2);
				}
			}
			continue;
		}
		/* We have now processed all the valid options. Anything else is
		 * an error
		 */
		fprintf(stderr, Name ": option %c not valid in %s mode\n",
			opt, map_num(modes, mode));
		exit(2);

	}

	if (!mode && devs_found) {
		mode = MISC;
		devmode = 'Q';
		if (devlist->disposition == 0)
			devlist->disposition = devmode;
	}
	if (!mode) {
		fputs(Usage, stderr);
		exit(2);
	}
	/* Ok, got the option parsing out of the way
	 * hopefully it's mostly right but there might be some stuff
	 * missing
	 *
	 * That is mosty checked in the per-mode stuff but...
	 *
	 * For @,B,C  and A without -s, the first device listed must be an md device
	 * we check that here and open it.
	 */

	if (mode==MANAGE || mode == BUILD || mode == CREATE || mode == GROW ||
	    (mode == ASSEMBLE && ! scan)) {
		if (devs_found < 1) {
			fprintf(stderr, Name ": an md device must be given in this mode\n");
			exit(2);
		}
		if ((int)ident.super_minor == -2 && autof) {
			fprintf(stderr, Name ": --super-minor=dev is incompatible with --auto\n");	
			exit(2);
		}
		mdfd = open_mddev(devlist->devname, autof);
		if (mdfd < 0)
			exit(1);
		if ((int)ident.super_minor == -2) {
			struct stat stb;
			fstat(mdfd, &stb);
			ident.super_minor = minor(stb.st_rdev);
		}
	}

	if (raiddisks) {
		if (raiddisks > max_disks) {
			fprintf(stderr, Name ": invalid number of raid devices: %d\n",
				raiddisks);
			exit(2);
		}
		if (raiddisks == 1 &&  !force && level != -5) {
			fprintf(stderr, Name ": '1' is an unusual number of drives for an array, so it is probably\n"
				"     a mistake.  If you really mean it you will need to specify --force before\n"
				"     setting the number of drives.\n");
			exit(2);
		}
	}
	if (sparedisks) {
		if ( sparedisks > max_disks - raiddisks) {
			fprintf(stderr, Name ": invalid number of spare-devices: %d\n",
				sparedisks);
			exit(2);
		}
	}

	rv = 0;
	switch(mode) {
	case MANAGE:
		/* readonly, add/remove, readwrite, runstop */
		if (readonly>0)
			rv = Manage_ro(devlist->devname, mdfd, readonly);
		if (!rv && devs_found>1)
			rv = Manage_subdevs(devlist->devname, mdfd,
					    devlist->next, verbose-quiet);
		if (!rv && readonly < 0)
			rv = Manage_ro(devlist->devname, mdfd, readonly);
		if (!rv && runstop)
			rv = Manage_runstop(devlist->devname, mdfd, runstop, 0);
		break;
	case ASSEMBLE:
		if (devs_found == 1 && ident.uuid_set == 0 &&
		    ident.super_minor == UnSet && ident.name[0] == 0 && !scan ) {
			/* Only a device has been given, so get details from config file */
			mddev_ident_t array_ident = conf_get_ident(configfile, devlist->devname);
			if (array_ident == NULL) {
				fprintf(stderr, Name ": %s not identified in config file.\n",
					devlist->devname);
				rv |= 1;
			} else {
				mdfd = open_mddev(devlist->devname, 
						  array_ident->autof ? array_ident->autof : autof);
				if (mdfd < 0)
					rv |= 1;
				else {
					rv |= Assemble(ss, devlist->devname, mdfd, array_ident, configfile,
						       NULL,
						       readonly, runstop, update, verbose-quiet, force);
					close(mdfd);
				}
			}
		} else if (!scan)
			rv = Assemble(ss, devlist->devname, mdfd, &ident, configfile,
				      devlist->next,
				      readonly, runstop, update, verbose-quiet, force);
		else if (devs_found>0) {
			if (update && devs_found > 1) {
				fprintf(stderr, Name ": can only update a single array at a time\n");
				exit(1);
			}
			for (dv = devlist ; dv ; dv=dv->next) {
				mddev_ident_t array_ident = conf_get_ident(configfile, dv->devname);
				if (array_ident == NULL) {
					fprintf(stderr, Name ": %s not identified in config file.\n",
						dv->devname);
					rv |= 1;
					continue;
				}
				mdfd = open_mddev(dv->devname, 
						  array_ident->autof ?array_ident->autof : autof);
				if (mdfd < 0) {
					rv |= 1;
					continue;
				}
				rv |= Assemble(ss, dv->devname, mdfd, array_ident, configfile,
					       NULL,
					       readonly, runstop, update, verbose-quiet, force);
				close(mdfd);
			}
		} else {
			mddev_ident_t array_list =  conf_get_ident(configfile, NULL);
			if (!array_list) {
				fprintf(stderr, Name ": No arrays found in config file\n");
				rv = 1;
			} else
				for (; array_list; array_list = array_list->next) {
					mdu_array_info_t array;
					mdfd = open_mddev(array_list->devname, 
							  array_list->autof ? array_list->autof : autof);
					if (mdfd < 0) {
						rv |= 1;
						continue;
					}
					if (ioctl(mdfd, GET_ARRAY_INFO, &array)>=0)
						/* already assembled, skip */
						;
					else
						rv |= Assemble(ss, array_list->devname, mdfd,
							       array_list, configfile,
							       NULL,
							       readonly, runstop, NULL, verbose-quiet, force);
					close(mdfd);
				}
		}
		break;
	case BUILD:
		if (bitmap_chunk == UnSet) bitmap_chunk = DEFAULT_BITMAP_CHUNK;
		if (delay == 0) delay = DEFAULT_BITMAP_DELAY;
		if (write_behind && !bitmap_file) {
			fprintf(stderr, Name ": write-behind mode requires a bitmap.\n");
			rv = 1;
			break;
		}

		if (bitmap_file) {
			if (strcmp(bitmap_file, "internal")==0) {
				fprintf(stderr, Name ": 'internal' bitmaps not supported with --build\n");
				rv |= 1;
				break;
			}
		}
		rv = Build(devlist->devname, mdfd, chunk, level, layout,
			   raiddisks, devlist->next, assume_clean,
			   bitmap_file, bitmap_chunk, write_behind, delay, verbose-quiet);
		break;
	case CREATE:
		if (delay == 0) delay = DEFAULT_BITMAP_DELAY;
		if (write_behind && !bitmap_file) {
			fprintf(stderr, Name ": write-behind mode requires a bitmap.\n");
			rv = 1;
			break;
		}
		if (ss == NULL) {
			for(i=0; !ss && superlist[i]; i++) 
				ss = superlist[i]->match_metadata_desc("default");
		}
		if (!ss) {
			fprintf(stderr, Name ": internal error - no default metadata style\n");
			exit(2);
		}

		rv = Create(ss, devlist->devname, mdfd, chunk, level, layout, size<0 ? 0 : size,
			    raiddisks, sparedisks, ident.name,
			    devs_found-1, devlist->next, runstop, verbose-quiet, force, assume_clean,
			    bitmap_file, bitmap_chunk, write_behind, delay);
		break;
	case MISC:

		if (devmode == 'E') {
			if (devlist == NULL && !scan) {
				fprintf(stderr, Name ": No devices to examine\n");
				exit(2);
			}
			if (devlist == NULL)
				devlist = conf_get_devs(configfile);
			if (devlist == NULL) {
				fprintf(stderr, Name ": No devices listed in %s\n", configfile?configfile:DefaultConfFile);
				exit(1);
			}
			rv = Examine(devlist, scan?(verbose>1?0:verbose+1):brief, scan, SparcAdjust, ss);
		} else {
			if (devlist == NULL) {
				if (devmode=='D' && scan) {
					/* apply --detail to all devices in /proc/mdstat */
					struct mdstat_ent *ms = mdstat_read(0);
					struct mdstat_ent *e;
					for (e=ms ; e ; e=e->next) {
						char *name = get_md_name(e->devnum);

						if (!name) {
							fprintf(stderr, Name ": cannot find device file for %s\n",
								e->dev);
							continue;
						}
						rv |= Detail(name, verbose>1?0:verbose+1, test);
						put_md_name(name);
					}
				} else	if (devmode == 'S' && scan) {
					/* apply --stop to all devices in /proc/mdstat */
					/* Due to possible stacking of devices, repeat until
					 * nothing more can be stopped
					 */
					int progress=1, err;
					int last = 0;
					do {
						struct mdstat_ent *ms = mdstat_read(0);
						struct mdstat_ent *e;

						if (!progress) last = 1;
						progress = 0; err = 0;
						for (e=ms ; e ; e=e->next) {
							char *name = get_md_name(e->devnum);

							if (!name) {
								fprintf(stderr, Name ": cannot find device file for %s\n",
									e->dev);
								continue;
							}
							mdfd = open_mddev(name, 0);
							if (mdfd >= 0) {
								if (Manage_runstop(name, mdfd, -1, !last))
									err = 1;
								else
									progress = 1;
								close(mdfd);
							}

							put_md_name(name);
						}
					} while (!last && err);
					if (err) rv |= 1;
				} else {
					fprintf(stderr, Name ": No devices given.\n");
					exit(2);
				}
			}
			for (dv=devlist ; dv; dv=dv->next) {
				switch(dv->disposition) {
				case 'D':
					rv |= Detail(dv->devname, brief?1+verbose:0, test); continue;
				case 'K': /* Zero superblock */
					rv |= Kill(dv->devname, force); continue;
				case 'Q':
					rv |= Query(dv->devname); continue;
				case 'X':
					rv |= ExamineBitmap(dv->devname, brief, ss); continue;
				}
				mdfd = open_mddev(dv->devname, 0);
				if (mdfd>=0) {
					switch(dv->disposition) {
					case 'R':
						rv |= Manage_runstop(dv->devname, mdfd, 1, 0); break;
					case 'S':
						rv |= Manage_runstop(dv->devname, mdfd, -1, 0); break;
					case 'o':
						rv |= Manage_ro(dv->devname, mdfd, 1); break;
					case 'w':
						rv |= Manage_ro(dv->devname, mdfd, -1); break;
					}
					close(mdfd);
				} else
					rv |= 1;
			}
		}
		break;
	case MONITOR:
		if (!devlist && !scan) {
			fprintf(stderr, Name ": Cannot monitor: need --scan or at least one device\n");
			rv = 1;
			break;
		}
		if (pidfile && !daemonise) {
			fprintf(stderr, Name ": Cannot write a pid file when not in daemon mode\n");
			rv = 1;
			break;
		}
		rv= Monitor(devlist, mailaddr, program,
			    delay?delay:60, daemonise, scan, oneshot,
			    dosyslog, configfile, test, pidfile);
		break;

	case GROW:
		if (devs_found > 1) {
			
			/* must be '-a'. */
			if (size >= 0 || raiddisks) {
				fprintf(stderr, Name ": --size, --raiddisks, and --add are exclusing in --grow mode\n");
				rv = 1;
				break;
			}
			for (dv=devlist->next; dv ; dv=dv->next) {
				rv = Grow_Add_device(devlist->devname, mdfd, dv->devname);
				if (rv)
					break;
			}
		} else if ((size >= 0) + (raiddisks != 0) +  (layout != UnSet) + (bitmap_file != NULL)> 1) {
			fprintf(stderr, Name ": can change at most one of size, raiddisks, bitmap, and layout\n");
			rv = 1;
			break;
		} else if (layout != UnSet)
			rv = Manage_reconfig(devlist->devname, mdfd, layout);
		else if (size >= 0 || raiddisks)
			rv = Manage_resize(devlist->devname, mdfd, size, raiddisks);
		else if (bitmap_file) {
			if (delay == 0) delay = DEFAULT_BITMAP_DELAY;
			rv = Grow_addbitmap(devlist->devname, mdfd, bitmap_file,
					    bitmap_chunk, delay, write_behind);
		} else
			fprintf(stderr, Name ": no changes to --grow\n");
		break;
	}
	exit(rv);
}
