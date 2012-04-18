/*
 * mdadm - manage Linux "md" devices aka RAID arrays.
 *
 * Copyright (C) 2001-2009 Neil Brown <neilb@suse.de>
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
	char *c;
	int rv;
	int i;

	int chunk = 0;
	long long size = -1;
	long long array_size = -1;
	int level = UnSet;
	int layout = UnSet;
	char *layout_str = NULL;
	int raiddisks = 0;
	int sparedisks = 0;
	struct mddev_ident ident;
	char *configfile = NULL;
	char *cp;
	char *update = NULL;
	int scan = 0;
	int devmode = 0;
	int runstop = 0;
	int readonly = 0;
	int write_behind = 0;
	int bitmap_fd = -1;
	char *bitmap_file = NULL;
	char *backup_file = NULL;
	int invalid_backup = 0;
	int bitmap_chunk = UnSet;
	int SparcAdjust = 0;
	struct mddev_dev *devlist = NULL;
	struct mddev_dev **devlistend = & devlist;
	struct mddev_dev *dv;
	int devs_found = 0;
	int verbose = 0;
	int quiet = 0;
	int brief = 0;
	int force = 0;
	int test = 0;
	int export = 0;
	int assume_clean = 0;
	char *prefer = NULL;
	char *symlinks = NULL;
	int grow_continue = 0;
	/* autof indicates whether and how to create device node.
	 * bottom 3 bits are style.  Rest (when shifted) are number of parts
	 * 0  - unset
	 * 1  - don't create (no)
	 * 2  - if is_standard, then create (yes)
	 * 3  - create as 'md' - reject is_standard mdp (md)
	 * 4  - create as 'mdp' - reject is_standard md (mdp)
	 * 5  - default to md if not is_standard (md in config file)
	 * 6  - default to mdp if not is_standard (part, or mdp in config file)
	 */
	int autof = 0;

	char *homehost = NULL;
	char sys_hostname[256];
	int require_homehost = 1;
	char *mailaddr = NULL;
	char *program = NULL;
	int increments = 20;
	int delay = 0;
	int daemonise = 0;
	char *pidfile = NULL;
	int oneshot = 0;
	int spare_sharing = 1;
	struct supertype *ss = NULL;
	int writemostly = 0;
	int re_add = 0;
	char *shortopt = short_options;
	int dosyslog = 0;
	int rebuild_map = 0;
	char *subarray = NULL;
	char *remove_path = NULL;
	char *udev_filename = NULL;

	int print_help = 0;
	FILE *outf;

	int mdfd = -1;

	int freeze_reshape = 0;

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
	ident.bitmap_file = NULL;
	ident.name[0] = 0;
	ident.container = NULL;
	ident.member = NULL;

	while ((option_index = -1) ,
	       (opt=getopt_long(argc, argv,
				shortopt, long_options,
				&option_index)) != -1) {
		int newmode = mode;
		/* firstly, some mode-independent options */
		switch(opt) {
		case HelpOptions:
			print_help = 2;
			continue;
		case 'h':
			print_help = 1;
			continue;

		case 'V':
			fputs(Version, stderr);
			exit(0);

		case 'v': verbose++;
			continue;

		case 'q': quiet++;
			continue;

		case 'b':
			if (mode == ASSEMBLE || mode == BUILD || mode == CREATE
			    || mode == GROW || mode == INCREMENTAL
			    || mode == MANAGE)
				break; /* b means bitmap */
		case Brief:
			brief = 1;
			continue;

		case 'Y': export++;
			continue;

		case HomeHost:
			if (strcasecmp(optarg, "<ignore>") == 0)
				require_homehost = 0;
			else
				homehost = optarg;
			continue;

		/*
		 * --offroot sets first char of argv[0] to @. This is used
		 * by systemd to signal that the tast was launched from
		 * initrd/initramfs and should be preserved during shutdown
		 */
		case OffRootOpt:
			argv[0][0] = '@';
			__offroot = 1;
			continue;

		case Prefer:
			if (prefer)
				free(prefer);
			if (asprintf(&prefer, "/%s/", optarg) <= 0)
				prefer = NULL;
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
		case ManageOpt:
			newmode = MANAGE;
			shortopt = short_bitmap_options;
			break;
		case 'a':
		case Add:
		case 'r':
		case Remove:
		case 'f':
		case Fail:
		case ReAdd: /* re-add */
			if (!mode) {
				newmode = MANAGE;
				shortopt = short_bitmap_options;
			}
			break;

		case 'A': newmode = ASSEMBLE; shortopt = short_bitmap_auto_options; break;
		case 'B': newmode = BUILD; shortopt = short_bitmap_auto_options; break;
		case 'C': newmode = CREATE; shortopt = short_bitmap_auto_options; break;
		case 'F': newmode = MONITOR;break;
		case 'G': newmode = GROW;
			shortopt = short_bitmap_options;
			break;
		case 'I': newmode = INCREMENTAL;
			shortopt = short_bitmap_auto_options; break;
		case AutoDetect:
			newmode = AUTODETECT;
			break;

		case MiscOpt:
		case 'D':
		case 'E':
		case 'X':
		case 'Q':
			newmode = MISC;
			break;

		case 'R':
		case 'S':
		case 'o':
		case 'w':
		case 'W':
		case WaitOpt:
		case Waitclean:
		case DetailPlatform:
		case KillSubarray:
		case UpdateSubarray:
		case UdevRules:
		case 'K':
			if (!mode)
				newmode = MISC;
			break;

		case NoSharing:
			newmode = MONITOR;
			break;
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
			if (mode == MISC && devs_found) {
				fprintf(stderr, Name ": No action given for %s in --misc mode\n",
					devlist->devname);
				fprintf(stderr,"       Action options must come before device names\n");
				exit(2);
			}
		} else {
			/* special case of -c --help */
			if ((opt == 'c' || opt == ConfigFile) &&
			    ( strncmp(optarg, "--h", 3)==0 ||
			      strncmp(optarg, "-h", 2)==0)) {
				fputs(Help_config, stdout);
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
					dv->used = 0;
					dv->next = NULL;
					*devlistend = dv;
					devlistend = &dv->next;

					devs_found++;
					continue;
				}
				/* No mode yet, and this is the second device ... */
				fprintf(stderr, Name ": An option must be given to set the mode before a second device\n"
					"       (%s) is listed\n", optarg);
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
		case ManageOpt:
		case MiscOpt:
		case 'A':
		case 'B':
		case 'C':
		case 'F':
		case 'G':
		case 'I':
		case AutoDetect:
			continue;
		}
		if (opt == 1) {
		        /* an undecorated option - must be a device name.
			 */
			if (devs_found > 0 && mode == MANAGE && !devmode) {
				fprintf(stderr, Name ": Must give one of -a/-r/-f"
					" for subsequent devices at %s\n", optarg);
				exit(2);
			}
			if (devs_found > 0 && mode == GROW && !devmode) {
				fprintf(stderr, Name ": Must give -a/--add for"
					" devices to add: %s\n", optarg);
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
			dv->used = 0;
			dv->next = NULL;
			*devlistend = dv;
			devlistend = &dv->next;

			devs_found++;
			continue;
		}

		/* We've got a mode, and opt is now something else which
		 * could depend on the mode */
#define O(a,b) ((a<<16)|b)
		switch (O(mode,opt)) {
		case O(GROW,'c'):
		case O(GROW,ChunkSize):
		case O(CREATE,'c'):
		case O(CREATE,ChunkSize):
		case O(BUILD,'c'): /* chunk or rounding */
		case O(BUILD,ChunkSize): /* chunk or rounding */
			if (chunk) {
				fprintf(stderr, Name ": chunk/rounding may only be specified once. "
					"Second value is %s.\n", optarg);
				exit(2);
			}
			chunk = parse_size(optarg);
			if (chunk < 8 || (chunk&1)) {
				fprintf(stderr, Name ": invalid chunk/rounding value: %s\n",
					optarg);
				exit(2);
			}
			/* Convert sectors to K */
			chunk /= 2;
			continue;

		case O(INCREMENTAL, 'e'):
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
			continue;

		case O(MANAGE,'W'):
		case O(MANAGE,WriteMostly):
		case O(BUILD,'W'):
		case O(BUILD,WriteMostly):
		case O(CREATE,'W'):
		case O(CREATE,WriteMostly):
			/* set write-mostly for following devices */
			writemostly = 1;
			continue;

		case O(MANAGE,'w'):
			/* clear write-mostly for following devices */
			writemostly = 2;
			continue;


		case O(GROW,'z'):
		case O(CREATE,'z'):
		case O(BUILD,'z'): /* size */
			if (size >= 0) {
				fprintf(stderr, Name ": size may only be specified once. "
					"Second value is %s.\n", optarg);
				exit(2);
			}
			if (strcmp(optarg, "max")==0)
				size = 0;
			else {
				size = parse_size(optarg);
				if (size < 8) {
					fprintf(stderr, Name ": invalid size: %s\n",
						optarg);
					exit(2);
				}
				/* convert sectors to K */
				size /= 2;
			}
			continue;

		case O(GROW,'Z'): /* array size */
			if (array_size >= 0) {
				fprintf(stderr, Name ": array-size may only be specified once. "
					"Second value is %s.\n", optarg);
				exit(2);
			}
			if (strcmp(optarg, "max") == 0)
				array_size = 0;
			else {
				array_size = parse_size(optarg);
				if (array_size <= 0) {
					fprintf(stderr, Name ": invalid array size: %s\n",
						optarg);
					exit(2);
				}
			}
			continue;

		case O(GROW,'l'):
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
			if (level != 0 && level != LEVEL_LINEAR && level != 1 &&
			    level != LEVEL_MULTIPATH && level != LEVEL_FAULTY &&
			    level != 10 &&
			    mode == BUILD) {
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

		case O(GROW, 'p'): /* new layout */
		case O(GROW, Layout):
			if (layout_str) {
				fprintf(stderr,Name ": layout may only be sent once.  "
					"Second value was %s\n", optarg);
				exit(2);
			}
			layout_str = optarg;
			/* 'Grow' will parse the value */
			continue;

		case O(CREATE,'p'): /* raid5 layout */
		case O(CREATE,Layout):
		case O(BUILD,'p'): /* faulty layout */
		case O(BUILD,Layout):
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
				layout = map_name(r5layout, optarg);
				if (layout==UnSet) {
					fprintf(stderr, Name ": layout %s not understood for raid5.\n",
						optarg);
					exit(2);
				}
				break;
			case 6:
				layout = map_name(r6layout, optarg);
				if (layout==UnSet) {
					fprintf(stderr, Name ": layout %s not understood for raid6.\n",
						optarg);
					exit(2);
				}
				break;

			case 10:
				layout = parse_layout_10(optarg);
				if (layout < 0) {
					fprintf(stderr, Name ": layout for raid10 must be 'nNN', 'oNN' or 'fNN' where NN is a number, not %s\n", optarg);
					exit(2);
				}
				break;
			case LEVEL_FAULTY:
				/* Faulty
				 * modeNNN
				 */
				layout = parse_layout_faulty(optarg);
				if (layout == -1) {
					fprintf(stderr, Name ": layout %s not understood for faulty.\n",
						optarg);
					exit(2);
				}
				break;
			}
			continue;

		case O(CREATE,AssumeClean):
		case O(BUILD,AssumeClean): /* assume clean */
		case O(GROW,AssumeClean):
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
		case O(CREATE,Auto):
		case O(BUILD,'a'):
		case O(BUILD,Auto):
		case O(INCREMENTAL,'a'):
		case O(INCREMENTAL,Auto):
		case O(ASSEMBLE,'a'):
		case O(ASSEMBLE,Auto): /* auto-creation of device node */
			autof = parse_auto(optarg, "--auto flag", 0);
			continue;

		case O(CREATE,Symlinks):
		case O(BUILD,Symlinks):
		case O(ASSEMBLE,Symlinks): /* auto creation of symlinks in /dev to /dev/md */
			symlinks = optarg;
			continue;

		case O(BUILD,'f'): /* force honouring '-n 1' */
		case O(BUILD,Force): /* force honouring '-n 1' */
		case O(GROW,'f'): /* ditto */
		case O(GROW,Force): /* ditto */
		case O(CREATE,'f'): /* force honouring of device list */
		case O(CREATE,Force): /* force honouring of device list */
		case O(ASSEMBLE,'f'): /* force assembly */
		case O(ASSEMBLE,Force): /* force assembly */
		case O(MISC,'f'): /* force zero */
		case O(MISC,Force): /* force zero */
		case O(MANAGE,Force): /* add device which is too large */
			force=1;
			continue;
			/* now for the Assemble options */
		case O(ASSEMBLE, FreezeReshape):   /* Freeze reshape during
						    * initrd phase */
		case O(INCREMENTAL, FreezeReshape):
			freeze_reshape = 1;
			continue;
		case O(CREATE,'u'): /* uuid of array */
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
		case O(MISC,'N'):
			if (ident.name[0]) {
				fprintf(stderr, Name ": name cannot be set twice.   "
					"Second value %s.\n", optarg);
				exit(2);
			}
			if (mode == MISC && !subarray) {
				fprintf(stderr, Name ": -N/--name only valid with --update-subarray in misc mode\n");
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
		case O(ASSEMBLE,SuperMinor):
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
		case O(MISC,'U'):
			if (update) {
				fprintf(stderr, Name ": Can only update one aspect"
					" of superblock, both %s and %s given.\n",
					update, optarg);
				exit(2);
			}
			if (mode == MISC && !subarray) {
				fprintf(stderr, Name ": Only subarrays can be"
					" updated in misc mode\n");
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
			if (strcmp(update, "uuid")==0)
				continue;
			if (strcmp(update, "name")==0)
				continue;
			if (strcmp(update, "homehost")==0)
				continue;
			if (strcmp(update, "devicesize")==0)
				continue;
			if (strcmp(update, "no-bitmap")==0)
				continue;
			if (strcmp(update, "byteorder")==0) {
				if (ss) {
					fprintf(stderr,
						Name ": must not set metadata"
						" type with --update=byteorder.\n");
					exit(2);
				}
				for(i=0; !ss && superlist[i]; i++)
					ss = superlist[i]->match_metadata_desc(
						"0.swap");
				if (!ss) {
					fprintf(stderr, Name ": INTERNAL ERROR"
						" cannot find 0.swap\n");
					exit(2);
				}

				continue;
			}
			if (strcmp(update,"?") == 0 ||
			    strcmp(update, "help") == 0) {
				outf = stdout;
				fprintf(outf, Name ": ");
			} else {
				outf = stderr;
				fprintf(outf,
					Name ": '--update=%s' is invalid.  ",
					update);
			}
			fprintf(outf, "Valid --update options are:\n"
		"     'sparc2.2', 'super-minor', 'uuid', 'name', 'resync',\n"
		"     'summaries', 'homehost', 'byteorder', 'devicesize',\n"
		"     'no-bitmap'\n");
			exit(outf == stdout ? 0 : 2);

		case O(MANAGE,'U'):
			/* update=devicesize is allowed with --re-add */
			if (devmode != 'a' || re_add != 1) {
				fprintf(stderr, Name "--update in Manage mode only"
					" allowed with --re-add.\n");
				exit(1);
			}
			if (update) {
				fprintf(stderr, Name ": Can only update one aspect"
					" of superblock, both %s and %s given.\n",
					update, optarg);
				exit(2);
			}
			update = optarg;
			if (strcmp(update, "devicesize") != 0) {
				fprintf(stderr, Name ": only 'devicesize' can be"
					" updated with --re-add\n");
				exit(2);
			}
			continue;

		case O(INCREMENTAL,NoDegraded):
			fprintf(stderr, Name ": --no-degraded is deprecated in Incremental mode\n");
		case O(ASSEMBLE,NoDegraded): /* --no-degraded */
			runstop = -1; /* --stop isn't allowed for --assemble,
				       * so we overload slightly */
			continue;

		case O(ASSEMBLE,'c'):
		case O(ASSEMBLE,ConfigFile):
		case O(INCREMENTAL, 'c'):
		case O(INCREMENTAL, ConfigFile):
		case O(MISC, 'c'):
		case O(MISC, ConfigFile):
		case O(MONITOR,'c'):
		case O(MONITOR,ConfigFile):
			if (configfile) {
				fprintf(stderr, Name ": configfile cannot be set twice.  "
					"Second value is %s.\n", optarg);
				exit(2);
			}
			configfile = optarg;
			set_conffile(configfile);
			/* FIXME possibly check that config file exists.  Even parse it */
			continue;
		case O(ASSEMBLE,'s'): /* scan */
		case O(MISC,'s'):
		case O(MONITOR,'s'):
		case O(INCREMENTAL,'s'):
			scan = 1;
			continue;

		case O(MONITOR,'m'): /* mail address */
		case O(MONITOR,EMail):
			if (mailaddr)
				fprintf(stderr, Name ": only specify one mailaddress. %s ignored.\n",
					optarg);
			else
				mailaddr = optarg;
			continue;

		case O(MONITOR,'p'): /* alert program */
		case O(MONITOR,ProgramOpt): /* alert program */
			if (program)
				fprintf(stderr, Name ": only specify one alter program. %s ignored.\n",
					optarg);
			else
				program = optarg;
			continue;

		case O(MONITOR,'r'): /* rebuild increments */
		case O(MONITOR,Increment):
			increments = atoi(optarg);
			if (increments>99 || increments<1) {
				fprintf(stderr, Name ": please specify positive integer between 1 and 99 as rebuild increments.\n");
				exit(2);
			}
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
		case O(MONITOR,Fork):
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
			spare_sharing = 0;
			continue;
		case O(MONITOR,'t'): /* test */
			test = 1;
			continue;
		case O(MONITOR,'y'): /* log messages to syslog */
			openlog("mdadm", LOG_PID, SYSLOG_FACILITY);
			dosyslog = 1;
			continue;
		case O(MONITOR, NoSharing):
			spare_sharing = 0;
			continue;
			/* now the general management options.  Some are applicable
			 * to other modes. None have arguments.
			 */
		case O(GROW,'a'):
		case O(GROW,Add):
		case O(MANAGE,'a'):
		case O(MANAGE,Add): /* add a drive */
			devmode = 'a';
			re_add = 0;
			continue;
		case O(MANAGE,ReAdd):
			devmode = 'a';
			re_add = 1;
			continue;
		case O(MANAGE,'r'): /* remove a drive */
		case O(MANAGE,Remove):
			devmode = 'r';
			continue;
		case O(MANAGE,'f'): /* set faulty */
		case O(MANAGE,Fail):
		case O(INCREMENTAL,'f'):
		case O(INCREMENTAL,Remove):
		case O(INCREMENTAL,Fail): /* r for incremental is taken, use f
					  * even though we will both fail and
					  * remove the device */
			devmode = 'f';
			continue;
		case O(INCREMENTAL,'R'):
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
		case O(MANAGE,'t'):
			test = 1;
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
		case O(MISC,'W'):
		case O(MISC, WaitOpt):
		case O(MISC, Waitclean):
		case O(MISC, DetailPlatform):
		case O(MISC, KillSubarray):
		case O(MISC, UpdateSubarray):
			if (opt == KillSubarray || opt == UpdateSubarray) {
				if (subarray) {
					fprintf(stderr, Name ": subarray can only"
						" be specified once\n");
					exit(2);
				}
				subarray = optarg;
			}
			if (devmode && devmode != opt &&
			    (devmode == 'E' || (opt == 'E' && devmode != 'Q'))) {
				fprintf(stderr, Name ": --examine/-E cannot be given with ");
				if (devmode == 'E') {
					if (option_index >= 0)
						fprintf(stderr, "--%s\n",
							long_options[option_index].name);
					else
						fprintf(stderr, "-%c\n", opt);
				} else if (isalpha(devmode))
					fprintf(stderr, "-%c\n", devmode);
				else
					fprintf(stderr, "previous option\n");
				exit(2);
			}
			devmode = opt;
			continue;
               case O(MISC, UdevRules):
		       if (devmode && devmode != opt) {
                               fprintf(stderr, Name ": --udev-rules must"
				       " be the only option.\n");
		       } else {
			       if (udev_filename)
				       fprintf(stderr, Name ": only specify one udev "
					       "rule filename. %s ignored.\n",
					       optarg);
			       else
				       udev_filename = optarg;
		       }
		       devmode = opt;
		       continue;
		case O(MISC,'t'):
			test = 1;
			continue;

		case O(MISC, Sparc22):
			if (devmode != 'E') {
				fprintf(stderr, Name ": --sparc2.2 only allowed with --examine\n");
				exit(2);
			}
			SparcAdjust = 1;
			continue;

		case O(ASSEMBLE,'b'): /* here we simply set the bitmap file */
		case O(ASSEMBLE,Bitmap):
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

		case O(ASSEMBLE, BackupFile):
		case O(GROW, BackupFile):
			/* Specify a file into which grow might place a backup,
			 * or from which assemble might recover a backup
			 */
			if (backup_file) {
				fprintf(stderr, Name ": backup file already specified, rejecting %s\n", optarg);
				exit(2);
			}
			backup_file = optarg;
			continue;

		case O(GROW, Continue):
			/* Continue interrupted grow
			 */
			grow_continue = 1;
			continue;
		case O(ASSEMBLE, InvalidBackup):
			/* Acknowledge that the backupfile is invalid, but ask
			 * to continue anyway
			 */
			invalid_backup = 1;
			continue;

		case O(BUILD,'b'):
		case O(BUILD,Bitmap):
		case O(CREATE,'b'):
		case O(CREATE,Bitmap): /* here we create the bitmap */
			if (strcmp(optarg, "none") == 0) {
				fprintf(stderr, Name ": '--bitmap none' only"
					" support for --grow\n");
				exit(2);
			}
			/* FALL THROUGH */
		case O(GROW,'b'):
		case O(GROW,Bitmap):
			if (strcmp(optarg, "internal")== 0 ||
			    strcmp(optarg, "none")== 0 ||
			    strchr(optarg, '/') != NULL) {
				bitmap_file = optarg;
				continue;
			}
			/* probable typo */
			fprintf(stderr, Name ": bitmap file must contain a '/', or be 'internal', or 'none'\n"
				"       not '%s'\n", optarg);
			exit(2);

		case O(GROW,BitmapChunk):
		case O(BUILD,BitmapChunk):
		case O(CREATE,BitmapChunk): /* bitmap chunksize */
			bitmap_chunk = parse_size(optarg);
			if (bitmap_chunk <= 0 ||
			    bitmap_chunk & (bitmap_chunk - 1)) {
				fprintf(stderr,
					Name ": invalid bitmap chunksize: %s\n",
					optarg);
				exit(2);
			}
			bitmap_chunk = bitmap_chunk * 512;
			continue;

		case O(GROW, WriteBehind):
		case O(BUILD, WriteBehind):
		case O(CREATE, WriteBehind): /* write-behind mode */
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

		case O(INCREMENTAL, 'r'):
		case O(INCREMENTAL, RebuildMapOpt):
			rebuild_map = 1;
			continue;
		case O(INCREMENTAL, IncrementalPath):
			remove_path = optarg;
			continue;
		}
		/* We have now processed all the valid options. Anything else is
		 * an error
		 */
		if (option_index > 0)
			fprintf(stderr, Name ":option --%s not valid in %s mode\n",
				long_options[option_index].name,
				map_num(modes, mode));
		else
			fprintf(stderr, Name ": option -%c not valid in %s mode\n",
				opt, map_num(modes, mode));
		exit(2);

	}

	if (print_help) {
		char *help_text = Help;
		if (print_help == 2)
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
			case INCREMENTAL:help_text= Help_incr; break;
			}
		fputs(help_text,stdout);
		exit(0);
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

	if (symlinks) {
		struct createinfo *ci = conf_get_create_info();

		if (strcasecmp(symlinks, "yes") == 0)
			ci->symlinks = 1;
		else if (strcasecmp(symlinks, "no") == 0)
			ci->symlinks = 0;
		else {
			fprintf(stderr, Name ": option --symlinks must be 'no' or 'yes'\n");
			exit(2);
		}
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
		if (mode == MANAGE || mode == GROW) {
			mdfd = open_mddev(devlist->devname, 1);
			if (mdfd < 0)
				exit(1);
		} else
			/* non-existent device is OK */
			mdfd = open_mddev(devlist->devname, 0);
		if (mdfd == -2) {
			fprintf(stderr, Name ": device %s exists but is not an "
				"md array.\n", devlist->devname);
			exit(1);
		}
		if ((int)ident.super_minor == -2) {
			struct stat stb;
			if (mdfd < 0) {
				fprintf(stderr, Name ": --super-minor=dev given, and "
					"listed device %s doesn't exist.\n",
					devlist->devname);
				exit(1);
			}
			fstat(mdfd, &stb);
			ident.super_minor = minor(stb.st_rdev);
		}
		if (mdfd >= 0 && mode != MANAGE && mode != GROW) {
			/* We don't really want this open yet, we just might
			 * have wanted to check some things
			 */
			close(mdfd);
			mdfd = -1;
		}
	}

	if (raiddisks) {
		if (raiddisks == 1 &&  !force && level != -5) {
			fprintf(stderr, Name ": '1' is an unusual number of drives for an array, so it is probably\n"
				"     a mistake.  If you really mean it you will need to specify --force before\n"
				"     setting the number of drives.\n");
			exit(2);
		}
	}

	if (homehost == NULL)
		homehost = conf_get_homehost(&require_homehost);
	if (homehost == NULL || strcasecmp(homehost, "<system>")==0) {
		if (gethostname(sys_hostname, sizeof(sys_hostname)) == 0) {
			sys_hostname[sizeof(sys_hostname)-1] = 0;
			homehost = sys_hostname;
		}
	}
	if (homehost && (!homehost[0] || strcasecmp(homehost, "<none>") == 0)) {
		homehost = NULL;
		require_homehost = 0;
	}

	if (!((mode == MISC && devmode == 'E')
	      || (mode == MONITOR && spare_sharing == 0)) &&
	    geteuid() != 0) {
		fprintf(stderr, Name ": must be super-user to perform this action\n");
		exit(1);
	}

	ident.autof = autof;

	rv = 0;
	switch(mode) {
	case MANAGE:
		/* readonly, add/remove, readwrite, runstop */
		if (readonly>0)
			rv = Manage_ro(devlist->devname, mdfd, readonly);
		if (!rv && devs_found>1)
			rv = Manage_subdevs(devlist->devname, mdfd,
					    devlist->next, verbose-quiet, test,
					    update, force);
		if (!rv && readonly < 0)
			rv = Manage_ro(devlist->devname, mdfd, readonly);
		if (!rv && runstop)
			rv = Manage_runstop(devlist->devname, mdfd, runstop, quiet);
		break;
	case ASSEMBLE:
		if (devs_found == 1 && ident.uuid_set == 0 &&
		    ident.super_minor == UnSet && ident.name[0] == 0 && !scan ) {
			/* Only a device has been given, so get details from config file */
			struct mddev_ident *array_ident = conf_get_ident(devlist->devname);
			if (array_ident == NULL) {
				fprintf(stderr, Name ": %s not identified in config file.\n",
					devlist->devname);
				rv |= 1;
				if (mdfd >= 0)
					close(mdfd);
			} else {
				if (array_ident->autof == 0)
					array_ident->autof = autof;
				rv |= Assemble(ss, devlist->devname, array_ident,
					       NULL, backup_file, invalid_backup,
					       readonly, runstop, update,
					       homehost, require_homehost,
					       verbose-quiet, force,
					       freeze_reshape);
			}
		} else if (!scan)
			rv = Assemble(ss, devlist->devname, &ident,
				      devlist->next, backup_file, invalid_backup,
				      readonly, runstop, update,
				      homehost, require_homehost,
				      verbose-quiet, force,
				      freeze_reshape);
		else if (devs_found>0) {
			if (update && devs_found > 1) {
				fprintf(stderr, Name ": can only update a single array at a time\n");
				exit(1);
			}
			if (backup_file && devs_found > 1) {
				fprintf(stderr, Name ": can only assemble a single array when providing a backup file.\n");
				exit(1);
			}
			for (dv = devlist ; dv ; dv=dv->next) {
				struct mddev_ident *array_ident = conf_get_ident(dv->devname);
				if (array_ident == NULL) {
					fprintf(stderr, Name ": %s not identified in config file.\n",
						dv->devname);
					rv |= 1;
					continue;
				}
				if (array_ident->autof == 0)
					array_ident->autof = autof;
				rv |= Assemble(ss, dv->devname, array_ident,
					       NULL, backup_file, invalid_backup,
					       readonly, runstop, update,
					       homehost, require_homehost,
					       verbose-quiet, force,
					       freeze_reshape);
			}
		} else {
			struct mddev_ident *a, *array_list =  conf_get_ident(NULL);
			struct mddev_dev *devlist = conf_get_devs();
			struct map_ent *map = NULL;
			int cnt = 0;
			int failures, successes;

			if (conf_verify_devnames(array_list)) {
				fprintf(stderr, Name
					": Duplicate MD device names in "
					"conf file were found.\n");
				exit(1);
			}
			if (devlist == NULL) {
				fprintf(stderr, Name ": No devices listed in conf file were found.\n");
				exit(1);
			}
			if (update) {
				fprintf(stderr, Name ": --update not meaningful with a --scan assembly.\n");
				exit(1);
			}
			if (backup_file) {
				fprintf(stderr, Name ": --backup_file not meaningful with a --scan assembly.\n");
				exit(1);
			}
			for (a = array_list; a ; a = a->next) {
				a->assembled = 0;
				if (a->autof == 0)
					a->autof = autof;
			}
			if (map_lock(&map))
				fprintf(stderr, Name " %s: failed to get "
					"exclusive lock on mapfile\n",
					__func__);
			do {
				failures = 0;
				successes = 0;
				rv = 0;
				for (a = array_list; a ; a = a->next) {
					int r;
					if (a->assembled)
						continue;
					if (a->devname &&
					    strcasecmp(a->devname, "<ignore>") == 0)
						continue;
				
					r = Assemble(ss, a->devname,
						     a,
						     NULL, NULL, 0,
						     readonly, runstop, NULL,
						     homehost, require_homehost,
						     verbose-quiet, force,
						     freeze_reshape);
					if (r == 0) {
						a->assembled = 1;
						successes++;
					} else
						failures++;
					rv |= r;
					cnt++;
				}
			} while (failures && successes);
			if (homehost && cnt == 0) {
				/* Maybe we can auto-assemble something.
				 * Repeatedly call Assemble in auto-assemble mode
				 * until it fails
				 */
				int rv2;
				int acnt;
				ident.autof = autof;
				do {
					struct mddev_dev *devlist = conf_get_devs();
					acnt = 0;
					do {
						rv2 = Assemble(ss, NULL,
							       &ident,
							       devlist, NULL, 0,
							       readonly,
							       runstop, NULL,
							       homehost,
							       require_homehost,
							       verbose-quiet,
							       force,
							       freeze_reshape);
						if (rv2==0) {
							cnt++;
							acnt++;
						}
					} while (rv2!=2);
					/* Incase there are stacked devices, we need to go around again */
				} while (acnt);
				if (cnt == 0 && rv == 0) {
					fprintf(stderr, Name ": No arrays found in config file or automatically\n");
					rv = 1;
				} else if (cnt)
					rv = 0;
			} else if (cnt == 0 && rv == 0) {
				fprintf(stderr, Name ": No arrays found in config file\n");
				rv = 1;
			}
			map_unlock(&map);
		}
		break;
	case BUILD:
		if (delay == 0) delay = DEFAULT_BITMAP_DELAY;
		if (write_behind && !bitmap_file) {
			fprintf(stderr, Name ": write-behind mode requires a bitmap.\n");
			rv = 1;
			break;
		}
		if (raiddisks == 0) {
			fprintf(stderr, Name ": no raid-devices specified.\n");
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
		rv = Build(devlist->devname, chunk, level, layout,
			   raiddisks, devlist->next, assume_clean,
			   bitmap_file, bitmap_chunk, write_behind,
			   delay, verbose-quiet, autof, size);
		break;
	case CREATE:
		if (delay == 0) delay = DEFAULT_BITMAP_DELAY;
		if (write_behind && !bitmap_file) {
			fprintf(stderr, Name ": write-behind mode requires a bitmap.\n");
			rv = 1;
			break;
		}
		if (raiddisks == 0) {
			fprintf(stderr, Name ": no raid-devices specified.\n");
			rv = 1;
			break;
		}

		rv = Create(ss, devlist->devname, chunk, level, layout, size<0 ? 0 : size,
			    raiddisks, sparedisks, ident.name, homehost,
			    ident.uuid_set ? ident.uuid : NULL,
			    devs_found-1, devlist->next, runstop, verbose-quiet, force, assume_clean,
			    bitmap_file, bitmap_chunk, write_behind, delay, autof);
		break;
	case MISC:
		if (devmode == 'E') {
			if (devlist == NULL && !scan) {
				fprintf(stderr, Name ": No devices to examine\n");
				exit(2);
			}
			if (devlist == NULL)
				devlist = conf_get_devs();
			if (devlist == NULL) {
				fprintf(stderr, Name ": No devices listed in %s\n", configfile?configfile:DefaultConfFile);
				exit(1);
			}
			if (brief && verbose)
				brief = 2;
			rv = Examine(devlist, scan?(verbose>1?0:verbose+1):brief,
				     export, scan,
				     SparcAdjust, ss, homehost);
		} else if (devmode == DetailPlatform) {
			rv = Detail_Platform(ss ? ss->ss : NULL, ss ? scan : 1, verbose);
		} else {
			if (devlist == NULL) {
				if ((devmode=='D' || devmode == Waitclean) && scan) {
					/* apply --detail or --wait-clean to
					 * all devices in /proc/mdstat
					 */
					struct mdstat_ent *ms = mdstat_read(0, 1);
					struct mdstat_ent *e;
					struct map_ent *map = NULL;
					int members;
					int v = verbose>1?0:verbose+1;

					for (members = 0; members <= 1; members++) {
					for (e=ms ; e ; e=e->next) {
						char *name;
						struct map_ent *me;
						int member = e->metadata_version &&
							strncmp(e->metadata_version,
								"external:/", 10) == 0;
						if (members != member)
							continue;
						me = map_by_devnum(&map, e->devnum);
						if (me && me->path
						    && strcmp(me->path, "/unknown") != 0)
							name = me->path;
						else
							name = get_md_name(e->devnum);

						if (!name) {
							fprintf(stderr, Name ": cannot find device file for %s\n",
								e->dev);
							continue;
						}
						if (devmode == 'D')
							rv |= Detail(name, v,
								     export, test,
								     homehost, prefer);
						else
							rv |= WaitClean(name, -1, v);
						put_md_name(name);
					}
					}
					free_mdstat(ms);
				} else	if (devmode == 'S' && scan) {
					/* apply --stop to all devices in /proc/mdstat */
					/* Due to possible stacking of devices, repeat until
					 * nothing more can be stopped
					 */
					int progress=1, err;
					int last = 0;
					do {
						struct mdstat_ent *ms = mdstat_read(0, 0);
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
							mdfd = open_mddev(name, 1);
							if (mdfd >= 0) {
								if (Manage_runstop(name, mdfd, -1, quiet?1:last?0:-1))
									err = 1;
								else
									progress = 1;
								close(mdfd);
							}

							put_md_name(name);
						}
						free_mdstat(ms);
					} while (!last && err);
					if (err) rv |= 1;
				} else if (devmode == UdevRules) {
					rv = Write_rules(udev_filename);
				} else {
					fprintf(stderr, Name ": No devices given.\n");
					exit(2);
				}
			}
			for (dv=devlist ; dv; dv=dv->next) {
				switch(dv->disposition) {
				case 'D':
					rv |= Detail(dv->devname,
						     brief?1+verbose:0,
						     export, test, homehost, prefer);
					continue;
				case 'K': /* Zero superblock */
					if (ss)
						rv |= Kill(dv->devname, ss, force, quiet,0);
					else {
						int q = quiet;
						do {
							rv |= Kill(dv->devname, NULL, force, q, 0);
							q = 1;
						} while (rv == 0);
						rv &= ~2;
					}
					continue;
				case 'Q':
					rv |= Query(dv->devname); continue;
				case 'X':
					rv |= ExamineBitmap(dv->devname, brief, ss); continue;
				case 'W':
				case WaitOpt:
					rv |= Wait(dv->devname); continue;
				case Waitclean:
					rv |= WaitClean(dv->devname, -1, verbose-quiet); continue;
				case KillSubarray:
					rv |= Kill_subarray(dv->devname, subarray, quiet);
					continue;
				case UpdateSubarray:
					if (update == NULL) {
						fprintf(stderr,
							Name ": -U/--update must be specified with --update-subarray\n");
						rv |= 1;
						continue;
					}
					rv |= Update_subarray(dv->devname, subarray, update, &ident, quiet);
					continue;
				}
				mdfd = open_mddev(dv->devname, 1);
				if (mdfd>=0) {
					switch(dv->disposition) {
					case 'R':
						rv |= Manage_runstop(dv->devname, mdfd, 1, quiet); break;
					case 'S':
						rv |= Manage_runstop(dv->devname, mdfd, -1, quiet); break;
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
		if (delay == 0) {
			if (get_linux_version() > 2006016)
				/* mdstat responds to poll */
				delay = 1000;
			else
				delay = 60;
		}
		rv= Monitor(devlist, mailaddr, program,
			    delay?delay:60, daemonise, scan, oneshot,
			    dosyslog, test, pidfile, increments,
			    spare_sharing, prefer);
		break;

	case GROW:
		if (array_size >= 0) {
			/* alway impose array size first, independent of
			 * anything else
			 * Do not allow level or raid_disks changes at the
			 * same time as that can be irreversibly destructive.
			 */
			struct mdinfo sra;
			int err;
			if (raiddisks || level != UnSet) {
				fprintf(stderr, Name ": cannot change array size in same operation "
					"as changing raiddisks or level.\n"
					"    Change size first, then check that data is still intact.\n");
				rv = 1;
				break;
			}
			sysfs_init(&sra, mdfd, 0);
			if (array_size == 0)
				err = sysfs_set_str(&sra, NULL, "array_size", "default");
			else
				err = sysfs_set_num(&sra, NULL, "array_size", array_size / 2);
			if (err < 0) {
				if (errno == E2BIG)
					fprintf(stderr, Name ": --array-size setting"
						" is too large.\n");
				else
					fprintf(stderr, Name ": current kernel does"
						" not support setting --array-size\n");
				rv = 1;
				break;
			}
		}
		if (devs_found > 1 && raiddisks == 0) {
			/* must be '-a'. */
			if (size >= 0 || chunk || layout_str != NULL || bitmap_file) {
				fprintf(stderr, Name ": --add cannot be used with "
					"other geometry changes in --grow mode\n");
				rv = 1;
				break;
			}
			for (dv=devlist->next; dv ; dv=dv->next) {
				rv = Grow_Add_device(devlist->devname, mdfd,
						     dv->devname);
				if (rv)
					break;
			}
		} else if (bitmap_file) {
			if (size >= 0 || raiddisks || chunk ||
			    layout_str != NULL || devs_found > 1) {
				fprintf(stderr, Name ": --bitmap changes cannot be "
					"used with other geometry changes "
					"in --grow mode\n");
				rv = 1;
				break;
			}
			if (delay == 0)
				delay = DEFAULT_BITMAP_DELAY;
			rv = Grow_addbitmap(devlist->devname, mdfd, bitmap_file,
					    bitmap_chunk, delay, write_behind, force);
		} else if (grow_continue)
			rv = Grow_continue_command(devlist->devname,
						   mdfd, backup_file,
						   verbose);
		else if (size >= 0 || raiddisks != 0 || layout_str != NULL
			   || chunk != 0 || level != UnSet) {
			rv = Grow_reshape(devlist->devname, mdfd, quiet, backup_file,
					  size, level, layout_str, chunk, raiddisks,
					  devlist->next,
					  assume_clean, force);
		} else if (array_size < 0)
			fprintf(stderr, Name ": no changes to --grow\n");
		break;
	case INCREMENTAL:
		if (rebuild_map) {
			RebuildMap();
		}
		if (scan) {
			if (runstop <= 0) {
				fprintf(stderr, Name
			 ": --incremental --scan meaningless without --run.\n");
				break;
			}
			if (devmode == 'f') {
				fprintf(stderr, Name
			 ": --incremental --scan --fail not supported.\n");
				break;
			}
			rv = IncrementalScan(verbose);
		}
		if (!devlist) {
			if (!rebuild_map && !scan) {
				fprintf(stderr, Name
					": --incremental requires a device.\n");
				rv = 1;
			}
			break;
		}
		if (devlist->next) {
			fprintf(stderr, Name
			       ": --incremental can only handle one device.\n");
			rv = 1;
			break;
		}
		if (devmode == 'f')
			rv = IncrementalRemove(devlist->devname, remove_path,
					       verbose-quiet);
		else
			rv = Incremental(devlist->devname, verbose-quiet,
					 runstop, ss, homehost,
					 require_homehost, autof,
					 freeze_reshape);
		break;
	case AUTODETECT:
		autodetect();
		break;
	}
	exit(rv);
}
