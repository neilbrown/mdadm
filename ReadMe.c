/*
 * mdctl - manage Linux "md" devices aka RAID arrays.
 *
 * Copyright (C) 2001 Neil Brown <neilb@cse.unsw.edu.au>
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

char Version[] = Name " - v0.5 - 23 August 2001\n";
/*
 * File: ReadMe.c
 *
 * This file contains general comments about the implementation
 * and the various usage messages that can be displayed by mdctl
 *
 * mdctl is a single program that can be used to control Linux md devices.
 * It is intended to provide all the functionality of the mdtools and
 * raidtools but with a very different interface.
 * mdctl can perform all functions without a configuration file.
 * There is the option of using a configuration file, but not in the same
 * way that raidtools uses one
 * raidtools uses a configuration file to describe how to create a RAID
 * array, and also uses this file partially to start a previously
 * created RAID array.  Further, raidtools requires the configuration
 * file for such things as stopping a raid array which needs to know
 * nothing about the array.
 *
 * The configuration file that can be used by mdctl lists two
 * different things:
 * 1/ a mapping from uuid to md device to identify which arrays are
 *    expect and what names (numbers) they should be given
 * 2/ a list of devices that should be scanned for md sub-devices
 *
 *
 */

/*
 * mdctl has 4 major modes of operation:
 * 1/ Create
 *     This mode is used to create a new array with a superbock
 *     It can progress in several step create-add-add-run
 *     or it can all happen with one command
 * 2/ Assemble
 *     This mode is used to assemble the parts of a previously created
 *     array into an active array.  Components can be explicitly given
 *     or can be searched for.  mdctl (optionally) check that the components
 *     do form a bonafide array, and can, on request, fiddle superblock
 *     version numbers so as to assemble a faulty array.
 * 3/ Build
 *     This is for building legacy arrays without superblocks
 * 4/ Manage
 *     This is for odd bits an pieces like hotadd, hotremove, setfaulty,
 *     stop, readonly,readwrite
 *     If an array is only partially setup by the Create/Assemble/Build
 *     command, subsequent Manage commands can finish the job.
 */

char short_options[]="-ABCDEFhVvc:l:p:m:n:x:u:c:d:z:sarfRSow";
struct option long_options[] = {
    {"manage",    0, 0, '@'},
    {"assemble",  0, 0, 'A'},
    {"build",     0, 0, 'B'},
    {"create",    0, 0, 'C'},
    {"detail",    0, 0, 'D'},
    {"examine",   0, 0, 'E'},
    {"follow",    0, 0, 'F'},

    /* synonyms */
    {"monitor",   0, 0, 'F'},
	    
    /* after those will normally come the name of the md device */
    {"help",      0, 0, 'h'},
    {"version",	  0, 0, 'V'},
    {"verbose",   0, 0, 'v'},

    /* For create or build: */
    {"chunk",	  1, 0, 'c'},
    {"rounding",  1, 0, 'c'}, /* for linear, chunk is really a rounding number */
    {"level",     1, 0, 'l'}, /* 0,1,4,5,linear */
    {"parity",    1, 0, 'p'}, /* {left,right}-{a,}symetric */
    {"layout",    1, 0, 'p'},
    {"raid-disks",1, 0, 'n'},
    {"spare-disks",1,0, 'x'},
    {"size"      ,1, 0, 'z'},

    /* For assemble */
    {"uuid",      1, 0, 'u'},
    {"super-minor",1,0, 'm'},
    {"config",    1, 0, 'c'},
    {"scan",      0, 0, 's'},
    {"force",	  0, 0, 'f'},
    /* Management */
    {"add",       0, 0, 'a'},
    {"remove",    0, 0, 'r'},
    {"fail",      0, 0, 'f'},
    {"set-faulty",0, 0, 'f'},
    {"run",       0, 0, 'R'},
    {"stop",      0, 0, 'S'},
    {"readonly",  0, 0, 'o'},
    {"readwrite", 0, 0, 'w'},

    /* For Follow/monitor */
    {"mail",      1, 0, 'm'},
    {"program",   1, 0, 'p'},
    {"alert",     1, 0, 'p'},
    {"delay",     1, 0, 'd'},
    
    
    {0, 0, 0, 0}
};

char Usage[] =
"Usage: mdctl --help\n"
"  for help\n"
;

char Help[] =
"Usage: mdctl --create device options...\n"
"       mdctl --assemble device options...\n"
"       mdctl --build device options...\n"
"       mdctl --detail device\n"
"       mdctl --examine device\n"
"       mdctl --follow options...\n"
"       mdctl device options...\n"
" mdctl is used for controlling Linux md devices (aka RAID arrays)\n"
" For detail help on major modes use, e.g.\n"
"         mdctl --assemble --help\n"
"\n"
"Any parameter that does not start with '-' is treated as a device name\n"
"The first such name is normally the name of an md device.  Subsequent\n"
"names are names of component devices."
"\n"
"Available options are:\n"
"  --create      -C   : Create a new array\n"
"  --assemble    -A   : Assemble an existing array\n"
"  --build       -B   : Build a legacy array without superblock\n"
"  --detail      -D   : Print detail of a given md array\n"
"  --examine     -E   : Print content of md superblock on device\n"
"  --follow      -F   : Follow (monitor) any changes to devices and respond to them\n"
"  --monitor          : same as --follow\n"
"\n"
"  --help        -h   : This help message or, after above option,\n"
"                       mode specific help message\n"
"  --version     -V   : Print version information for mdctl\n"
"  --verbose     -v   : Be more verbose about what is happening\n"
"\n"
" For create or build:\n"
"  --chunk=      -c   : chunk size of kibibytes\n"
"  --rounding=        : rounding factor for linear array (==chunck size)\n"
"  --level=      -l   : raid level: 0,1,4,5,linear.  0 or linear for build\n"
"  --paritiy=    -p   : raid5 parity algorith: {left,right}-{,a}symmetric\n"
"  --layout=          : same as --parity\n"
"  --raid-disks= -n   : number of active devices in array\n"
"  --spare-disks= -x  : number of spares (eXtras) to allow space for\n"
"  --size=       -z   : Size (in K) of each drive in RAID1/4/5 - optional\n"
"  --force       -f   : Honour devices as listed on command line.  Don't\n"
"                     : insert a missing drive for RAID5.\n"
"\n"
" For assemble:\n"
"  --uuid=       -u   : uuid of array to assemble. Devices which don't\n"
"                       have this uuid are excluded\n"
"  --super-minor= -m  : minor number to look for in super-block when\n"
"                       choosing devices to use.\n"
"  --config=     -c   : config file\n"
"  --scan        -s   : scan config file for missing information\n"
"  --force       -f   : Assemble the array even if some superblocks appear out-of-date\n"
"\n"
" For follow/monitor:\n"
"  --mail=       -m   : Address to mail alerts of failure to\n"
"  --program=    -p   : Program to run when an event is detected\n"
"  --alert=           : same as --program\n"
"  --delay=      -d   : seconds of delay between polling state. default=60\n"
"\n"
" General management:\n"
"  --add         -a   : add, or hotadd subsequent devices\n"
"  --remove      -r   : remove subsequent devices\n"
"  --fail        -f   : mark subsequent devices a faulty\n"
"  --set-faulty       : same as --fail\n"
"  --run         -R   : start a partially built array\n"
"  --stop        -S   : deactive array, releasing all resources\n"
"  --readonly    -o   : mark array as readonly\n"
"  --readwrite   -w   : mark array as readwrite\n"
;


char Help_create[] =
"Usage:  mdctl --create device -chunk=X --level=Y --raid-disks=Z devices\n"
"\n"
" This usage will initialise a new md array and possibly associate some\n"
" devices with it.  If enough devices are given to complete the array,\n"
" the array will be activated.  Otherwise it will be left inactive\n"
" to be completed and activated by subsequent management commands.\n"
"\n"
" As devices are added, they are checked to see if they contain\n"
" raid superblocks or filesystems.  They are also check to see if\n"
" the variance in device size exceeds 1%.\n"
" If any discrepancy is found, the array will not automatically\n"
" be run, though the presence of a '--run' can override this\n"
" caution.\n"
"\n"
" If the --size option is given, it is not necessary to list any subdevices\n"
" in this command.  They can be added later, before a --run.\n"
" If no --size is given, the apparent size of the smallest drive given\n"
" is used.\n"
"\n"
" The General management options that are valid with --create are:\n"
"   --run   : insist of running the array even if not all devices\n"
"             are present or some look odd.\n"
"   --readonly: start the array readonly - not supported yet.\n"
"\n"
;

char Help_build[] =
"Usage:  mdctl --build device -chunk=X --level=Y --raid-disks=Z devices\n"
"\n"
" This usage is similar to --create.  The difference is that it creates\n"
" a legacy array with a superblock.  With these arrays there is no\n"
" different between initially creating the array and subsequently\n"
" assembling the array, except that hopefully there is useful data\n"
" there in the second case.\n"
"\n"
" The level may only be 0 or linear.\n"
" All devices must be listed and the array will be started once complete.\n"
;

char Help_assemble[] =
"Usage: mdctl --assemble device options...\n"
"       mdctl --assemble --scan options...\n"
"\n"
"This usage assembles one or more raid arrays from pre-existing\n"
"components.\n"
"For each array, mdctl needs to know the md device, the identify of\n"
"the array, and a number of sub devices. These can be found in a number\n"
"of ways.\n"
"\n"
"The md device is either given on the command line or is found listed\n"
"in the config file.  The array identity is determined either from the\n"
"--uuid or --super-minor commandline arguments, or from the config file,\n"
"or from the first component device on the command line.\n"
"\n"
"The different combinations of these are as follows:\n"
" If the --scan option is not given, then only devices and identities\n"
" listed on the command line are considered.\n"
" The first device will be the array devices, and the remainder will\n"
" examined when looking for components.\n"
" If an explicit identity is given with --uuid or --super-minor, then\n"
" Each device with a superblock which matches that identity is considered,\n"
" otherwise every device listed is considered.\n"
"\n"
" If the --scan option is given, and no devices are listed, then\n"
" every array listed in the config file is considered for assembly.\n"
" The identity can candidate devices are determined from the config file.\n"
"\n"
" If the --scan option is given as well as one or more devices, then\n"
" Those devices are md devices that are to be assembled.  Their identity\n"
" and components are determined from the config file.\n"
"\n"
"The config file contains, apart from blank lines and comment lines that\n"
"start with a has, two sorts of configuration lines, array lines and\n"
"device lines.\n"
"Each configuration line is constructed of a number of space separated\n"
"words, and can be continued on subsequent physical lines by indenting\n"
"those lines.\n"
"\n"
"A device line starts with the word 'device' and then has a number of words\n"
"which identify devices.  These words should be names of devices in the filesystem,\n"
"and can contain wildcards. There can be multiple words or each device line,\n"
"and multiple device lines.  All devices so listed are checked for relevant\n"
"super blocks when assembling arrays.\n"
"\n"
"An array line start with the word 'array'.  This is followed by the name of\n"
"the array device in the filesystem, e.g. '/dev/md2'.  Subsequent words\n"
"describe the identity of the array, used to recognise devices to include in the\n"
"array.  The identity can be given as a UUID with a word starting 'uuid=', or\n"
"as a minor-number stored in the superblock using 'super-minor=', or as a list\n"
"of devices.  This is given as a comma separated list of names, possibly containing\n"
"wildcards, preceeded by 'devices='. If multiple critea are given, than a device\n"
"must match all of them to be considered.\n"
"\n"
;


/* name/number mappings */

mapping_t r5layout[] = {
	{ "left_asymmetric", 0},
	{ "right_asymmetric", 1},
	{ "left_symmetric", 2},
	{ "right_symmetric", 3},

	{ "default", 2},
	{ "la", 0},
	{ "ra", 1},
	{ "ls", 2},
	{ "rs", 3},
	{ NULL, 0}
};

mapping_t pers[] = {
	{ "linear", -1},
	{ "raid0", 0},
	{ "0", 0},
	{ "stripe", 0},
	{ "raid1", 1},
	{ "1", 1},
	{ "mirror", 1},
	{ "raid4", 4},
	{ "4", 4},
	{ "raid5", 5},
	{ "5", 5},
	{ NULL, 0}
};
