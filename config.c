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

#include	"mdctl.h"

/*
 * Read the config file
 *
 * conf_get_uuids gets a list of devicename+uuid pairs
 * conf_get_devs gets device names after expanding wildcards
 *
 * Each keeps the returned list and frees it when asked to make
 * a new list.
 *
 */

char DefaultConfFile[] = "/etc/mdctl.conf";

mddev_uuid_t conf_get_uuids(char *conffile)
{
    return NULL;
}

mddev_dev_t conf_get_devs(char *conffile)
{
    return NULL;
}

