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
 *    Added by Dale Stephenson
 *    steph@snapserver.com
 */

#include	"mdadm.h"
#include	"md_u.h"
#include	"md_p.h"

int Kill(char *dev, int force)
{
	/*
	 * Nothing fancy about Kill.  It just zeroes out a superblock
	 * Definitely not safe.
	 */

	mdp_super_t super;
	int fd, rv = 0;
		
	fd = open(dev, O_RDWR|O_EXCL);
	if (fd < 0) {
		fprintf(stderr, Name ": Couldn't open %s for write - not zeroing\n",
			dev);
		return 1;
	} 
	rv = load_super(fd, &super);
	if (force && rv >= 5)
		rv = 0; /* ignore bad data in superblock */
	switch(rv) {
	case 1:
		fprintf(stderr, Name ": cannot file device size for %s: %s\n",
			dev, strerror(errno));
		break;
	case 2:
		fprintf(stderr, Name ": %s is too small for md.\n", dev);
		break;
	case 3:
	case 4:
		fprintf(stderr, Name ": cannot access superblock on %s.\n", dev);
		break;
	case 5:
	case 6:
		fprintf(stderr, Name ": %s does not appear to have an MD superblock.\n", dev);
		break;
	}
	if (!rv) {
		memset(&super, 0, sizeof(super));
		if (store_super(fd, &super)) {
			fprintf(stderr, Name ": Could not zero superblock on %s\n",
				dev);
			rv = 1;
		}
	}
	close(fd);
	return rv;
}
