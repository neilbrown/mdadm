/* mdadm - manage Linux "md" devices aka RAID arrays.
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
 */

#include "xmalloc.h"
#include "mdadm_status.h"

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

static void *exit_memory_alloc_failure(void)
{
	fprintf(stderr, "Memory allocation failure - aborting\n");

	exit(MDADM_STATUS_MEM_FAIL);
}

void *xmalloc(size_t len)
{
	void *rv = malloc(len);

	if (rv)
		return rv;

	return exit_memory_alloc_failure();
}

void *xrealloc(void *ptr, size_t len)
{
	void *rv = realloc(ptr, len);

	if (rv)
		return rv;

	return exit_memory_alloc_failure();
}

void *xcalloc(size_t num, size_t size)
{
	void *rv = calloc(num, size);

	if (rv)
		return rv;

	return exit_memory_alloc_failure();
}

char *xstrdup(const char *str)
{
	char *rv = strdup(str);

	if (rv)
		return rv;

	return exit_memory_alloc_failure();
}

void *xmemalign(size_t alignment, size_t size)
{
	void *ptr = NULL;
	int result = posix_memalign(&ptr, alignment, size);

	if (result == 0)
		return ptr;

	return exit_memory_alloc_failure();
}
