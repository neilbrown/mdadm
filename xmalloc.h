// SPDX-License-Identifier: GPL-2.0-only

#ifndef XMALLOC_H
#define XMALLOC_H

#include <stddef.h>

void *xmalloc(size_t len);
void *xrealloc(void *ptr, size_t len);
void *xcalloc(size_t num, size_t size);
char *xstrdup(const char *str);
void *xmemalign(size_t alignment, size_t size);

#endif
