#
# mdctl - manage Linux "md" devices aka RAID arrays.
#
# Copyright (C) 2001-2002 Neil Brown <neilb@cse.unsw.edu.au>
#
#
#    This program is free software; you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation; either version 2 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program; if not, write to the Free Software
#    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
#
#    Author: Neil Brown
#    Email: <neilb@cse.unsw.edu.au>
#    Paper: Neil Brown
#           School of Computer Science and Engineering
#           The University of New South Wales
#           Sydney, 2052
#           Australia
#

CC = gcc
CFLAGS = -Wall,error,strict-prototypes -ggdb

INSTALL = /usr/bin/install
DESTDIR = /sbin

OBJS =  mdctl.o config.o  ReadMe.o util.o Manage.o Assemble.o Build.o Create.o Detail.o Examine.o Monitor.o dlink.o

all : mdctl mdctl.man

mdctl : $(OBJS)
	$(CC) -o mdctl $^

mdctl.man : mdctl.8
	nroff -man mdctl.8 > mdctl.man

$(OBJS) : mdctl.h

install : mdctl
	$(INSTALL) -m 755 mdctl $(DESTDIR)/sbin

clean : 
	rm -f mdctl $(OBJS) core mdctl.man

dist : clean
	./makedist

TAGS :
	etags *.h *.c
