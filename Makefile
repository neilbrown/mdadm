#
# mdadm - manage Linux "md" devices aka RAID arrays.
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
DESTDIR = /.
BINDIR  = /sbin
MANDIR  = /usr/share/man/man8

OBJS =  mdadm.o config.o  ReadMe.o util.o Manage.o Assemble.o Build.o Create.o Detail.o Examine.o Monitor.o dlink.o Kill.o

all : mdadm mdadm.man md.man mdadm.conf.man

mdadm : $(OBJS)
	$(CC) -o mdadm $^

mdadm.man : mdadm.8
	nroff -man mdadm.8 > mdadm.man

md.man : md.4
	nroff -man md.4 > md.man

mdadm.conf.man : mdadm.conf.5
	nroff -man mdadm.conf.5 > mdadm.conf.man

$(OBJS) : mdadm.h

install : mdadm mdadm.8
	$(INSTALL) -m 755 mdadm $(DESTDIR)/$(BINDIR)
	$(INSTALL) -m 644 mdadm.8 $(DESTDIR)/$(MANDIR)

clean : 
	rm -f mdadm $(OBJS) core mdadm.man

dist : clean
	./makedist

TAGS :
	etags *.h *.c
