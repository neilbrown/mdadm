Summary:     mdadm is used for controlling Linux md devices (aka RAID arrays)
Name:        mdadm
Version:     0.7.2
Release:     1
Source:      http://www.cse.unsw.edu.au/~neilb/source/mdadm/mdadm-%{version}.tgz
URL:         http://www.cse.unsw.edu.au/~neilb/source/mdadm/
License:     GPL
Group:       Utilities/System
BuildRoot:   ${_tmppath}/%{name}-root
Packager:    Danilo Godec <danci@agenda.si> (et.al.)
Obsoletes:   mdctl

%description 
mdadm is a program that can be used to create, manage, and monitor
Linux MD (Software RAID) devices.
As such is provides similar functionality to the raidtools packages.
The particular differences to raidtools is that mdadm is a single
program, and it can perform (almost) all functions without a
configuration file (that a config file can be used to help with
some common tasks).

%prep
%setup -q

%build
# This is a debatable issue. The author of this RPM spec file feels that
# people who install RPMs (especially given that the default RPM options
# will strip the binary) are not going to be running gdb against the
# program.
make CFLAGS="$RPM_OPT_FLAGS" SYSCONFDIR="%{_sysconfdir}"

%install
#rm -rf $RPM_BUILD_ROOT
mkdir -p $RPM_BUILD_ROOT/%{_sbindir}
install -m755 mdadm $RPM_BUILD_ROOT/%{_sbindir}
mkdir -p $RPM_BUILD_ROOT/${_sysconfdir}
install -m644 mdadm.conf-example $RPM_BUILD_ROOT/%{_sysconfdir}/mdadm.conf
mkdir -p $RPM_BUILD_ROOT/%{_mandir}/man4
mkdir -p $RPM_BUILD_ROOT/%{_mandir}/man5
mkdir -p $RPM_BUILD_ROOT/%{_mandir}/man8
install -m644 md.4 $RPM_BUILD_ROOT/%{_mandir}/man4/
install -m644 mdadm.conf.5 $RPM_BUILD_ROOT/%{_mandir}/man5/
install -m644 mdadm.8 $RPM_BUILD_ROOT/%{_mandir}/man8/

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(-,root,root)
%doc TODO ChangeLog mdadm.man mdadm.conf-example COPYING
%{_sbindir}/mdadm
%config(noreplace,missingok)/%{_sysconfdir}/mdadm.conf
%{_mandir}/man*/md*

%changelog
* Fri Mar 15 2002  <gleblanc@localhost.localdomain>
- beautification
- made mdadm.conf non-replaceable config
- renamed Copyright to License in the header
- added missing license file
- used macros for file paths

* Fri Mar 15 2002 Luca Berra <bluca@comedia.it>
- Added Obsoletes: mdctl
- missingok for configfile

* Wed Mar 12 2002 NeilBrown <neilb@cse.unsw.edu.au>
- Add md.4 and mdadm.conf.5 man pages

* Fri Mar 08 2002		Chris Siebenmann <cks@cquest.utoronto.ca>
- builds properly as non-root.

* Fri Mar 08 2002 Derek Vadala <derek@cynicism.com>
- updated for 0.7, fixed /usr/share/doc and added manpage

* Tue Aug 07 2001 Danilo Godec <danci@agenda.si>
- initial RPM build
