Summary: mdadm is used for controlling Linux md devices (aka RAID arrays)
Name: mdadm
Version: 0.7.1
Release: 1
Source: http://www.cse.unsw.edu.au/~neilb/source/mdadm/mdadm-%{version}.tgz
URL: http://www.cse.unsw.edu.au/~neilb/source/mdadm/
Copyright: GPL
Group: Utilities/System
BuildRoot: /var/tmp/%{name}-root
Packager: Danilo Godec <danci@agenda.si>

%description 
mdadm is a single program that can be used to control Linux md devices. It
is intended to provide all the functionality of the mdtools and raidtools
but with a very different interface.

mdadm can perform all functions without a configuration file. There is the
option of using a configuration file, but not in the same way that raidtools
uses one.

raidtools uses a configuration file to describe how to create a RAID array,
and also uses this file partially to start a previously created RAID array. 
Further, raidtools requires the configuration file for such things as
stopping a raid array, which needs to know nothing about the array.


%prep
%setup -q

%build
# This is a debatable issue. The author of this RPM spec file feels that
# people who install RPMs (especially given that the default RPM options
# will strip the binary) are not going to be running gdb against the
# program.
make CFLAGS="$RPM_OPT_FLAGS"

%install
#rm -rf $RPM_BUILD_ROOT
mkdir -p $RPM_BUILD_ROOT/sbin
install -m755 mdadm $RPM_BUILD_ROOT/sbin/
mkdir -p $RPM_BUILD_ROOT/etc
install -m644 mdadm.conf-example $RPM_BUILD_ROOT/etc/mdadm.conf
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
%doc TODO ChangeLog mdadm.man mdadm.conf-example
/sbin/mdadm
/etc/mdadm.conf
%{_mandir}/man*/md*

%changelog
* Wed Mar 12 2002 NeilBrown <neilb@cse.unsw.edu.au>
- Add md.4 and mdadm.conf.5 man pages
* Fri Mar 08 2002		Chris Siebenmann <cks@cquest.utoronto.ca>
- builds properly as non-root.
* Fri Mar 08 2002 Derek Vadala <derek@cynicism.com>
- updated for 0.7, fixed /usr/share/doc and added manpage
* Tue Aug 07 2001 Danilo Godec <danci@agenda.si>
- initial RPM build
