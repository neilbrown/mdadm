Summary: mdctl is used for controlling Linux md devices (aka RAID arrays)
Name: mdctl
Version: 0.5
Release: 1
Source0: http://www.cse.unsw.edu.au/~neilb/source/mdctl/mdctl-%{version}.tgz
URL: http://www.cse.unsw.edu.au/~neilb/source/mdctl/
Copyright: GPL
Group: Utilities/System
BuildRoot: /var/tmp/%{name}-root
Packager: Danilo Godec <danci@agenda.si>

%description 
mdctl is a single program that can be used to control Linux md devices. It
is intended to provide all the functionality of the mdtools and raidtools
but with a very different interface.

mdctl can perform all functions without a configuration file. There is the
option of using a configuration file, but not in the same way that raidtools
uses one.

raidtools uses a configuration file to describe how to create a RAID array,
and also uses this file partially to start a previously created RAID array. 
Further, raidtools requires the configuration file for such things as
stopping a raid array, which needs to know nothing about the array.


%prep
%setup -q -n mdctl

%build
make

%install
rm -rf $RPM_BUILD_ROOT
mkdir -p $RPM_BUILD_ROOT/sbin
install -m755 mdctl $RPM_BUILD_ROOT/sbin/

%clean
rm -rf $RPM_BUILD_ROOT

%files
%doc TODO testconfig testconfig2
/sbin/*

%changelog
* Tue Aug 07 2001 Danilo Godec <danci@agenda.si>
- initial RPM build
