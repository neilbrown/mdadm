# Release [mdadm-4.4](https://git.kernel.org/pub/scm/utils/mdadm/mdadm.git/log/?h=mdadm-4.4)

Features:
- Remobe custom bitmap file support from Yu Kuai.
- Custom device policies implementation from Mariusz Tkaczyk.
- Self encrypted drives (**SED**) support for IMSM metadata from Blazej Kucman.
- Support more than 4 disks for **IMSM** RAID10 from Mateusz Kusiak.
- Read **IMSM** license information from ACPI tables from Blazej Kucman.
- Support devnode in **--Incremental --remove** from Mariusz Tkaczyk.
- Printing **IMSM** license type in **--detail-platform** from Blazej Kucman.
- README.md from Mariusz Tkaczyk and Anna Sztukowska.

Fixes:
- Tests improvements from Xiao Ni and Kinga Stefaniuk.
- Mdmon's Checkpointing improvements from Mateusz Kusiak.
- Pass mdadm environment flags to systemd-env to enable tests from Mateusz Kusiak.
- Superblock 1.0 uuid printing fixes from Mariusz Tkaczyk.
- Find VMD bus manually if link is not available from Mariusz Tkaczyk.
- Unconditional devices count printing in --detail from Anna Sztukowska.
- Improve SIGTERM handling during reshape, from Mateusz Kusiak.
- **Monitor.c** renamed to **Mdmonitor.c** from Kinga Stefaniuk.
- Mdmonitor service documentation update from Mariusz Tkaczyk.
- Rework around writing to sysfs files from Mariusz Tkaczyk.
- Drop of HOT_REMOVE_DISK ioctl in Manage in favour of sysfs from Mariusz Tkaczyk.
- Delegate disk removal to managemon from Mariusz Tkaczyk.
- Some clean-ups of legacy code and functionalities like **--auto=md** from Mariusz Tkaczyk.
- Manual clean-up, references to old kernels removed from Mariusz Tkaczyk.
- Various static code analysis fixes.

In this release we created github repository and allowed participation through
Github. It allowed us to use Github actions adn create CI. Currently, we have:
- Compilation tests with various gcc.
- **mdadm** tests.
- Checkpatch test.

# Release [mdadm-4.3](https://git.kernel.org/pub/scm/utils/mdadm/mdadm.git/log/?h=mdadm-4.3)

Features:
- **IMSM_NO_PLATFORM** boot parameter support from Neil Brown.
- **--write-zeros** option support by Logan Gunthorpe.
- **IMSM** monetization by VMD register from Mateusz Grzonka.
- RST SATA under VMD support from Kevin Friedberg.
- Strong name rules from Mariusz Tkaczyk.

Fixes:
- Unify failed raid behavior from Coly Li.
- Rework of **--update** options from Mateusz Kusiak.
- **mdmon-initrd** service from Neil Brown.
- **IMSM** expand functionality rework from Mariusz Tkaczyk.
- Mdmonitor improvements from Mateusz Grzonka.
- Failed state verification from Mateusz Kusiak and Kinga Tanska.

# Release [mdadm-4.2](https://git.kernel.org/pub/scm/utils/mdadm/mdadm.git/log/?h=mdadm-4.2)

The release includes more than two years of development and bugfixes, so it is difficult to
remember everything. Highlights include enhancements and bug fixes including for **IMSM** RAID,
Partial Parity Log, clustered RAID support, improved testing, and gcc-9 support.

# Release [mdadm-4.1](https://git.kernel.org/pub/scm/utils/mdadm/mdadm.git/log/?h=mdadm-4.1)

The update constitutes more than one year of enhancements and bug fixes including for **IMSM**
RAID, Partial Parity Log, clustered RAID support, improved testing, and gcc-8 support.

# Release [mdadm-4.0](https://git.kernel.org/pub/scm/utils/mdadm/mdadm.git/log/?h=mdadm-4.0)

The update in major version number primarily indicates this is a release by it's new maintainer.
In addition it contains a large number of fixes in particular for IMSM RAID and clustered RAID
support. In addition, this release includes support for IMSM 4k sector drives, failfast and better
documentation for journaled RAID.

This is my first release of mdadm. Please thank Neil Brown for his previous work as maintainer and
blame me for all the bugs I caused since taking over.

# Release [mdadm-3.4](https://git.kernel.org/pub/scm/utils/mdadm/mdadm.git/log/?h=mdadm-3.4)

- Support for journalled RAID5/6 and clustered RAID1. This new support is probably still buggy.
  Please report bugs.

- There are also a number of fixes for **IMSM** support and an assortment of minor bug fixes.

- I plan for this to be the last release of mdadm that I provide as I am retiring from MD and mdadm
  maintenance. Jes Sorensen has volunteered to oversee mdadm for the next while. Thanks Jes!

# Release [mdadm-3.3.4](https://git.kernel.org/pub/scm/utils/mdadm/mdadm.git/log/?h=mdadm-3.3.4)

**I strongly recommend upgrading to 3.3.4 if you are using 3.3 or later with IMSM.**

- **IMSM** metadata assemble fixes.

  In mdadm-3.3 a change was made to how **IMSM** metadata was handled. Previously an **IMSM** array
  would only be assembled if it was attached to an **IMSM** controller. In 3.3 this was relaxed as
  there are circumstances where the controller is not properly detected. Unfortunately, this has
  negative consequences which have only just come to light.

  If you have an IMSM RAID1 configured and then disable RAID in the BIOS, the metadata will remain
  on the devices. If you then install some other OS on one device and then install Linux on the
  other, Linux might eventually start noticing the IMSM metadata (depending a bit on whether
  mdadm is included in the initramfs) and might start up the RAID1. This could copy one device over
  the other, thus trashing one of the installations.

  So, with this release IMSM arrays will only be assembled if attached to an **IMSM** controller,
  or if **--force** is given to **--assemble**, or if the environment variable
  **IMSM_NO_PLATFORM=1** is set (used primarily for testing).

# Release [mdadm-3.3.3](https://git.kernel.org/pub/scm/utils/mdadm/mdadm.git/log/?h=mdadm-3.3.3)

- The 100 changes since 3.3.3 are mostly little bugfixes and some improvements to the self-tests.
- raid6check now handle all RAID6 layouts including **DDF** correctly. See git log for the rest.

# Release [mdadm-3.3.2](https://git.kernel.org/pub/scm/utils/mdadm/mdadm.git/log/?h=mdadm-3.3.2)

- Little bugfixes and some man-page updates.

# Release [mdadm-3.3.1](https://git.kernel.org/pub/scm/utils/mdadm/mdadm.git/log/?h=mdadm-3.3.1)

- lots of work on **DDF** support.
- Improved interactions with **systemd**. Where possible, background tasks are run from systemd
  rather than forking.
- Number of other little bug fixes too.

# Release [mdadm-3.3](https://git.kernel.org/pub/scm/utils/mdadm/mdadm.git/log/?h=mdadm-3.3)

- Some array reshapes can proceed without needing backup file. This is done by changing the
  data_offset* so we never need to write any data back over where it was before. If there is no
  'head space' or 'tail space' to allow *data_offset* to change, the old mechanism with a backup
  file can still be used.

- RAID10 arrays can be reshaped to change the number of devices, change the chunk size, or change
  the layout between *near* and *offset*.
  This will always change *data_offset*, and will fail if there is no room for *data_offset* to be
  moved.

- **--assemble --update=metadata** can convert a **0.90** array to a **1.0** array.

- **bad-block-logs** are supported (but not heavily tested yet).

- **--assemble --update=revert-reshape** can be used to undo a reshape that has just been started
  but isn't really wanted. This is very new and while it passes basic tests it cannot be
  guaranteed.

- improved locking between **--incremental** and **--assemble**.

- uses systemd to run **mdmon** if systemd is configured to do that.
- kernel names of md devices can be non-numeric. e.g. "md_home" rather than
  "md0". This will probably confuse lots of other tools, so you need to
  **echo CREATE names=yes >> /etc/mdadm.conf** or the feature will not be used (you also need a
  reasonably new kernel).

- **--stop** can be given a kernel name instead of a device name. i.e. **mdadm --stop md4** will
  work even if /dev/md4 doesn't exist.

- **--detail --export** has some information about the devices in the array.
- **--dump** and **--restore** can be used to backup and restore the metadata on an array.
- Hot-replace is supported with **mdadm /dev/mdX --replace /dev/foo** and
  **mdadm /dev/mdX --replace /dev/foo --with /dev/bar**.

- Config file can be a directory in which case all "*.conf" files are read in lexical order.
  Default is to read **/etc/mdadm.conf** and then **/etc/mdadm.conf.d**. Thus
  **echo CREATE name=yes > /etc/mdadm.conf.d/names.conf** will also enable the use of named md
  devices.

- Lots of improvements to **DDF** support including adding support for RAID10 (thanks Martin Wilck).

# Release [mdadm-3.2.6](https://git.kernel.org/pub/scm/utils/mdadm/mdadm.git/log/?h=mdadm-3.2.6)

- There are no real stand-out fixes, just lots of little bits and pieces.

# Release [mdadm-3.2.5](https://git.kernel.org/pub/scm/utils/mdadm/mdadm.git/log/?h=mdadm-3.2.5)

- This release primarily fixes a serious regression in 3.2.4. This regression does *not* cause
  any risk to data. It simply means that adding a device with **--add** would sometime fail
  when it should not.
- The fix also includes a couple of minor fixes such as making the **--layout=preserve** option to
  **--grow** work again.

# Release [mdadm-3.2.4](https://git.kernel.org/pub/scm/utils/mdadm/mdadm.git/log/?h=mdadm-3.2.4)

 - **--offroot** argument to improve interactions between mdmon and initrd.
 - **--prefer** argument to select which */dev* names to display in some circumstances.
 - relax restrictions on when **--add** will be allowed.
 - Fix bug with adding write-intent-bitmap to active array.
 - Now defaults to */run/mdadm* for storing run-time files.

# Release [mdadm-3.2.3](https://git.kernel.org/pub/scm/utils/mdadm/mdadm.git/log/?h=mdadm-3.2.3)

- The largest single area of change is support for reshape of Intel IMSM arrays (OnLine Capacity
  Expansion and Level Migration).
- Among other fixes, this now has a better chance of surviving if a device fails during reshape.

# Release [mdadm-3.2.2](https://git.kernel.org/pub/scm/utils/mdadm/mdadm.git/log/?h=mdadm-3.2.2)

- reshaping IMSM (Intel metadata) arrays is no longer 'experimental', it should work properly and be
  largely compatible with IMSM drivers in other platforms.
- **--assume-clean** can be used with **--grow --size** to avoid resyncing the new part of the
  array. This is only support with very new kernels.
- RAID0 arrays can have chunksize which is not a power of 2. This has been supported in the kernel
  for a while but is only now supported by mdadm.

- A new tool **raid6check** is available, which can check a RAID6 array, or part of it and report
  which device is most inconsistent with the others if any stripe is inconsistent. This is still
  under development and does not have a man page yet. If anyone tries it out and has any questions
  or experience to report, they would be most welcome on linux-raid@vger.kernel.org.

# Release [mdadm-3.2.1](https://git.kernel.org/pub/scm/utils/mdadm/mdadm.git/log/?h=mdadm-3.2.1)

- Policy framework

  Policy can be expressed for moving spare devices between arrays, and for how to handle hot-plugged
  devices. This policy can be different for devices plugged in to different controllers etc. This,
  for example, allows a configuration where when a device is plugged in it is immediately included
  in an md array as a hot spare and possibly starts recovery immediately if an array is degraded.

- Some understanding of mbr and gpt paritition tables. This is primarily to support the new
  hot-plug support. If a device is plugged in and policy suggests it should have a partition table,
  the partition table will be copied from a suitably similar device, and then the partitions will
  hot-plug and can then be added to md arrays.

- **--incremental --remove** can remember where a device was removed from so if a device gets
  plugged back in the same place, special policy applies to it, allowing it to be included in an
  array even if a general hotplug will not be included.

- Enhanced reshape options, including growing a RAID0 by converting to RAID4, restriping, and
  converting back. Also convertions between RAID0 and RAID10 and between RAID1 and RAID10 are
  possible (with a suitably recent kernel).

- Spare migration for IMSM arrays. Spare migration can now work across 'containers' using
  non-native metadata and specifically Intel's IMSM arrays support spare migrations.

- OLCE and level migration for Intel IMSM arrays. OnLine Capacity Expansion and level migration
  (e.g. RAID0 -> RAID5) is supported for Intel Matrix Storage Manager arrays. This support is
  currently *experimental* for technical reasons. It can be enabled with
  **export MDADM_EXPERIMENTAL=1**.

- avoid including wayward devices.

  If you split a RAID1, mount the two halves as two separate degraded RAID1s, and then later bring
  the two back together, it is possible that the md metadata won't properly show that one must
  over-ride the other. Mdadm now does extra checking to detect this possibility and avoid
  potentially corrupting data.

- Remove any possible confusion between similar options. e.g. **--brief** and **--bitmap** were
  mapped to 'b' and mdadm wouldn't notice if one was used where the other was expected.

- Allow K,M,G suffixes on chunk sizes.

# Release [mdadm-3.2](https://git.kernel.org/pub/scm/utils/mdadm/mdadm.git/log/?h=mdadm-3.2)

- By far the most significant change in this release related to the management of reshaping arrays.
  This code has been substantially re-written so that it can work with **externally managed
  metadata** -Intel's IMSM in particular. We now support level migration and OnLine Capacity
  Expansion on these arrays.

- Various policy statements can be made in the *mdadm.conf* to guide the behavior of mdadm,
  particular with regards to how new devices are treated by **--incremental**. Depending on the
  *action* associated with a device (identified by its *path*) such need devices can be
  automatically re-added to and existing array that they previously fell out off, or automatically
  added as a spare if they appear to contain no data.

- mdadm now has a limited understanding of partition tables. This allows the policy framework to
  make decisions about partitioned devices as well.

- **--incremental --remove** can be told what **--path** the device was on, and this info will be
  recorded so that another device appearing at the same physical location can be preferentially
  added to the same array (provides the spare-same-slot action policy applied to the path).

- A new flags **--invalid-backup** flag is available in **--assemble** mode. This can be used to
  re-assemble an array which was stopping in the middle of a reshape, and for which the
  *backup file* is no longer available or is corrupted. The array may have some corruption in it
  at the point where reshape was up to, but at least the rest of the array will become available.

- Policy framework.
- Various internal restructuring - more is needed.

# Release [mdadm-3.1.5](https://git.kernel.org/pub/scm/utils/mdadm/mdadm.git/log/?h=mdadm-3.1.5)

- Fixes for **v1.x** metadata on big-endian machines.
- man page improvements.
- Improve **--detail --export** when run on partitions of an md array.
- Fix regression with removing *failed* or *detached* devices.
- Fixes for **--assemble --force** in various unusual cases.
- Allow **-Y** to mean **--export**. This was documented but not implemented.
- Various fixes for handling **ddf** metadata. This is now more reliable but could benefit from
  more interoperability testing.
- Correctly list subarrays of a container in **--detail** output.
- Improve checks on whether the requested number of devices is supported by the metadata, both for
  **--create** and **--grow**.
- Don't remove partitions from a device that is being included in an array until we are fully
  committed to including it.
- Allow **--assemble --update=no-bitmap** so an array with a corrupt bitmap can still be assembled.
- Don't allow **--add** to succeed if it looks like a **--re-add** is probably wanted, but cannot
  succeed. This avoids inadvertently turning devices into spares when an array is failed.

# Release [mdadm-3.1.4](https://git.kernel.org/pub/scm/utils/mdadm/mdadm.git/log/?h=mdadm-3.1.4)

Two fixes related to configs that aren't using udev:
- Don't remove md devices which 'standard' names on **--stop**.
- Allow dev_open to work on read-only */dev*.

And fixed regressions:
- Allow **--incremental** to add spares to an array.
- Accept **--no-degraded** as a deprecated option rather than throwing an error.
- Return correct success status when **--incremental** assembling a container which does not yet
  have enough devices.
- Don't link mdadm with pthreads, only mdmon needs it.
- Fix compiler warning due to bad use of snprintf.

# Release [mdadm-3.1.3](https://git.kernel.org/pub/scm/utils/mdadm/mdadm.git/log/?h=mdadm-3.1.3)

- mapfile now lives in a fixed location which default to */dev/.mdadm/map*, but can be changed at
  compile time. This location is chosen and most distros provide it during early boot and preserve
  it through. As long a */dev* exists and is writable, */dev/.mdadm* will be created. Other files
  communication with mdmon live here too. This fixes a bug reported by Debian and Gentoo users where
  udev would spin in early-boot.

- IMSM and DDF metadata will not be recognized on partitions as they should only be used on
  whole-disks.

- Various overflows causes by 2G drives have been addressed.

- A subarray of an IMSM contain can now be killed with **--kill-subarray**. Also, subarrays can be
  renamed with **--update-subarray --update=name**.

- **-If** (or **--incremental --fail**) can be used from udev to fail and remove from all arrays
  a device which has been unplugged from the system i.e. hot-unplug-support.

- **/dev/mdX --re-add missing** will look for any device that looks like it should be a member of
  */dev/mdX* but isn't and will automatically **--re-add** it.

- Now compile with *-Wextra* to get extra warnings.
- Lots of minor bug fixes, documentation improvements, etc.

# Release [mdadm-3.1.2](https://git.kernel.org/pub/scm/utils/mdadm/mdadm.git/log/?h=mdadm-3.1.2)

- The default metadata has change again (sorry about that). It is now **v1.2** and will hopefully
  stay that way. It turned out there with boot-block issues with **v1.1** which make it unsuitable
  for a default, though in many cases it is still suitable to use.

- Add *homehost* to the valid words for the **AUTO** config file line. When followed by *-all*,
  this causes mdadm to auto-assemble any array belonging to this host, but not auto-assemble
  anything else.

- VAR_RUN can be easily changed at compile time just like ALT_RUN. This gives distros more
  flexibility in how to manage the pid and sock files that mdmon needs.

- If mdadm.conf lists arrays which have inter-dependencies, the previously had to be listed in the
  "right" order. Now, any order should work.

- Fix some bugs with **--grow --chunksize=**.
- Stopping a container is not permitted when members are still active.
- Various mdmon fixes.
- Alway make bitmap 4K-aligned if at all possible.
- Fix **--force** assembly of **v1.x** arrays which are in the process of recovering.
- Add section on 'scrubbing' to 'md' man page.
- Various command-line-option parsing improvements.
- ... and lots of other bug fixes.

# Release [mdadm-3.1.1](https://git.kernel.org/pub/scm/utils/mdadm/mdadm.git/log/?h=mdadm-3.1.1)

- Multiple fixes for new **--grow** levels including fixes for serious data corruption
  problems.
- Change default metadata to **v1.1**.
- Change default chunk size to 512K.
- Change default bitmap chunk size to 64MB.
- When **--re-add** is used, don't fall back to **--add** as this can destroy data.

# Release [mdadm-3.1](https://git.kernel.org/pub/scm/utils/mdadm/mdadm.git/log/?h=mdadm-3.1)

- Support **--grow** to change the layout of RAID 4/5/6.
- Support **--grow** to change the chunk size of RAID 4/5/6.
- Support **--grow** to change level from RAID1 -> RAID5 -> RAID6 and back.
- Support **--grow** to reduce the number of devices in RAID 4/5/6.
- Support restart of these grow options which assembling an array which is partially grown.
- Assorted tests of this code, and of different RAID6 layouts.

# Release [mdadm-3.0.3](https://git.kernel.org/pub/scm/utils/mdadm/mdadm.git/log/?h=mdadm-3.0.3)

- Improvements for creating arrays giving just a name, like *foo*, rather than the full
  */dev/md/foo*.
- Improvements for assembling member arrays of containers.
- Improvements to test suite.
- Add option to change increment for *RebuildNN* messages reported by **--monitor**.
- Improvements to **mdmon** hand-over from initrd to final root.
- Handle merging of devices that have left an IMSM array and are being re-incorporated.
- Add missing space in **--detail --brief** output.

# Release [mdadm-3.0.2](https://git.kernel.org/pub/scm/utils/mdadm/mdadm.git/log/?h=mdadm-3.0.2)

- Fix crash when **homehost** is not set, as often happens in early boot.

# Release [mdadm-3.0.1](https://git.kernel.org/pub/scm/utils/mdadm/mdadm.git/log/?h=mdadm-3.0.1)

- Fix various segfaults.
- Fixed for **--examine** with containers.
- Lots of other little fixes.

# Release [mdadm-3.0](https://git.kernel.org/pub/scm/utils/mdadm/mdadm.git/log/?h=mdadm-3.0)

- Support for **externally managed metadata**, specifically DDF and IMSM.
- Depend on udev to create entries in */dev*, rather than creating them ourselves.
- Remove **--auto-update-home-hosts**.
- New config file line **auto**.
- New *ignore* and *any* options for **homehost**.
- Numerous bug fixes and minor enhancements.
