**mdadm** is a utility used to create and manage **software RAID** devices implemented through
**Multiple devices driver (MD)** in kernel. It supports following RAID metadata formats:

* [Linux native RAID](https://raid.wiki.kernel.org/index.php/RAID_superblock_formats):

  Known as **native** or **native RAID**. First and default metadata format. Metadata management
  is implemented in **MD driver**.

* Matrix Storage Manager Support (no reference, metadata format documentation is proprietary).

  Known as **IMSM**. Metadata format developed and maintained by **Intel®** as a part of **VROC**
  solution. There are some functional differences between **native** and **imsm**. The most
  important difference is that the metadata is managed from userspace.

  **CAUTION:** **imsm** is compatible with **Intel RST**, however it is not officially supported.
  You are using it on your own risk.

* [Common RAID DDF Specification Revision](https://www.snia.org/sites/default/files/SNIA_DDF_Technical_Position_v2.0.pdf)

    **IMPORTANT:** DDF is in **maintenance only** mode. There is no active development around it.
    Please do not use it in new solutions.

# Questions and Support

This Github site is **not** right place to ask if your are looking for:
- support from Linux Raid Community;
- support with kernel issues;

This is the place where development of mdadm application is done. Please, do not use for
looking for support. You should always ask on [Mailing List](https://lore.kernel.org/linux-raid/).

Please use issues if you have confirmation that issue you are experiencing is related to mdadm
components:
- mdadm;
- mdmon;
- raid6check;
- swap_super;
- test_stripe;
- systemd services ( see systemd/);
- udev rules;
- manual pages (including md.man)

For example:
- mdadm issues (e.g segfaults, memory leaks, crashes, bad communication with MD driver);
- feature requests for mdadm;
- suggestions or minor fixes requested (e.g. better error messages);

Generally, if you are not sure it is better to ask on
[Mailing List](https://lore.kernel.org/linux-raid/) first.

# How to Contribute

Effective immediately [Github](https://github.com/md-raid-utilities/mdadm) is the primary
location for **mdadm**. Please use pull requests to contribute.

It was originally hosted on [kernel.org](https://kernel.org/). You can access the old repository
[here](https://git.kernel.org/pub/scm/utils/mdadm/mdadm.git).

While this is the preferred contribution method, mailing list submissions are still welcome and
will be handled as has always been the case for mdadm. Please add "mdadm:" to the subject to allow
automation to create Github Pull Request and run checks.

**NOTE:** Maintainers may ask you to send RFC to mailing list if the proposed code requires
consultation with kernel developers.

Kernel coding style is used. Please familiarize with general kernel
[submitting patches](https://www.kernel.org/doc/html/v4.17/process/submitting-patches.html)
documentation. Formatting, tags and commit message guidelines applies to **mdadm**.

[Checkpatch](https://docs.kernel.org/dev-tools/checkpatch.html) script is run on
every patch in pull request so be sure that your commits are not generating
issues. There are some excludes, so the best is to follow github checkpatch action result.

Pull Request are closed by `Rebase and Merge` option, so it requires to keep every commit
meaningful. Kernel style requires that. The review changes must be pushed with **push --force**
to the chosen branch, then Pull Request will be automatically updated.

# Maintainers of mdadm repository on kernel.org

See [Maintainers File](MAINTAINERS.md).

# Minimal supported kernel version

We do not support kernel versions below **v3.10**. Please be aware that maintainers may remove
workarounds and fixes for legacy issues.

# Dependencies

The following packages are required for compilation:

| RHEL | SLES | Debian/Ubuntu |
| :---: | :---: | :---: |
| `pkgconf` | `pkg-config` | `pkg-config` |
| `gcc` | `gcc` | `gcc` |
| `make` | `make` | `make` |
| `libudev-devel` | `libudev-devel` | `libudev-dev` |

# Compiling mdadm

Run `make` command to compile mdadm.

Specifying more jobs e.g. `make -j4` can decrease compilation time significantly.

Various values can be specified for the `CXFLAGS` variable to customize the build process:
- Run `make CXFLAGS=-ggdb` to include gdb debugging information.
- Run `make CXFLAGS=-DDEBUG` to enable additional debug information through dprintf statements
and call traces.
- Run `make CXFLAGS=-DNO_LIBUDEV` to compile without `libudev`.

To build with more than one option specified in `CXFLAGS`, separate each option with a space, e.g.
`make CXFLAGS="-ggdb -DDEBUG"`.

Additionally, the `EXTRAVERSION` variable can be set to build with user-friendly version label,
useful when customizing mdadm builds or labeling some instance in between major releases,
e.g. `make EXTRAVERSION="custom-label"`.

# Installing mdadm

Before installing mdadm, it is advised to uninstall vendor-provided packages (mdadm.deb, mdadm.rpm
etc.) in order to avoid configuration issues.

Run `make install` command to install mdadm. This command invokes the following targets:
- `install-bin`
- `install-man`
- `install-udev`

After installing mdadm, consider rebuilding initramfs to ensure the changes take effect.

List of installation targets:
- Run `make install-bin` to install the mdadm and mdmon binary files.
- Run `make install-systemd` to install the systemd services.
- Run `make install-udev` to install the udev rules.
- Run `make install-man` to install the manual pages (`mdadm.8`, `md.4`, `mdadm.conf.5`,
`mdmon.8`).

The following targets are deprecated and should not be used:
- `install-static`
- `install-tcc`
- `install-uclibc`
- `install-klibc`

# License

It is released under the terms of the **GNU General Public License version 2** as published
by the **Free Software Foundation**.
