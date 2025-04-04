#!/bin/bash

# We test mdadm on loop-back block devices.
# dir for storing files should be settable by command line maybe
size=20000
# super0, round down to multiple of 64 and substract 64
mdsize0=19904
# super00 is nested, subtract 128
mdsize00=19840
# super1.0 round down to multiple of 2, subtract 8
mdsize1=19992
mdsize1a=19988
mdsize12=19988
# super1.2 for linear: round to multiple of 2, subtract 4
mdsize1_l=19996
mdsize2_l=19996
# subtract another 4 for bitmaps
mdsize1b=19988
mdsize11=19992
mdsize11a=19456
mdsize12=19988

# ddf needs bigger devices as 32Meg is reserved!
ddfsize=65536

# Systemd flags
devname_as_serial_flag="IMSM_DEVNAME_AS_SERIAL=1"
no_platform_flag="IMSM_NO_PLATFORM=1"

# Common colors
COLOR_FAIL='\033[0;31m' #RED
COLOR_WARN='\033[1;33m' #YELLOW
COLOR_SUCCESS='\033[0;32m' #GREEN
COLOR_NONE='\033[0m'

fail() {
	printf "${COLOR_FAIL}$1${COLOR_NONE}"
}

warn() {
	printf "${COLOR_WARN}$1${COLOR_NONE}"
}

succeed() {
	printf "${COLOR_SUCCESS}$1${COLOR_NONE}"
}

# $1 is optional parameter, it shows why to save log
save_log() {
	status=$1
	logfile="$status""$_basename".log

	cat $targetdir/stderr >> $targetdir/log
	cp $targetdir/log $logdir/$_basename.log
	echo "## $HOSTNAME: saving dmesg." >> $logdir/$logfile
	dmesg -c >> $logdir/$logfile
	echo "## $HOSTNAME: saving proc mdstat." >> $logdir/$logfile
	cat /proc/mdstat >> $logdir/$logfile
	array=($(mdadm -Ds | cut -d' ' -f2))
	[ "$1" == "fail" ] &&
		fail "FAILED"
		echo " - see $logdir/$_basename.log and $logdir/$logfile for details"
	if [ $DEVTYPE == 'lvm' ]
	then
		# not supported lvm type yet
		echo
	elif [ "$DEVTYPE" == 'loop' -o "$DEVTYPE" == 'disk' ]
	then
		if [ ! -z "$array" -a ${#array[@]} -ge 1 ]
		then
			echo "## $HOSTNAME: mdadm -D ${array[@]}" >> $logdir/$logfile
			$mdadm -D ${array[@]} >> $logdir/$logfile
			# ignore saving external(external file, imsm...) bitmap
			cat /proc/mdstat | grep -q "linear\|external" && return 0
			md_disks=($($mdadm -D -Y ${array[@]} | grep "/dev/" | cut -d'=' -f2))
			cat /proc/mdstat | grep -q "bitmap"
			if [ $? -eq 0 ]
			then
				echo "## $HOSTNAME: mdadm -X ${md_disks[@]}" >> $logdir/$logfile
				$mdadm -X ${md_disks[@]} >> $logdir/$logfile
				echo "## $HOSTNAME: mdadm -E ${md_disks[@]}" >> $logdir/$logfile
				$mdadm -E ${md_disks[@]} >> $logdir/$logfile
			fi
		else
			echo "## $HOSTNAME: no array assembled!" >> $logdir/$logfile
		fi
	fi
}

cleanup() {
	udevadm settle
	$mdadm -Ssq 2> /dev/null
	case $DEVTYPE in
	loop )
		for d in 0 1 2 3 4 5 6 7  8 9 10 11 12 13
		do
			losetup -d /dev/loop$d &> /dev/null
			rm -f /dev/disk/by-path/loop*
			rm -f /var/tmp/mdtest$d
		done
	;;
	lvm )
		for d in 0 1 2 3 4 5 6 7  8 9 10 11 12 13
		do
			eval "lvremove --quiet -f \$dev$d"
		done
	;;
	disk )
		$mdadm --zero ${disks[@]} &> /dev/null
	;;
	esac
	clean_systemd_env
}

do_clean()
{
	mdadm -Ss > /dev/null
	mdadm --zero $devlist 2> /dev/null
	dmesg -c > /dev/null
}

check_env() {
	user=$(id -un)
	[ "X$user" != "Xroot" ] && {
		echo "test: testing can only be done as 'root'."
		exit 1
	}
	[ \! -x $mdadm ] && {
		echo "test: please run make everything before perform testing."
		exit 1
	}
	cmds=(mdadm lsblk df udevadm losetup mkfs.ext3 fsck seq)
	for cmd in ${cmds[@]}
	do
		which $cmd > /dev/null || {
			echo "$cmd command not found!"
			exit 1
		}
	done
	if $(lsblk -a | grep -iq raid)
	then
		# donot run mdadm -Ss directly if there are RAIDs working.
		echo "test: please run test suite without running RAIDs environment."
		exit 1
	fi
	# Check whether to run multipath tests
	modprobe multipath 2> /dev/null
	grep -sq 'Personalities : .*multipath' /proc/mdstat &&
		MULTIPATH="yes"
	if [ "$MULTIPATH" != "yes" ]; then
		echo "test: skipping tests for multipath, which is removed in upstream 6.8+ kernels"
		skipping_multipath="yes"
	fi

	# Check whether to run linear tests
	modprobe linear 2> /dev/null
	grep -sq 'Personalities : .*linear' /proc/mdstat &&
		LINEAR="yes"
	if [ "$LINEAR" != "yes" ]; then
		echo "test: skipping tests for linear, which is removed in upstream 6.8+ kernels"
		skipping_linear="yes"
	fi
}

record_system_speed_limit() {
	system_speed_limit_max=`cat /proc/sys/dev/raid/speed_limit_max`
	system_speed_limit_min=`cat /proc/sys/dev/raid/speed_limit_min`
}

# To avoid sync action finishes before checking it, it needs to limit
# the sync speed
control_system_speed_limit() {
	echo $test_speed_limit_min > /proc/sys/dev/raid/speed_limit_min
	echo $test_speed_limit_max > /proc/sys/dev/raid/speed_limit_max
}

restore_system_speed_limit() {
	echo $system_speed_limit_min > /proc/sys/dev/raid/speed_limit_max
	echo $system_speed_limit_max > /proc/sys/dev/raid/speed_limit_max
}

is_raid_foreign() {

	name=$1
	# super1 uses this formula strlen(homehost)+1+strlen(name) < 32
	# to decide if an array is foreign or local. It adds homehost if
	# one array is local
	hostname=$(hostname)
	if [ `expr length "$(hostname):$name"` -lt 31 ]; then
		is_foreign="no"
	else
		is_foreign="yes"
	fi
}

record_selinux() {
	sys_selinux=`getenforce`
	setenforce Permissive
}

restore_selinux() {
	setenforce $sys_selinux
}

wait_for_reshape_end() {
	# wait for grow-continue to finish but break if sync_action does not
	# contain any reshape value
	while true
	do
		sync_action=$(grep -Ec '(resync|recovery|reshape|check|repair) *=' /proc/mdstat)
		if (( "$sync_action" != 0 )); then
			sleep 2
			continue
		elif [[ $(pgrep -f "mdadm --grow --continue" > /dev/null) != "" ]]; then
			echo "Grow continue did not finish but reshape is done" >&2
			exit 1
		else
			break
		fi
	done
}

setup_systemd_env() {
	warn "Warning! Test suite will set up systemd environment!\n"
	echo "Use \"systemctl show-environment\" to show systemd environment variables"
	for env_var in $devname_as_serial_flag $no_platform_flag
	do
		systemctl set-environment $env_var
		echo "Added $env_var" to systemd environment, use \
		     \"systemctl unset-environment $env_var\" to remove it.
	done
}

clean_systemd_env() {
	for env_var in $devname_as_serial_flag $no_platform_flag
	do
		systemctl unset-environment $env_var
		echo "Removed $env_var from systemd environment."
	done
}

do_setup() {
	trap cleanup 0 1 3 15
	trap ctrl_c 2

	check_env
	setup_systemd_env
	[ -d $logdir ] || mkdir -p $logdir

	devlist=
	if [ "$DEVTYPE" == "loop" ]
	then
		# make sure there are no loop devices remaining.
		# udev started things can sometimes prevent them being stopped
		# immediately
		while grep loop /proc/partitions > /dev/null 2>&1
		do
			$mdadm -Ssq
			losetup -d /dev/loop[0-9]* 2> /dev/null
			sleep 0.2
		done
	elif [ "$DEVTYPE" == "disk" ]
	then
		if [ ! -z "$disks" ]
		then
			for d in $(seq 0 ${#disks[@]})
			do
				eval "dev$d=${disks[$d]}"
				eval devlist=\"\$devlist \$dev$d\"
				eval devlist$d=\"\$devlist\"
			done
			$mdadm --zero ${disks[@]} &> /dev/null
		else
			echo "Forget to provide physical devices for disk mode."
			exit 1
		fi
	fi
	for d in 0 1 2 3 4 5 6 7 8 9 10 11 12 13
	do
		sz=$size
		[ $d -gt 7 ] && sz=$ddfsize
		case $DEVTYPE in
		loop)
			[ -f $targetdir/mdtest$d ] ||
				dd if=/dev/zero of=$targetdir/mdtest$d count=$sz bs=1K > /dev/null 2>&1
			# make sure udev doesn't touch
			mdadm --zero $targetdir/mdtest$d 2> /dev/null
			if [ $d -eq 7 ]
			then
				losetup /dev/loop$d $targetdir/mdtest6 # for multipath use
			else
				losetup /dev/loop$d $targetdir/mdtest$d
			fi
			eval dev$d=/dev/loop$d
			eval file$d=$targetdir/mdtest$d
		;;
		lvm)
			unset MULTIPATH
			eval dev$d=/dev/mapper/${LVM_VOLGROUP}-mdtest$d
			if ! lvcreate --quiet -L ${sz}K -n mdtest$d $LVM_VOLGROUP
			then
				trap '' 0 # make sure lvremove is not called
				eval echo error creating \$dev$d
				exit 129
			fi
		;;
		ram)
			unset MULTIPATH
			eval dev$d=/dev/ram$d
		;;
		esac
		eval devlist=\"\$devlist \$dev$d\"
		eval devlist$d=\"\$devlist\"
		#" <-- add this quote to un-confuse vim syntax highlighting
	done
	path0=$dev6
	path1=$dev7
	ulimit -c unlimited
	[ -f /proc/mdstat ] || modprobe md_mod
	echo 0 > /sys/module/md_mod/parameters/start_ro
	record_system_speed_limit
	record_selinux
}

# check various things
check() {
	case $1 in
	opposite_result )
		if [ $? -eq 0 ]; then
			die "This command shouldn't run successfully"
		fi
	;;
	spares )
		spares=$(tr '] ' '\012\012' < /proc/mdstat | grep -c '(S)' || exit 0)
		[ $spares -ne $2 ] &&
			die "expected $2 spares, found $spares"
	;;
	raid* | linear )
		grep -sq "active $1 " /proc/mdstat ||
			die "active $1 not found"
	;;
	algorithm )
		grep -sq " algorithm $2 " /proc/mdstat ||
			die "algorithm $2 not found"
	;;
	resync | recovery | reshape )
		cnt=5
		while ! grep -sq $1 /proc/mdstat
		do
			if [ $cnt -gt 0 ] && grep -v idle /sys/block/md*/md/sync_action > /dev/null
			then # Something isn't idle - wait a bit
				sleep 0.5
				cnt=$[cnt-1]
			else
				die "no $1 happening"
			fi
		done
	;;
	nosync )
		sleep 0.5
		# Since 4.2 we delay the close of recovery until there has been a chance for
		# spares to be activated.  That means that a recovery that finds nothing
		# to do can still take a little longer than expected.
		# add an extra check: is sync_completed shows the end is reached, assume
		# there is no recovery.
		if grep -sq -E '(resync|recovery|reshape) *=' /proc/mdstat
		then
			incomplete=`grep / /sys/block/md*/md/sync_completed 2> /dev/null | sed '/^ *\([0-9]*\) \/ \1/d'`
			[ -n "$incomplete" ] &&
				die "resync or recovery is happening!"
		fi
	;;
	wait )
		min=`cat /proc/sys/dev/raid/speed_limit_min`
		max=`cat /proc/sys/dev/raid/speed_limit_max`
		echo 200000 > /proc/sys/dev/raid/speed_limit_max
		sleep 0.1
		iterations=0
		# Wait 10 seconds for one of the actions appears in sync_action.
		while [ $iterations -le 10 ]
		do
			sync_action=$(grep -Ec '(resync|recovery|reshape|check|repair) *=' /proc/mdstat)
			if (( "$sync_action" == 0 )); then
				sleep 2
				iterations=$(( $iterations + 1 ))
				continue
			else
				break
			fi
		done
		echo "Reshape has not started after 10 seconds"

		# Now let's wait for reshape to finish.
		echo "Waiting for grow-continue to finish"
		wait_for_reshape_end
		# If we have matrix-raid there's a second process ongoing
		sleep 5
		wait_for_reshape_end

		echo $min > /proc/sys/dev/raid/speed_limit_min
		echo $max > /proc/sys/dev/raid/speed_limit_max
	;;
	state )
		grep -sq "blocks.*\[$2\]\$" /proc/mdstat ||
			die "state $2 not found!"
		sleep 0.5
	;;
	bitmap )
		grep -sq bitmap /proc/mdstat ||
			die "no bitmap"
	;;
	nobitmap )
		grep -sq "bitmap" /proc/mdstat &&
			die "bitmap present"
	;;
	readonly )
		grep -sq "read-only" /proc/mdstat ||
			die "array is not read-only!"
	;;
	inactive )
		grep -sq "inactive" /proc/mdstat ||
			die "array is not inactive!"
	;;
	# It only can be used when there is only one raid
	chunk )
		chunk_size=`awk -F',' '/chunk/{print $2}' /proc/mdstat | awk -F'[a-z]' '{print $1}'`
		if [ "$chunk_size" -ne "$2" ] ; then
			die "chunksize should be $2, but it's $chunk_size"
		fi
	;;
	* )
		die "unknown check $1"
	;;
	esac
}

no_errors() {
	if [ -s $targetdir/stderr ]
	then
		echo Bad errors from mdadm:
		cat $targetdir/stderr
		exit 2
	fi
}

# basic device test
testdev() {
	lsblk -no name $1 || die "$1 isn't a block device."
	[ "$DEVTYPE" == "disk" ] && return 0
	udevadm settle
	dev=$1
	cnt=$2
	dvsize=$3
	chunk=$4
	if [ -z "$5" ]
	then
		mkfs.ext3 -F -j $dev > /dev/null 2>&1 && fsck -fn $dev >&2
	fi
	dsize=$[dvsize/chunk]
	dsize=$[dsize*chunk]
	rasize=$[dsize*2*cnt]
	# rasize is in sectors
	if [ -n "$DEV_ROUND_K" ]
	then
		rasize=$[rasize/DEV_ROUND_K/2]
		rasize=$[rasize*DEV_ROUND_K*2]
	fi
	[ `/sbin/blockdev --getsize $dev` -eq 0 ] && sleep 2
	_sz=`/sbin/blockdev --getsize $dev`
	[ $rasize -lt $_sz -o $[rasize*4/5] -gt $_sz ] &&
		die "size is wrong for $dev: $cnt * $dvsize (chunk=$chunk) = $rasize, not $_sz"
	return 0
}

rotest() {
	dev=$1
	fsck -fn $dev >&2
}
