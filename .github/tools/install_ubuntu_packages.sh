#!/usr/bin/bash

VERSION_CODENAME=$(grep -oP '(?<=^VERSION_CODENAME=).+' /etc/os-release | tr -d '"')
echo "Detected VERSION_CODENAME: $VERSION_CODENAME"

# Add ubuntu repository
sudo add-apt-repository -y "deb [arch=amd64] http://archive.ubuntu.com/ubuntu $VERSION_CODENAME \
        main universe"
# Install gcc
sudo apt-get -y install gcc-$1 --no-upgrade --no-install-recommends --no-install-suggests
# Install dependencies
sudo apt-get -y install make gcc libudev-dev devscripts --no-upgrade --no-install-recommends --no-install-suggests
