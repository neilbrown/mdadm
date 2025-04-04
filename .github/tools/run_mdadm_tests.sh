#!/usr/bin/bash

sudo make clean
sudo make -j$(nproc)
sudo make install
sudo mdadm -Ss
sudo ./test setup

# Uncomment and adjust this to minimalize testing time for CI or test improvements.
# --tests=test1,test2,...     Comma separated list of tests to run

#sudo ./test --tests=00createnames

sudo ./test --skip-broken --no-error --disable-integrity --disable-multipath --disable-linear --keep-going --skip-bigcase

ret=$?
sudo ./test cleanup
exit $ret
