#!/bin/bash
./configure CC=gcc CXX=g++ MPICHLIB_CFLAGS="-g" --prefix=$HOME/ANL/stdmpich --with-device=ch4:ofi --enable-ch4-netmod-inline=no --enable-ch4-shm-inline=no --disable-fortran --enable-fast=-O0 --with-libfabric=embedded
