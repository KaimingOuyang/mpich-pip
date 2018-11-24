#!/bin/bash
# turn on ch4
# link with xpmem

./configure CFLAGS="-I$HOME/lib/xpmem/include -I$HOME/lib/papi/include" LDFLAGS="-L$HOME/lib/papi/lib -lpapi -Wl,-rpath $HOME/lib/papi/lib -L$HOME/lib/xpmem/lib -lxpmem -Wl,-rpath $HOME/lib/xpmem/lib -Wl,-rpath $HOME/lib/pipglibc/lib -Wl,--dynamic-linker=$HOME/lib/pipglibc/lib/ld-2.17.so" --prefix=$HOME/ANL/xpmem/ --with-device=ch4:ofi --with-libfabric=$HOME/lib/libfabric --disable-fortran --enable-ch4-netmod-inline=no --enable-ch4-shm-inline=no
