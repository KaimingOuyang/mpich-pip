#!/bin/bash
# turn on ch4
# link with xpmem

./configure CFLAGS="-I$HOME/lib/xpmem/include -I$HOME/lib/papi/include" LDFLAGS="-L$HOME/lib/papi/lib -lpapi -Wl,-rpath $HOME/lib/papi/lib -L$HOME/lib/xpmem/lib -lxpmem -Wl,-rpath $HOME/lib/xpmem/lib" --prefix=$HOME/ANL/xpmem/ --with-device=ch4:ofi --with-libfabric=$HOME/lib/libfabric --disable-fortran --enable-ch4-netmod-inline=no --enable-ch4-shm-inline=no
