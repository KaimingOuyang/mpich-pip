./configure CFLAGS="-I$HOME/lib/papi/include" LDFLAGS="-L$HOME/lib/papi/lib -lpapi -Wl,-rpath $HOME/lib/papi/lib -L$HOME/lib/pip/lib -Wl,--dynamic-linker $HOME/lib/pipglibc/lib/ld-2.17.so -Wl,-rpath $HOME/lib/pip/lib" --prefix=$HOME/ANL/pip --with-device=ch4:ofi --with-libfabric=$HOME/lib/libfabric --disable-fortran --enable-ch4-netmod-inline=no --enable-ch4-shm-inline=no --with-pip-prefix=$HOME/lib/pip