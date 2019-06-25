#./configure CC=gcc CXX=g++ --prefix=$HOME/ANL/req-pool --with-device=ch4:ofi --with-libfabric=embedded --enable-ch4-netmod-inline=no --enable-ch4-shm-inline=no --disable-fortran
#./configure CC=gcc CXX=g++ MPICHLIB_CFLAGS="-g" --prefix=$HOME/ANL/pip-steal --with-device=ch4:ofi --enable-ch4-netmod-inline=no --enable-ch4-shm-inline=no --disable-fortran --enable-fast=-O0 --with-libfabric=embedded
./configure CC=gcc CXX=g++ MPICHLIB_CFLAGS="-g -I$HOME/lib/papi/include" LDFLAGS="-L$HOME/lib/papi/lib -Wl,-rpath=$HOME/lib/papi/lib -lpapi" --prefix=$HOME/ANL/pip-steal --with-device=ch4:ofi --enable-ch4-netmod-inline=no --enable-ch4-shm-inline=no --disable-fortran --enable-fast=-O0 --with-libfabric=embedded
#./configure CC=gcc CXX=g++  MPICHLIB_CFLAGS="-ldl-2.23 -Wl,--dynamic-linker=/lib/x86_64-linux-gnu/ld-2.23.so -g -DMPI_PIP_SHM_TASK_STEAL -L$HOME/lib/pip/lib -Wl,-rpath=$HOME/lib/pip/lib" --prefix=$HOME/ANL/pip-steal --with-device=ch4:ofi --enable-ch4-netmod-inline=no --enable-ch4-shm-inline=no --disable-fortran --enable-fast=-O0 --with-libfabric=embedded --with-pip-prefix=$HOME/lib/pip
#./configure CC=gcc CXX=g++ MPICHLIB_CFLAGS="-g -L$HOME/lib/xpmem/lib -I$HOME/lib/xpmem/include -lxpmem -Wl,-rpath=$HOME/lib/xpmem/lib" --prefix=$HOME/ANL/req-pool-xpmem --with-device=ch4:ofi --with-libfabric=embedded --enable-ch4-netmod-inline=no --enable-ch4-shm-inline=no --disable-fortran --enable-fast=-O0
#./configure CC=gcc CXX=g++ MPICHLIB_CFLAGS="-DNO_PIP_REDUCE_LOCAL -DSHMEM_MODULE_PIP" LDFLAGS="-Wl,--dynamic-linker=$HOME/lib/glibc/lib/ld-2.17.so -L$HOME/lib/pip/lib -Wl,-rpath $HOME/lib/pip/lib -lnuma" --prefix=$HOME/ANL/pip --with-device=ch4:ofi --with-libfabric=embedded --enable-ch4-netmod-inline=no --enable-ch4-shm-inline=no --with-pip-prefix=$HOME/lib/pip --disable-fortran
