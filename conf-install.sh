# Compile with stealing enable, pip enanle, libfabric enable 
#./configure CC=gcc CXX=g++ MPICHLIB_CFLAGS="-DMPI_PIP_SHM_TASK_STEAL" LDFLAGS="-L$HOME/local/exp/pip/lib -Wl,-rpath=$HOME/local/exp/pip/lib" LIBS="-lnuma" --prefix=$HOME/ANL/installed/mpich-pip --with-device=ch4:ofi --with-libfabric=embedded --enable-ch4-netmod-inline=no --enable-ch4-shm-inline=no --with-pip-prefix=$HOME/local/exp/pip --disable-fortran

# Compile with stealing disable, pip disable
#./configure CC=gcc CXX=g++ LIBS="-lnuma" --prefix=$HOME/ANL/installed/mpich-pip --with-device=ch4:ofi --with-libfabric=embedded --enable-ch4-netmod-inline=no --enable-ch4-shm-inline=no --disable-fortran

# Compile with stealing disable, pip disable, profile flags -g -O0
{
./configure CC=gcc CXX=g++ FC=gfortran MPICHLIB_CFLAGS="-g" LIBS="-lnuma" --prefix=$HOME/ANL/installed/mpich-pip-steal-chance --with-device=ch4:ofi --with-libfabric=embedded --enable-ch4-netmod-inline=no --enable-ch4-shm-inline=no --enable-fast=O0
make -j 4
make install
} | tee install.log


