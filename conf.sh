# Compile with stealing enable, pip enable, libfabric enable 
#./configure CC=gcc CXX=g++ MPICHLIB_CFLAGS="-DMPI_PIP_SHM_TASK_STEAL" LDFLAGS="-L$HOME/local/exp/pip/lib -Wl,-rpath=$HOME/local/exp/pip/lib" LIBS="-lnuma" --prefix=$HOME/ANL/installed/mpich-pip --with-device=ch4:ofi --with-libfabric=embedded --enable-ch4-netmod-inline=no --enable-ch4-shm-inline=no --with-pip-prefix=$HOME/local/exp/pip

# Compile with stealing enable, pip enable, ofi enable
./configure CC=gcc CXX=g++ MPICHLIB_CFLAGS="-DMPIDI_PIP_OFI_GET_ACC_STEALING -DMPIDI_PIP_OFI_ACC_STEALING -DMPIDI_PIP_SHM_ACC_STEALING -DMPIDI_PIP_STEALING_ENABLE -DENABLE_REVERSE_TASK_ENQUEUE -DENABLE_CONTIG_STEALING -DENABLE_NON_CONTIG_STEALING -DENABLE_OFI_STEALING -DENABLE_PARTNER_STEALING -DENABLE_DYNAMIC_CHUNK -L$HOME/local/exp/pip/lib -Wl,-rpath=$HOME/local/exp/pip/lib" --prefix=$HOME/ANL/installed/mpich-pip --with-device=ch4:ofi --enable-ch4-netmod-inline=no --enable-ch4-shm-inline=no --with-pip-prefix=$HOME/local/exp/pip --enable-g=log --with-ch4-shmmods=pip --with-pip-prefix=$HOME/local/exp/pip

# Compile with stealing disable, pip disable
#./configure CC=gcc CXX=g++ FC=gfortran LIBS="-lnuma" --prefix=$HOME/ANL/installed/mpich-pip --with-device=ch4:ofi --with-libfabric=embedded --enable-ch4-netmod-inline=no --enable-ch4-shm-inline=no

# Compile with stealing disable, pip disable, profile flags -g -O0
#./configure CC=gcc CXX=g++ MPICHLIB_CFLAGS="-g" LIBS="-lnuma" --prefix=$HOME/ANL/installed/mpich-pip --with-device=ch4:ofi --with-libfabric=embedded --enable-ch4-netmod-inline=no --enable-ch4-shm-inline=no --enable-fast=O0

# Compile with stealing disable, pip enable
#./configure CC=gcc CXX=g++ FC=gfortran LIBS="-lnuma" --prefix=$HOME/ANL/installed/mpich-pip --with-device=ch4:ofi --with-libfabric=embedded --enable-ch4-netmod-inline=no --enable-ch4-shm-inline=no --with-pip-prefix=$HOME/local/exp/pip
