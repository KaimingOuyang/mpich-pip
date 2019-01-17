## -*- Mode: Makefile; -*-
## vim: set ft=automake :
##
## (C) 2016 by Argonne National Laboratory.
##     See COPYRIGHT in top-level directory.
##
##  Portions of this code were written by Intel Corporation.
##  Copyright (C) 2011-2016 Intel Corporation.  Intel provides this material
##  to Argonne National Laboratory subject to Software Grant and Corporate
##  Contributor License Agreement dated February 8, 2012.
##

if BUILD_SHM_POSIX

# if !AM_CPPFLAGS
# AM_CPPFLAGS = -I$(top_srcdir)/src/mpid/ch4/shm/xpmem/include
# else
AM_CPPFLAGS += -I$(top_srcdir)/src/mpid/ch4/shm/xpmem/include
# endif


noinst_HEADERS += src/mpid/ch4/shm/xpmem/xpmem_send.h 	\
				src/mpid/ch4/shm/xpmem/xpmem_recv.h 	\
				src/mpid/ch4/shm/xpmem/xpmem_progress.h \
				src/mpid/ch4/shm/xpmem/xpmem_inline.h
				

#				src/mpid/ch4/shm/xpmem/xpmem_rma.h 		\
# if !AM_LDFLAGS
# LDFLAGS += src/mpid/ch4/shm/xpmem/lib/libxpmem.a
# else
# AM_LDFLAGS += src/mpid/ch4/shm/xpmem/lib/libxpmem.a
# endif 

endif
