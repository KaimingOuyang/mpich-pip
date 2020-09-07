/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil ; -*- */
/*
 *  (C) 2019 by Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#ifndef PIP_NOINLINE_H_INCLUDED
#define PIP_NOINLINE_H_INCLUDED

int MPIDI_PIP_mpi_init_hook(int rank, int size);
int MPIDI_PIP_mpi_finalize_hook(void);
int MPIDI_PIP_mpi_comm_create_hook(MPIR_Comm * comm);

#endif /* PIP_NOINLINE_H_INCLUDED */
