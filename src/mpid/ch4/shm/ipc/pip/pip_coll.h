/*
 * Copyright (C) by Argonne National Laboratory
 *     See COPYRIGHT in top-level directory
 */
#ifndef PIP_COLL_H_INCLUDED
#define PIP_COLL_H_INCLUDED
int MPIDI_PIP_Scatter_nway_tree(const void *sendbuf, int sendcount,
                                MPI_Datatype sendtype, void *recvbuf, int recvcount,
                                MPI_Datatype recvtype, int root, MPIR_Comm * comm,
                                MPIR_Errflag_t * errflag);

#endif /* PIP_COLL_H_INCLUDED */
