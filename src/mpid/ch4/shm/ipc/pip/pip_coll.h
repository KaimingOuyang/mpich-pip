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
int MPIDI_PIP_Scatter_nway_tree_internode(const void **sendbuf, int sendcount,
                                          MPI_Datatype sendtype, void *recvbuf, int recvcount,
                                          MPI_Datatype recvtype, int root, MPIR_Comm * comm,
                                          MPIR_Errflag_t * errflag);
int MPIDI_PIP_Scatter_nway_tree_intranode(const void *sendbuf, int sendcount,
                                          MPI_Datatype sendtype, void *recvbuf, int recvcount,
                                          MPI_Datatype recvtype, int root, MPIR_Comm * comm,
                                          MPIR_Errflag_t * errflag);
int MPIDI_PIP_Bcast_intranode(void *buffer, int count, MPI_Datatype datatype, int root,
                              MPIR_Comm * comm, MPIR_Errflag_t * errflag);
int MPIDI_PIP_Gather_intranode(const void *sendbuf, int sendcount,
                               MPI_Datatype sendtype, void *recvbuf, int recvcount,
                               MPI_Datatype recvtype, int root, MPIR_Comm * comm,
                               MPIR_Errflag_t * errflag);
                               
/* allgather algorithm interface */
int MPIDI_PIP_Allgather_bruck_internode(const void *sendbuf, int sendcount,
                                        MPI_Datatype sendtype, void *recvbuf, int recvcount,
                                        MPI_Datatype recvtype, MPIR_Comm * comm,
                                        MPIR_Errflag_t * errflag);
int MPIDI_PIP_Allgather_ring_internode(const void *sendbuf, int sendcount,
                                       MPI_Datatype sendtype, void *recvbuf, int recvcount,
                                       MPI_Datatype recvtype, MPIR_Comm * comm,
                                       MPIR_Errflag_t * errflag);
int MPIDI_PIP_Allgatherv_ring_internode(const void *sendbuf, int sendcount, MPI_Datatype sendtype,
                                        void *recvbuf, const int *recvcounts, const int *displs,
                                        MPI_Datatype recvtype, MPIR_Comm * comm,
                                        MPIR_Errflag_t * errflag);
int MPIDI_PIP_Allgather_impl(const void *sendbuf, int sendcount,
                             MPI_Datatype sendtype, void *recvbuf, int recvcount,
                             MPI_Datatype recvtype, MPIR_Comm * comm, MPIR_Errflag_t * errflag);
#endif /* PIP_COLL_H_INCLUDED */
