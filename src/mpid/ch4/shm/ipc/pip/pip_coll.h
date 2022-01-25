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
int MPIDI_PIP_partial_bcast_intranode(void *buffer, int count, MPI_Datatype datatype, int root,
                                      MPIR_Comm * comm, MPIR_Errflag_t * errflag);
// int MPIDI_PIP_Reduce_bcast_intranode(void *buffer, int count, MPI_Datatype datatype, int root,
//                                      MPIR_Comm * pcomm, MPIR_Errflag_t * errflag);
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
/* allreduce algorithm interface */
int MPIDI_PIP_Allreduce_impl(const void *sendbuf, void *recvbuf, int count,
                             MPI_Datatype datatype, MPI_Op op, MPIR_Comm * comm,
                             MPIR_Errflag_t * errflag);
int MPIDI_PIP_Allreduce_reduce_scatter_internode(const void *sendbuf, void *recvbuf, int count,
                                                 MPI_Datatype datatype, MPI_Op op, MPIR_Comm * comm,
                                                 MPIR_Errflag_t * errflag);
int MPIDI_PIP_Allreduce_recursive_bruck_internode(const void *sendbuf, void *recvbuf, int count,
                                                  MPI_Datatype datatype, MPI_Op op,
                                                  MPIR_Comm * comm, int comm_size,
                                                  MPIR_Errflag_t * errflag);
/* reduce algorithm interface */
int MPIDI_PIP_Reduce_recursive_bruck_internode(const void *sendbuf, void *recvbuf, int count,
                                               MPI_Datatype datatype, MPI_Op op,
                                               MPIR_Comm * comm, int rem_step,
                                               MPIR_Errflag_t * errflag);
int MPIR_Reduce_leader_intra_binomial_tree(const void *sendbuf,
                                           void *recvbuf,
                                           int count,
                                           MPI_Datatype datatype, int leader_num,
                                           MPI_Op op, int root, MPIR_Comm * comm_ptr,
                                           MPIR_Errflag_t * errflag);
int MPIR_Reduce_leader_rem_intra_binomial_tree(const void *sendbuf,
                                               void *recvbuf,
                                               int count,
                                               MPI_Datatype datatype, int leader_num,
                                               MPI_Op op, int root, MPIR_Comm * comm_ptr,
                                               MPIR_Errflag_t * errflag);
int MPIDI_PIP_intranode_reduce(const void *sendbuf, void *recvbuf, int count, MPI_Datatype datatype,
                               MPI_Op op, int root, MPIR_Comm * comm, MPIR_Errflag_t * errflag);
// int MPIDI_PIP_Reduce_partial_intranode(const void *sendbuf, void *recvbuf, int count,
//                                        MPI_Datatype datatype, MPI_Op op, int root, MPIR_Comm * comm,
//                                        MPIR_Errflag_t * errflag);
// int MPIDI_PIP_Reduce_binomial_intranode(const void *sendbuf, void *recvbuf, int count,
//                                         MPI_Datatype datatype, MPI_Op op, int root,
//                                         MPIR_Comm * comm, MPIR_Errflag_t * errflag);
// int MPIDI_PIP_Reduce_leader_binomial_intranode(const void *sendbuf, void *recvbuf, int count,
//                                                MPI_Datatype datatype, MPI_Op op, int root,
//                                                int comm_size, int local_rank, int round,
//                                                MPIR_Comm * comm, MPIR_Errflag_t * errflag);
// int MPIDI_PIP_Reduce_leader_rem_intranode(const void *sendbuf, void *recvbuf, void *rem_buf,
//                                           int rem, int count, MPI_Datatype datatype, MPI_Op op,
//                                           int root, int comm_size, int local_rank, int round,
//                                           MPIR_Comm * comm, MPIR_Errflag_t * errflag);
#endif /* PIP_COLL_H_INCLUDED */
