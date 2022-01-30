#include "mpiimpl.h"
#include "pip_pre.h"
#include "pip_coll.h"
#include <math.h>

// int MPIDI_PIP_Reduce_binomial_intranode(const void *sendbuf, void *recvbuf,
//                                         int count, MPI_Datatype datatype, MPI_Op op,
//                                         int root, MPIR_Comm * comm, MPIR_Errflag_t * errflag)
// {
//     int mpi_errno = MPI_SUCCESS;
//     MPI_Status status;
//     int comm_size, local_rank;
//     int mask, relrank, source, lroot;
//     MPI_Aint true_lb, true_extent, extent;
//     int index = 0;
//     int round = *comm->round_ptr;
//     int free_flag = 0;
//     int recv_copied = 0;
//     volatile MPIDI_PIP_Coll_task_t *local_task;
//     MPIDI_PIP_Coll_task_t *volatile ***tcoll_queue_array = comm->tcoll_queue_array;

//     MPIR_CHKLMEM_DECL(2);

//     if (count == 0)
//         return MPI_SUCCESS;

//     /* FIXME: right now we assume root == 0 and don't consider non-commutative op */
//     MPIR_Assert(root == 0);
//     comm_size = comm->local_size;
//     local_rank = comm->rank;

//     /* Create a temporary buffer */

//     MPIR_Type_get_true_extent_impl(datatype, &true_lb, &true_extent);
//     MPIR_Datatype_get_extent_macro(datatype, extent);

//     if (local_rank != root && recvbuf == NULL) {
//         MPIR_CHKLMEM_MALLOC(recvbuf, void *, count * (MPL_MAX(extent, true_extent)),
//                             mpi_errno, "temporary buffer", MPL_MEM_BUFFER);

//         /* adjust for potential negative lower bound in datatype */
//         recvbuf = (void *) ((char *) recvbuf - true_lb);
//         free_flag = 1;
//     }

//     if (local_rank == root && sendbuf != MPI_IN_PLACE) {
//         mpi_errno = MPIR_Localcopy(sendbuf, count, datatype, recvbuf, count, datatype);
//         MPIR_ERR_CHECK(mpi_errno);
//         recv_copied = 1;
//     }

//     mask = 0x1;
//     relrank = local_rank;
//     while (mask < comm_size) {
//         /* Receive */
//         if ((mask & relrank) == 0) {
//             source = (relrank | mask);
//             if (source < comm_size) {
//                 while (tcoll_queue_array[source][round][index] == NULL)
//                     MPL_sched_yield();
//                 local_task = tcoll_queue_array[source][round][index];

//                 if (local_rank != root && recv_copied == 0) {
//                     mpi_errno = MPIR_Localcopy(sendbuf, count, datatype, recvbuf, count, datatype);
//                     MPIR_ERR_CHECK(mpi_errno);
//                     recv_copied = 1;
//                 }

//                 mpi_errno = MPIR_Reduce_local(local_task->addr, recvbuf, count, datatype, op);
//                 MPIR_ERR_CHECK(mpi_errno);

//                 sendbuf = recvbuf;
//                 __sync_fetch_and_add(&local_task->cnt, 1);
//             }
//         } else {
//             /* I've received all that I'm going to.  Send my result to
//              * my parent */
//             local_task = (MPIDI_PIP_Coll_task_t *) MPIR_Handle_obj_alloc(&MPIDI_Coll_task_mem);
//             local_task->addr = (void *) sendbuf;
//             local_task->cnt = 0;
//             local_task->type = TMPI_Reduce;
//             __sync_synchronize();
//             comm->tcoll_queue[round][index] = (MPIDI_PIP_Coll_task_t *) local_task;

//             while (local_task->cnt != 1)
//                 MPL_sched_yield();
//             if (free_flag)
//                 free(recvbuf);

//             MPIR_Handle_obj_free(&MPIDI_Coll_task_mem, (void *) local_task);
//             break;
//         }

//         mask <<= 1;
//     }

//     comm->tcoll_queue[round][index] = NULL;
//     *comm->round_ptr = round ^ 1;

//   fn_exit:
//     MPIR_CHKLMEM_FREEALL();
//     if (*errflag != MPIR_ERR_NONE)
//         MPIR_ERR_SET(mpi_errno, *errflag, "**coll_fail");
//     return mpi_errno;
//   fn_fail:
//     goto fn_exit;
// }

// int MPIDI_PIP_Reduce_partial_intranode(const void *sendbuf, void *recvbuf,
//                                        int count, MPI_Datatype datatype, MPI_Op op,
//                                        int root, MPIR_Comm * comm, MPIR_Errflag_t * errflag)
// {
//     int mpi_errno = MPI_SUCCESS;
//     MPI_Aint extent;
//     int index = 0;
//     int round = *comm->round_ptr;
//     int local_size = comm->local_size;
//     int local_rank = comm->rank;
//     int leader_num = comm->node_procs_min;
//     int scnt, ecnt, copy_cnt;
//     void *src_buf, *root_buf;
//     volatile MPIDI_PIP_Coll_task_t *local_task;
//     volatile MPIDI_PIP_Coll_task_t *root_task;
//     MPIDI_PIP_Coll_task_t *volatile ***tcoll_queue_array = comm->tcoll_queue_array;

//     if (local_size == leader_num) {
//         if (local_rank == root && sendbuf != MPI_IN_PLACE) {
//             mpi_errno = MPIR_Localcopy(sendbuf, count, datatype, recvbuf, count, datatype);
//             MPIR_ERR_CHECK(mpi_errno);
//         }
//         goto fn_exit;
//     }

//     MPIR_Assert(count >= local_size);
//     scnt = count * local_rank / local_size;
//     ecnt = count * (local_rank + 1) / local_size;
//     copy_cnt = ecnt - scnt;
//     MPIR_Datatype_get_extent_macro(datatype, extent);

//     if (local_rank == root) {
//         if (sendbuf != MPI_IN_PLACE) {
//             mpi_errno = MPIR_Localcopy(sendbuf, count, datatype, recvbuf, count, datatype);
//             MPIR_ERR_CHECK(mpi_errno);
//         }
//         local_task = (MPIDI_PIP_Coll_task_t *) MPIR_Handle_obj_alloc(&MPIDI_Coll_task_mem);
//         local_task->addr = (void *) recvbuf;
//         local_task->cnt = 0;
//         local_task->type = TMPI_Reduce;
//         __sync_synchronize();
//         comm->tcoll_queue[round][index] = (MPIDI_PIP_Coll_task_t *) local_task;
//     } else if (local_rank >= leader_num) {
//         local_task = (MPIDI_PIP_Coll_task_t *) MPIR_Handle_obj_alloc(&MPIDI_Coll_task_mem);
//         local_task->addr = (void *) sendbuf;
//         local_task->cnt = 0;
//         local_task->type = TMPI_Reduce;
//         __sync_synchronize();
//         comm->tcoll_queue[round][index] = (MPIDI_PIP_Coll_task_t *) local_task;
//     }

//     while (tcoll_queue_array[root][round][index] == NULL)
//         MPL_sched_yield();
//     root_task = tcoll_queue_array[root][round][index];

//     root_buf = root_task->addr;
//     for (int i = leader_num; i < local_size; ++i) {
//         while (tcoll_queue_array[i][round][index] == NULL)
//             MPL_sched_yield();
//         local_task = tcoll_queue_array[i][round][index];
//         src_buf = local_task->addr;
//         mpi_errno =
//             MPIR_Reduce_local((char *) src_buf + scnt * extent, (char *) root_buf + scnt * extent,
//                               copy_cnt, datatype, op);
//         MPIR_ERR_CHECK(mpi_errno);

//         __sync_fetch_and_add(&local_task->cnt, 1);
//     }

//     __sync_fetch_and_add(&root_task->cnt, 1);

//     if (local_rank == root || local_rank >= leader_num) {
//         local_task = comm->tcoll_queue[round][index];
//         while (local_task->cnt != local_size)
//             MPL_sched_yield();
//         MPIR_Handle_obj_free(&MPIDI_Coll_task_mem, (void *) local_task);
//     }

//     comm->tcoll_queue[round][index] = NULL;
//     *comm->round_ptr = round ^ 1;

//   fn_exit:
//     if (*errflag != MPIR_ERR_NONE)
//         MPIR_ERR_SET(mpi_errno, *errflag, "**coll_fail");
//     return mpi_errno;
//   fn_fail:
//     goto fn_exit;
// }

int MPIR_Reduce_leader_intra_binomial_tree(const void *sendbuf,
                                           void *recvbuf,
                                           int count,
                                           MPI_Datatype datatype, int leader_num,
                                           MPI_Op op, int root, MPIR_Comm * comm_ptr,
                                           MPIR_Errflag_t * errflag)
{
    int mpi_errno = MPI_SUCCESS;
    int mpi_errno_ret = MPI_SUCCESS;
    MPI_Status status;
    int comm_size, rank, is_commutative, type_size ATTRIBUTE((unused));
    int mask, relrank, source, lroot;
    MPI_Aint true_lb, true_extent, extent;
    void *tmp_buf, *recvbuf_tmp = NULL;
    MPIR_CHKLMEM_DECL(3);

    if (count == 0)
        return MPI_SUCCESS;

    comm_size = leader_num;
    rank = comm_ptr->rank;

    /* Create a temporary buffer */

    MPIR_Type_get_true_extent_impl(datatype, &true_lb, &true_extent);
    MPIR_Datatype_get_extent_macro(datatype, extent);

    is_commutative = MPIR_Op_is_commutative(op);

    MPIR_CHKLMEM_MALLOC(tmp_buf, void *, count * (MPL_MAX(extent, true_extent)),
                        mpi_errno, "temporary buffer", MPL_MEM_BUFFER);
    /* adjust for potential negative lower bound in datatype */
    tmp_buf = (void *) ((char *) tmp_buf - true_lb);

    /* If I'm not the root, then my recvbuf may not be valid, therefore
     * I have to allocate a temporary one */
    MPIR_Assert(recvbuf != NULL && sendbuf != MPI_IN_PLACE);

    if (sendbuf != MPI_IN_PLACE) {
        if (rank != root) {
            mpi_errno = MPIR_Localcopy(sendbuf, count, datatype, recvbuf, count, datatype);
            MPIR_ERR_CHECK(mpi_errno);
        } else {
            void *exchg_p;
            MPIR_CHKLMEM_MALLOC(recvbuf_tmp, void *, count * (MPL_MAX(extent, true_extent)),
                                mpi_errno, "temporary buffer", MPL_MEM_BUFFER);
            mpi_errno = MPIR_Localcopy(recvbuf, count, datatype, recvbuf_tmp, count, datatype);
            MPIR_ERR_CHECK(mpi_errno);

            mpi_errno = MPIR_Reduce_local(sendbuf, recvbuf_tmp, count, datatype, op);
            MPIR_ERR_CHECK(mpi_errno);

            exchg_p = recvbuf;
            recvbuf = recvbuf_tmp;
            recvbuf_tmp = exchg_p;
        }
    }

    MPIR_Datatype_get_size_macro(datatype, type_size);

    mask = 0x1;
    if (is_commutative)
        lroot = root;
    else
        lroot = 0;
    relrank = (rank - lroot + comm_size) % comm_size;

    while (/*(mask & relrank) == 0 && */ mask < comm_size) {
        /* Receive */
        if ((mask & relrank) == 0) {
            source = (relrank | mask);
            if (source < comm_size) {
                source = (source + lroot) % comm_size;
                mpi_errno = MPIC_Recv(tmp_buf, count, datatype, source,
                                      MPIR_REDUCE_TAG, comm_ptr, &status, errflag);
                if (mpi_errno) {
                    /* for communication errors, just record the error but continue */
                    *errflag =
                        MPIX_ERR_PROC_FAILED ==
                        MPIR_ERR_GET_CLASS(mpi_errno) ? MPIR_ERR_PROC_FAILED : MPIR_ERR_OTHER;
                    MPIR_ERR_SET(mpi_errno, *errflag, "**fail");
                    MPIR_ERR_ADD(mpi_errno_ret, mpi_errno);
                }

                /* The sender is above us, so the received buffer must be
                 * the second argument (in the noncommutative case). */
                if (is_commutative) {
                    mpi_errno = MPIR_Reduce_local(tmp_buf, recvbuf, count, datatype, op);
                    MPIR_ERR_CHECK(mpi_errno);
                } else {
                    mpi_errno = MPIR_Reduce_local(recvbuf, tmp_buf, count, datatype, op);
                    MPIR_ERR_CHECK(mpi_errno);

                    mpi_errno = MPIR_Localcopy(tmp_buf, count, datatype, recvbuf, count, datatype);
                    MPIR_ERR_CHECK(mpi_errno);
                }
            }
        } else {
            /* I've received all that I'm going to.  Send my result to
             * my parent */
            source = ((relrank & (~mask)) + lroot) % comm_size;
            mpi_errno = MPIC_Send(recvbuf, count, datatype,
                                  source, MPIR_REDUCE_TAG, comm_ptr, errflag);
            if (mpi_errno) {
                /* for communication errors, just record the error but continue */
                *errflag =
                    MPIX_ERR_PROC_FAILED ==
                    MPIR_ERR_GET_CLASS(mpi_errno) ? MPIR_ERR_PROC_FAILED : MPIR_ERR_OTHER;
                MPIR_ERR_SET(mpi_errno, *errflag, "**fail");
                MPIR_ERR_ADD(mpi_errno_ret, mpi_errno);
            }
            break;
        }
        mask <<= 1;
    }

    if (!is_commutative && (root != 0)) {
        if (rank == 0) {
            mpi_errno = MPIC_Send(recvbuf, count, datatype, root,
                                  MPIR_REDUCE_TAG, comm_ptr, errflag);
        } else if (rank == root) {
            mpi_errno = MPIC_Recv(recvbuf, count, datatype, 0,
                                  MPIR_REDUCE_TAG, comm_ptr, &status, errflag);
        }
        if (mpi_errno) {
            /* for communication errors, just record the error but continue */
            *errflag =
                MPIX_ERR_PROC_FAILED ==
                MPIR_ERR_GET_CLASS(mpi_errno) ? MPIR_ERR_PROC_FAILED : MPIR_ERR_OTHER;
            MPIR_ERR_SET(mpi_errno, *errflag, "**fail");
            MPIR_ERR_ADD(mpi_errno_ret, mpi_errno);
        }
    }

    if (rank == root && recvbuf_tmp) {
        mpi_errno = MPIR_Localcopy(recvbuf, count, datatype, recvbuf_tmp, count, datatype);
        MPIR_ERR_CHECK(mpi_errno);
    }

  fn_exit:
    MPIR_CHKLMEM_FREEALL();
    if (mpi_errno_ret)
        mpi_errno = mpi_errno_ret;
    else if (*errflag != MPIR_ERR_NONE)
        MPIR_ERR_SET(mpi_errno, *errflag, "**coll_fail");
    return mpi_errno;
  fn_fail:
    goto fn_exit;
}


int MPIR_Reduce_leader_rem_intra_binomial_tree(const void *sendbuf,
                                               void *recvbuf,
                                               int count,
                                               MPI_Datatype datatype, int leader_num,
                                               MPI_Op op, int root, MPIR_Comm * comm_ptr,
                                               MPIR_Errflag_t * errflag)
{
    int mpi_errno = MPI_SUCCESS;
    int mpi_errno_ret = MPI_SUCCESS;
    MPI_Status status;
    int comm_size, rank, is_commutative, type_size ATTRIBUTE((unused));
    int mask, relrank, source, lroot;
    MPI_Aint true_lb, true_extent, extent;
    void *tmp_buf, *recvbuf_tmp = NULL;
    MPIR_CHKLMEM_DECL(3);

    if (count == 0)
        return MPI_SUCCESS;

    comm_size = leader_num;
    rank = comm_ptr->rank;

    /* Create a temporary buffer */
    MPIR_Type_get_true_extent_impl(datatype, &true_lb, &true_extent);
    MPIR_Datatype_get_extent_macro(datatype, extent);

    is_commutative = MPIR_Op_is_commutative(op);

    MPIR_CHKLMEM_MALLOC(tmp_buf, void *, count * (MPL_MAX(extent, true_extent)),
                        mpi_errno, "temporary buffer", MPL_MEM_BUFFER);
    /* adjust for potential negative lower bound in datatype */
    tmp_buf = (void *) ((char *) tmp_buf - true_lb);

    /* If I'm not the root, then my recvbuf may not be valid, therefore
     * I have to allocate a temporary one */
    MPIR_Assert(recvbuf != NULL && sendbuf != MPI_IN_PLACE);

    if (sendbuf != MPI_IN_PLACE) {
        mpi_errno = MPIR_Localcopy(sendbuf, count, datatype, recvbuf, count, datatype);
        MPIR_ERR_CHECK(mpi_errno);
    }

    MPIR_Datatype_get_size_macro(datatype, type_size);

    mask = 0x1;
    if (is_commutative)
        lroot = root;
    else
        lroot = 0;
    relrank = (rank - lroot + comm_size) % comm_size;

    while (/*(mask & relrank) == 0 && */ mask < comm_size) {
        /* Receive */
        if ((mask & relrank) == 0) {
            source = (relrank | mask);
            if (source < comm_size) {
                source = (source + lroot) % comm_size;
                mpi_errno = MPIC_Recv(tmp_buf, count, datatype, source,
                                      MPIR_REDUCE_TAG, comm_ptr, &status, errflag);
                if (mpi_errno) {
                    /* for communication errors, just record the error but continue */
                    *errflag =
                        MPIX_ERR_PROC_FAILED ==
                        MPIR_ERR_GET_CLASS(mpi_errno) ? MPIR_ERR_PROC_FAILED : MPIR_ERR_OTHER;
                    MPIR_ERR_SET(mpi_errno, *errflag, "**fail");
                    MPIR_ERR_ADD(mpi_errno_ret, mpi_errno);
                }

                /* The sender is above us, so the received buffer must be
                 * the second argument (in the noncommutative case). */
                if (is_commutative) {
                    mpi_errno = MPIR_Reduce_local(tmp_buf, recvbuf, count, datatype, op);
                    MPIR_ERR_CHECK(mpi_errno);
                } else {
                    mpi_errno = MPIR_Reduce_local(recvbuf, tmp_buf, count, datatype, op);
                    MPIR_ERR_CHECK(mpi_errno);

                    mpi_errno = MPIR_Localcopy(tmp_buf, count, datatype, recvbuf, count, datatype);
                    MPIR_ERR_CHECK(mpi_errno);
                }
            }
        } else {
            /* I've received all that I'm going to.  Send my result to
             * my parent */
            source = ((relrank & (~mask)) + lroot) % comm_size;
            mpi_errno = MPIC_Send(recvbuf, count, datatype,
                                  source, MPIR_REDUCE_TAG, comm_ptr, errflag);
            if (mpi_errno) {
                /* for communication errors, just record the error but continue */
                *errflag =
                    MPIX_ERR_PROC_FAILED ==
                    MPIR_ERR_GET_CLASS(mpi_errno) ? MPIR_ERR_PROC_FAILED : MPIR_ERR_OTHER;
                MPIR_ERR_SET(mpi_errno, *errflag, "**fail");
                MPIR_ERR_ADD(mpi_errno_ret, mpi_errno);
            }
            break;
        }
        mask <<= 1;
    }

    if (!is_commutative && (root != 0)) {
        if (rank == 0) {
            mpi_errno = MPIC_Send(recvbuf, count, datatype, root,
                                  MPIR_REDUCE_TAG, comm_ptr, errflag);
        } else if (rank == root) {
            mpi_errno = MPIC_Recv(recvbuf, count, datatype, 0,
                                  MPIR_REDUCE_TAG, comm_ptr, &status, errflag);
        }
        if (mpi_errno) {
            /* for communication errors, just record the error but continue */
            *errflag =
                MPIX_ERR_PROC_FAILED ==
                MPIR_ERR_GET_CLASS(mpi_errno) ? MPIR_ERR_PROC_FAILED : MPIR_ERR_OTHER;
            MPIR_ERR_SET(mpi_errno, *errflag, "**fail");
            MPIR_ERR_ADD(mpi_errno_ret, mpi_errno);
        }
    }

  fn_exit:
    MPIR_CHKLMEM_FREEALL();
    if (mpi_errno_ret)
        mpi_errno = mpi_errno_ret;
    else if (*errflag != MPIR_ERR_NONE)
        MPIR_ERR_SET(mpi_errno, *errflag, "**coll_fail");
    return mpi_errno;
  fn_fail:
    goto fn_exit;
}


int MPIDI_PIP_Reduce_recursive_bruck_internode(const void *sendbuf, void *recvbuf, int count,
                                               MPI_Datatype datatype, MPI_Op op,
                                               MPIR_Comm * comm, int rem_step,
                                               MPIR_Errflag_t * errflag)
{
    int node_id = comm->node_id;
    int pofk_1 = 1;
    int local_rank = comm->local_rank;
    int basek_1 = comm->node_procs_min + 1;
    int rank, comm_size, rem_leader_num;
    int leader_num = comm->node_procs_min;
    int local_size = leader_num;
    int mpi_errno = MPI_SUCCESS;
    int mpi_errno_ret = MPI_SUCCESS;
    MPI_Aint recvtype_extent;
    int src, rem, src_node;
    int tmp_rem, my_rem = 0;
    void *tmp_buf = NULL, *dst_buf, *rem_buf = NULL;
    int dst, dst_node, offset, new_leader_num;
    int rem_step_limit = rem_step / basek_1;
    MPIDI_PIP_Coll_easy_task_t *shared_addr;
    int root_completee_step = 1, if_send, rem_proc, rem_count;

    MPIR_CHKLMEM_DECL(2);

    if ((count == 0) && (sendbuf != MPI_IN_PLACE))
        goto fn_exit;

    comm_size = comm->node_count;
    rank = comm->rank;
    MPIR_Datatype_get_extent_macro(datatype, recvtype_extent);

    /* keep first rem results for rem */
    pofk_1 = 1;
    while (pofk_1 <= rem_step_limit)
        pofk_1 *= basek_1;
    rem_count = (rem_step - pofk_1) % pofk_1;
    rem = rem_step - pofk_1;
    if_send = (rem_count == 0) ? 0 : 1;
    rem_proc = rem / pofk_1;
    new_leader_num = rem_proc + if_send;

    rem = rem - pofk_1 * local_rank;
    my_rem = rem > pofk_1 ? pofk_1 : rem;

    if (rem_count > 0) {
        MPIR_CHKLMEM_MALLOC(rem_buf, void *, count * recvtype_extent, mpi_errno, "rem_buf",
                        MPL_MEM_BUFFER);
    }

    if (sendbuf != MPI_IN_PLACE) {
        mpi_errno = MPIR_Localcopy(sendbuf, count, datatype, recvbuf, count, datatype);
        MPIR_ERR_CHECK(mpi_errno);
    }

    if (local_rank == 0) {
        shared_addr = MPIR_Comm_post_easy_task(recvbuf, TMPI_Allreduce, 0, 0, 0, comm);
        // rem_addr = MPIR_Comm_post_easy_task(rem_buf, TMPI_Rem, 0, 1, 1, comm);
    } else {
        shared_addr = MPIR_Comm_get_easy_task(comm, 0, TMPI_Allreduce);
        // rem_addr = MPIR_Comm_get_easy_task(comm, 0, TMPI_Rem);
    }
    dst_buf = shared_addr->addr;

    /* allocate a temporary buffer of the same size as recvbuf to receive intermediate results. */
    MPIR_Assert(recvbuf != NULL);
    MPIR_CHKLMEM_MALLOC(tmp_buf, void *, count * recvtype_extent, mpi_errno, "tmp_buf",
                        MPL_MEM_BUFFER);

    pofk_1 = 1;
    while (pofk_1 <= rem_step_limit) {
        offset = (local_rank + 1) * pofk_1;
        src_node = (node_id + offset) % comm_size;
        dst_node = (node_id - offset + comm_size) % comm_size;
        src = src_node * comm->node_procs_min + local_rank;
        dst = dst_node * comm->node_procs_min + local_rank;

        mpi_errno = MPIC_Sendrecv(dst_buf, count, datatype, dst,
                                  MPIR_ALLREDUCE_TAG, tmp_buf, count, datatype,
                                  src, MPIR_ALLREDUCE_TAG, comm, MPI_STATUS_IGNORE, errflag);
        if (mpi_errno) {
            /* for communication errors, just record the error but continue */
            *errflag =
                MPIX_ERR_PROC_FAILED ==
                MPIR_ERR_GET_CLASS(mpi_errno) ? MPIR_ERR_PROC_FAILED : MPIR_ERR_OTHER;
            MPIR_ERR_SET(mpi_errno, *errflag, "**fail");
            MPIR_ERR_ADD(mpi_errno_ret, mpi_errno);
        }

        mpi_errno =
            MPIR_Reduce_leader_intra_binomial_tree(tmp_buf, recvbuf, count, datatype,
                                                   comm->node_procs_min, op, 0,
                                                   comm->node_comm, errflag);
        MPIR_ERR_CHECK(mpi_errno);

        if (if_send && pofk_1 == 1 && local_rank < new_leader_num) {
            mpi_errno =
                MPIR_Reduce_leader_rem_intra_binomial_tree(tmp_buf, rem_buf, count, datatype,
                                                           new_leader_num, op, new_leader_num - 1,
                                                           comm->node_comm, errflag);
            MPIR_ERR_CHECK(mpi_errno);
        }

        pofk_1 *= basek_1;

        if (local_rank == 0) {
            __sync_synchronize();
            shared_addr->root_complete += 1;
        } else {
            while (shared_addr->root_complete != root_completee_step)
                MPL_sched_yield();
            root_completee_step++;
        }
    }

    /* if comm_size is not a power of k + 1, one more step is needed */
    if (my_rem > 0) {
        offset = (local_rank + 1) * pofk_1;
        src_node = (node_id + offset) % comm_size;
        dst_node = (node_id - offset + comm_size) % comm_size;
        src = src_node * comm->node_procs_min + local_rank;
        dst = dst_node * comm->node_procs_min + local_rank;

        if (my_rem == pofk_1) {
            mpi_errno = MPIC_Sendrecv(dst_buf, count, datatype,
                                      dst, MPIR_ALLREDUCE_TAG, tmp_buf, count, datatype,
                                      src, MPIR_ALLREDUCE_TAG, comm, MPI_STATUS_IGNORE, errflag);
        } else {
            MPIR_Assert(local_rank == new_leader_num - 1 && rem_buf != NULL);
            mpi_errno = MPIC_Sendrecv(rem_buf, count, datatype,
                                      dst, MPIR_ALLREDUCE_TAG, tmp_buf, count, datatype,
                                      src, MPIR_ALLREDUCE_TAG, comm, MPI_STATUS_IGNORE, errflag);
        }
        MPIR_ERR_CHECK(mpi_errno);

        mpi_errno =
            MPIR_Reduce_leader_intra_binomial_tree(tmp_buf, recvbuf, count, datatype,
                                                   new_leader_num, op, 0, comm->node_comm, errflag);
        MPIR_ERR_CHECK(mpi_errno);
    }

    /* bcast results */
    // if (leader_num > 1) {
    //     if (local_rank == 0) {
    //         __sync_synchronize();
    //         shared_addr->root_complete += 1;

    //         mpi_errno = MPIR_Localcopy(recvbuf, count, datatype, orig_recvbuf, count, datatype);
    //         MPIR_ERR_CHECK(mpi_errno);

    //     } else {
    //         while (shared_addr->root_complete != root_completee_step)
    //             MPL_sched_yield();

    //         mpi_errno = MPIR_Localcopy(dst_buf, count, datatype, recvbuf, count, datatype);
    //         MPIR_ERR_CHECK(mpi_errno);
    //         __sync_fetch_and_add(&shared_addr->complete, 1);
    //     }
    // }

  fn_exit:
    MPIR_CHKLMEM_FREEALL();
    if (mpi_errno_ret)
        mpi_errno = mpi_errno_ret;
    else if (*errflag != MPIR_ERR_NONE)
        MPIR_ERR_SET(mpi_errno, *errflag, "**coll_fail");
    return mpi_errno;

  fn_fail:
    goto fn_exit;
}

int MPIDI_PIP_intranode_reduce(const void *sendbuf, void *recvbuf,
                               int count, MPI_Datatype datatype, MPI_Op op,
                               int root, MPIR_Comm * comm, MPIR_Errflag_t * errflag)
{
    int mpi_errno = MPI_SUCCESS;
    MPI_Aint extent;
    int index = 0;
    int local_size = comm->local_size;
    int local_rank = comm->rank;
    int scnt, ecnt, copy_cnt;
    void *src_buf, *root_buf;
    MPIDI_PIP_Coll_easy_task_t *local_task;
    MPIDI_PIP_Coll_easy_task_t *root_task;

    MPIR_Assert(count >= local_size);
    scnt = count * local_rank / local_size;
    ecnt = count * (local_rank + 1) / local_size;
    copy_cnt = ecnt - scnt;
    MPIR_Datatype_get_extent_macro(datatype, extent);

    if (sendbuf != MPI_IN_PLACE) {
        mpi_errno = MPIR_Localcopy(sendbuf, count, datatype, recvbuf, count, datatype);
        MPIR_ERR_CHECK(mpi_errno);
    }

    local_task = MPIR_Comm_post_easy_task(recvbuf, TMPI_Reduce, 0, 0, local_size, comm);
    root_task = MPIR_Comm_get_easy_task(comm, 0, TMPI_Reduce);
    root_buf = root_task->addr;

    for (int i = 0; i < local_size; ++i) {
        if (i == root)
            continue;
        local_task = MPIR_Comm_get_easy_task(comm, i, TMPI_Reduce);
        src_buf = local_task->addr;
        mpi_errno =
            MPIR_Reduce_local((char *) src_buf + scnt * extent, (char *) root_buf + scnt * extent,
                              copy_cnt, datatype, op);
        MPIR_ERR_CHECK(mpi_errno);

        __sync_fetch_and_add(&local_task->complete, 1);
    }

    __sync_fetch_and_add(&root_task->complete, 1);
    while (root_task->complete != root_task->target_cmpl)
        MPL_sched_yield();

  fn_exit:
    if (*errflag != MPIR_ERR_NONE)
        MPIR_ERR_SET(mpi_errno, *errflag, "**coll_fail");
    return mpi_errno;
  fn_fail:
    goto fn_exit;
}

// int MPIDI_PIP_Reduce_leader_binomial_intranode(const void *sendbuf, void *recvbuf,
//                                                int count, MPI_Datatype datatype, MPI_Op op,
//                                                int root, int comm_size, int local_rank,
//                                                int round, MPIR_Comm * comm,
//                                                MPIR_Errflag_t * errflag)
// {
//     int mpi_errno = MPI_SUCCESS;
//     MPI_Status status;
//     int mask, relrank, source, lroot;
//     MPI_Aint true_lb, true_extent, extent;
//     int index = 0;
//     int free_flag = 0;
//     int recv_reduced = 0;
//     volatile MPIDI_PIP_Coll_task_t *local_task;

//     MPIR_CHKLMEM_DECL(2);

//     if (count == 0)
//         return MPI_SUCCESS;

//     if (local_rank == root && sendbuf != MPI_IN_PLACE && comm_size == 1) {
//         mpi_errno = MPIR_Reduce_local(sendbuf, recvbuf, count, datatype, op);
//         MPIR_ERR_CHECK(mpi_errno);
//         goto fn_exit;
//     }

//     /* FIXME: right now we assume root == 0 and don't consider non-commutative op */
//     MPIR_Assert(root == 0);

//     /* Create a temporary buffer */

//     MPIR_Type_get_true_extent_impl(datatype, &true_lb, &true_extent);
//     MPIR_Datatype_get_extent_macro(datatype, extent);

//     if (local_rank != root && recvbuf == NULL) {
//         MPIR_CHKLMEM_MALLOC(recvbuf, void *, count * (MPL_MAX(extent, true_extent)),
//                             mpi_errno, "temporary buffer", MPL_MEM_BUFFER);

//         /* adjust for potential negative lower bound in datatype */
//         recvbuf = (void *) ((char *) recvbuf - true_lb);
//         free_flag = 1;
//     }

//     mask = 0x1;
//     relrank = local_rank;
//     while (mask < comm_size) {
//         /* Receive */
//         if ((mask & relrank) == 0) {
//             source = (relrank | mask);
//             if (source < comm_size) {
//                 while (reduce_addr_array[source][round] == NULL)
//                     MPL_sched_yield();
//                 local_task = reduce_addr_array[source][round];

//                 if (recv_reduced == 0) {
//                     if (local_rank != root) {
//                         mpi_errno =
//                             MPIR_Localcopy(sendbuf, count, datatype, recvbuf, count, datatype);
//                     } else {
//                         mpi_errno = MPIR_Reduce_local(sendbuf, recvbuf, count, datatype, op);
//                     }
//                     MPIR_ERR_CHECK(mpi_errno);
//                     recv_reduced = 1;
//                 }

//                 mpi_errno = MPIR_Reduce_local(local_task->addr, recvbuf, count, datatype, op);
//                 MPIR_ERR_CHECK(mpi_errno);

//                 sendbuf = recvbuf;
//                 __sync_fetch_and_add(&local_task->cnt, 1);
//             }
//         } else {
//             /* I've received all that I'm going to.  Send my result to
//              * my parent */
//             local_task = (MPIDI_PIP_Coll_task_t *) MPIR_Handle_obj_alloc(&MPIDI_Coll_task_mem);
//             local_task->addr = (void *) sendbuf;
//             local_task->cnt = 0;
//             local_task->type = TMPI_Reduce;
//             __sync_synchronize();
//             reduce_addr[round] = (MPIDI_PIP_Coll_task_t *) local_task;

//             while (local_task->cnt != 1)
//                 MPL_sched_yield();
//             if (free_flag)
//                 free(recvbuf);

//             MPIR_Handle_obj_free(&MPIDI_Coll_task_mem, (void *) local_task);
//             break;
//         }

//         mask <<= 1;
//     }

//   fn_exit:
//     MPIR_CHKLMEM_FREEALL();
//     if (*errflag != MPIR_ERR_NONE)
//         MPIR_ERR_SET(mpi_errno, *errflag, "**coll_fail");
//     return mpi_errno;
//   fn_fail:
//     goto fn_exit;
// }


// int MPIDI_PIP_Reduce_leader_rem_intranode(const void *sendbuf, void *recvbuf, void *rem_buf,
//                                           int rem, int count, MPI_Datatype datatype, MPI_Op op,
//                                           int root, int comm_size, int local_rank, int round,
//                                           MPIR_Comm * comm, MPIR_Errflag_t * errflag)
// {
//     int mpi_errno = MPI_SUCCESS;

//     if (local_rank < rem) {
//         mpi_errno =
//             MPIDI_PIP_Reduce_leader_binomial_intranode(sendbuf, rem_buf, count, datatype, op, 0,
//                                                        rem, local_rank, round, comm, errflag);
//         MPIR_ERR_CHECK(mpi_errno);
//     }

//     mpi_errno =
//         MPIDI_PIP_Reduce_leader_binomial_intranode(sendbuf, recvbuf, count, datatype, op, 0,
//                                                    comm_size, local_rank, round, comm, errflag);

//   fn_exit:
//     return mpi_errno;
//   fn_fail:
//     goto fn_exit;
// }
