/*
 * Copyright (C) by Argonne National Laboratory
 *     See COPYRIGHT in top-level directory
 */

#include "mpiimpl.h"
#include "pip_pre.h"
#include "pip_coll.h"
#include <math.h>

int MPIDI_PIP_Allgather_bruck_internode(const void *sendbuf, int sendcount,
                                        MPI_Datatype sendtype, void *recvbuf, int recvcount,
                                        MPI_Datatype recvtype, MPIR_Comm * comm,
                                        MPIR_Errflag_t * errflag)
{
    int node_id = comm->node_id;
    int pofk_1 = 1;
    int local_rank = comm->local_rank;
    int basek_1 = comm->node_procs_min + 1;
    int comm_size, rank;
    int mpi_errno = MPI_SUCCESS;
    int mpi_errno_ret = MPI_SUCCESS;
    MPI_Aint recvtype_extent, recvtype_sz, sendtype_extent;
    int src, rem, src_node;
    void *tmp_buf = NULL;
    int curr_cnt, dst, dst_node;
    int offset, my_rem, tmp_rem;
    int leader_num = comm->node_procs_min;
    MPIDI_PIP_Coll_easy_task_t *shared_addr;
    int limit_comm_size, node_recv_size;

    MPIR_CHKLMEM_DECL(1);

    if (sendcount == 0 || recvcount == 0)
        goto fn_exit;

    comm_size = comm->node_count;
    limit_comm_size = comm_size / basek_1;

    MPIR_Datatype_get_extent_macro(recvtype, recvtype_extent);
    MPIR_Datatype_get_extent_macro(sendtype, sendtype_extent);
    MPIR_Datatype_get_size_macro(recvtype, recvtype_sz);
    node_recv_size = recvcount * recvtype_sz;

    /* allocate a temporary buffer of the same size as recvbuf. */
    if (local_rank == 0) {
        MPIR_CHKLMEM_MALLOC(tmp_buf, void *, recvcount * comm_size * recvtype_sz, mpi_errno,
                            "tmp_buf", MPL_MEM_BUFFER);
        /* copy local data to the top of tmp_buf */
        if (sendbuf != MPI_IN_PLACE) {
            mpi_errno = MPIR_Localcopy(sendbuf, sendcount, sendtype,
                                       tmp_buf, node_recv_size, MPI_BYTE);
        } else {
            mpi_errno =
                MPIR_Localcopy((char *) recvbuf + node_id * recvcount * recvtype_extent, recvcount,
                               recvtype, tmp_buf, node_recv_size, MPI_BYTE);
        }
        MPIR_ERR_CHECK(mpi_errno);

        /* post tmp buffer */
        shared_addr = MPIR_Comm_post_easy_task(tmp_buf, TMPI_Allgather, 0, 0, leader_num, comm);
    } else {
        shared_addr = MPIR_Comm_get_easy_task(comm, 0, TMPI_Allgather);
        tmp_buf = shared_addr->addr;
    }

    curr_cnt = recvcount;
    pofk_1 = 1;
    while (pofk_1 <= limit_comm_size) {
        offset = (local_rank + 1) * pofk_1;
        src_node = (node_id + offset) % comm_size;
        dst_node = (node_id - offset + comm_size) % comm_size;
        src = src_node * leader_num + local_rank;
        dst = dst_node * leader_num + local_rank;

        mpi_errno = MPIC_Sendrecv(tmp_buf, curr_cnt * recvtype_sz, MPI_BYTE, dst,
                                  MPIR_ALLGATHER_TAG,
                                  ((char *) tmp_buf + curr_cnt * recvtype_sz * (local_rank + 1)),
                                  curr_cnt * recvtype_sz, MPI_BYTE,
                                  src, MPIR_ALLGATHER_TAG, comm, MPI_STATUS_IGNORE, errflag);
        if (mpi_errno) {
            /* for communication errors, just record the error but continue */
            *errflag =
                MPIX_ERR_PROC_FAILED ==
                MPIR_ERR_GET_CLASS(mpi_errno) ? MPIR_ERR_PROC_FAILED : MPIR_ERR_OTHER;
            MPIR_ERR_SET(mpi_errno, *errflag, "**fail");
            MPIR_ERR_ADD(mpi_errno_ret, mpi_errno);
        }
        curr_cnt *= basek_1;
        pofk_1 *= basek_1;
        MPIR_PIP_Comm_opt_intra_barrier(comm, comm->node_procs_min);
    }

    /* if comm_size is not a power of k + 1, one more step is needed */
    rem = (comm_size - pofk_1) - pofk_1 * local_rank;
    my_rem = rem > pofk_1 ? pofk_1 : rem;

    if (my_rem > 0) {
        offset = (local_rank + 1) * pofk_1;
        src_node = (node_id + offset) % comm_size;
        dst_node = (node_id - offset + comm_size) % comm_size;
        src = src_node * leader_num + local_rank;
        dst = dst_node * leader_num + local_rank;

        mpi_errno = MPIC_Sendrecv(tmp_buf, my_rem * node_recv_size, MPI_BYTE,
                                  dst, MPIR_ALLGATHER_TAG,
                                  ((char *) tmp_buf + curr_cnt * recvtype_sz * (local_rank + 1)),
                                  my_rem * node_recv_size, MPI_BYTE,
                                  src, MPIR_ALLGATHER_TAG, comm, MPI_STATUS_IGNORE, errflag);
        if (mpi_errno) {
            /* for communication errors, just record the error but continue */
            *errflag =
                MPIX_ERR_PROC_FAILED ==
                MPIR_ERR_GET_CLASS(mpi_errno) ? MPIR_ERR_PROC_FAILED : MPIR_ERR_OTHER;
            MPIR_ERR_SET(mpi_errno, *errflag, "**fail");
            MPIR_ERR_ADD(mpi_errno_ret, mpi_errno);
        }
    }

    /* Rotate blocks in tmp_buf down by (rank) blocks and store
     * result in recvbuf. */
    MPIR_PIP_Comm_opt_intra_barrier(comm, comm->node_procs_min);
    mpi_errno =
        MPIR_Localcopy(tmp_buf, (comm_size - node_id) * node_recv_size, MPI_BYTE,
                       (char *) recvbuf + node_id * recvcount * recvtype_extent,
                       (comm_size - node_id) * recvcount, recvtype);
    MPIR_ERR_CHECK(mpi_errno);

    if (node_id) {
        mpi_errno = MPIR_Localcopy((char *) tmp_buf +
                                   (comm_size - node_id) * node_recv_size,
                                   node_id * node_recv_size, MPI_BYTE, recvbuf,
                                   node_id * recvcount, recvtype);
        MPIR_ERR_CHECK(mpi_errno);
    }
    __sync_fetch_and_add(&shared_addr->complete, 1);
    if (local_rank == 0) {
        while (shared_addr->target_cmpl != shared_addr->complete)
            MPL_sched_yield();
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

int MPIDI_PIP_Allgather_ring_internode(const void *sendbuf, int sendcount,
                                       MPI_Datatype sendtype, void *recvbuf, int recvcount,
                                       MPI_Datatype recvtype, MPIR_Comm * comm,
                                       MPIR_Errflag_t * errflag)
{
    int comm_size, rank;
    int mpi_errno = MPI_SUCCESS;
    int mpi_errno_ret = MPI_SUCCESS;
    size_t recvtype_extent;
    int local_rank = comm->local_rank;
    int local_size = comm->node_procs_min;
    int intranode_size = comm->intranode_size;
    int node_id = comm->node_id;
    int j, i, left_node, right_node;
    size_t j_node, jnext_node;
    size_t cnt_offset, real_cnt;
    int left, right;
    MPIDI_PIP_Coll_easy_task_t *local_task;
    void *root_buf;
    MPIR_Request *reqs[2] = { NULL, NULL };
    MPIDI_PIP_Coll_easy_task_t *shared_addr;
    size_t bcast_offset = 0, bcast_bsize;

    MPIR_Assert(sendbuf == MPI_IN_PLACE);

    comm_size = comm->node_count;
    MPIR_Datatype_get_extent_macro(recvtype, recvtype_extent);

    /*
     * Now, send left to right.  This fills in the receive area in
     * reverse order.
     */
    left_node = (comm_size + node_id - 1) % comm_size;
    right_node = (node_id + 1) % comm_size;
    left = left_node * local_size + local_rank;
    right = right_node * local_size + local_rank;
    bcast_bsize = (size_t) recvcount *recvtype_extent;

    MPIR_Assert(recvcount >= local_size);
    j_node = node_id;
    jnext_node = left_node;
    cnt_offset = recvcount * local_rank / local_size;
    real_cnt = recvcount * (local_rank + 1) / local_size - cnt_offset;

    if (local_rank == 0) {
        shared_addr = MPIR_Comm_post_easy_task(recvbuf, TMPI_Allgather, 0, 0, 1, comm);
    } else {
        shared_addr = MPIR_Comm_get_easy_task(comm, 0, TMPI_Allgather);
    }

    root_buf = shared_addr->addr;

    for (i = 1; i < comm_size; i++) {
        if (local_rank == 0) {
            /* post intranode task */
            local_task =
                MPIR_Comm_post_easy_task((char *) recvbuf +
                                         j_node * (size_t) recvcount * recvtype_extent, TMPI_Bcast,
                                         bcast_bsize, 0, local_size - 1, comm);
        }

        mpi_errno =
            MPIC_Isend(((char *) root_buf +
                        (j_node * (size_t) recvcount + cnt_offset) * recvtype_extent), real_cnt,
                       recvtype, right, MPIR_ALLGATHER_TAG, comm, &reqs[0], errflag);
        MPIR_ERR_CHECK(mpi_errno);

        mpi_errno =
            MPIC_Irecv(((char *) root_buf +
                        (jnext_node * (size_t) recvcount + cnt_offset) * recvtype_extent), real_cnt,
                       recvtype, left, MPIR_ALLGATHER_TAG, comm, &reqs[1]);
        MPIR_ERR_CHECK(mpi_errno);

        /* intranode copy */
        if (local_rank != 0) {
            local_task = MPIR_Comm_get_easy_task(comm, 0, TMPI_Bcast);
            bcast_offset = j_node * (size_t) recvcount *recvtype_extent;
            mpi_errno =
                MPIR_Localcopy(local_task->addr, recvcount, recvtype,
                               (char *) recvbuf + bcast_offset, recvcount, recvtype);
            MPIR_ERR_CHECK(mpi_errno);


            __sync_fetch_and_add(&local_task->complete, 1);
        }

        mpi_errno = MPIC_Waitall(2, reqs, MPI_STATUSES_IGNORE, errflag);
        j_node = jnext_node;
        jnext_node = (comm_size + jnext_node - 1) % comm_size;
        reqs[0] = reqs[1] = NULL;

        MPIR_PIP_Comm_opt_intra_barrier(comm, comm->node_procs_min);
    }

    /* post last one */
    if (local_rank == 0) {
        /* post intranode task */
        local_task =
            MPIR_Comm_post_easy_task((char *) recvbuf + j_node * recvcount * recvtype_extent,
                                     TMPI_Bcast, bcast_bsize, 0, local_size - 1, comm);
        while (local_task->target_cmpl != local_task->complete)
            MPL_sched_yield();
        shared_addr->complete = 1;
    } else {
        local_task = MPIR_Comm_get_easy_task(comm, 0, TMPI_Bcast);
        bcast_offset = j_node * (size_t) recvcount *recvtype_extent;
        mpi_errno =
            MPIR_Localcopy(local_task->addr, recvcount, recvtype,
                           (char *) recvbuf + bcast_offset, recvcount, recvtype);
        MPIR_ERR_CHECK(mpi_errno);
        __sync_fetch_and_add(&local_task->complete, 1);
    }

  fn_exit:
    return mpi_errno;

  fn_fail:
    goto fn_exit;
}

int MPIDI_PIP_Allgatherv_ring_internode(const void *sendbuf, int sendcount, MPI_Datatype sendtype,
                                        void *recvbuf, const int *recvcounts, const int *displs,
                                        MPI_Datatype recvtype, MPIR_Comm * comm,
                                        MPIR_Errflag_t * errflag)
{
    int comm_size, rank;
    int mpi_errno = MPI_SUCCESS;
    int mpi_errno_ret = MPI_SUCCESS;
    MPI_Aint recvtype_extent;
    int local_rank = comm->local_rank;
    int local_size = comm->node_procs_min;
    int node_id = comm->node_id;
    int j, i, left_node, right_node;
    int j_node, jnext_node;
    int cnt_offset, real_cnt;
    int left, right, jnext;
    MPIDI_PIP_Coll_easy_task_t *local_task;
    MPIR_Request *reqs[2] = { NULL, NULL };
    void *root_buf;
    MPIDI_PIP_Coll_easy_task_t *shared_addr;
    int my_send_offset, my_send_cnt;
    int my_recv_offset, my_recv_cnt;
    int bcast_bsize;
    size_t bcast_offset;

    comm_size = comm->node_count;
    MPIR_Datatype_get_extent_macro(recvtype, recvtype_extent);

    if (local_rank == 0) {
        shared_addr = MPIR_Comm_post_easy_task(recvbuf, TMPI_Allgather, 0, 0, 1, comm);
    } else {
        shared_addr = MPIR_Comm_get_easy_task(comm, 0, TMPI_Allgather);
    }

    root_buf = shared_addr->addr;

    /*
     * Now, send left to right.  This fills in the receive area in
     * reverse order.
     */
    left_node = (comm_size + node_id - 1) % comm_size;
    right_node = (node_id + 1) % comm_size;
    left = left_node * comm->node_procs_min + local_rank;
    right = right_node * comm->node_procs_min + local_rank;

    MPIR_Assert(recvcounts[node_id] >= local_size);
    j_node = node_id;
    jnext_node = left_node;
    for (i = 1; i < comm_size; i++) {
        if (local_rank == 0) {
            /* post intranode task */
            // bcast_bsize = recvcounts[j_node] * recvtype_extent;
            local_task =
                MPIR_Comm_post_easy_task((char *) recvbuf + displs[j_node] * recvtype_extent,
                                         TMPI_Bcast, recvcounts[j_node], 0, local_size - 1, comm);
        }

        my_send_offset = recvcounts[j_node] * local_rank / local_size;
        my_send_cnt = recvcounts[j_node] * (local_rank + 1) / local_size - my_send_offset;

        my_recv_offset = recvcounts[jnext_node] * local_rank / local_size;
        my_recv_cnt = recvcounts[jnext_node] * (local_rank + 1) / local_size - my_recv_offset;

        mpi_errno =
            MPIC_Isend(((char *) root_buf + (displs[j_node] + my_send_offset) * recvtype_extent),
                       my_send_cnt, recvtype, right, MPIR_ALLGATHERV_TAG, comm, &reqs[0], errflag);
        MPIR_ERR_CHECK(mpi_errno);

        mpi_errno =
            MPIC_Irecv(((char *) root_buf +
                        (displs[jnext_node] + my_recv_offset) * recvtype_extent), my_recv_cnt,
                       recvtype, left, MPIR_ALLGATHERV_TAG, comm, &reqs[1]);
        MPIR_ERR_CHECK(mpi_errno);

        /* intranode copy */
        if (local_rank != 0) {
            local_task = MPIR_Comm_get_easy_task(comm, 0, TMPI_Bcast);
            bcast_offset = displs[j_node] * recvtype_extent;
            mpi_errno =
                MPIR_Localcopy(local_task->addr, recvcounts[j_node], recvtype,
                               (char *) recvbuf + bcast_offset, recvcounts[j_node], recvtype);
            MPIR_ERR_CHECK(mpi_errno);

            __sync_fetch_and_add(&local_task->complete, 1);
        }

        mpi_errno = MPIC_Waitall(2, reqs, MPI_STATUSES_IGNORE, errflag);
        j_node = jnext_node;
        jnext_node = (comm_size + jnext_node - 1) % comm_size;
        reqs[0] = reqs[1] = NULL;
        MPIR_PIP_Comm_opt_intra_barrier(comm, comm->node_procs_min);
    }

    /* post last one */
    if (local_rank == 0) {
        /* post intranode task */
        // bcast_bsize = recvcounts[j_node] * recvtype_extent;
        local_task =
            MPIR_Comm_post_easy_task((char *) recvbuf + displs[j_node] * recvtype_extent,
                                     TMPI_Bcast, recvcounts[j_node], 0, local_size - 1, comm);
        while (local_task->target_cmpl != local_task->complete)
            MPL_sched_yield();
        shared_addr->complete = 1;
    } else {
        local_task = MPIR_Comm_get_easy_task(comm, 0, TMPI_Bcast);
        bcast_offset = displs[j_node] * recvtype_extent;
        mpi_errno =
            MPIR_Localcopy(local_task->addr, recvcounts[j_node], recvtype,
                           (char *) recvbuf + bcast_offset, recvcounts[j_node], recvtype);
        MPIR_ERR_CHECK(mpi_errno);
        __sync_fetch_and_add(&local_task->complete, 1);
    }

  fn_exit:
    return mpi_errno;

  fn_fail:
    goto fn_exit;
}

int MPIDI_PIP_Allgather_impl(const void *sendbuf, int sendcount,
                             MPI_Datatype sendtype, void *recvbuf, int recvcount,
                             MPI_Datatype recvtype, MPIR_Comm * comm, MPIR_Errflag_t * errflag)
{
    size_t data_sz, rtype_size;
    int mpi_errno = MPI_SUCCESS;
    void *local_root0_buf = NULL;
    int local_rank;
    size_t local_size;
    int gsize = comm->local_size;
    size_t node_id = comm->node_id;
    size_t node_recvcount, recvtype_extent;

    if (comm->node_comm) {
        local_rank = comm->node_comm->rank;
        local_size = comm->node_comm->local_size;
    } else {
        local_rank = 0;
        local_size = 1;
    }

    /* FIXME: we assume now #procs on each node is equal. */
    MPIR_Datatype_get_extent_macro(recvtype, recvtype_extent);
    node_recvcount = (size_t) recvcount *local_size;
    local_root0_buf = (char *) recvbuf + node_recvcount * node_id * recvtype_extent;

    if (comm->node_comm) {
        mpi_errno =
            MPIDI_PIP_Gather_intranode(sendbuf, sendcount, sendtype, local_root0_buf, recvcount,
                                       recvtype, 0, comm->node_comm, errflag);
    } else {
        mpi_errno =
            MPIR_Localcopy(sendbuf, sendcount, sendtype, local_root0_buf, recvcount, recvtype);
    }
    MPIR_ERR_CHECK(mpi_errno);

    if (comm->pip_roots_comm) {
        MPIR_Datatype_get_size_macro(recvtype, rtype_size);
        data_sz = recvcount * rtype_size * comm->node_comm->local_size;

        if (data_sz < MPIR_CVAR_ALLGATHER_SHORT_MSG_SIZE) {
            /* TODO: here should use allgatherv, but for research, we assume #procs is equal on each node. */
            mpi_errno =
                MPIDI_PIP_Allgather_bruck_internode(MPI_IN_PLACE, node_recvcount, recvtype,
                                                    recvbuf, node_recvcount,
                                                    recvtype, comm->pip_roots_comm, errflag);
        } else {
            mpi_errno =
                MPIDI_PIP_Allgather_ring_internode(MPI_IN_PLACE, node_recvcount, recvtype,
                                                   recvbuf, node_recvcount,
                                                   recvtype, comm->pip_roots_comm, errflag);
        }
        MPIR_ERR_CHECK(mpi_errno);
    }

    if (comm->node_comm && comm->node_comm->local_size > comm->node_procs_min) {
        mpi_errno =
            MPIDI_PIP_partial_bcast_intranode(recvbuf, recvcount * gsize, recvtype, 0,
                                              comm->node_comm, errflag);
    }

  fn_exit:
    return mpi_errno;
  fn_fail:
    goto fn_exit;
}
