/*
 * Copyright (C) by Argonne National Laboratory
 *     See COPYRIGHT in top-level directory
 */

#include "mpiimpl.h"
#include "pip_pre.h"
#include <math.h>

int MPIDI_PIP_Scatter_nway_tree_internode(const void **sendbuf, int sendcount,
                                          MPI_Datatype sendtype, void *recvbuf, int recvcount,
                                          MPI_Datatype recvtype, int root, MPIR_Comm * comm,
                                          MPIR_Errflag_t * errflag)
{
    int mpi_errno = MPI_SUCCESS;
    int *node_procs_sum = comm->node_procs_sum;
    int roots_num = comm->node_procs_min;
    int seg_num = roots_num + 1;
    int cur_seg_num, cur_node_num, cur_seg_id, cur_node_id;
    int node_id = comm->node_id;
    int i = 0, j = comm->node_count;
    void *itm_buf = NULL;
    void *lsend_buf = (void *) *sendbuf;
    void *root_buf = NULL;
    int extent, local_rank, local_size, rank;
    int send_cur_seg_id, recv_next_i, recv_next_j, recv_rank, send_procs, prev_procs;
    int seg_procs, recv_size, send_rank;
    volatile MPIDI_PIP_Coll_task_t *local_task;
    MPIR_Request **request = NULL;
    int eindex = *comm->eindex_ptr;
    int round = *comm->round_ptr;
    int rcnt = 0;
    MPIDI_PIP_Coll_task_t *volatile ***tcoll_queue_array = comm->tcoll_queue_array;

    rank = comm->rank;
    local_rank = comm->rank % roots_num;
    local_size = roots_num;
    MPIR_Datatype_get_extent_macro(sendtype, extent);

    request = (MPIR_Request **) calloc(comm->max_depth, sizeof(MPIR_Request *));
    /* TODO: if root != 0, rank should be relative rank */
    MPIR_Assert(eindex == 0);
    while (i < j - 1) {
        cur_node_num = j - i;
        cur_seg_num = seg_num > cur_node_num ? cur_node_num : seg_num;
        cur_node_id = node_id - i;
        cur_seg_id =
            (int) ceil(((double) cur_node_id + 1.0) * (double) cur_seg_num /
                       (double) cur_node_num) - 1;
        int next_i = cur_node_num * cur_seg_id / cur_seg_num + i;
        int next_j = cur_node_num * (cur_seg_id + 1) / cur_seg_num + i;
        if (node_id == i) {
            if (itm_buf == NULL) {
                /* post root data to intra-node processes */
                if (local_rank == 0) {
                    local_task =
                        (MPIDI_PIP_Coll_task_t *) MPIR_Handle_obj_alloc(&MPIDI_Coll_task_mem);
                    local_task->addr = lsend_buf;
                    local_task->cnt = 0;
                    local_task->type = TMPI_Scatter;
                    if (lsend_buf != *sendbuf)
                        local_task->free = 1;
                    else
                        local_task->free = 0;
                    __sync_synchronize();
                    comm->tcoll_queue[round][eindex] = (MPIDI_PIP_Coll_task_t *) local_task;
                } else {
                    while (tcoll_queue_array[0][round][eindex] == NULL)
                        MPL_sched_yield();
                    local_task = (MPIDI_PIP_Coll_task_t *) tcoll_queue_array[0][round][eindex];
                    MPIR_Assert(local_task->type == TMPI_Scatter);
                }

                itm_buf = local_task->addr;
            }

            if (local_rank < cur_seg_num - 1) {
                send_cur_seg_id = local_rank + 1;
                recv_next_i = cur_node_num * send_cur_seg_id / cur_seg_num + i;
                recv_next_j = cur_node_num * (send_cur_seg_id + 1) / cur_seg_num + i;
                recv_rank = recv_next_i * roots_num;

                prev_procs = node_procs_sum[recv_next_i] - node_procs_sum[i];
                send_procs = node_procs_sum[recv_next_j] - node_procs_sum[recv_next_i];

                mpi_errno =
                    MPIC_Isend((char *) itm_buf + prev_procs * sendcount * extent,
                               send_procs * sendcount, sendtype, recv_rank, MPIR_SCATTER_TAG,
                               comm, &request[rcnt++], errflag);
                MPIR_ERR_CHECK(mpi_errno);
            }
        } else if (node_id == next_i) {
            /* recv message from the leader of current node segments */
            if (local_rank == 0) {
                seg_procs = node_procs_sum[next_j] - node_procs_sum[next_i];
                recv_size = seg_procs * sendcount * extent;
                send_rank = cur_seg_id - 1 + i * roots_num;

                lsend_buf = malloc(recv_size);
                mpi_errno =
                    MPIC_Recv(lsend_buf, seg_procs * sendcount, sendtype, send_rank,
                              MPIR_SCATTER_TAG, comm, MPI_STATUS_IGNORE, errflag);
                MPIR_ERR_CHECK(mpi_errno);
            }
        }
        i = next_i;
        j = next_j;
    }

    if (itm_buf == NULL) {
        if (local_rank == 0) {
            local_task = (MPIDI_PIP_Coll_task_t *) MPIR_Handle_obj_alloc(&MPIDI_Coll_task_mem);
            local_task->addr = lsend_buf;
            local_task->cnt = 0;
            local_task->type = TMPI_Scatter;
            if (lsend_buf != *sendbuf)
                local_task->free = 1;
            else
                local_task->free = 0;
            __sync_synchronize();
            comm->tcoll_queue[round][eindex] = (MPIDI_PIP_Coll_task_t *) local_task;
        } else {
            while (tcoll_queue_array[0][round][eindex] == NULL)
                MPL_sched_yield();
            local_task = (MPIDI_PIP_Coll_task_t *) tcoll_queue_array[0][round][eindex];
            MPIR_Assert(local_task->type == TMPI_Scatter);
        }

        itm_buf = local_task->addr;
    }
#ifndef MPIDI_PIP_DISABLE_OVERLAP
    mpi_errno = MPIR_Localcopy((char *) itm_buf + extent * sendcount * local_rank,
                               sendcount, sendtype, recvbuf, recvcount, recvtype);
    MPIR_ERR_CHECK(mpi_errno);

    if (rcnt) {
        mpi_errno = MPIC_Waitall(rcnt, request, MPI_STATUSES_IGNORE, errflag);
        MPIR_ERR_CHECK(mpi_errno);
    }
    free(request);

    __sync_fetch_and_add(&local_task->cnt, 1);
    *comm->eindex_ptr = 1;
#else
    if (local_rank == 0) {
        mpi_errno = MPIR_Localcopy((char *) itm_buf + extent * sendcount * local_rank,
                                   sendcount, sendtype, recvbuf, recvcount, recvtype);
        MPIR_ERR_CHECK(mpi_errno);

        __sync_fetch_and_add(&local_task->cnt, 1);
        *comm->eindex_ptr = 1;
    }

    if (rcnt) {
        mpi_errno = MPIC_Waitall(rcnt, request, MPI_STATUSES_IGNORE, errflag);
        MPIR_ERR_CHECK(mpi_errno);
    }
    free(request);
#endif

    *sendbuf = itm_buf;

  fn_exit:
    return mpi_errno;
  fn_fail:
    goto fn_exit;
}

int MPIDI_PIP_Scatter_nway_tree_intranode(const void *sendbuf, int sendcount,
                                          MPI_Datatype sendtype, void *recvbuf, int recvcount,
                                          MPI_Datatype recvtype, int root, MPIR_Comm * comm,
                                          MPIR_Errflag_t * errflag)
{
    int local_rank = comm->rank;
    int local_size = comm->local_size;
    int extent;
    int mpi_errno = MPI_SUCCESS;
    int eindex = *comm->eindex_ptr;
    int sindex = *comm->sindex_ptr;
    int round = *comm->round_ptr;
    volatile MPIDI_PIP_Coll_task_t *local_task;
    MPIDI_PIP_Coll_task_t *volatile ***tcoll_queue_array = comm->tcoll_queue_array;

    MPIR_Datatype_get_extent_macro(sendtype, extent);
    if (local_rank == root) {
        if (sindex == eindex) {
            local_task = (MPIDI_PIP_Coll_task_t *) MPIR_Handle_obj_alloc(&MPIDI_Coll_task_mem);
            local_task->addr = (void *) sendbuf;
            local_task->cnt = 0;
            local_task->type = TMPI_Scatter;
            local_task->free = 0;
            __sync_synchronize();
            comm->tcoll_queue[round][eindex] = (MPIDI_PIP_Coll_task_t *) local_task;

            mpi_errno = MPIR_Localcopy((char *) local_task->addr + extent * sendcount * local_rank,
                                       sendcount, sendtype, recvbuf, recvcount, recvtype);
            MPIR_ERR_CHECK(mpi_errno);

            __sync_fetch_and_add(&local_task->cnt, 1);
            eindex = (eindex + 1) % MPIDI_COLL_TASK_PREALLOC;
        }

        while (sindex != eindex) {
            local_task = comm->tcoll_queue[round][sindex];
            while (local_task->cnt != local_size)
                MPL_sched_yield();
            if (local_task->free)
                free(local_task->addr);
            comm->tcoll_queue[round][sindex] = NULL;
            MPIR_Handle_obj_free(&MPIDI_Coll_task_mem, (void *) local_task);
            sindex = (sindex + 1) % MPIDI_COLL_TASK_PREALLOC;
        }
    } else if (sindex == eindex) {
        while (tcoll_queue_array[root][round][eindex] == NULL)
            MPL_sched_yield();
        local_task = (MPIDI_PIP_Coll_task_t *) tcoll_queue_array[root][round][eindex];
        MPIR_Assert(local_task->type == TMPI_Scatter);

        mpi_errno = MPIR_Localcopy((char *) local_task->addr + extent * sendcount * local_rank,
                                   sendcount, sendtype, recvbuf, recvcount, recvtype);
        MPIR_ERR_CHECK(mpi_errno);

        __sync_fetch_and_add(&local_task->cnt, 1);
    }

    *comm->sindex_ptr = *comm->eindex_ptr = 0;
    *comm->round_ptr = round ^ 1;

  fn_exit:
    return mpi_errno;
  fn_fail:
    goto fn_exit;
}

int MPIDI_PIP_Scatter_nway_tree(const void *sendbuf, int sendcount,
                                MPI_Datatype sendtype, void *recvbuf, int recvcount,
                                MPI_Datatype recvtype, int root, MPIR_Comm * comm,
                                MPIR_Errflag_t * errflag)
{
    int mpi_errno = MPI_SUCCESS;
    const void *root_sendbuf = sendbuf;
    /* right now just support root == 0 to test function and performance. */
    MPIR_Assert(root == 0);

    if (comm->pip_roots_comm) {
        mpi_errno =
            MPIDI_PIP_Scatter_nway_tree_internode(&root_sendbuf, sendcount, sendtype, recvbuf,
                                                  recvcount, recvtype, root, comm->pip_roots_comm,
                                                  errflag);
        MPIR_ERR_CHECK(mpi_errno);
    }

    if (comm->node_comm) {
        mpi_errno =
            MPIDI_PIP_Scatter_nway_tree_intranode(root_sendbuf, sendcount, sendtype, recvbuf,
                                                  recvcount, recvtype, 0, comm->node_comm, errflag);
    }

  fn_exit:
    return mpi_errno;
  fn_fail:
    goto fn_exit;
}
