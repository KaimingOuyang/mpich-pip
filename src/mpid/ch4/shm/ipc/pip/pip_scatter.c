#include "mpiimpl.h"
#include "pip_pre.h"
#include "pip_coll.h"
#include <math.h>

/*
=== BEGIN_MPI_T_CVAR_INFO_BLOCK ===

cvars:
    - name        : MPIR_CVAR_PIP_BUFFER_SIZE_FREE_THD
      category    : COLLECTIVE
      type        : int
      default     : 65536
      class       : none
      verbosity   : MPI_T_VERBOSITY_USER_BASIC
      scope       : MPI_T_SCOPE_ALL_EQ
      description : >-
        pipelined allreduce chunk size

=== END_MPI_T_CVAR_INFO_BLOCK ===
*/

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
    int local_rank, local_size, rank;
    int send_cur_seg_id, recv_next_i, recv_next_j, recv_rank;
    int recv_size = 0, send_rank;
    MPIDI_PIP_Coll_easy_task_t *local_task;
    MPIR_Request **request = NULL;
    int rcnt = 0;
    size_t extent, addr_offset, send_procs, prev_procs, seg_procs;

    rank = comm->rank;
    local_rank = comm->local_rank;
    local_size = roots_num;
    MPIR_Datatype_get_extent_macro(sendtype, extent);

    request = (MPIR_Request **) calloc(comm->max_depth, sizeof(MPIR_Request *));
    /* TODO: if root != 0, rank should be relative rank */
    // MPIR_Assert(eindex == 0);
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
                        MPIR_Comm_post_easy_task((void *) lsend_buf, TMPI_Scatter, 0,
                                                 lsend_buf != *sendbuf &&
                                                 recv_size <
                                                 MPIR_CVAR_PIP_BUFFER_SIZE_FREE_THD ? 1 : 0,
                                                 roots_num, comm);
                } else {
                    local_task = MPIR_Comm_get_easy_task(comm, root, TMPI_Scatter);
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

                addr_offset = prev_procs * (size_t) sendcount *extent;
                mpi_errno =
                    MPIC_Isend((char *) itm_buf + addr_offset,
                               send_procs * sendcount, sendtype, recv_rank, MPIR_SCATTER_TAG,
                               comm, &request[rcnt++], errflag);
                MPIR_ERR_CHECK(mpi_errno);
            }
        } else if (node_id == next_i) {
            /* recv message from the leader of current node segments */
            if (local_rank == 0) {
                seg_procs = node_procs_sum[next_j] - node_procs_sum[next_i];
                recv_size = seg_procs * (size_t) sendcount *extent;
                send_rank = cur_seg_id - 1 + i * roots_num;

                lsend_buf = malloc(recv_size);
                mpi_errno =
                    MPIC_Recv(lsend_buf, seg_procs * (size_t) sendcount, sendtype, send_rank,
                              MPIR_SCATTER_TAG, comm, MPI_STATUS_IGNORE, errflag);
                MPIR_ERR_CHECK(mpi_errno);
            }
        }
        i = next_i;
        j = next_j;
    }

    if (itm_buf == NULL) {
        if (local_rank == 0) {
            local_task =
                MPIR_Comm_post_easy_task((void *) lsend_buf, TMPI_Scatter, 0,
                                         lsend_buf != *sendbuf && recv_size <
                                         MPIR_CVAR_PIP_BUFFER_SIZE_FREE_THD ? 1 : 0, roots_num,
                                         comm);
        } else {
            local_task = MPIR_Comm_get_easy_task(comm, root, TMPI_Scatter);
        }
        itm_buf = local_task->addr;
    }

    addr_offset = extent * sendcount * local_rank;
#ifndef MPIDI_PIP_DISABLE_OVERLAP
    mpi_errno = MPIR_Localcopy((char *) itm_buf + addr_offset,
                               sendcount, sendtype, recvbuf, recvcount, recvtype);
    MPIR_ERR_CHECK(mpi_errno);

    if (rcnt) {
        mpi_errno = MPIC_Waitall(rcnt, request, MPI_STATUSES_IGNORE, errflag);
        MPIR_ERR_CHECK(mpi_errno);
    }
    free(request);

    // *comm->eindex_ptr = 1;
#else
    if (rcnt) {
        mpi_errno = MPIC_Waitall(rcnt, request, MPI_STATUSES_IGNORE, errflag);
        MPIR_ERR_CHECK(mpi_errno);
    }
    mpi_errno = MPIR_Localcopy((char *) itm_buf + addr_offset,
                               sendcount, sendtype, recvbuf, recvcount, recvtype);
    MPIR_ERR_CHECK(mpi_errno);
    free(request);
#endif

    __sync_fetch_and_add(&local_task->complete, 1);
    if (local_rank == 0) {
        while (local_task->complete != local_task->target_cmpl)
            MPL_sched_yield();
        if (lsend_buf != *sendbuf && recv_size >= MPIR_CVAR_PIP_BUFFER_SIZE_FREE_THD)
            free(local_task->addr);
    }

  fn_exit:
    *sendbuf = itm_buf;
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
    MPIDI_PIP_Coll_easy_task_t *local_task;

    MPIR_Datatype_get_extent_macro(sendtype, extent);
    if (local_rank == root) {
        local_task = MPIR_Comm_post_easy_task(sendbuf, TMPI_Scatter, 0, 0, local_size - 1, comm);
        mpi_errno =
            MPIR_Localcopy((char *) local_task->addr, sendcount, sendtype, recvbuf, recvcount,
                           recvtype);
        MPIR_ERR_CHECK(mpi_errno);
    } else {
        local_task = MPIR_Comm_get_easy_task(comm, root, TMPI_Scatter);
        mpi_errno = MPIR_Localcopy((char *) local_task->addr + extent * sendcount * local_rank,
                                   sendcount, sendtype, recvbuf, recvcount, recvtype);
        MPIR_ERR_CHECK(mpi_errno);
        __sync_fetch_and_add(&local_task->complete, 1);
    }

  fn_exit:
    return mpi_errno;
  fn_fail:
    goto fn_exit;
}

int MPIDI_PIP_Scatter_nway_tree_partial_intranode(const void *sendbuf, int sendcount,
                                                  MPI_Datatype sendtype, void *recvbuf,
                                                  int recvcount, MPI_Datatype recvtype, int root,
                                                  MPIR_Comm * comm, MPIR_Errflag_t * errflag)
{
    int local_rank = comm->rank;
    int local_size = comm->local_size;
    int extent;
    int mpi_errno = MPI_SUCCESS;
    MPIDI_PIP_Coll_easy_task_t *local_task;

    MPIR_Datatype_get_extent_macro(sendtype, extent);
    if (local_rank == root) {
        local_task =
            MPIR_Comm_post_easy_task(sendbuf, TMPI_Scatter, 0, 0, local_size - comm->node_procs_min,
                                     comm);
    } else {
        local_task = MPIR_Comm_get_easy_task(comm, root, TMPI_Scatter);
        mpi_errno = MPIR_Localcopy((char *) local_task->addr + extent * sendcount * local_rank,
                                   sendcount, sendtype, recvbuf, recvcount, recvtype);
        MPIR_ERR_CHECK(mpi_errno);
        __sync_fetch_and_add(&local_task->complete, 1);
    }

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
    void *root_sendbuf = (void *) sendbuf;
    /* right now just support root == 0 to test function and performance. */
    MPIR_Assert(root == 0);

    if (comm->pip_roots_comm) {
        mpi_errno =
            MPIDI_PIP_Scatter_nway_tree_internode(&root_sendbuf, sendcount, sendtype, recvbuf,
                                                  recvcount, recvtype, root, comm->pip_roots_comm,
                                                  errflag);
        MPIR_ERR_CHECK(mpi_errno);

        if (comm->node_comm && comm->node_comm->local_size > comm->node_procs_min) {
            mpi_errno =
                MPIDI_PIP_Scatter_nway_tree_partial_intranode(root_sendbuf, sendcount, sendtype,
                                                              recvbuf, recvcount, recvtype, 0,
                                                              comm->node_comm, errflag);
        }
    } else {
        mpi_errno =
            MPIDI_PIP_Scatter_nway_tree_intranode(root_sendbuf, sendcount, sendtype, recvbuf,
                                                  recvcount, recvtype, 0, comm->node_comm, errflag);
    }

    MPIR_ERR_CHECK(mpi_errno);

  fn_exit:
    return mpi_errno;
  fn_fail:
    goto fn_exit;
}
