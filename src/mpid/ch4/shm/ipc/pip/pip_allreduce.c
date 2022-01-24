#include "mpiimpl.h"
#include "pip_pre.h"
#include "pip_coll.h"
#include <math.h>

/*
=== BEGIN_MPI_T_CVAR_INFO_BLOCK ===

cvars:
    - name        : MPIR_CVAR_ALLREDUCE_CHUNK_SIZE
      category    : COLLECTIVE
      type        : int
      default     : 262144
      class       : none
      verbosity   : MPI_T_VERBOSITY_USER_BASIC
      scope       : MPI_T_SCOPE_ALL_EQ
      description : >-
        pipelined allreduce chunk size

=== END_MPI_T_CVAR_INFO_BLOCK ===
*/

/* Routine to calculate log_2 of an integer */
static inline int MPL_2log(int number)
{
    int i = 0;

    while (number > 1) {
        ++i;
        number >>= 1;
    }

    return i;
}

int MPIR_Allreduce_leader_intra_recursive_doubling(const void *sendbuf,
                                                   void *recvbuf,
                                                   int count,
                                                   MPI_Datatype datatype,
                                                   int leader_num,
                                                   MPI_Op op, MPIR_Comm * comm_ptr,
                                                   MPIR_Errflag_t * errflag)
{
    MPIR_CHKLMEM_DECL(1);
    int comm_size, rank;
    int mpi_errno = MPI_SUCCESS;
    int mpi_errno_ret = MPI_SUCCESS;
    int mask, dst, is_commutative, pof2, newrank, rem, newdst;
    MPI_Aint true_extent, true_lb, extent;
    void *tmp_buf;
    MPIDI_PIP_Coll_easy_task_t *shared_addr;

    comm_size = leader_num;
    rank = comm_ptr->local_rank;

    if (sendbuf == NULL)
        goto copy_res;

    is_commutative = MPIR_Op_is_commutative(op);

    /* need to allocate temporary buffer to store incoming data */
    MPIR_Type_get_true_extent_impl(datatype, &true_lb, &true_extent);
    MPIR_Datatype_get_extent_macro(datatype, extent);

    MPIR_CHKLMEM_MALLOC(tmp_buf, void *, count * (MPL_MAX(extent, true_extent)), mpi_errno,
                        "temporary buffer", MPL_MEM_BUFFER);

    /* adjust for potential negative lower bound in datatype */
    tmp_buf = (void *) ((char *) tmp_buf - true_lb);

    /* copy local data into recvbuf */
    if (sendbuf != MPI_IN_PLACE) {
        mpi_errno = MPIR_Localcopy(sendbuf, count, datatype, recvbuf, count, datatype);
        MPIR_ERR_CHECK(mpi_errno);
    }

    /* get nearest power-of-two less than or equal to comm_size */
    pof2 = MPL_pof2(comm_size);

    rem = comm_size - pof2;

    /* In the non-power-of-two case, all even-numbered
     * processes of rank < 2*rem send their data to
     * (rank+1). These even-numbered processes no longer
     * participate in the algorithm until the very end. The
     * remaining processes form a nice power-of-two. */

    if (rank < 2 * rem) {
        if (rank % 2 == 0) {    /* even */
            mpi_errno = MPIC_Send(recvbuf, count,
                                  datatype, rank + 1, MPIR_ALLREDUCE_TAG, comm_ptr, errflag);
            if (mpi_errno) {
                /* for communication errors, just record the error but continue */
                *errflag =
                    MPIX_ERR_PROC_FAILED ==
                    MPIR_ERR_GET_CLASS(mpi_errno) ? MPIR_ERR_PROC_FAILED : MPIR_ERR_OTHER;
                MPIR_ERR_SET(mpi_errno, *errflag, "**fail");
                MPIR_ERR_ADD(mpi_errno_ret, mpi_errno);
            }

            /* temporarily set the rank to -1 so that this
             * process does not pariticipate in recursive
             * doubling */
            newrank = -1;
        } else {        /* odd */
            mpi_errno = MPIC_Recv(tmp_buf, count,
                                  datatype, rank - 1,
                                  MPIR_ALLREDUCE_TAG, comm_ptr, MPI_STATUS_IGNORE, errflag);
            if (mpi_errno) {
                /* for communication errors, just record the error but continue */
                *errflag =
                    MPIX_ERR_PROC_FAILED ==
                    MPIR_ERR_GET_CLASS(mpi_errno) ? MPIR_ERR_PROC_FAILED : MPIR_ERR_OTHER;
                MPIR_ERR_SET(mpi_errno, *errflag, "**fail");
                MPIR_ERR_ADD(mpi_errno_ret, mpi_errno);
            }

            /* do the reduction on received data. since the
             * ordering is right, it doesn't matter whether
             * the operation is commutative or not. */
            mpi_errno = MPIR_Reduce_local(tmp_buf, recvbuf, count, datatype, op);
            MPIR_ERR_CHECK(mpi_errno);

            /* change the rank */
            newrank = rank / 2;
        }
    } else      /* rank >= 2*rem */
        newrank = rank - rem;

    /* If op is user-defined or count is less than pof2, use
     * recursive doubling algorithm. Otherwise do a reduce-scatter
     * followed by allgather. (If op is user-defined,
     * derived datatypes are allowed and the user could pass basic
     * datatypes on one process and derived on another as long as
     * the type maps are the same. Breaking up derived
     * datatypes to do the reduce-scatter is tricky, therefore
     * using recursive doubling in that case.) */

    if (newrank != -1) {
        mask = 0x1;
        while (mask < pof2) {
            newdst = newrank ^ mask;
            /* find real rank of dest */
            dst = (newdst < rem) ? newdst * 2 + 1 : newdst + rem;

            /* Send the most current data, which is in recvbuf. Recv
             * into tmp_buf */
            mpi_errno = MPIC_Sendrecv(recvbuf, count, datatype,
                                      dst, MPIR_ALLREDUCE_TAG, tmp_buf,
                                      count, datatype, dst,
                                      MPIR_ALLREDUCE_TAG, comm_ptr, MPI_STATUS_IGNORE, errflag);
            if (mpi_errno) {
                /* for communication errors, just record the error but continue */
                *errflag =
                    MPIX_ERR_PROC_FAILED ==
                    MPIR_ERR_GET_CLASS(mpi_errno) ? MPIR_ERR_PROC_FAILED : MPIR_ERR_OTHER;
                MPIR_ERR_SET(mpi_errno, *errflag, "**fail");
                MPIR_ERR_ADD(mpi_errno_ret, mpi_errno);
            }

            /* tmp_buf contains data received in this step.
             * recvbuf contains data accumulated so far */

            if (is_commutative || (dst < rank)) {
                /* op is commutative OR the order is already right */
                mpi_errno = MPIR_Reduce_local(tmp_buf, recvbuf, count, datatype, op);
                MPIR_ERR_CHECK(mpi_errno);
            } else {
                /* op is noncommutative and the order is not right */
                mpi_errno = MPIR_Reduce_local(recvbuf, tmp_buf, count, datatype, op);
                MPIR_ERR_CHECK(mpi_errno);

                /* copy result back into recvbuf */
                mpi_errno = MPIR_Localcopy(tmp_buf, count, datatype, recvbuf, count, datatype);
                MPIR_ERR_CHECK(mpi_errno);
            }
            mask <<= 1;
        }
    }
    /* In the non-power-of-two case, all odd-numbered
     * processes of rank < 2*rem send the result to
     * (rank-1), the ranks who didn't participate above. */
    if (rank < 2 * rem) {
        if (rank % 2)   /* odd */
            mpi_errno = MPIC_Send(recvbuf, count,
                                  datatype, rank - 1, MPIR_ALLREDUCE_TAG, comm_ptr, errflag);
        else    /* even */
            mpi_errno = MPIC_Recv(recvbuf, count,
                                  datatype, rank + 1,
                                  MPIR_ALLREDUCE_TAG, comm_ptr, MPI_STATUS_IGNORE, errflag);
        if (mpi_errno) {
            /* for communication errors, just record the error but continue */
            *errflag =
                MPIX_ERR_PROC_FAILED ==
                MPIR_ERR_GET_CLASS(mpi_errno) ? MPIR_ERR_PROC_FAILED : MPIR_ERR_OTHER;
            MPIR_ERR_SET(mpi_errno, *errflag, "**fail");
            MPIR_ERR_ADD(mpi_errno_ret, mpi_errno);
        }
    }
  copy_res:
    if (leader_num != comm_ptr->node_procs_min) {
        if (rank == 0) {
            shared_addr =
                MPIR_Comm_post_easy_task(recvbuf, TMPI_Allreduce, 0, 0,
                                         comm_ptr->node_procs_min - leader_num, comm_ptr);
            while (shared_addr->complete != shared_addr->target_cmpl)
                MPL_sched_yield();
        } else {
            shared_addr = MPIR_Comm_get_easy_task(comm_ptr, 0, TMPI_Allreduce);
            if (rank >= leader_num) {
                mpi_errno =
                    MPIR_Localcopy(shared_addr->addr, count, datatype, recvbuf, count, datatype);
                MPIR_ERR_CHECK(mpi_errno);
                __sync_fetch_and_add(&shared_addr->complete, 1);
            }
        }
    }

  fn_exit:
    MPIR_CHKLMEM_FREEALL();
    return mpi_errno;
  fn_fail:
    goto fn_exit;
}


int MPIDI_PIP_Allreduce_recursive_bruck_internode(const void *sendbuf, void *recvbuf, int count,
                                                  MPI_Datatype datatype, MPI_Op op,
                                                  MPIR_Comm * comm, int rem_step,
                                                  MPIR_Errflag_t * errflag)
{
    int node_id = comm->node_id;
    int local_rank = comm->local_rank;
    int leader_num = comm->node_procs_min;
    int mpi_errno = MPI_SUCCESS, recvtype_extent;
    void *recvtmp_buf;
    MPIDI_PIP_Coll_easy_task_t *bcast_task;

    mpi_errno = MPIDI_PIP_Reduce_recursive_bruck_internode(sendbuf, recvbuf, count,
                                                           datatype, op, comm, rem_step, errflag);
    /* bcast results */
    if (leader_num > 1) {
        if (local_rank == 0) {
            bcast_task = MPIR_Comm_post_easy_task(recvbuf, TMPI_Bcast, 0, 1, leader_num - 1, comm);
            while (bcast_task->complete != bcast_task->target_cmpl)
                MPL_sched_yield();
        } else {
            bcast_task = MPIR_Comm_get_easy_task(comm, 0, TMPI_Bcast);

            mpi_errno = MPIR_Localcopy(bcast_task->addr, count, datatype, recvbuf, count, datatype);
            MPIR_ERR_CHECK(mpi_errno);
            __sync_fetch_and_add(&bcast_task->complete, 1);
        }
    }

  fn_exit:
    if (*errflag != MPIR_ERR_NONE)
        MPIR_ERR_SET(mpi_errno, *errflag, "**coll_fail");
    return mpi_errno;

  fn_fail:
    goto fn_exit;
}


int MPIDI_PIP_Allreduce_reduce_scatter_internode(const void *sendbuf, void *recvbuf, int count,
                                                 MPI_Datatype datatype, MPI_Op op, MPIR_Comm * comm,
                                                 MPIR_Errflag_t * errflag)
{
    int node_count = comm->node_count;
    int node_id = comm->node_id;
    int src, dst;
    int leader_num = comm->node_procs_min;
    int local_rank = comm->local_rank;
    int sseg, eseg, seg_cnt, extent;
    MPIDI_PIP_Coll_easy_task_t *shared_addr, *local_addr, *root_addr;
    MPIDI_PIP_Coll_easy_task_t *local_task;
    int scnt, ecnt, real_cnt;
    MPIR_Request *rreq = NULL;
    MPIR_Request **reqs;
    int req_cnt = 0;
    void *dst_buf, *tmp_buf, *src_buf;
    int mpi_errno = MPI_SUCCESS;
    int target_cnt, max_req_cnt;

    if (count == 0)
        goto fn_exit;

    MPIR_CHKLMEM_DECL(2);
    MPIR_Assert(count >= node_count);
    sseg = node_count * local_rank / leader_num;
    eseg = node_count * (local_rank + 1) / leader_num;

    if (local_rank == 0)
        shared_addr = MPIR_Comm_post_easy_task(recvbuf, TMPI_Allreduce, 0, 0, leader_num, comm);
    else
        shared_addr = MPIR_Comm_get_easy_task(comm, 0, TMPI_Allreduce);
    dst_buf = shared_addr->addr;

    if (sseg == eseg)
        goto fn_exit;

    MPIR_Datatype_get_extent_macro(datatype, extent);
    max_req_cnt = node_count / leader_num + 1;
    MPIR_CHKLMEM_MALLOC(reqs, MPIR_Request **, sizeof(MPIR_Request *) * max_req_cnt, mpi_errno,
                        "reqs", MPL_MEM_BUFFER);
    memset(reqs, 0, sizeof(MPIR_Request *) * max_req_cnt);

    for (int i = sseg; i < eseg; ++i) {
        if (i == node_id)
            continue;
        /* deal with node i data */
        scnt = count * i / node_count;
        ecnt = count * (i + 1) / node_count;
        real_cnt = ecnt - scnt;

        mpi_errno =
            MPIC_Isend((char *) dst_buf + scnt * extent,
                       real_cnt, datatype, i * leader_num + local_rank, MPIR_ALLREDUCE_TAG,
                       comm, &reqs[req_cnt++], errflag);
        MPIR_ERR_CHECK(mpi_errno);
    }

    if (sseg <= node_id && node_id < eseg) {
        MPIR_CHKLMEM_MALLOC(tmp_buf, void *, (count / node_count + 1) * extent, mpi_errno,
                            "tmp_buf", MPL_MEM_BUFFER);
        scnt = count * node_id / node_count;
        ecnt = count * (node_id + 1) / node_count;
        real_cnt = ecnt - scnt;
        for (int i = 0; i < node_count; ++i) {
            if (i == node_id)
                continue;

            mpi_errno =
                MPIC_Recv(tmp_buf, real_cnt, datatype, i * leader_num + local_rank,
                          MPIR_ALLREDUCE_TAG, comm, MPI_STATUS_IGNORE, errflag);
            MPIR_ERR_CHECK(mpi_errno);

            mpi_errno =
                MPIR_Reduce_local(tmp_buf, (char *) dst_buf + scnt * extent,
                                  real_cnt, datatype, op);
            MPIR_ERR_CHECK(mpi_errno);
        }
    }

    MPIR_Assert(req_cnt <= max_req_cnt);
    if (req_cnt) {
        mpi_errno = MPIC_Waitall(req_cnt, reqs, MPI_STATUSES_IGNORE, errflag);
        MPIR_ERR_CHECK(mpi_errno);
    }

  fn_exit:
    __sync_fetch_and_add(&shared_addr->complete, 1);
    while (shared_addr->complete != leader_num)
        MPL_sched_yield();
    MPIR_CHKLMEM_FREEALL();
    if (*errflag != MPIR_ERR_NONE)
        MPIR_ERR_SET(mpi_errno, *errflag, "**coll_fail");
    return mpi_errno;

  fn_fail:
    goto fn_exit;
}

int MPIDI_PIP_Allreduce_impl(const void *sendbuf, void *recvbuf, int count,
                             MPI_Datatype datatype, MPI_Op op, MPIR_Comm * comm,
                             MPIR_Errflag_t * errflag)
{
    size_t data_sz;
    int type_size;
    int mpi_errno = MPI_SUCCESS;

    MPIR_Datatype_get_size_macro(datatype, type_size);
    data_sz = count * type_size;

    if (comm->node_comm) {
        if (data_sz < MPIR_CVAR_ALLREDUCE_SHORT_MSG_SIZE) {
            mpi_errno = MPIR_Reduce_intra_binomial(sendbuf, recvbuf,
                                                   count, datatype, op, 0,
                                                   comm->node_comm, errflag);
        } else {
            mpi_errno =
                MPIDI_PIP_intranode_reduce(sendbuf, recvbuf, count, datatype, op, 0,
                                           comm->node_comm, errflag);
        }
    } else {
        if (sendbuf != MPI_IN_PLACE)
            mpi_errno = MPIR_Reduce_local(sendbuf, recvbuf, count, datatype, op);
    }
    MPIR_ERR_CHECK(mpi_errno);

    if (comm->pip_roots_comm) {
        if (data_sz < MPIR_CVAR_ALLREDUCE_SHORT_MSG_SIZE) {
            mpi_errno =
                MPIDI_PIP_Allreduce_recursive_bruck_internode(MPI_IN_PLACE, recvbuf, count,
                                                              datatype, op, comm->pip_roots_comm,
                                                              comm->node_count, errflag);
        } else {
            int *recvcounts, *displs;
            int node_count = comm->node_count;
            int local_rank = comm->node_comm->rank;

            MPIR_Assert(node_count > 1);
            mpi_errno =
                MPIDI_PIP_Allreduce_reduce_scatter_internode(MPI_IN_PLACE, recvbuf, count,
                                                             datatype, op, comm->pip_roots_comm,
                                                             errflag);
            MPIR_ERR_CHECK(mpi_errno);

            recvcounts = (int *) malloc(node_count * sizeof(int));
            displs = (int *) malloc(node_count * sizeof(int));
            for (int i = 0; i < node_count; ++i) {
                displs[i] = count * i / node_count;
                recvcounts[i] = count * (i + 1) / node_count - displs[i];
            }
            mpi_errno =
                MPIDI_PIP_Allgatherv_ring_internode(MPI_IN_PLACE, recvcounts[comm->node_id],
                                                    datatype, recvbuf, recvcounts, displs, datatype,
                                                    comm->pip_roots_comm, errflag);
            free(recvcounts);
            free(displs);
        }
        MPIR_ERR_CHECK(mpi_errno);
    }

    if (comm->node_comm && comm->node_comm->local_size > comm->node_procs_min) {
        mpi_errno =
            MPIDI_PIP_partial_bcast_intranode(recvbuf, count, datatype, 0, comm->node_comm,
                                              errflag);
    }

  fn_exit:
    return mpi_errno;
  fn_fail:
    goto fn_exit;
}
