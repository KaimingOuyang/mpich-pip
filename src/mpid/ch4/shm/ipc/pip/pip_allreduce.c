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

int MPIDI_PIP_Allreduce_limit_num_intranode(const void *sendbuf, void *recvbuf,
                                            int count, MPI_Datatype datatype, MPI_Op op,
                                            int limit_num, int original_num,
                                            MPIR_Comm * comm, MPIR_Errflag_t * errflag)
{
    int mpi_errno = MPI_SUCCESS;
    int local_rank;
    int mask;
    int reduce_round = 0;
    int max_pof2, rem, dest;
    volatile MPIDI_PIP_Coll_task_t *reduce_addr;
    volatile MPIDI_PIP_Coll_task_t *rem_addr;
    int extent, step;
    int max_pof2_step;
    void **tmp_buf = comm->tmp_buf;
    void *local_send_buf = NULL;
    int null_procs, null_target_procs;
    MPIDI_PIP_Coll_task_t *volatile **reduce_addr_array = comm->reduce_addr_array;
    void *exchg_tmp;
    local_rank = comm->local_rank;
    max_pof2 = MPL_pof2(limit_num);
    rem = limit_num - max_pof2;
    MPIR_Assert(recvbuf != NULL);

    if (sendbuf != MPI_IN_PLACE && sendbuf != NULL) {
        mpi_errno = MPIR_Localcopy(sendbuf, count, datatype, recvbuf, count, datatype);
        MPIR_ERR_CHECK(mpi_errno);
    }

    if (local_rank >= limit_num)
        goto null_reduce;

    MPIR_Datatype_get_extent_macro(datatype, extent);
    if (max_pof2 - rem <= local_rank && local_rank < max_pof2) {
        /* wait for rem to post data */
        dest = local_rank + rem;
        local_send_buf = malloc(count * extent);
        memcpy(local_send_buf, sendbuf, count * extent);

        rem_addr = MPIR_PIP_Comm_get_task(reduce_addr_array[dest], 0);
        mpi_errno = MPIR_Reduce_local(rem_addr->addr, local_send_buf, count, datatype, op);
        MPIR_ERR_CHECK(mpi_errno);
        sendbuf = local_send_buf;
        __sync_fetch_and_add(&rem_addr->cnt, 1);
    } else if (max_pof2 <= local_rank && local_rank < limit_num) {
        rem_addr = MPIR_PIP_Comm_post_task(comm->reduce_addr, 0, 0, (void *) sendbuf);
        while (rem_addr->cnt != 1)
            MPL_sched_yield();
        comm->reduce_addr[0] = NULL;
        MPIR_Handle_obj_free(&MPIDI_Coll_task_mem, (void *) rem_addr);
        goto rem_reduce;
    }

    max_pof2_step = MPL_2log(max_pof2);
    tmp_buf[0] = (void *) sendbuf;
    exchg_tmp = tmp_buf[max_pof2_step];
    tmp_buf[max_pof2_step] = recvbuf;
    step = 0;
    mask = 1;
    while (mask < max_pof2) {
        if (local_rank < max_pof2) {
            MPIR_PIP_Comm_post_task(comm->reduce_addr, reduce_round, 0, tmp_buf[step]);

            dest = local_rank ^ mask;
            reduce_addr = MPIR_PIP_Comm_get_task(reduce_addr_array[dest], reduce_round);

            mpi_errno =
                MPIR_Localcopy(tmp_buf[step], count, datatype, tmp_buf[step + 1], count, datatype);
            MPIR_ERR_CHECK(mpi_errno);

            mpi_errno =
                MPIR_Reduce_local(reduce_addr->addr, tmp_buf[step + 1], count, datatype, op);
            MPIR_ERR_CHECK(mpi_errno);
            __sync_fetch_and_add(&reduce_addr->cnt, 1);
        }

        step += 1;
        reduce_round += 1;
        MPIR_Assert(reduce_round < MPIDI_COLL_TASK_PREALLOC);
        mask <<= 1;
    }

    tmp_buf[max_pof2_step] = exchg_tmp;
    
  rem_reduce:
    if (max_pof2 - rem <= local_rank && local_rank < max_pof2) {
        /* wait for rem to post data */
        dest = local_rank + rem;
        rem_addr = MPIR_PIP_Comm_get_task(reduce_addr_array[dest], 1);
        mpi_errno = MPIR_Localcopy(recvbuf, count, datatype, rem_addr->addr, count, datatype);
        MPIR_ERR_CHECK(mpi_errno);
        __sync_fetch_and_add(&rem_addr->cnt, 1);
    } else if (max_pof2 <= local_rank && local_rank < limit_num) {
        rem_addr = MPIR_PIP_Comm_post_task(comm->reduce_addr, 1, 0, recvbuf);
        while (rem_addr->cnt != 1)
            MPL_sched_yield();
        comm->reduce_addr[1] = NULL;
        MPIR_Handle_obj_free(&MPIDI_Coll_task_mem, (void *) rem_addr);
    }

  null_reduce:
    null_procs = original_num - limit_num;
    if (null_procs > 0) {
        if (limit_num <= local_rank) {
            rem_addr = MPIR_PIP_Comm_post_task(comm->reduce_addr, 0, 0, recvbuf);
            while (rem_addr->cnt != 1)
                MPL_sched_yield();
            comm->reduce_addr[0] = NULL;
            MPIR_Handle_obj_free(&MPIDI_Coll_task_mem, (void *) rem_addr);
        } else {
            null_target_procs = local_rank + limit_num;
            while (null_target_procs < original_num) {
                rem_addr = MPIR_PIP_Comm_get_task(reduce_addr_array[null_target_procs], 0);
                mpi_errno =
                    MPIR_Localcopy(recvbuf, count, datatype, rem_addr->addr, count, datatype);
                MPIR_ERR_CHECK(mpi_errno);
                __sync_fetch_and_add(&rem_addr->cnt, 1);
                null_target_procs += limit_num;
            }
        }
    }

    MPIR_PIP_Comm_reclaim_all_tasks(comm->reduce_addr, 1, reduce_round);
    MPIR_PIP_Comm_opt_intra_barrier(comm, original_num);

  fn_exit:
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
    int dst, dst_node, offset;
    // int rem_round = *comm->rem_round_ptr;
    volatile MPIDI_PIP_Coll_task_t *shared_addr;
    int rem_step_limit = rem_step / basek_1;
    // volatile MPIDI_PIP_Coll_task_t *rem_addr;
    MPIDI_PIP_Coll_task_t *volatile ***tcoll_queue_array = comm->tcoll_queue_array;
    int shared_round, reduce_round;
    MPIDI_PIP_Coll_task_t *volatile *root_shared_addr_ptr = comm->comms_array[0]->shared_addr;
    // MPIDI_PIP_Coll_task_t *volatile *root_rem_addr_ptr = comm->comms_array[0]->rem_addr;

    MPIR_CHKLMEM_DECL(2);

    if ((count == 0) && (sendbuf != MPI_IN_PLACE))
        goto fn_exit;

    if (sendbuf != MPI_IN_PLACE){
        mpi_errno = MPIR_Localcopy(sendbuf, count, datatype, recvbuf, count, datatype);
        MPIR_ERR_CHECK(mpi_errno);
    }

    comm_size = comm->node_count;
    rank = comm->rank;
    MPIR_Datatype_get_extent_macro(datatype, recvtype_extent);

    /* keep first rem results for rem */
    pofk_1 = 1;
    while (pofk_1 <= rem_step_limit)
        pofk_1 *= basek_1;
    rem = rem_step - pofk_1;
    while (rem >= pofk_1)
        rem -= pofk_1;
    if (rem > 0) {
        MPIR_CHKLMEM_MALLOC(rem_buf, void *, count * recvtype_extent, mpi_errno, "tmp_buf",
                            MPL_MEM_BUFFER);
        if (sendbuf == MPI_IN_PLACE)
            mpi_errno =
                MPIDI_PIP_Allreduce_recursive_bruck_internode(recvbuf, rem_buf, count, datatype, op,
                                                              comm, rem, errflag);
        else
            mpi_errno =
                MPIDI_PIP_Allreduce_recursive_bruck_internode(sendbuf, rem_buf, count, datatype, op,
                                                              comm, rem, errflag);
    }

    /* allocate a temporary buffer of the same size as recvbuf to receive intermediate results. */
    MPIR_Assert(recvbuf != NULL);
    MPIR_CHKLMEM_MALLOC(tmp_buf, void *, count * recvtype_extent, mpi_errno, "tmp_buf",
                        MPL_MEM_BUFFER);

    if (local_rank == 0 && sendbuf != MPI_IN_PLACE) {
        mpi_errno = MPIR_Localcopy(sendbuf, count, datatype, recvbuf, count, datatype);
        MPIR_ERR_CHECK(mpi_errno);
    }

    pofk_1 = 1;
    while (pofk_1 <= rem_step_limit) {
        offset = (local_rank + 1) * pofk_1;
        src_node = (node_id + offset) % comm_size;
        dst_node = (node_id - offset + comm_size) % comm_size;
        src = src_node * comm->node_procs_min + local_rank;
        dst = dst_node * comm->node_procs_min + local_rank;

        mpi_errno = MPIC_Sendrecv(recvbuf, count, datatype, dst,
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

        if (local_rank == 0) {
            mpi_errno = MPIR_Reduce_local(recvbuf, tmp_buf, count, datatype, op);
            MPIR_ERR_CHECK(mpi_errno);
        }
        mpi_errno =
            MPIDI_PIP_Allreduce_limit_num_intranode(tmp_buf, recvbuf, count, datatype, op,
                                                    leader_num, local_size, comm, errflag);
        MPIR_ERR_CHECK(mpi_errno);
        pofk_1 *= basek_1;
    }

    /* if comm_size is not a power of k + 1, one more step is needed */
    rem = rem_step - pofk_1;
    for (rem_leader_num = 0; rem_leader_num < local_size && rem > 0; ++rem_leader_num) {
        tmp_rem = rem > pofk_1 ? pofk_1 : rem;
        if (rem_leader_num == local_rank)
            my_rem = tmp_rem;
        rem -= tmp_rem;
    }

    if (my_rem > 0) {
        offset = (local_rank + 1) * pofk_1;
        src_node = (node_id + offset) % comm_size;
        dst_node = (node_id - offset + comm_size) % comm_size;
        src = src_node * comm->node_procs_min + local_rank;
        dst = dst_node * comm->node_procs_min + local_rank;

        if (my_rem == pofk_1) {
            mpi_errno = MPIC_Sendrecv(recvbuf, count, datatype,
                                      dst, MPIR_ALLREDUCE_TAG, tmp_buf, count, datatype,
                                      src, MPIR_ALLREDUCE_TAG, comm, MPI_STATUS_IGNORE, errflag);
        } else {
            mpi_errno = MPIC_Sendrecv(rem_buf, count, datatype,
                                      dst, MPIR_ALLREDUCE_TAG, tmp_buf, count, datatype,
                                      src, MPIR_ALLREDUCE_TAG, comm, MPI_STATUS_IGNORE, errflag);
        }
        if (mpi_errno) {
            /* for communication errors, just record the error but continue */
            *errflag =
                MPIX_ERR_PROC_FAILED ==
                MPIR_ERR_GET_CLASS(mpi_errno) ? MPIR_ERR_PROC_FAILED : MPIR_ERR_OTHER;
            MPIR_ERR_SET(mpi_errno, *errflag, "**fail");
            MPIR_ERR_ADD(mpi_errno_ret, mpi_errno);
        }

        if (local_rank == 0) {
            mpi_errno = MPIR_Reduce_local(recvbuf, tmp_buf, count, datatype, op);
            MPIR_ERR_CHECK(mpi_errno);
        }
        mpi_errno =
            MPIDI_PIP_Allreduce_limit_num_intranode(tmp_buf, recvbuf, count, datatype, op,
                                                    rem_leader_num, local_size, comm, errflag);
        MPIR_ERR_CHECK(mpi_errno);
    } else {
        mpi_errno =
            MPIDI_PIP_Allreduce_limit_num_intranode(NULL, recvbuf, count, datatype, op,
                                                    rem_leader_num, local_size, comm, errflag);
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

int MPIDI_PIP_Allreduce_reduce_scatter_internode(const void *sendbuf, void *recvbuf, int count,
                                                 MPI_Datatype datatype, MPI_Op op, MPIR_Comm * comm,
                                                 MPIR_Errflag_t * errflag)
{
    int chunk_sz = MPIR_CVAR_ALLREDUCE_CHUNK_SIZE;
    int node_count = comm->node_count;
    int node_id = comm->node_id;
    int src, dst;
    int leader_num = comm->node_procs_min;
    int local_rank = comm->local_rank;
    int sseg, eseg, seg_cnt, extent;
    MPIDI_PIP_Coll_task_t *shared_addr, *local_addr, *root_addr;
    MPIDI_PIP_Coll_task_t *local_task;
    int scnt;
    int ecnt;
    MPIR_Request *rreq = NULL;
    MPIR_Request **sreq;
    int sreq_cnt = 0;
    int round = *comm->round_ptr;
    void *dst_buf, *tmp_buf, *src_buf;
    int mpi_errno = MPI_SUCCESS;
    int shared_round = *comm->shared_round_ptr;
    int target_cnt, max_req_cnt;
    MPIDI_PIP_Coll_task_t *volatile ***tcoll_queue_array = comm->tcoll_queue_array;
    MPIDI_PIP_Coll_task_t *volatile *root_shared_addr_ptr = comm->comms_array[0]->shared_addr;

    if ((count == 0) && (sendbuf != MPI_IN_PLACE))
        goto fn_exit;

    MPIR_CHKLMEM_DECL(2);
    MPIR_Assert(count >= node_count);
    sseg = node_count * local_rank / leader_num;
    eseg = node_count * (local_rank + 1) / leader_num;

    MPIR_Datatype_get_extent_macro(datatype, extent);

    if (local_rank == 0) {
        shared_addr = (MPIDI_PIP_Coll_task_t *) MPIR_Handle_obj_alloc(&MPIDI_Coll_task_mem);
        shared_addr->addr = recvbuf;
        shared_addr->cnt = 0;
        __sync_synchronize();
        comm->shared_addr[shared_round] = shared_addr;
    } else {
        shared_addr = (MPIDI_PIP_Coll_task_t *) MPIR_Handle_obj_alloc(&MPIDI_Coll_task_mem);
        shared_addr->addr = (void *) sendbuf;
        shared_addr->cnt = 0;
        __sync_synchronize();
        comm->shared_addr[shared_round] = (void *) shared_addr;
    }
    MPIR_PIP_Comm_opt_intra_barrier(comm, leader_num);

    MPIR_CHKLMEM_MALLOC(tmp_buf, void *, MPIR_CVAR_ALLREDUCE_CHUNK_SIZE * extent, mpi_errno,
                        "tmp_buf", MPL_MEM_BUFFER);
    max_req_cnt = (count / node_count + 1) / chunk_sz + 1;
    MPIR_CHKLMEM_MALLOC(sreq, MPIR_Request **, sizeof(MPIR_Request *) * max_req_cnt, mpi_errno,
                        "sreq", MPL_MEM_BUFFER);
    memset(sreq, 0, sizeof(MPIR_Request *) * max_req_cnt);

    while (root_shared_addr_ptr[shared_round] == NULL)
        MPL_sched_yield();
    root_addr = root_shared_addr_ptr[shared_round];
    dst_buf = root_addr->addr;

    for (int i = sseg; i < eseg; ++i) {
        /* deal with node i data */
        int j, real_cnt;
        scnt = count * i / node_count;
        ecnt = count * (i + 1) / node_count;

        for (j = scnt; j < ecnt; j += chunk_sz) {
            int cur_recv_node = 0;
            real_cnt = chunk_sz > (ecnt - j) ? (ecnt - j) : chunk_sz;
            /* reduce local result */
            for (int k = 1; k < leader_num; ++k) {
                if (i == node_id && cur_recv_node < node_count) {
                    /* I am processing my own data, need to receive final results from others. */
                    if (cur_recv_node != node_id) {
                        mpi_errno =
                            MPIC_Irecv(tmp_buf, real_cnt, datatype,
                                       cur_recv_node * leader_num + local_rank, MPIR_ALLREDUCE_TAG,
                                       comm, &rreq);
                        MPIR_ERR_CHECK(mpi_errno);
                    }
                    cur_recv_node++;
                }
                // while (comm->comms_array[k]->shared_addr[shared_round] == NULL)
                //     MPL_sched_yield();
                local_addr = comm->comms_array[k]->shared_addr[shared_round];
                src_buf = local_addr->addr;
                mpi_errno =
                    MPIR_Reduce_local((char *) src_buf + j * extent, (char *) dst_buf + j * extent,
                                      real_cnt, datatype, op);
                MPIR_ERR_CHECK(mpi_errno);

                if (rreq) {
                    mpi_errno = MPIC_Wait(rreq, errflag);
                    MPIR_ERR_CHECK(mpi_errno);
                    rreq = NULL;

                    mpi_errno =
                        MPIR_Reduce_local(tmp_buf, (char *) dst_buf + j * extent,
                                          real_cnt, datatype, op);
                    MPIR_ERR_CHECK(mpi_errno);
                }
            }

            if (i != node_id) {
                mpi_errno =
                    MPIC_Isend((char *) dst_buf + j * extent,
                               real_cnt, datatype, i * leader_num + local_rank, MPIR_ALLREDUCE_TAG,
                               comm, &sreq[sreq_cnt++], errflag);
                MPIR_ERR_CHECK(mpi_errno);
            } else if (cur_recv_node < node_count) {
                /* receive the rest of chunks */
                for (int nindex = cur_recv_node; nindex < node_count; ++nindex) {
                    if (nindex == node_id)
                        continue;
                    mpi_errno =
                        MPIC_Recv(tmp_buf, real_cnt, datatype, nindex * leader_num + local_rank,
                                  MPIR_ALLREDUCE_TAG, comm, MPI_STATUS_IGNORE, errflag);
                    MPIR_ERR_CHECK(mpi_errno);
                    mpi_errno =
                        MPIR_Reduce_local(tmp_buf, (char *) dst_buf + j * extent,
                                          real_cnt, datatype, op);
                    MPIR_ERR_CHECK(mpi_errno);
                }
            }
        }

        if (sreq_cnt) {
            mpi_errno = MPIC_Waitall(sreq_cnt, sreq, MPI_STATUSES_IGNORE, errflag);
            MPIR_ERR_CHECK(mpi_errno);
            memset(sreq, 0, sizeof(MPIR_Request *) * sreq_cnt);
            sreq_cnt = 0;
        }
    }

    for (int i = 1; i < leader_num; ++i)
        __sync_fetch_and_add(&comm->comms_array[i]->shared_addr[shared_round]->cnt, 1);

    __sync_fetch_and_add(&root_addr->cnt, 1);
    while (comm->shared_addr[shared_round]->cnt != leader_num)
        MPL_sched_yield();
    MPIR_Handle_obj_free(&MPIDI_Coll_task_mem, (void *) comm->shared_addr[shared_round]);
    comm->shared_addr[shared_round] = NULL;

    *comm->shared_round_ptr = shared_round ^ 1;

  fn_exit:
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
            mpi_errno =
                MPIDI_PIP_Allreduce_limit_num_intranode(sendbuf, recvbuf, count, datatype, op,
                                                        comm->node_comm->local_size,
                                                        comm->node_comm->local_size,
                                                        comm->node_comm, errflag);
        } else {
            if (comm->node_count > 1) {
                mpi_errno =
                    MPIDI_PIP_Reduce_partial_intranode(sendbuf, recvbuf, count, datatype, op, 0,
                                                       comm->node_comm, errflag);
            } else {
                mpi_errno =
                    MPIDI_PIP_Reduce_full_intranode(sendbuf, recvbuf, count, datatype, op, 0,
                                                    comm->node_comm, errflag);
            }
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

            if (local_rank == 0) {
                mpi_errno =
                    MPIDI_PIP_Allreduce_reduce_scatter_internode(MPI_IN_PLACE, recvbuf, count,
                                                                 datatype, op, comm->pip_roots_comm,
                                                                 errflag);
            } else {
                mpi_errno =
                    MPIDI_PIP_Allreduce_reduce_scatter_internode(sendbuf, recvbuf, count, datatype,
                                                                 op, comm->pip_roots_comm, errflag);
            }
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

    if (comm->node_comm) {
        if (data_sz < MPIR_CVAR_ALLREDUCE_SHORT_MSG_SIZE) {
            mpi_errno =
                MPIDI_PIP_Reduce_bcast_intranode(recvbuf, count, datatype, 0, comm, errflag);
        } else {
            mpi_errno =
                MPIDI_PIP_Bcast_intranode(recvbuf, count, datatype, 0, comm->node_comm, errflag);
        }
    }

  fn_exit:
    return mpi_errno;
  fn_fail:
    goto fn_exit;
}
