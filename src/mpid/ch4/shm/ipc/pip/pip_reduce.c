#include "mpiimpl.h"
#include "pip_pre.h"
#include "pip_coll.h"
#include <math.h>

int MPIDI_PIP_Reduce_binomial_intranode(const void *sendbuf, void *recvbuf,
                                        int count, MPI_Datatype datatype, MPI_Op op,
                                        int root, MPIR_Comm * comm, MPIR_Errflag_t * errflag)
{
    int mpi_errno = MPI_SUCCESS;
    MPI_Status status;
    int comm_size, local_rank;
    int mask, relrank, source, lroot;
    MPI_Aint true_lb, true_extent, extent;
    int index = 0;
    int round = *comm->round_ptr;
    int free_flag = 0;
    int recv_copied = 0;
    volatile MPIDI_PIP_Coll_task_t *local_task;
    MPIDI_PIP_Coll_task_t *volatile ***tcoll_queue_array = comm->tcoll_queue_array;

    MPIR_CHKLMEM_DECL(2);

    if (count == 0)
        return MPI_SUCCESS;

    /* FIXME: right now we assume root == 0 and don't consider non-commutative op */
    MPIR_Assert(root == 0);
    comm_size = comm->local_size;
    local_rank = comm->rank;

    /* Create a temporary buffer */

    MPIR_Type_get_true_extent_impl(datatype, &true_lb, &true_extent);
    MPIR_Datatype_get_extent_macro(datatype, extent);

    if (local_rank != root && recvbuf == NULL) {
        MPIR_CHKLMEM_MALLOC(recvbuf, void *, count * (MPL_MAX(extent, true_extent)),
                            mpi_errno, "temporary buffer", MPL_MEM_BUFFER);

        /* adjust for potential negative lower bound in datatype */
        recvbuf = (void *) ((char *) recvbuf - true_lb);
        free_flag = 1;
    }

    if (local_rank == root && sendbuf != MPI_IN_PLACE) {
        mpi_errno = MPIR_Localcopy(sendbuf, count, datatype, recvbuf, count, datatype);
        MPIR_ERR_CHECK(mpi_errno);
        recv_copied = 1;
    }

    mask = 0x1;
    relrank = local_rank;
    while (mask < comm_size) {
        /* Receive */
        if ((mask & relrank) == 0) {
            source = (relrank | mask);
            if (source < comm_size) {
                while (tcoll_queue_array[source][round][index] == NULL)
                    MPL_sched_yield();
                local_task = tcoll_queue_array[source][round][index];

                if (local_rank != root && recv_copied == 0) {
                    mpi_errno = MPIR_Localcopy(sendbuf, count, datatype, recvbuf, count, datatype);
                    MPIR_ERR_CHECK(mpi_errno);
                    recv_copied = 1;
                }

                mpi_errno = MPIR_Reduce_local(local_task->addr, recvbuf, count, datatype, op);
                MPIR_ERR_CHECK(mpi_errno);

                sendbuf = recvbuf;
                __sync_fetch_and_add(&local_task->cnt, 1);
            }
        } else {
            /* I've received all that I'm going to.  Send my result to
             * my parent */
            local_task = (MPIDI_PIP_Coll_task_t *) MPIR_Handle_obj_alloc(&MPIDI_Coll_task_mem);
            local_task->addr = (void *) sendbuf;
            local_task->cnt = 0;
            local_task->type = TMPI_Reduce;
            __sync_synchronize();
            comm->tcoll_queue[round][index] = (MPIDI_PIP_Coll_task_t *) local_task;

            while (local_task->cnt != 1)
                MPL_sched_yield();
            if (free_flag)
                free(recvbuf);

            MPIR_Handle_obj_free(&MPIDI_Coll_task_mem, (void *) local_task);
            break;
        }

        mask <<= 1;
    }

    comm->tcoll_queue[round][index] = NULL;
    *comm->round_ptr = round ^ 1;

  fn_exit:
    MPIR_CHKLMEM_FREEALL();
    if (*errflag != MPIR_ERR_NONE)
        MPIR_ERR_SET(mpi_errno, *errflag, "**coll_fail");
    return mpi_errno;
  fn_fail:
    goto fn_exit;
}

int MPIDI_PIP_Reduce_partial_intranode(const void *sendbuf, void *recvbuf,
                                       int count, MPI_Datatype datatype, MPI_Op op,
                                       int root, MPIR_Comm * comm, MPIR_Errflag_t * errflag)
{
    int mpi_errno = MPI_SUCCESS;
    MPI_Aint extent;
    int index = 0;
    int round = *comm->round_ptr;
    int local_size = comm->local_size;
    int local_rank = comm->rank;
    int leader_num = comm->node_procs_min;
    int scnt, ecnt, copy_cnt;
    void *src_buf, *root_buf;
    volatile MPIDI_PIP_Coll_task_t *local_task;
    volatile MPIDI_PIP_Coll_task_t *root_task;
    MPIDI_PIP_Coll_task_t *volatile ***tcoll_queue_array = comm->tcoll_queue_array;

    if (local_size == leader_num) {
        if (local_rank == root && sendbuf != MPI_IN_PLACE) {
            mpi_errno = MPIR_Localcopy(sendbuf, count, datatype, recvbuf, count, datatype);
            MPIR_ERR_CHECK(mpi_errno);
        }
        goto fn_exit;
    }

    MPIR_Assert(count >= local_size);
    scnt = count * local_rank / local_size;
    ecnt = count * (local_rank + 1) / local_size;
    copy_cnt = ecnt - scnt;
    MPIR_Datatype_get_extent_macro(datatype, extent);

    if (local_rank == root) {
        if (sendbuf != MPI_IN_PLACE) {
            mpi_errno = MPIR_Localcopy(sendbuf, count, datatype, recvbuf, count, datatype);
            MPIR_ERR_CHECK(mpi_errno);
        }
        local_task = (MPIDI_PIP_Coll_task_t *) MPIR_Handle_obj_alloc(&MPIDI_Coll_task_mem);
        local_task->addr = (void *) recvbuf;
        local_task->cnt = 0;
        local_task->type = TMPI_Reduce;
        __sync_synchronize();
        comm->tcoll_queue[round][index] = (MPIDI_PIP_Coll_task_t *) local_task;
    } else if (local_rank >= leader_num) {
        local_task = (MPIDI_PIP_Coll_task_t *) MPIR_Handle_obj_alloc(&MPIDI_Coll_task_mem);
        local_task->addr = (void *) sendbuf;
        local_task->cnt = 0;
        local_task->type = TMPI_Reduce;
        __sync_synchronize();
        comm->tcoll_queue[round][index] = (MPIDI_PIP_Coll_task_t *) local_task;
    }

    while (tcoll_queue_array[root][round][index] == NULL)
        MPL_sched_yield();
    root_task = tcoll_queue_array[root][round][index];

    root_buf = root_task->addr;
    for (int i = leader_num; i < local_size; ++i) {
        while(tcoll_queue_array[i][round][index] == NULL)
            MPL_sched_yield();
        local_task = tcoll_queue_array[i][round][index];
        src_buf = local_task->addr;
        mpi_errno =
            MPIR_Reduce_local((char *) src_buf + scnt * extent, (char *) root_buf + scnt * extent,
                              copy_cnt, datatype, op);
        MPIR_ERR_CHECK(mpi_errno);

        __sync_fetch_and_add(&local_task->cnt, 1);
    }

    __sync_fetch_and_add(&root_task->cnt, 1);

    if (local_rank == root || local_rank >= leader_num) {
        local_task = comm->tcoll_queue[round][index];
        while (local_task->cnt != local_size)
            MPL_sched_yield();
        MPIR_Handle_obj_free(&MPIDI_Coll_task_mem, (void *) local_task);
    }

    comm->tcoll_queue[round][index] = NULL;
    *comm->round_ptr = round ^ 1;

  fn_exit:
    if (*errflag != MPIR_ERR_NONE)
        MPIR_ERR_SET(mpi_errno, *errflag, "**coll_fail");
    return mpi_errno;
  fn_fail:
    goto fn_exit;
}

int MPIDI_PIP_Reduce_full_intranode(const void *sendbuf, void *recvbuf,
                                    int count, MPI_Datatype datatype, MPI_Op op,
                                    int root, MPIR_Comm * comm, MPIR_Errflag_t * errflag)
{
    int mpi_errno = MPI_SUCCESS;
    MPI_Aint extent;
    int index = 0;
    int round = *comm->round_ptr;
    int local_size = comm->local_size;
    int local_rank = comm->rank;
    int leader_num = comm->node_procs_min;
    int scnt, ecnt, copy_cnt;
    void *src_buf, *root_buf;
    volatile MPIDI_PIP_Coll_task_t *local_task;
    volatile MPIDI_PIP_Coll_task_t *root_task;
    MPIDI_PIP_Coll_task_t *volatile ***tcoll_queue_array = comm->tcoll_queue_array;

    MPIR_Assert(count >= local_size);
    scnt = count * local_rank / local_size;
    ecnt = count * (local_rank + 1) / local_size;
    copy_cnt = ecnt - scnt;
    MPIR_Datatype_get_extent_macro(datatype, extent);

    if (local_rank == root) {
        if (sendbuf != MPI_IN_PLACE) {
            mpi_errno = MPIR_Localcopy(sendbuf, count, datatype, recvbuf, count, datatype);
            MPIR_ERR_CHECK(mpi_errno);
        }
        local_task = (MPIDI_PIP_Coll_task_t *) MPIR_Handle_obj_alloc(&MPIDI_Coll_task_mem);
        local_task->addr = (void *) recvbuf;
        local_task->cnt = 0;
        local_task->type = TMPI_Reduce;
        __sync_synchronize();
        comm->tcoll_queue[round][index] = (MPIDI_PIP_Coll_task_t *) local_task;
    } else {
        local_task = (MPIDI_PIP_Coll_task_t *) MPIR_Handle_obj_alloc(&MPIDI_Coll_task_mem);
        local_task->addr = (void *) sendbuf;
        local_task->cnt = 0;
        local_task->type = TMPI_Reduce;
        __sync_synchronize();
        comm->tcoll_queue[round][index] = (MPIDI_PIP_Coll_task_t *) local_task;
    }

    while (tcoll_queue_array[root][round][index] == NULL)
        MPL_sched_yield();
    root_task = tcoll_queue_array[root][round][index];

    root_buf = root_task->addr;
    for (int i = 0; i < local_size; ++i) {
        if (i == root)
            continue;
        while(tcoll_queue_array[i][round][index] == NULL)
            MPL_sched_yield();
        local_task = tcoll_queue_array[i][round][index];
        src_buf = local_task->addr;
        mpi_errno =
            MPIR_Reduce_local((char *) src_buf + scnt * extent, (char *) root_buf + scnt * extent,
                              copy_cnt, datatype, op);
        MPIR_ERR_CHECK(mpi_errno);

        __sync_fetch_and_add(&local_task->cnt, 1);
    }

    __sync_fetch_and_add(&root_task->cnt, 1);

    local_task = comm->tcoll_queue[round][index];
    while (local_task->cnt != local_size)
        MPL_sched_yield();
    MPIR_Handle_obj_free(&MPIDI_Coll_task_mem, (void *) local_task);

    comm->tcoll_queue[round][index] = NULL;
    *comm->round_ptr = round ^ 1;

  fn_exit:
    if (*errflag != MPIR_ERR_NONE)
        MPIR_ERR_SET(mpi_errno, *errflag, "**coll_fail");
    return mpi_errno;
  fn_fail:
    goto fn_exit;
}

int MPIDI_PIP_Reduce_leader_binomial_intranode(const void *sendbuf, void *recvbuf,
                                               int count, MPI_Datatype datatype, MPI_Op op,
                                               int root, int comm_size, int local_rank,
                                               int round, MPIR_Comm * comm,
                                               MPIR_Errflag_t * errflag)
{
    int mpi_errno = MPI_SUCCESS;
    MPI_Status status;
    int mask, relrank, source, lroot;
    MPI_Aint true_lb, true_extent, extent;
    int index = 0;
    int free_flag = 0;
    int recv_reduced = 0;
    volatile MPIDI_PIP_Coll_task_t *local_task;
    MPIDI_PIP_Coll_task_t **reduce_addr = comm->reduce_addr;
    MPIDI_PIP_Coll_task_t *volatile **reduce_addr_array = comm->reduce_addr_array;

    MPIR_CHKLMEM_DECL(2);

    if (count == 0)
        return MPI_SUCCESS;

    if (local_rank == root && sendbuf != MPI_IN_PLACE && comm_size == 1) {
        mpi_errno = MPIR_Reduce_local(sendbuf, recvbuf, count, datatype, op);
        MPIR_ERR_CHECK(mpi_errno);
        goto fn_exit;
    }

    /* FIXME: right now we assume root == 0 and don't consider non-commutative op */
    MPIR_Assert(root == 0);

    /* Create a temporary buffer */

    MPIR_Type_get_true_extent_impl(datatype, &true_lb, &true_extent);
    MPIR_Datatype_get_extent_macro(datatype, extent);

    if (local_rank != root && recvbuf == NULL) {
        MPIR_CHKLMEM_MALLOC(recvbuf, void *, count * (MPL_MAX(extent, true_extent)),
                            mpi_errno, "temporary buffer", MPL_MEM_BUFFER);

        /* adjust for potential negative lower bound in datatype */
        recvbuf = (void *) ((char *) recvbuf - true_lb);
        free_flag = 1;
    }

    mask = 0x1;
    relrank = local_rank;
    while (mask < comm_size) {
        /* Receive */
        if ((mask & relrank) == 0) {
            source = (relrank | mask);
            if (source < comm_size) {
                while (reduce_addr_array[source][round] == NULL)
                    MPL_sched_yield();
                local_task = reduce_addr_array[source][round];

                if (recv_reduced == 0) {
                    if (local_rank != root) {
                        mpi_errno =
                            MPIR_Localcopy(sendbuf, count, datatype, recvbuf, count, datatype);
                    } else {
                        mpi_errno = MPIR_Reduce_local(sendbuf, recvbuf, count, datatype, op);
                    }
                    MPIR_ERR_CHECK(mpi_errno);
                    recv_reduced = 1;
                }

                mpi_errno = MPIR_Reduce_local(local_task->addr, recvbuf, count, datatype, op);
                MPIR_ERR_CHECK(mpi_errno);

                sendbuf = recvbuf;
                __sync_fetch_and_add(&local_task->cnt, 1);
            }
        } else {
            /* I've received all that I'm going to.  Send my result to
             * my parent */
            local_task = (MPIDI_PIP_Coll_task_t *) MPIR_Handle_obj_alloc(&MPIDI_Coll_task_mem);
            local_task->addr = (void *) sendbuf;
            local_task->cnt = 0;
            local_task->type = TMPI_Reduce;
            __sync_synchronize();
            reduce_addr[round] = (MPIDI_PIP_Coll_task_t *) local_task;

            while (local_task->cnt != 1)
                MPL_sched_yield();
            if (free_flag)
                free(recvbuf);

            MPIR_Handle_obj_free(&MPIDI_Coll_task_mem, (void *) local_task);
            break;
        }

        mask <<= 1;
    }

  fn_exit:
    MPIR_CHKLMEM_FREEALL();
    if (*errflag != MPIR_ERR_NONE)
        MPIR_ERR_SET(mpi_errno, *errflag, "**coll_fail");
    return mpi_errno;
  fn_fail:
    goto fn_exit;
}


int MPIDI_PIP_Reduce_leader_rem_intranode(const void *sendbuf, void *recvbuf, void *rem_buf,
                                          int rem, int count, MPI_Datatype datatype, MPI_Op op,
                                          int root, int comm_size, int local_rank, int round,
                                          MPIR_Comm * comm, MPIR_Errflag_t * errflag)
{
    int mpi_errno = MPI_SUCCESS;

    if (local_rank < rem) {
        mpi_errno =
            MPIDI_PIP_Reduce_leader_binomial_intranode(sendbuf, rem_buf, count, datatype, op, 0,
                                                       rem, local_rank, round, comm, errflag);
        MPIR_ERR_CHECK(mpi_errno);
    }

    mpi_errno =
        MPIDI_PIP_Reduce_leader_binomial_intranode(sendbuf, recvbuf, count, datatype, op, 0,
                                                   comm_size, local_rank, round, comm, errflag);

  fn_exit:
    return mpi_errno;
  fn_fail:
    goto fn_exit;
}
