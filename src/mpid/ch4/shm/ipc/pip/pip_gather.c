#include "mpiimpl.h"
#include "pip_pre.h"
#include "pip_coll.h"
#include <math.h>

int MPIDI_PIP_Gather_intranode(const void *sendbuf, int sendcount,
                               MPI_Datatype sendtype, void *recvbuf, int recvcount,
                               MPI_Datatype recvtype, int root, MPIR_Comm * comm,
                               MPIR_Errflag_t * errflag)
{
    int recv_extent;
    int local_rank = comm->rank;
    int local_size = comm->local_size;
    int mpi_errno = MPI_SUCCESS;
    int sindex = *comm->sindex_ptr;
    int eindex = *comm->eindex_ptr;
    int round = *comm->round_ptr;
    volatile MPIDI_PIP_Coll_task_t *local_task;
    MPIDI_PIP_Coll_task_t *volatile ***tcoll_queue_array = comm->tcoll_queue_array;

    MPIR_Datatype_get_extent_macro(recvtype, recv_extent);
    MPIR_Assert(sindex == eindex);
    if (local_rank == root) {
        local_task = (MPIDI_PIP_Coll_task_t *) MPIR_Handle_obj_alloc(&MPIDI_Coll_task_mem);
        local_task->addr = recvbuf;
        local_task->cnt = 0;
        local_task->type = TMPI_Gather;
        local_task->free = 0;
        __sync_synchronize();
        comm->tcoll_queue[round][eindex] = (MPIDI_PIP_Coll_task_t *) local_task;

        mpi_errno =
            MPIR_Localcopy(sendbuf, sendcount, sendtype,
                           (char *) recvbuf + recv_extent * recvcount * local_rank, recvcount,
                           recvtype);
        MPIR_ERR_CHECK(mpi_errno);
        __sync_fetch_and_add(&local_task->cnt, 1);

        while (local_task->cnt != local_size)
            MPL_sched_yield();
        if (local_task->free)
            free(local_task->addr);
        comm->tcoll_queue[round][eindex] = NULL;
        MPIR_Handle_obj_free(&MPIDI_Coll_task_mem, (void *) local_task);
    } else {
        while (tcoll_queue_array[root][round][eindex] == NULL)
            MPL_sched_yield();

        local_task = tcoll_queue_array[root][round][eindex];
        MPIR_Assert(local_task->type == TMPI_Gather);

        mpi_errno =
            MPIR_Localcopy(sendbuf, sendcount, sendtype,
                           (char *) local_task->addr + recv_extent * recvcount * local_rank,
                           recvcount, recvtype);
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
