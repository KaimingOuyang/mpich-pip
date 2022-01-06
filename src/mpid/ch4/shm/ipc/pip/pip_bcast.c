#include "mpiimpl.h"
#include "pip_pre.h"
#include "pip_coll.h"
#include <math.h>

int MPIDI_PIP_Bcast_intranode(void *buffer, int count, MPI_Datatype datatype, int root,
                              MPIR_Comm * comm, MPIR_Errflag_t * errflag)
{
    int local_rank = comm->rank;
    int local_size = comm->local_size;
    int mpi_errno = MPI_SUCCESS;
    MPIDI_PIP_Coll_easy_task_t *local_task;

    if (local_rank == root) {
        local_task = MPIR_Comm_post_easy_task(buffer, TMPI_Bcast, 0, 0, local_size - 1, comm);
        while (local_task->complete != local_task->target_cmpl)
            MPL_sched_yield();

    } else {
        local_task = MPIR_Comm_get_easy_task(comm, root, TMPI_Bcast);
        mpi_errno = MPIR_Localcopy(local_task->addr, count, datatype, buffer, count, datatype);
        MPIR_ERR_CHECK(mpi_errno);
        __sync_fetch_and_add(&local_task->complete, 1);
    }

  fn_exit:
    return mpi_errno;
  fn_fail:
    goto fn_exit;
}

int MPIDI_PIP_partial_bcast_intranode(void *buffer, int count, MPI_Datatype datatype, int root,
                                      MPIR_Comm * comm, MPIR_Errflag_t * errflag)
{
    int local_rank = comm->rank;
    int local_size = comm->local_size;
    int mpi_errno = MPI_SUCCESS;
    MPIDI_PIP_Coll_easy_task_t *local_task;

    if (local_rank == root) {
        local_task =
            MPIR_Comm_post_easy_task(buffer, TMPI_Bcast, 0, 0, local_size - comm->node_procs_min,
                                     comm);
        while (local_task->complete != local_task->target_cmpl)
            MPL_sched_yield();

    } else if (local_rank >= comm->node_procs_min) {
        local_task = MPIR_Comm_get_easy_task(comm, root, TMPI_Bcast);
        mpi_errno = MPIR_Localcopy(local_task->addr, count, datatype, buffer, count, datatype);
        MPIR_ERR_CHECK(mpi_errno);
        __sync_fetch_and_add(&local_task->complete, 1);
    }

  fn_exit:
    return mpi_errno;
  fn_fail:
    goto fn_exit;
}
