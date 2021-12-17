#include "mpiimpl.h"
#include "pip_pre.h"
#include "pip_coll.h"
#include <math.h>

int MPIDI_PIP_Gather_intranode(const void *sendbuf, int sendcount,
                               MPI_Datatype sendtype, void *recvbuf, int recvcount,
                               MPI_Datatype recvtype, int root, MPIR_Comm * comm,
                               MPIR_Errflag_t * errflag)
{
    size_t recv_extent;
    int local_rank = comm->rank;
    int local_size = comm->local_size;
    int mpi_errno = MPI_SUCCESS;
    volatile MPIDI_PIP_Coll_easy_task_t *local_task;

    MPIR_Datatype_get_extent_macro(recvtype, recv_extent);
    if (local_rank == root) {
        local_task = MPIR_Comm_post_easy_task(recvbuf, TMPI_Allgather, 0, 0, local_size - 1, comm);
        mpi_errno =
            MPIR_Localcopy(sendbuf, sendcount, sendtype,
                           (char *) recvbuf +
                           recv_extent * (size_t) recvcount * (size_t) local_rank, recvcount,
                           recvtype);
        MPIR_ERR_CHECK(mpi_errno);
        while (local_task->target_cmpl != local_task->complete)
            MPL_sched_yield();
    } else {
        local_task = MPIR_Comm_get_easy_task(comm, root, TMPI_Allgather);
        mpi_errno =
            MPIR_Localcopy(sendbuf, sendcount, sendtype,
                           (char *) local_task->addr +
                           recv_extent * (size_t) recvcount * (size_t) local_rank, recvcount,
                           recvtype);
        MPIR_ERR_CHECK(mpi_errno);
        __sync_fetch_and_add(&local_task->complete, 1);
    }

  fn_exit:
    return mpi_errno;
  fn_fail:
    goto fn_exit;
}
