#include "mpiimpl.h"
#include "pip_pre.h"
#include "pip_coll.h"
#include <math.h>

int MPIDI_PIP_Bcast_intranode(void *buffer, int count, MPI_Datatype datatype, int root,
                              MPIR_Comm * comm, MPIR_Errflag_t * errflag)
{
    int extent;
    int local_rank = comm->rank;
    int local_size = comm->local_size;
    int mpi_errno = MPI_SUCCESS;
    void *root_buf;
    int sindex = *comm->sindex_ptr;
    int eindex = *comm->eindex_ptr;
    int round = *comm->round_ptr;
    volatile MPIDI_PIP_Coll_task_t *local_task;
    int done_count = 0;
    MPIDI_PIP_Coll_task_type_t task_type;
    MPIDI_PIP_Coll_task_t *volatile ***tcoll_queue_array = comm->tcoll_queue_array;

    MPIR_Datatype_get_extent_macro(datatype, extent);
    if (local_rank == root) {
        if (sindex == eindex) {
            local_task = (MPIDI_PIP_Coll_task_t *) MPIR_Handle_obj_alloc(&MPIDI_Coll_task_mem);
            local_task->addr = (void *) buffer;
            local_task->offset = 0;
            local_task->cnt = 1;
            local_task->type = TMPI_Bcast_End;
            local_task->count = count;
            local_task->free = 0;
            __sync_synchronize();
            comm->tcoll_queue[round][eindex] = (MPIDI_PIP_Coll_task_t *) local_task;
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
        do {
            while (tcoll_queue_array[root][round][eindex] == NULL)
                MPL_sched_yield();
            local_task = tcoll_queue_array[root][round][eindex];
            mpi_errno =
                MPIR_Localcopy(local_task->addr, local_task->count, datatype,
                               (char *) buffer + local_task->offset, local_task->count, datatype);
            MPIR_ERR_CHECK(mpi_errno);
            task_type = local_task->type;

            __sync_fetch_and_add(&local_task->cnt, 1);
            eindex = (eindex + 1) % MPIDI_COLL_TASK_PREALLOC;
        } while (task_type == TMPI_Bcast);
        MPIR_Assert(task_type == TMPI_Bcast_End);
    }

    *comm->sindex_ptr = 0;
    *comm->eindex_ptr = 0;
    *comm->round_ptr = round ^ 1;

  fn_exit:
    return mpi_errno;
  fn_fail:
    goto fn_exit;
}
