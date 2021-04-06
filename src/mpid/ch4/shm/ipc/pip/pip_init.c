/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil ; -*- */
/*
 *  (C) 2019 by Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#include "mpiimpl.h"
#include "mpidu_init_shm.h"
#include "pip_pre.h"

#ifdef MPIDI_CH4_SHM_ENABLE_PIP

#include <numa.h>
#include <sched.h>

extern int shmmod_progress_cnt;

int MPIDI_PIP_mpi_init_task_queue(MPIDI_PIP_task_queue_t * task_queue)
{
    int err;
    int mpi_errno = MPI_SUCCESS;
    task_queue->head = task_queue->tail = NULL;
    MPID_Thread_mutex_create(&task_queue->lock, &err);
    if (err) {
        fprintf(stderr, "Init queue lock error\n");
        mpi_errno = MPI_ERR_OTHER;
    }
    task_queue->task_num = 0;
    return mpi_errno;
}

void MPIDI_PIP_init_progress_funcs()
{
    int num_local = MPIR_Process.local_size;

    MPL_thread_self(&MPIDI_PIP_global.self);

    MPIDI_PIP_global.pm = MPL_malloc(sizeof(MPIDI_PIP_progress_t), MPL_MEM_OTHER);

    MPIDI_PIP_global.pm->ch4_progress_counts = MPIDI_global.progress_counts;
    shmmod_progress_cnt = 0;
    MPIDI_PIP_global.pm->shmmod_progress_cnt = &shmmod_progress_cnt;
    MPIDI_PIP_global.pm->in_progress = 0;
    MPIDI_PIP_global.pm->netmod_avail = 0;
    MPIDI_PIP_global.pm->shmmod_avail = 0;
    MPIDI_global.in_progress = &MPIDI_PIP_global.pm->in_progress;
    MPIDI_global.netmod_avail = &MPIDI_PIP_global.pm->netmod_avail;
    MPIDI_global.shmmod_avail = &MPIDI_PIP_global.pm->shmmod_avail;

    /* here I assume we use global progress */
    MPIDI_PIP_global.pm->vci = MPIDI_global.vci;
    MPIDI_PIP_global.pm->enable = 0;
    MPIDI_global.pm_enable = &MPIDI_PIP_global.pm->enable;

    MPIDI_PIP_global.pm->netmod_progress = MPIDI_NM_progress;
    MPIDI_PIP_global.pm->shmmod_progress = MPIDI_SHM_progress;

    MPIDI_PIP_global.pm_array =
        (MPIDI_PIP_progress_t **) MPL_malloc(sizeof(MPIDI_PIP_progress_t *) * num_local,
                                             MPL_MEM_OTHER);

    MPIDU_Init_shm_put(&MPIDI_PIP_global.pm, sizeof(MPIDI_PIP_progress_t *));
    MPIDU_Init_shm_barrier();

    for (int i = 0; i < num_local; i++)
        MPIDU_Init_shm_get(i, sizeof(MPIDI_PIP_progress_t *), &MPIDI_PIP_global.pm_array[i]);
    MPIDU_Init_shm_barrier();

    return;
}

void MPIDI_PIP_finalize_progress_funcs()
{
    MPL_free(MPIDI_PIP_global.pm_array);
    MPL_free(MPIDI_PIP_global.pm);
    return;
}

int MPIDI_PIP_init_numa_info()
{
    int mpi_errno = MPI_SUCCESS;
    int cpu = sched_getcpu();
    int local_numa_id = numa_node_of_cpu(cpu);
    int num_numa_node = numa_num_task_nodes();
    MPIDI_PIP_global.max_numa_node = num_numa_node;
    MPIDI_PIP_global.local_numa_id = local_numa_id;

    if (MPIR_Process.comm_world->node_comm) {
        mpi_errno =
            MPIR_Comm_split_impl(MPIR_Process.comm_world->node_comm, local_numa_id,
                                 MPIR_Process.local_rank, &MPIDI_PIP_global.numa_comm);
        MPIR_ERR_CHECK(mpi_errno);
    } else {
        MPIDI_PIP_global.numa_comm = NULL;
    }

    int numa_nprocs, max_numa_nprocs;
    if (MPIDI_PIP_global.numa_comm) {
        numa_nprocs = MPIDI_PIP_global.numa_comm->local_size;
        MPIR_Errflag_t errflag = MPIR_ERR_NONE;
        mpi_errno =
            MPIR_Allreduce(&numa_nprocs, &max_numa_nprocs, 1, MPI_INT, MPI_MAX,
                           MPIR_Process.comm_world->node_comm, &errflag);
        MPIR_ERR_CHECK(mpi_errno);
    } else {
        numa_nprocs = max_numa_nprocs = 1;
    }

    int *numa_cur_ranks;
    MPIDI_PIP_global.numa_map = MPL_malloc(num_numa_node * sizeof(int *), MPL_MEM_OTHER);
    MPIR_Assert(MPIDI_PIP_global.numa_map);

    numa_cur_ranks = MPL_malloc(num_numa_node * sizeof(int), MPL_MEM_OTHER);
    MPIR_Assert(numa_cur_ranks);
    memset(numa_cur_ranks, 0, sizeof(int) * num_numa_node);

    for (int i = 0; i < num_numa_node; ++i) {
        MPIDI_PIP_global.numa_map[i] = MPL_malloc(max_numa_nprocs * sizeof(int), MPL_MEM_OTHER);
        MPIR_Assert(MPIDI_PIP_global.numa_map);
    }

    MPIDU_Init_shm_put(&local_numa_id, sizeof(int));
    MPIDU_Init_shm_barrier();
    for (int i = 0; i < MPIR_Process.local_size; i++) {
        int numa_id;
        MPIDU_Init_shm_get(i, sizeof(int), &numa_id);
        MPIDI_PIP_global.numa_map[numa_id][numa_cur_ranks[numa_id]++] = i;
    }
    MPIDU_Init_shm_barrier();

    MPIDI_PIP_global.numa_local_size = numa_cur_ranks;
    MPIDI_PIP_global.stealing_initialized = 1;

  fn_exit:
    return mpi_errno;
  fn_fail:
    goto fn_exit;
}

void MPIDI_PIP_finalize_numa_info()
{
    MPIDI_PIP_global.stealing_initialized = 0;
    if (MPIDI_PIP_global.numa_comm) {
        MPL_free(MPIDI_PIP_global.numa_local_size);
        for (int i = 0; i < MPIDI_PIP_global.max_numa_node; ++i)
            MPL_free(MPIDI_PIP_global.numa_map[i]);
        MPL_free(MPIDI_PIP_global.numa_map);
        MPIR_Comm_free_impl(MPIDI_PIP_global.numa_comm);
    }
    return;
}

int MPIDI_PIP_mpi_init_hook(int rank, int size)
{
    int mpi_errno = MPI_SUCCESS;
    int i;

    MPIR_FUNC_VERBOSE_STATE_DECL(MPID_STATE_MPIDI_PIP_INIT_HOOK);
    MPIR_FUNC_VERBOSE_ENTER(MPID_STATE_MPIDI_PIP_INIT_HOOK);
    MPIR_CHKPMEM_DECL(4);

#ifdef MPL_USE_DBG_LOGGING
    extern MPL_dbg_class MPIDI_CH4_SHM_PIP_GENERAL;
    MPIDI_CH4_SHM_PIP_GENERAL = MPL_dbg_class_alloc("SHM_PIP", "shm_pip");
#endif /* MPL_USE_DBG_LOGGING */

    int num_local = MPIR_Process.local_size;
    MPIDI_PIP_global.num_local = num_local;
    MPIDI_PIP_global.local_rank = MPIR_Process.local_rank;
    MPIDI_PIP_global.rank = rank;
    MPIDI_PIP_global.stealing_initialized = 0;

    /* bind local rank to core id */
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(MPIR_Process.local_rank, &set);
    if (sched_setaffinity(getpid(), sizeof(set), &set) == -1) {
        printf("set affinity fails\n");
        exit(1);
    }

    /* Allocate task queue */
    MPIR_CHKPMEM_MALLOC(MPIDI_PIP_global.task_queue, MPIDI_PIP_task_queue_t *,
                        sizeof(MPIDI_PIP_task_queue_t), mpi_errno, "pip task queue", MPL_MEM_SHM);
    mpi_errno = MPIDI_PIP_mpi_init_task_queue(MPIDI_PIP_global.task_queue);
    MPIR_ERR_CHECK(mpi_errno);

    /* Init local completion queue */
    MPIR_CHKPMEM_MALLOC(MPIDI_PIP_global.compl_queue, MPIDI_PIP_task_queue_t *,
                        sizeof(MPIDI_PIP_task_queue_t), mpi_errno, "pip compl queue", MPL_MEM_SHM);
    mpi_errno = MPIDI_PIP_mpi_init_task_queue(MPIDI_PIP_global.compl_queue);
    MPIR_ERR_CHECK(mpi_errno);

    /* Get task queue array */
    MPIDU_Init_shm_put(&MPIDI_PIP_global.task_queue, sizeof(MPIDI_PIP_task_queue_t *));
    MPIDU_Init_shm_barrier();
    MPIR_CHKPMEM_MALLOC(MPIDI_PIP_global.task_queue_array, MPIDI_PIP_task_queue_t **,
                        sizeof(MPIDI_PIP_task_queue_t *) * num_local,
                        mpi_errno, "pip task queue array", MPL_MEM_SHM);
    for (i = 0; i < num_local; i++)
        MPIDU_Init_shm_get(i, sizeof(MPIDI_PIP_task_queue_t *),
                           &MPIDI_PIP_global.task_queue_array[i]);
    MPIDU_Init_shm_barrier();

    /* Share MPIDI_PIP_global for future information inquiry purpose */
    MPIDU_Init_shm_put(&MPIDI_PIP_global, sizeof(MPIDI_PIP_global_t *));
    MPIDU_Init_shm_barrier();
    MPIR_CHKPMEM_MALLOC(MPIDI_PIP_global.pip_global_array, MPIDI_PIP_global_t **,
                        sizeof(MPIDI_PIP_task_queue_t *) * num_local,
                        mpi_errno, "pip global array", MPL_MEM_SHM);
    for (i = 0; i < num_local; i++)
        MPIDU_Init_shm_get(i, sizeof(MPIDI_PIP_global_t *), &MPIDI_PIP_global.pip_global_array[i]);
    MPIDU_Init_shm_barrier();

    /* For stealing rand seeds */
    srand(time(NULL) + MPIDI_PIP_global.local_rank * MPIDI_PIP_global.local_rank);

    MPIDI_global.in_progress = NULL;
#ifdef PIP_PROGRESS_STEALING_ENABLE
    MPIDI_PIP_init_progress_funcs();
#endif
    /* Disable XPMEM module */
    MPIR_CVAR_CH4_XPMEM_ENABLE = 0;

  fn_exit:
    MPIR_FUNC_VERBOSE_EXIT(MPID_STATE_MPIDI_PIP_INIT_HOOK);
    return mpi_errno;
  fn_fail:
    MPIR_CHKPMEM_REAP();
    goto fn_exit;
}

int MPIDI_PIP_mpi_finalize_hook(void)
{
    int mpi_errno = MPI_SUCCESS;
    int i, ret = 0;
    MPIR_FUNC_VERBOSE_STATE_DECL(MPID_STATE_MPIDI_PIP_FINALIZE_HOOK);
    MPIR_FUNC_VERBOSE_ENTER(MPID_STATE_MPIDI_PIP_FINALIZE_HOOK);

    MPIR_Assert(MPIDI_PIP_global.task_queue->task_num == 0);
    MPL_free(MPIDI_PIP_global.task_queue);

    MPIR_Assert(MPIDI_PIP_global.compl_queue->task_num == 0);
    MPL_free(MPIDI_PIP_global.compl_queue);

    MPL_free(MPIDI_PIP_global.task_queue_array);
    MPL_free(MPIDI_PIP_global.pip_global_array);

#ifdef PIP_PROGRESS_STEALING_ENABLE
    MPIDI_PIP_finalize_progress_funcs();
#endif

  fn_exit:
    MPIR_FUNC_VERBOSE_EXIT(MPID_STATE_MPIDI_PIP_FINALIZE_HOOK);
    return mpi_errno;
  fn_fail:
    goto fn_exit;
}

/* does not allow progress stealing at finalize */
int MPIDI_PIP_mpi_stealing_shutdown()
{
    int mpi_errno = MPI_SUCCESS;
    MPIR_Errflag_t errflag = MPIR_ERR_NONE;

    MPIDI_PIP_global.pm->enable = 0;
    if (MPIR_Process.comm_world->node_comm != NULL)
        mpi_errno = MPIR_Barrier(MPIR_Process.comm_world->node_comm, &errflag);
    return mpi_errno;
}

#endif
