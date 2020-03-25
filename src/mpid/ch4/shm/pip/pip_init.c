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
#include <sys/sysinfo.h>

int MPIDI_PIP_mpi_init_task_queue(MPIDI_PIP_task_queue_t * task_queue)
{
    int err;
    int mpi_errno = MPI_SUCCESS;
    task_queue->head = NULL;
    task_queue->partner = -1;
    MPID_Thread_mutex_create(&task_queue->lock, &err);
    if (err) {
        fprintf(stderr, "Init queue lock error\n");
        mpi_errno = MPI_ERR_OTHER;
    }
    return mpi_errno;
}

int MPIDI_PIP_mpi_init_hook(int rank, int size)
{
    int mpi_errno = MPI_SUCCESS;
    int i;

    MPIR_FUNC_VERBOSE_STATE_DECL(MPID_STATE_MPIDI_PIP_INIT_HOOK);
    MPIR_FUNC_VERBOSE_ENTER(MPID_STATE_MPIDI_PIP_INIT_HOOK);
    MPIR_CHKPMEM_DECL(7);

#ifdef MPL_USE_DBG_LOGGING
    extern MPL_dbg_class MPIDI_CH4_SHM_PIP_GENERAL;
    MPIDI_CH4_SHM_PIP_GENERAL = MPL_dbg_class_alloc("SHM_PIP", "shm_pip");
#endif /* MPL_USE_DBG_LOGGING */

    int num_local = MPIR_Process.local_size;
    int local_rank = MPIR_Process.local_rank;
    MPIDI_PIP_global.num_local = num_local;
    MPIDI_PIP_global.local_rank = local_rank;
    MPIDI_PIP_global.rank = rank;
    MPIDI_PIP_global.grank = MPIR_Process.node_local_map[local_rank];
    // printf("init - local_rank %d\n", MPIDI_PIP_global.local_rank);
    // fflush(stdout);
    /* Share MPIDI_PIP_global for future information inquiry purpose */
    uint64_t pip_global_addr = (uint64_t) & MPIDI_PIP_global;
    MPIDU_Init_shm_put(&pip_global_addr, sizeof(MPIDI_PIP_global_t *));
    MPIDU_Init_shm_barrier();
    MPIR_CHKPMEM_MALLOC(MPIDI_PIP_global.pip_global_array, MPIDI_PIP_global_t **,
                        sizeof(MPIDI_PIP_global_t *) * num_local,
                        mpi_errno, "pip global array", MPL_MEM_SHM);
    for (i = 0; i < num_local; i++)
        MPIDU_Init_shm_get(i, sizeof(MPIDI_PIP_global_t *), &MPIDI_PIP_global.pip_global_array[i]);
    MPIDU_Init_shm_barrier();

    /* bind rank to cpu */
    int num_numa_node = numa_num_task_nodes();
    char *MODE = getenv("BIND_MODE");

    if (strcmp(MODE, "INTER-P2P") == 0) {
        int cpus_per_numa = get_nprocs() / num_numa_node;
        if (local_rank == 1) {
            // printf("cpus/numa - %d\n", cpus_per_numa);
            cpu_set_t mask;
            CPU_ZERO(&mask);
            CPU_SET(cpus_per_numa, &mask);
            sched_setaffinity(getpid(), sizeof(cpu_set_t), &mask);
        } else if (local_rank == cpus_per_numa) {
            cpu_set_t mask;
            CPU_ZERO(&mask);
            CPU_SET(1, &mask);
            sched_setaffinity(getpid(), sizeof(cpu_set_t), &mask);
        }
    }

    /* NUMA info */
    int cpu = sched_getcpu();
    int local_numa_id = numa_node_of_cpu(cpu);
    MPIDI_PIP_global.num_numa_node = num_numa_node;
    MPIDI_PIP_global.local_numa_id = local_numa_id;
    MPIDI_PIP_global.partner_numa = local_numa_id ^ 1;

    /* Get NUMA info */
    MPIR_CHKPMEM_MALLOC(MPIDI_PIP_global.numa_cores_to_ranks, int **,
                        sizeof(int *) * num_numa_node, mpi_errno, "num numa array", MPL_MEM_SHM);
    for (i = 0; i < num_numa_node; ++i) {
        MPIDI_PIP_global.numa_cores_to_ranks[i] =
            MPL_malloc(sizeof(int) * num_local, MPL_MEM_OTHER);
        if (MPIDI_PIP_global.numa_cores_to_ranks[i] == NULL) {
            fprintf(stderr, "Allocating core to rank map array fails.\n");
            fflush(stdout);
            goto fn_fail;
        }
    }

    MPIR_CHKPMEM_MALLOC(MPIDI_PIP_global.numa_lrank_to_nid, int *,
                        sizeof(int *) * num_local, mpi_errno, "numa_lrank_to_nid", MPL_MEM_SHM);

    MPIDU_Init_shm_put(&MPIDI_PIP_global.local_numa_id, sizeof(int));
    MPIDU_Init_shm_barrier();

    MPIR_CHKPMEM_MALLOC(MPIDI_PIP_global.numa_num_procs, int *,
                        sizeof(int) * num_numa_node, mpi_errno, "numa # of procs", MPL_MEM_OTHER);
    memset(MPIDI_PIP_global.numa_num_procs, 0, sizeof(int) * num_numa_node);
    for (i = 0; i < num_local; ++i) {
        int numa_id;
        MPIDU_Init_shm_get(i, sizeof(int), &numa_id);
        if (i == local_rank)
            MPIDI_PIP_global.numa_local_rank = MPIDI_PIP_global.numa_num_procs[numa_id];
        MPIDI_PIP_global.numa_lrank_to_nid[i] = numa_id;
        MPIDI_PIP_global.numa_cores_to_ranks[numa_id][MPIDI_PIP_global.numa_num_procs[numa_id]++] =
            i;
    }
    MPIDU_Init_shm_barrier();

    MPIDI_PIP_global.numa_root_rank = MPIDI_PIP_global.numa_cores_to_ranks[local_numa_id][0];

    int local_root = MPIDI_PIP_global.numa_root_rank;
    if (local_root == local_rank) {
        /* root process in eahc NUMA node */
        MPIR_CHKPMEM_MALLOC(MPIDI_PIP_global.local_copy_state, int *,
                            sizeof(int) * MPIDI_PIP_global.numa_num_procs[local_numa_id], mpi_errno,
                            "local copy state", MPL_MEM_OTHER);

        memset(MPIDI_PIP_global.local_copy_state, 1,
               sizeof(int) * MPIDI_PIP_global.numa_num_procs[local_numa_id]);

        // MPIDI_PIP_global.local_idle_state =
        //     (int *) MPL_malloc(sizeof(int) * MPIDI_PIP_global.numa_num_procs[local_numa_id],
        //                        MPL_MEM_OTHER);
        // memset(MPIDI_PIP_global.local_idle_state, 0,
        //        sizeof(int) * MPIDI_PIP_global.numa_num_procs[local_numa_id]);

        MPIDI_PIP_global.bdw_checking =
            (OPA_int_t *) MPL_malloc(sizeof(OPA_int_t) * MPIDI_PIP_global.num_numa_node,
                                     MPL_MEM_OTHER);
        for (i = 0; i < MPIDI_PIP_global.num_numa_node; ++i)
            OPA_store_int(&MPIDI_PIP_global.bdw_checking[i], 0);

        MPIDI_PIP_global.allow_rmt_stealing =
            (int *) MPL_malloc(sizeof(int) * MPIDI_PIP_global.num_numa_node, MPL_MEM_OTHER);
        for (i = 0; i < MPIDI_PIP_global.num_numa_node; ++i)
            MPIDI_PIP_global.allow_rmt_stealing[i] = 0;
        MPIDU_Init_shm_barrier();
    } else {
        MPIDU_Init_shm_barrier();
        // MPIDI_PIP_global.numa_rmt_access =
        //     MPIDI_PIP_global.pip_global_array[local_root]->numa_rmt_access;
        MPIDI_PIP_global.local_copy_state =
            MPIDI_PIP_global.pip_global_array[local_root]->local_copy_state;
        // MPIDI_PIP_global.local_idle_state =
        //     MPIDI_PIP_global.pip_global_array[local_root]->local_idle_state;
    }

    MPIDI_PIP_global.allow_rmt_stealing_ptr =
        MPIDI_PIP_global.pip_global_array[local_root]->allow_rmt_stealing;
    MPIDI_PIP_global.bdw_checking_ptr = MPIDI_PIP_global.pip_global_array[local_root]->bdw_checking;

    /* Debug */
    // if (rank == 0) {
    //     for (i = 0; i < num_numa_node; ++i) {
    //         printf("NUMA %d [number %d] - ", i, MPIDI_PIP_global.numa_num_procs[i]);
    //         int j;
    //         for (j = 0; j < MPIDI_PIP_global.numa_num_procs[i]; ++j) {
    //             printf("%d ", MPIDI_PIP_global.numa_cores_to_ranks[i][j]);
    //         }
    //         printf("\n");
    //     }
    // }

    // MPIDU_Init_shm_barrier();
    // if (rank == 1) {
    //     for (i = 0; i < num_numa_node; ++i) {
    //         printf("NUMA %d [number %d] - ", i, MPIDI_PIP_global.numa_num_procs[i]);
    //         int j;
    //         for (j = 0; j < MPIDI_PIP_global.numa_num_procs[i]; ++j) {
    //             printf("%d ", MPIDI_PIP_global.numa_cores_to_ranks[i][j]);
    //         }
    //         printf("\n");
    //     }
    // }

    /* Allocate task queue */
    MPIR_CHKPMEM_MALLOC(MPIDI_PIP_global.task_queue, MPIDI_PIP_task_queue_t *,
                        sizeof(MPIDI_PIP_task_queue_t), mpi_errno, "pip task queue", MPL_MEM_SHM);
    mpi_errno = MPIDI_PIP_mpi_init_task_queue(MPIDI_PIP_global.task_queue);
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

    /* one-time barrier */
    OPA_store_int(&MPIDI_PIP_global.fin_procs, 0);
    MPIDU_Init_shm_barrier();
    MPIDI_PIP_global.fin_procs_ptr = &MPIDI_PIP_global.pip_global_array[0]->fin_procs;
    /* init partner queue */
    MPIDI_PIP_global.intrap_queue.head = MPIDI_PIP_global.intrap_queue.tail = NULL;
    MPIDI_PIP_global.interp_queue.head = MPIDI_PIP_global.interp_queue.tail = NULL;

    /* global rank to local rank */
    MPIDI_PIP_global.grank_to_lrank = (int *) MPL_malloc(MPIR_Process.size * sizeof(int),
                                                         MPL_MEM_SHM);
    for (i = 0; i < MPIR_Process.size; ++i) {
        MPIDI_PIP_global.grank_to_lrank[i] = -1;
    }
    for (i = 0; i < MPIR_Process.local_size; i++) {
        MPIDI_PIP_global.grank_to_lrank[MPIR_Process.node_local_map[i]] = i;
    }
    MPIDI_PIP_global.local_try = 0;
    MPIDI_PIP_global.rmt_stealing_cnt = 0;
    MPIDI_PIP_global.total_stealing_cnt = 0;
    /* For stealing rand seeds */
    srand(time(NULL) + MPIDI_PIP_global.local_rank * MPIDI_PIP_global.local_rank);

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

    OPA_add_int(MPIDI_PIP_global.fin_procs_ptr, 1);
    while (OPA_load_int(MPIDI_PIP_global.fin_procs_ptr) != MPIDI_PIP_global.num_local);
    if (MPIDI_PIP_global.local_rank == 0) {
        int rmt_stealing_cnt = 0;
        int total_stealing_cnt = 0;
        for (i = 0; i < MPIDI_PIP_global.num_local; ++i) {
            rmt_stealing_cnt += MPIDI_PIP_global.pip_global_array[i]->rmt_stealing_cnt;
            total_stealing_cnt += MPIDI_PIP_global.pip_global_array[i]->total_stealing_cnt;
        }
        printf("Node %d - total remote stealing cnt %d, total stealing %d\n",
               MPIDI_PIP_global.grank / 36, rmt_stealing_cnt, total_stealing_cnt);
        fflush(stdout);
    }
    // printf("rank %d - finalize pip\n", MPIDI_PIP_global.local_rank);
    // fflush(stdout);
    MPIR_Assert(MPIDI_PIP_global.task_queue->head == NULL);
    MPL_free(MPIDI_PIP_global.task_queue);

    MPL_free(MPIDI_PIP_global.task_queue_array);
    MPL_free(MPIDI_PIP_global.pip_global_array);
    MPL_free(MPIDI_PIP_global.grank_to_lrank);

    if (MPIDI_PIP_global.local_rank == MPIDI_PIP_global.numa_root_rank) {
        // MPL_free(MPIDI_PIP_global.numa_rmt_access);
        MPL_free(MPIDI_PIP_global.local_copy_state);
        // MPL_free(MPIDI_PIP_global.local_idle_state);
        MPL_free(MPIDI_PIP_global.bdw_checking);
        MPL_free(MPIDI_PIP_global.allow_rmt_stealing);
    }

    MPL_free(MPIDI_PIP_global.numa_num_procs);
    for (i = 0; i < MPIDI_PIP_global.num_numa_node; ++i)
        MPL_free(MPIDI_PIP_global.numa_cores_to_ranks[i]);
    MPL_free(MPIDI_PIP_global.numa_cores_to_ranks);

  fn_exit:
    MPIR_FUNC_VERBOSE_EXIT(MPID_STATE_MPIDI_PIP_FINALIZE_HOOK);
    return mpi_errno;
  fn_fail:
    goto fn_exit;
}

#endif
