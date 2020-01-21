/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil ; -*- */
/*
 *  (C) 2006 by Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 *
 *  Portions of this code were written by Intel Corporation.
 *  Copyright (C) 2011-2016 Intel Corporation.  Intel provides this material
 *  to Argonne National Laboratory subject to Software Grant and Corporate
 *  Contributor License Agreement dated February 8, 2012.
 */

#ifndef PIP_IMPL_H_INCLUDED
#define PIP_IMPL_H_INCLUDED

#include "pip_pre.h"

MPL_STATIC_INLINE_PREFIX void MPIDI_PIP_Task_safe_enqueue(MPIDI_PIP_task_queue_t * task_queue,
                                                          MPIDI_PIP_task_t * task)
{
    int err;
    MPID_Thread_mutex_lock(&task_queue->lock, &err);
    if (task_queue->tail) {
        task_queue->tail->task_next = task;
        task_queue->tail = task;
    } else {
        task_queue->head = task_queue->tail = task;
    }
    task_queue->task_num++;
    MPID_Thread_mutex_unlock(&task_queue->lock, &err);
    return;
}

MPL_STATIC_INLINE_PREFIX void MPIDI_PIP_Task_safe_dequeue(MPIDI_PIP_task_queue_t * task_queue,
                                                          MPIDI_PIP_task_t ** task)
{
    int err;
    MPIDI_PIP_task_t *old_head;
    MPID_Thread_mutex_lock(&task_queue->lock, &err);
    old_head = task_queue->head;
    if (old_head) {
        task_queue->head = old_head->task_next;
        if (task_queue->head == NULL)
            task_queue->tail = NULL;
        task_queue->task_num--;
    }
    MPID_Thread_mutex_unlock(&task_queue->lock, &err);

    *task = old_head;
    return;
}

MPL_STATIC_INLINE_PREFIX int MPIDI_PIP_Compl_task_enqueue(MPIDI_PIP_task_queue_t * compl_queue,
                                                          MPIDI_PIP_task_t * task)
{
    int mpi_errno = MPI_SUCCESS;

    if (compl_queue->tail) {
        compl_queue->tail->compl_next = task;
        compl_queue->tail = task;
    } else {
        compl_queue->head = compl_queue->tail = task;
    }

    compl_queue->task_num++;
    return mpi_errno;
}

MPL_STATIC_INLINE_PREFIX int MPIDI_PIP_Compl_task_delete_head(MPIDI_PIP_task_queue_t * compl_queue)
{
    int mpi_errno = MPI_SUCCESS, err;

    MPIDI_PIP_task_t *old_head = compl_queue->head;
    if (old_head) {
        compl_queue->head = old_head->compl_next;
        if (compl_queue->head == NULL)
            compl_queue->tail = NULL;
        compl_queue->task_num--;
    }
    return mpi_errno;
}

MPL_STATIC_INLINE_PREFIX void MPIDI_PIP_fflush_compl_task(MPIDI_PIP_task_queue_t * compl_queue)
{
    MPIDI_PIP_task_t *task = compl_queue->head;
    while (task && task->compl_flag) {
        MPIDI_PIP_Compl_task_delete_head(compl_queue);
        MPIR_Handle_obj_free(&MPIDI_Task_mem, task);
        task = compl_queue->head;
    }
    return;
}

MPL_STATIC_INLINE_PREFIX void MPIDI_PIP_do_task_copy(MPIDI_PIP_task_t * task)
{
    /* Note: now we only consider contiguous data copy */
    int task_kind = task->task_kind;
    int numa_local_rank = MPIDI_PIP_global.numa_local_rank;
    // MPIDI_PIP_global.local_copy_state[task_kind][numa_local_rank] = 1;
    MPIR_Memcpy(task->dest_buf, task->src_buf, task->data_sz);
    OPA_write_barrier();
    // MPIDI_PIP_global.local_copy_state[task_kind][numa_local_rank] = 0;
    task->compl_flag = 1;
    return;
}

MPL_STATIC_INLINE_PREFIX void MPIDI_PIP_exec_one_task(MPIDI_PIP_task_queue_t * task_queue,
                                                      MPIDI_PIP_task_queue_t * compl_queue)
{
    MPIDI_PIP_task_t *task;
    if (task_queue->head) {
        MPIDI_PIP_Task_safe_dequeue(task_queue, &task);
        if (task) {
            int numa_local_rank = MPIDI_PIP_global.numa_local_rank;
            MPIDI_PIP_global.local_copy_state[numa_local_rank] = 1;
            MPIDI_PIP_do_task_copy(task);
            MPIDI_PIP_global.local_copy_state[numa_local_rank] = 0;
        }
    }

    MPIDI_PIP_task_t *old_head = compl_queue->head;
    while (old_head != NULL && old_head == compl_queue->head)
        MPIDI_PIP_fflush_compl_task(compl_queue);
    return;
}

MPL_STATIC_INLINE_PREFIX void MPIDI_PIP_fflush_task()
{
    MPIDI_PIP_task_t *task;
    while (MPIDI_PIP_global.task_queue->head) {
        MPIDI_PIP_Task_safe_dequeue(MPIDI_PIP_global.task_queue, &task);
        if (task) {
            int numa_local_rank = MPIDI_PIP_global.numa_local_rank;
            MPIDI_PIP_global.local_copy_state[numa_local_rank] = 1;
            MPIDI_PIP_do_task_copy(task);
            MPIDI_PIP_global.local_copy_state[numa_local_rank] = 0;
        }
    }
    return;
}

MPL_STATIC_INLINE_PREFIX void MPIDI_PIP_Task_safe_dequeue_and_thd_test(MPIDI_PIP_task_queue_t *
                                                                       task_queue,
                                                                       int numa_num_procs,
                                                                       int cur_rmt_stealing_procs,
                                                                       MPIDI_PIP_global_t *
                                                                       victim_pip_global,
                                                                       MPIDI_PIP_task_t ** task)
{
    int err;
    MPIDI_PIP_task_t *old_head;
    int i;
    int cur_local_intra_copy = 0;
    int cur_local_inter_copy = 0;
    int *local_copy_array = victim_pip_global->local_copy_state;
    int *local_idle_array = victim_pip_global->local_idle_state;
    for (i = 0; i < numa_num_procs; ++i) {
        /* intra local copy */
        if (local_copy_array[i] || local_idle_array[i])
            cur_local_intra_copy++;
    }

    for (i = 0; i < MPIDI_PIP_THRESHOLD_CASE; ++i) {
        if (cur_local_intra_copy <= MPIDI_PIP_local_stealing_num[i])
            break;
    }

    if (i < MPIDI_PIP_THRESHOLD_CASE && cur_rmt_stealing_procs < MPIDI_PIP_local_stealing_map[i]) {
        MPID_Thread_mutex_lock(&task_queue->lock, &err);
        old_head = task_queue->head;
        if (old_head) {
            // int task_kind = old_head->task_kind;
            // printf("rmt rank %d - victim %d, cur_local_intra_copy %d (threshold %d), "
            //        " cur_local_inter_copy %d, cur_rmt_stealing_procs %d (threshold %d)\n",
            //        MPIDI_PIP_global.local_rank, victim_pip_global->local_rank, cur_local_intra_copy,
            //        MPIDI_PIP_upperbound_threshold[MPIDI_PIP_INTRA_TASK], cur_local_inter_copy,
            //        cur_rmt_stealing_procs,
            //        MPIDI_PIP_thp_map[MPIDI_PIP_INTRA_TASK][cur_local_intra_copy]);
            // fflush(stdout);
            task_queue->head = old_head->task_next;
            if (task_queue->head == NULL)
                task_queue->tail = NULL;
            task_queue->task_num--;
        }
        MPID_Thread_mutex_unlock(&task_queue->lock, &err);
    } else
        old_head = NULL;

    *task = old_head;
    return;
}

/* Stealing procedure */
MPL_STATIC_INLINE_PREFIX void MPIDI_PIP_steal_task()
{
#ifdef MPIDI_PIP_STEALING_ENABLE
    /* local stealing */
    int numa_id = MPIDI_PIP_global.local_numa_id;
    int numa_num_procs = MPIDI_PIP_global.numa_num_procs[numa_id];
    int victim = MPIDI_PIP_global.numa_cores_to_ranks[numa_id][rand() % numa_num_procs];
    MPIDI_PIP_task_t *task = NULL;

    if (victim != MPIDI_PIP_global.local_rank) {
        MPIDI_PIP_task_queue_t *victim_queue = MPIDI_PIP_global.task_queue_array[victim];
        if (victim_queue->head) {
            MPIDI_PIP_Task_safe_dequeue(victim_queue, &task);

            if (task) {
                // printf("rank %d - victim %d, task data_sz %ld, victim_numa_id %d, my_numa_id %d\n",
                //        MPIDI_PIP_global.local_rank, victim, task->data_sz,
                //        MPIDI_PIP_global.pip_global_array[victim]->local_numa_id,
                //        MPIDI_PIP_global.local_numa_id);
                // fflush(stdout);
                int numa_local_rank = MPIDI_PIP_global.numa_local_rank;
                MPIDI_PIP_global.local_copy_state[numa_local_rank] = 1;
                MPIDI_PIP_do_task_copy(task);
                MPIDI_PIP_global.local_copy_state[numa_local_rank] = 0;
                return;
            }
        }
    }

    /* check whether local tasks exists */
    int i, j;
    for (i = 0; i < numa_num_procs; ++i) {
        j = MPIDI_PIP_global.numa_cores_to_ranks[numa_id][i];
        if (MPIDI_PIP_global.task_queue_array[j]->head)
            return;
    }

    /* remote stealing */
    numa_id = rand() % MPIDI_PIP_global.num_numa_node;
    numa_num_procs = MPIDI_PIP_global.numa_num_procs[numa_id];

    if (numa_num_procs != 0 && numa_id != MPIDI_PIP_global.local_numa_id) {
        victim = MPIDI_PIP_global.numa_cores_to_ranks[numa_id][rand() % numa_num_procs];
        MPIDI_PIP_task_queue_t *victim_queue = MPIDI_PIP_global.task_queue_array[victim];

        if (victim_queue->head) {
            MPIDI_PIP_global_t *victim_pip_global = MPIDI_PIP_global.pip_global_array[victim];
            int cur_rmt_stealing_procs =
                OPA_fetch_and_add_int(victim_pip_global->rmt_steal_procs_ptr, 1);
            MPIDI_PIP_Task_safe_dequeue_and_thd_test(victim_queue, numa_num_procs,
                                                     cur_rmt_stealing_procs, victim_pip_global,
                                                     &task);
            // task = NULL;
            if (task) {
                MPIDI_PIP_do_task_copy(task);
            }
            OPA_decr_int(victim_pip_global->rmt_steal_procs_ptr);
        }
    }
#endif /* MPIDI_PIP_STEALING_ENABLE */
    return;
}

#endif
