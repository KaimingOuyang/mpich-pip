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
    MPIR_Memcpy(task->dest_buf, task->src_buf, task->data_sz);
    MPL_atomic_write_barrier();
    task->compl_flag = 1;
    return;
}

MPL_STATIC_INLINE_PREFIX void MPIDI_PIP_exec_one_task(MPIDI_PIP_task_queue_t * task_queue,
                                                      MPIDI_PIP_task_queue_t * compl_queue)
{
    MPIDI_PIP_task_t *task;
    if (task_queue->head) {
        MPIDI_PIP_Task_safe_dequeue(task_queue, &task);
        if (task)
            MPIDI_PIP_do_task_copy(task);
    }
    MPIDI_PIP_fflush_compl_task(compl_queue);
    return;
}

MPL_STATIC_INLINE_PREFIX void MPIDI_PIP_fflush_task()
{
    MPIDI_PIP_task_t *task;
    while (MPIDI_PIP_global.task_queue->head) {
        MPIDI_PIP_Task_safe_dequeue(MPIDI_PIP_global.task_queue, &task);
        if (task)
            MPIDI_PIP_do_task_copy(task);
    }
    return;
}

/* Stealing procedure */
MPL_STATIC_INLINE_PREFIX void MPIDI_PIP_steal_task()
{
#ifdef MPIDI_PIP_STEALING_ENABLE
    int victim = rand() % MPIDI_PIP_global.num_local;
    MPIDI_PIP_task_t *task = NULL;

    if (victim != MPIDI_PIP_global.local_rank) {
        MPIDI_PIP_task_queue_t *victim_queue = MPIDI_PIP_global.task_queue_array[victim];
        if (victim_queue->head) {
            MPIDI_PIP_Task_safe_dequeue(victim_queue, &task);
            if (task) {
                // printf("rank %d - I am stealing process %d, task data_sz %ld\n", MPIDI_PIP_global.local_rank, victim, task->data_sz);
                // fflush(stdout);
                MPIDI_PIP_do_task_copy(task);
            }
        }
    }
#endif /* MPIDI_PIP_STEALING_ENABLE */
    return;
}

#endif
