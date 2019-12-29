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

MPL_STATIC_INLINE_PREFIX int MPIR_PIP_Type_dup(MPIR_Datatype * old_dtp, MPI_Datatype * newtype)
{
    int mpi_errno = MPI_SUCCESS;
    MPIR_Datatype *new_dtp;
    MPIR_FUNC_TERSE_STATE_DECL(MPID_STATE_MPI_PIP_TYPE_DUP);

    MPIR_ERRTEST_INITIALIZED_ORDIE();

    MPIR_FUNC_TERSE_ENTER(MPID_STATE_MPI_PIP_TYPE_DUP);
    MPID_THREAD_CS_ENTER(GLOBAL, MPIR_THREAD_GLOBAL_ALLFUNC_MUTEX);

    /* allocate new datatype object and handle */
    new_dtp = (MPIR_Datatype *) MPIR_Handle_obj_alloc(&MPIR_Datatype_mem);
    if (!new_dtp) {
        /* --BEGIN ERROR HANDLING-- */
        mpi_errno = MPIR_Err_create_code(MPI_SUCCESS, MPIR_ERR_RECOVERABLE,
                                         "MPIR_Type_dup", __LINE__, MPI_ERR_OTHER, "**nomem", 0);
        goto fn_fail;
        /* --END ERROR HANDLING-- */
    }

    /* fill in datatype */
    MPIR_Object_set_ref(new_dtp, 1);
    /* new_dtp->handle is filled in by MPIR_Handle_obj_alloc() */
    new_dtp->is_contig = old_dtp->is_contig;
    new_dtp->size = old_dtp->size;
    new_dtp->extent = old_dtp->extent;
    new_dtp->ub = old_dtp->ub;
    new_dtp->lb = old_dtp->lb;
    new_dtp->true_ub = old_dtp->true_ub;
    new_dtp->true_lb = old_dtp->true_lb;
    new_dtp->alignsize = old_dtp->alignsize;
    new_dtp->has_sticky_ub = old_dtp->has_sticky_ub;
    new_dtp->has_sticky_lb = old_dtp->has_sticky_lb;
    new_dtp->is_committed = old_dtp->is_committed;

    new_dtp->attributes = NULL; /* Attributes are copied in the
                                 * top-level MPI_Type_dup routine */
    new_dtp->name[0] = 0;       /* The Object name is not copied on
                                 * a dup */
    new_dtp->n_builtin_elements = old_dtp->n_builtin_elements;
    new_dtp->builtin_element_size = old_dtp->builtin_element_size;
    new_dtp->basic_type = old_dtp->basic_type;

    new_dtp->max_contig_blocks = old_dtp->max_contig_blocks;

    new_dtp->typerep = NULL;
    *newtype = new_dtp->handle;

    new_dtp->contents = NULL;

    if (old_dtp->is_committed) {
        MPIR_Assert(old_dtp->typerep != NULL);
        MPIR_Typerep_dup(old_dtp->typerep, &new_dtp->typerep);
        MPID_Type_commit_hook(new_dtp);
    } else {
        mpi_errno = MPI_ERR_OTHER;
        goto fn_fail;
    }

  fn_exit:
    MPIR_FUNC_TERSE_EXIT(MPID_STATE_MPI_PIP_TYPE_DUP);
    MPID_THREAD_CS_EXIT(GLOBAL, MPIR_THREAD_GLOBAL_ALLFUNC_MUTEX);
    return mpi_errno;

  fn_fail:
    /* --BEGIN ERROR HANDLING-- */
    *newtype = MPI_DATATYPE_NULL;
    mpi_errno = MPIR_Err_return_comm(NULL, __func__, mpi_errno);
    goto fn_exit;
    /* --END ERROR HANDLING-- */
}


/* use MPIR_Datatype_free(datatype_ptr); to free the duplicated datatype */


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

MPL_STATIC_INLINE_PREFIX void MPIDI_PIP_fflush_intermediate_task(MPIDI_PIP_task_queue_t *
                                                                 intermediate_queue)
{
    MPIDI_PIP_task_t *task = intermediate_queue->head;
    while (task && task->compl_flag) {
        MPIDI_PIP_Compl_task_delete_head(intermediate_queue);
        /* task is in intermediate queue */
        task->task_next = NULL;
        task->compl_next = NULL;
        MPIDI_PIP_Task_safe_enqueue(MPIDI_PIP_global.task_queue, task);
        MPIDI_PIP_Compl_task_enqueue(MPIDI_PIP_global.compl_queue, task);
        task = intermediate_queue->head;
    }
    return;
}

MPL_STATIC_INLINE_PREFIX void MPIDI_PIP_fflush_compl_task(MPIDI_PIP_task_queue_t * compl_queue)
{
    MPIDI_PIP_task_t *task = compl_queue->head;
    while (task && task->compl_flag == MPIDI_PIP_COMPLETE) {
        MPIDI_PIP_Compl_task_delete_head(compl_queue);
        if (task->copy_kind == MPIDI_PIP_PACK_UNPACK)
            MPIR_Handle_obj_free(&MPIDI_Cell_mem, task->cell);
        MPIR_Handle_obj_free(&MPIDI_Task_mem, task);

        task = compl_queue->head;
    }
    return;
}

MPL_STATIC_INLINE_PREFIX void MPIDI_PIP_steal_task_pack(MPIDI_PIP_task_t * task)
{
    MPI_Datatype src_dt_dup;
    MPI_Aint actual_bytes;
    // printf("steal rank %d - pack task->src_dt_ptr->typerep %p\n", MPIDI_PIP_global.local_rank, task->src_dt_ptr->typerep);
    // fflush(stdout);
    MPIR_PIP_Type_dup(task->src_dt_ptr, &src_dt_dup);

    // printf("steal rank %d - pack done\n", MPIDI_PIP_global.local_rank);
    // fflush(stdout);
    MPIR_Typerep_pack(task->src_buf, task->src_count, src_dt_dup, task->src_offset, task->dest_buf,
                      task->data_sz, &actual_bytes);
    MPIR_Assert(actual_bytes == task->data_sz);

    MPIR_Type_free_impl(&src_dt_dup);
    return;
}

MPL_STATIC_INLINE_PREFIX void MPIDI_PIP_self_task_pack(MPIDI_PIP_task_t * task)
{
    MPI_Aint actual_bytes;
    MPIR_Typerep_pack(task->src_buf, task->src_count, task->src_dt_ptr->handle, task->src_offset,
                      task->dest_buf, task->data_sz, &actual_bytes);
    MPIR_Assert(actual_bytes == task->data_sz);
    return;
}

MPL_STATIC_INLINE_PREFIX void MPIDI_PIP_steal_task_unpack(MPIDI_PIP_task_t * task)
{
    MPI_Datatype dest_dt_dup;
    MPI_Aint actual_bytes;
    MPIR_PIP_Type_dup(task->dest_dt_ptr, &dest_dt_dup);

    MPIR_Typerep_unpack(task->src_buf, task->data_sz, task->dest_buf, task->dest_count, dest_dt_dup,
                        task->dest_offset, &actual_bytes);
    MPIR_Assert(actual_bytes == task->data_sz);

    MPIR_Type_free_impl(&dest_dt_dup);
    return;
}

MPL_STATIC_INLINE_PREFIX void MPIDI_PIP_self_task_unpack(MPIDI_PIP_task_t * task)
{
    MPI_Aint actual_bytes;
    MPIR_Typerep_unpack(task->src_buf, task->data_sz, task->dest_buf, task->dest_count,
                        task->dest_dt_ptr->handle, task->dest_offset, &actual_bytes);
    MPIR_Assert(actual_bytes == task->data_sz);
    return;
}

MPL_STATIC_INLINE_PREFIX void MPIDI_PIP_steal_task_pack_unpack(MPIDI_PIP_task_t * task)
{
    if (task->compl_flag == MPIDI_PIP_NOT_COMPLETE) {
        MPI_Datatype src_dt_dup;
        MPI_Aint actual_bytes;
        // printf("steal rank %d - pack task->src_dt_ptr->typerep %p\n", MPIDI_PIP_global.local_rank, task->src_dt_ptr->typerep);
        // fflush(stdout);
        MPIR_PIP_Type_dup(task->src_dt_ptr, &src_dt_dup);
        // printf("steal rank %d - pack done\n", MPIDI_PIP_global.local_rank);
        // fflush(stdout);
        MPIR_Typerep_pack(task->src_buf, task->src_count, src_dt_dup, task->src_offset,
                          task->cell->load, task->data_sz, &actual_bytes);
        MPIR_Assert(actual_bytes == task->data_sz);

        MPIR_Type_free_impl(&src_dt_dup);
    } else {
        MPI_Datatype dest_dt_dup;
        MPI_Aint actual_bytes;
        // printf("steal rank %d - unpack task->dest_dt_ptr->typerep %p\n", MPIDI_PIP_global.local_rank, task->dest_dt_ptr->typerep);
        // fflush(stdout);
        MPIR_PIP_Type_dup(task->dest_dt_ptr, &dest_dt_dup);
        // printf("steal rank %d - unpack done\n", MPIDI_PIP_global.local_rank);
        // fflush(stdout);
        MPIR_Typerep_unpack(task->cell->load, task->data_sz, task->dest_buf, task->dest_count,
                            dest_dt_dup, task->dest_offset, &actual_bytes);
        MPIR_Assert(actual_bytes == task->data_sz);

        MPIR_Type_free_impl(&dest_dt_dup);
    }

    return;
}

MPL_STATIC_INLINE_PREFIX void MPIDI_PIP_self_task_pack_unpack(MPIDI_PIP_task_t * task)
{
    if (task->compl_flag == MPIDI_PIP_NOT_COMPLETE) {
        MPI_Aint actual_bytes;
        MPIR_Typerep_pack(task->src_buf, task->src_count, task->src_dt_ptr->handle,
                          task->src_offset, task->cell->load, task->data_sz, &actual_bytes);
        MPIR_Assert(actual_bytes == task->data_sz);
    } else {
        MPI_Aint actual_bytes;
        MPIR_Typerep_unpack(task->cell->load, task->data_sz, task->dest_buf, task->dest_count,
                            task->dest_dt_ptr->handle, task->dest_offset, &actual_bytes);
        MPIR_Assert(actual_bytes == task->data_sz);
    }

    return;
}

MPL_STATIC_INLINE_PREFIX void MPIDI_PIP_do_task_copy(MPIDI_PIP_task_t * task)
{
    /* Note: now we only consider contiguous data copy */
    int task_kind = task->task_kind;
    int copy_kind = task->copy_kind;
    int numa_local_rank = MPIDI_PIP_global.numa_local_rank;
    MPIDI_PIP_global.local_copy_state[task_kind][numa_local_rank] = 1;
    switch (copy_kind) {
        case MPIDI_PIP_MEMCPY:
            MPIR_Memcpy(task->dest_buf, task->src_buf, task->data_sz);
            break;
        case MPIDI_PIP_PACK:
            MPIDI_PIP_steal_task_pack(task);
            break;
        case MPIDI_PIP_UNPACK:
            MPIDI_PIP_steal_task_unpack(task);
            break;
        case MPIDI_PIP_PACK_UNPACK:
            MPIDI_PIP_steal_task_pack_unpack(task);
            break;
    }

    OPA_write_barrier();
    MPIDI_PIP_global.local_copy_state[task_kind][numa_local_rank] = 0;
    if (copy_kind == MPIDI_PIP_PACK_UNPACK && task->compl_flag == MPIDI_PIP_NOT_COMPLETE) {
        task->compl_flag = MPIDI_PIP_NEED_UNPACK;
    } else {
        task->compl_flag = MPIDI_PIP_COMPLETE;
    }

    return;
}

MPL_STATIC_INLINE_PREFIX void MPIDI_PIP_do_self_task_copy(MPIDI_PIP_task_t * task)
{
    /* Note: now we only consider contiguous data copy */
    int task_kind = task->task_kind;
    int copy_kind = task->copy_kind;
    int numa_local_rank = MPIDI_PIP_global.numa_local_rank;
    MPIDI_PIP_global.local_copy_state[task_kind][numa_local_rank] = 1;
    switch (copy_kind) {
        case MPIDI_PIP_MEMCPY:
            MPIR_Memcpy(task->dest_buf, task->src_buf, task->data_sz);
            break;
        case MPIDI_PIP_PACK:
            MPIDI_PIP_self_task_pack(task);
            break;
        case MPIDI_PIP_UNPACK:
            MPIDI_PIP_self_task_unpack(task);
            break;
        case MPIDI_PIP_PACK_UNPACK:
            MPIDI_PIP_self_task_pack_unpack(task);
            break;
    }

    OPA_write_barrier();
    MPIDI_PIP_global.local_copy_state[task_kind][numa_local_rank] = 0;
    if (copy_kind == MPIDI_PIP_PACK_UNPACK && task->compl_flag == MPIDI_PIP_NOT_COMPLETE) {
        task->compl_flag = MPIDI_PIP_NEED_UNPACK;
    } else {
        task->compl_flag = MPIDI_PIP_COMPLETE;
    }

    return;
}

/* only process itself can call MPIDI_PIP_exec_one_task and
 * MPIDI_PIP_fflush_task these two functions */
MPL_STATIC_INLINE_PREFIX void MPIDI_PIP_exec_one_task(MPIDI_PIP_task_queue_t * task_queue,
                                                      MPIDI_PIP_task_queue_t * compl_queue)
{
    MPIDI_PIP_task_t *task;
    if (task_queue->head) {
        MPIDI_PIP_Task_safe_dequeue(task_queue, &task);
        if (task)
            MPIDI_PIP_do_self_task_copy(task);
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
        if (task)
            MPIDI_PIP_do_self_task_copy(task);
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
    int **local_copy_array = victim_pip_global->local_copy_state;
    int *local_idle_array = victim_pip_global->local_idle_state;
    for (i = 0; i < numa_num_procs; ++i) {
        /* intra local copy */
        if (local_copy_array[MPIDI_PIP_INTRA_TASK][i] || local_idle_array[i])
            cur_local_intra_copy++;
        /* inter local copy */
        if (local_copy_array[MPIDI_PIP_INTER_TASK][i])
            cur_local_inter_copy++;
    }

    if (cur_local_intra_copy < MPIDI_PIP_upperbound_threshold[MPIDI_PIP_INTRA_TASK] &&
        cur_rmt_stealing_procs < MPIDI_RMT_COPY_PROCS_THRESHOLD &&
        (cur_rmt_stealing_procs + cur_local_inter_copy) <
        MPIDI_PIP_thp_map[MPIDI_PIP_INTRA_TASK][cur_local_intra_copy]) {
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
                MPIDI_PIP_do_task_copy(task);
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
