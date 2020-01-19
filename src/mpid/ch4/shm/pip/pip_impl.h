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

MPL_STATIC_INLINE_PREFIX void MPIDI_PIP_exec_stolen_task(MPIDI_PIP_task_queue_t * task_queue);
MPL_STATIC_INLINE_PREFIX void MPIDI_PIP_exec_self_task(MPIDI_PIP_task_queue_t * task_queue);
MPL_STATIC_INLINE_PREFIX int MPIDI_PIP_copy_size_decision(MPIDI_PIP_task_t * task);
MPL_STATIC_INLINE_PREFIX void MPIDI_PIP_exec_memcpy_task(MPIDI_PIP_task_t * task, int copy_sz,
                                                         MPI_Aint offset);

MPL_STATIC_INLINE_PREFIX void MPIDI_PIP_publish_task(MPIDI_PIP_task_queue_t * task_queue,
                                                     MPIDI_PIP_task_t * task)
{
    int err;
    MPID_Thread_mutex_lock(&task_queue->lock, &err);
    task_queue->head = task;
    MPID_Thread_mutex_unlock(&task_queue->lock, &err);
}


MPL_STATIC_INLINE_PREFIX void MPIDI_PIP_cancel_task(MPIDI_PIP_task_queue_t * task_queue)
{
    int err;
    MPID_Thread_mutex_lock(&task_queue->lock, &err);
    task_queue->head = NULL;
    MPID_Thread_mutex_unlock(&task_queue->lock, &err);
}


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


/*************************************************/
/********** process exec its own tasks ***********/
/*************************************************/
MPL_STATIC_INLINE_PREFIX void MPIDI_PIP_exec_self_pack_task(MPIDI_PIP_task_t * task, int copy_sz,
                                                            MPI_Aint offset)
{
    MPI_Datatype src_dt_dup = task->src_dt_ptr->handle;
    MPI_Aint actual_bytes;
    int numa_local_rank = MPIDI_PIP_global.numa_local_rank;
    void *cur_dest_buf = (void *) ((uint64_t) task->dest_buf + offset);

    MPIR_Typerep_pack(task->src_buf, task->src_count, src_dt_dup,
                      task->init_src_offset + offset, cur_dest_buf, copy_sz, &actual_bytes);
    OPA_write_barrier();

    OPA_add_int(&task->done_data_sz, copy_sz);
    MPIR_Assert(actual_bytes == copy_sz);
    return;
}


MPL_STATIC_INLINE_PREFIX void MPIDI_PIP_exec_self_unpack_task(MPIDI_PIP_task_t * task, int copy_sz,
                                                              MPI_Aint offset)
{
    MPI_Datatype dest_dt_dup = task->dest_dt_ptr->handle;
    MPI_Aint actual_bytes;
    int numa_local_rank = MPIDI_PIP_global.numa_local_rank;
    void *cur_src_buf = (void *) ((uint64_t) task->src_buf + offset);

    MPIR_Typerep_unpack(cur_src_buf, copy_sz, task->dest_buf, task->dest_count, dest_dt_dup,
                        task->init_dest_offset + offset, &actual_bytes);
    OPA_write_barrier();

    OPA_add_int(&task->done_data_sz, copy_sz);
    MPIR_Assert(actual_bytes == copy_sz);
    return;
}


MPL_STATIC_INLINE_PREFIX void MPIDI_PIP_exec_self_pack_unpack_task(MPIDI_PIP_task_t * task,
                                                                   int copy_sz, MPI_Aint offset)
{

    MPI_Datatype src_dt_dup = task->src_dt_ptr->handle;
    MPI_Datatype dest_dt_dup = task->dest_dt_ptr->handle;
    MPI_Aint actual_bytes;
    int numa_local_rank = MPIDI_PIP_global.numa_local_rank;

    MPIR_Typerep_pack(task->src_buf, task->src_count, src_dt_dup,
                      task->init_src_offset + offset, MPIDI_PIP_global.pkt_load, copy_sz,
                      &actual_bytes);
    MPIR_Assert(actual_bytes == copy_sz);

    MPIR_Typerep_unpack(MPIDI_PIP_global.pkt_load, copy_sz, task->dest_buf, task->dest_count,
                        dest_dt_dup, task->init_dest_offset + offset, &actual_bytes);
    MPIR_Assert(actual_bytes == copy_sz);
    OPA_write_barrier();

    OPA_add_int(&task->done_data_sz, copy_sz);
    return;
}


MPL_STATIC_INLINE_PREFIX void MPIDI_PIP_exec_self_task(MPIDI_PIP_task_queue_t * task_queue)
{
    void *src_buf, *dest_buf;
    int copy_sz, err, copy_kind;
    MPI_Aint offset;
    MPIDI_PIP_task_t *task;

    MPID_Thread_mutex_lock(&task_queue->lock, &err);
    task = task_queue->head;
    if (task) {
        if (task->reverse_enqueue) {
            if (task->cur_offset != 0) {
                copy_sz = MPIDI_PIP_copy_size_decision(task);
                task->cur_offset -= copy_sz;
                offset = task->cur_offset;
                copy_kind = task->copy_kind;
            } else {
                copy_sz = 0;
            }
        } else {
            if (task->cur_offset != task->orig_data_sz) {
                copy_sz = MPIDI_PIP_copy_size_decision(task);
                offset = task->cur_offset;
                task->cur_offset += copy_sz;
                copy_kind = task->copy_kind;
            } else {
                copy_sz = 0;
            }
        }
    } else {
        copy_sz = 0;
    }

    MPID_Thread_mutex_unlock(&task_queue->lock, &err);

    if (copy_sz) {
        int numa_local_rank = MPIDI_PIP_global.numa_local_rank;
        MPIDI_PIP_global.local_copy_state[numa_local_rank] = 1;
        switch (copy_kind) {
            case MPIDI_PIP_MEMCPY:
                MPIDI_PIP_exec_memcpy_task(task, copy_sz, offset);
                break;
            case MPIDI_PIP_PACK:
                MPIDI_PIP_exec_self_pack_task(task, copy_sz, offset);
                break;
            case MPIDI_PIP_UNPACK:
                MPIDI_PIP_exec_self_unpack_task(task, copy_sz, offset);
                break;
            case MPIDI_PIP_PACK_UNPACK:
                MPIDI_PIP_exec_self_pack_unpack_task(task, copy_sz, offset);
                break;
        }
        MPIDI_PIP_global.local_copy_state[numa_local_rank] = 0;
    }

    return;
}


MPL_STATIC_INLINE_PREFIX void MPIDI_PIP_init_memcpy_task(MPIDI_PIP_task_t * task, void *src_buf,
                                                         void *dest_buf, MPI_Aint data_sz)
{
    task->copy_kind = MPIDI_PIP_MEMCPY;

    task->src_buf = src_buf;
    task->dest_buf = dest_buf;

    task->orig_data_sz = data_sz;
    if (data_sz >= MPIDI_REVERSE_ENQUEUE_THRESHOLD) {
        task->cur_offset = data_sz;
        task->reverse_enqueue = 1;
    } else {
        task->cur_offset = 0;
        task->reverse_enqueue = 0;
    }
    OPA_store_int(&task->done_data_sz, 0);
    return;
}


MPL_STATIC_INLINE_PREFIX void MPIDI_PIP_memcpy_task_enqueue(char *src_buf,
                                                            char *dest_buf, MPI_Aint data_sz)
{
    if (data_sz <= MPIDI_PIP_PKT_32KB) {
        MPIDI_PIP_global.local_copy_state[MPIDI_PIP_global.numa_local_rank] = 1;
        MPIR_Memcpy(dest_buf, src_buf, data_sz);
        OPA_write_barrier();
        MPIDI_PIP_global.local_copy_state[MPIDI_PIP_global.numa_local_rank] = 0;
    } else {
        MPIDI_PIP_task_t *task = (MPIDI_PIP_task_t *) MPIR_Handle_obj_alloc(&MPIDI_Task_mem);
        MPIDI_PIP_init_memcpy_task(task, src_buf, dest_buf, data_sz);
        MPIDI_PIP_publish_task(MPIDI_PIP_global.task_queue, task);
        do {
            // if (task->cur_offset != 0)
            MPIDI_PIP_exec_self_task(MPIDI_PIP_global.task_queue);
        } while (OPA_load_int(&task->done_data_sz) != task->orig_data_sz);

        MPIDI_PIP_cancel_task(MPIDI_PIP_global.task_queue);
        MPIR_Handle_obj_free(&MPIDI_Task_mem, task);
    }
    return;
}


MPL_STATIC_INLINE_PREFIX void MPIDI_PIP_init_pack_task(MPIDI_PIP_task_t * task, void *src_buf,
                                                       MPI_Aint src_count, MPI_Aint inoffset,
                                                       MPIR_Datatype * src_dt_ptr, void *dest_buf,
                                                       MPI_Aint data_sz)
{
    task->copy_kind = MPIDI_PIP_PACK;

    task->src_buf = src_buf;
    task->dest_buf = dest_buf;
    task->cur_offset = data_sz;
    task->orig_data_sz = data_sz;
    OPA_store_int(&task->done_data_sz, 0);

    task->src_count = src_count;
    task->src_dt_ptr = src_dt_ptr;
    task->init_src_offset = inoffset;
    return;
}

MPL_STATIC_INLINE_PREFIX void MPIDI_PIP_pack_task_enqueue(void *src_buf, MPI_Aint src_count,
                                                          MPIR_Datatype * src_dt_ptr,
                                                          char *dest_buf, MPI_Aint data_sz)
{
    MPI_Datatype src_dt_dup;

    MPIR_PIP_Type_dup(src_dt_ptr, &src_dt_dup);
    if (data_sz <= MPIDI_PIP_PKT_32KB) {
        MPI_Aint actual_bytes;
        MPIDI_PIP_global.local_copy_state[MPIDI_PIP_global.numa_local_rank] = 1;
        MPIR_Typerep_pack(src_buf, src_count, src_dt_dup, 0, dest_buf, data_sz, &actual_bytes);
        OPA_write_barrier();
        MPIDI_PIP_global.local_copy_state[MPIDI_PIP_global.numa_local_rank] = 0;
        MPIR_Assert(actual_bytes == data_sz);
    } else {
        MPIR_Datatype *src_dt_dup_ptr;
        MPIDI_PIP_task_t *task = (MPIDI_PIP_task_t *) MPIR_Handle_obj_alloc(&MPIDI_Task_mem);
        MPIR_Datatype_get_ptr(src_dt_dup, src_dt_dup_ptr);

        MPIDI_PIP_init_pack_task(task, src_buf, src_count, 0, src_dt_dup_ptr, dest_buf, data_sz);
        MPIDI_PIP_publish_task(MPIDI_PIP_global.task_queue, task);
        do {
            if (task->cur_offset != 0)
                MPIDI_PIP_exec_self_task(MPIDI_PIP_global.task_queue);
        } while (OPA_load_int(&task->done_data_sz) != task->orig_data_sz);

        MPIDI_PIP_cancel_task(MPIDI_PIP_global.task_queue);
        MPIR_Handle_obj_free(&MPIDI_Task_mem, task);
    }

    MPIR_Type_free_impl(&src_dt_dup);
    return;
}


MPL_STATIC_INLINE_PREFIX void MPIDI_PIP_init_unpack_task(MPIDI_PIP_task_t * task, void *src_buf,
                                                         void *dest_buf, MPI_Aint dest_count,
                                                         MPI_Aint outoffset,
                                                         MPIR_Datatype * dest_dt_ptr,
                                                         MPI_Aint data_sz)
{
    task->copy_kind = MPIDI_PIP_UNPACK;

    task->src_buf = src_buf;
    task->dest_buf = dest_buf;
    task->cur_offset = data_sz;
    task->orig_data_sz = data_sz;
    OPA_store_int(&task->done_data_sz, 0);

    task->dest_count = dest_count;
    task->dest_dt_ptr = dest_dt_ptr;
    task->init_dest_offset = outoffset;
    return;
}

MPL_STATIC_INLINE_PREFIX void MPIDI_PIP_unpack_task_enqueue(void *src_buf,
                                                            void *dest_buf, MPI_Aint dest_count,
                                                            MPI_Datatype dest_dt, MPI_Aint data_sz)
{
    if (data_sz <= MPIDI_PIP_PKT_32KB) {
        MPI_Aint actual_bytes;
        MPIDI_PIP_global.local_copy_state[MPIDI_PIP_global.numa_local_rank] = 1;
        MPIR_Typerep_unpack(src_buf, data_sz, dest_buf, dest_count, dest_dt, 0, &actual_bytes);
        OPA_write_barrier();
        MPIDI_PIP_global.local_copy_state[MPIDI_PIP_global.numa_local_rank] = 0;
        MPIR_Assert(actual_bytes == data_sz);
    } else {
        MPIR_Datatype *dest_dt_ptr;
        MPIDI_PIP_task_t *task = (MPIDI_PIP_task_t *) MPIR_Handle_obj_alloc(&MPIDI_Task_mem);
        MPIR_Datatype_get_ptr(dest_dt, dest_dt_ptr);

        MPIDI_PIP_init_unpack_task(task, src_buf, dest_buf, dest_count, 0, dest_dt_ptr, data_sz);
        MPIDI_PIP_publish_task(MPIDI_PIP_global.task_queue, task);
        do {
            if (task->cur_offset != 0)
                MPIDI_PIP_exec_self_task(MPIDI_PIP_global.task_queue);
        } while (OPA_load_int(&task->done_data_sz) != task->orig_data_sz);

        MPIDI_PIP_cancel_task(MPIDI_PIP_global.task_queue);
        MPIR_Handle_obj_free(&MPIDI_Task_mem, task);
    }
    return;
}


MPL_STATIC_INLINE_PREFIX void MPIDI_PIP_init_pack_unpack_task(MPIDI_PIP_task_t * task,
                                                              void *src_buf, MPI_Aint src_count,
                                                              MPI_Aint inoffset,
                                                              MPIR_Datatype * src_dt_ptr,
                                                              void *dest_buf, MPI_Aint dest_count,
                                                              MPI_Aint outoffset,
                                                              MPIR_Datatype * dest_dt_ptr,
                                                              MPI_Aint data_sz)
{
    task->copy_kind = MPIDI_PIP_PACK_UNPACK;

    task->src_buf = src_buf;
    task->dest_buf = dest_buf;
    task->cur_offset = data_sz;
    task->orig_data_sz = data_sz;
    OPA_store_int(&task->done_data_sz, 0);

    task->src_count = src_count;
    task->src_dt_ptr = src_dt_ptr;
    task->init_src_offset = inoffset;

    task->dest_count = dest_count;
    task->dest_dt_ptr = dest_dt_ptr;
    task->init_dest_offset = outoffset;
    return;
}

MPL_STATIC_INLINE_PREFIX void MPIDI_PIP_pack_unpack_task_enqueue(void *src_buf,
                                                                 MPI_Aint src_count,
                                                                 MPIR_Datatype * src_dt_ptr,
                                                                 void *dest_buf,
                                                                 MPI_Aint dest_count,
                                                                 MPI_Datatype dest_dt,
                                                                 MPI_Aint data_sz)
{
    MPI_Datatype src_dt_dup;

    MPIR_PIP_Type_dup(src_dt_ptr, &src_dt_dup);
    if (data_sz <= MPIDI_PIP_PKT_32KB) {
        MPI_Aint actual_bytes;
        MPIDI_PIP_global.local_copy_state[MPIDI_PIP_global.numa_local_rank] = 1;
        MPIR_Typerep_pack(src_buf, src_count, src_dt_dup, 0, MPIDI_PIP_global.pkt_load, data_sz,
                          &actual_bytes);
        MPIR_Assert(actual_bytes == data_sz);
        MPIR_Typerep_unpack(MPIDI_PIP_global.pkt_load, data_sz, dest_buf, dest_count, dest_dt, 0,
                            &actual_bytes);
        MPIR_Assert(actual_bytes == data_sz);
        OPA_write_barrier();
        MPIDI_PIP_global.local_copy_state[MPIDI_PIP_global.numa_local_rank] = 0;
    } else {
        MPIR_Datatype *src_dt_dup_ptr;
        MPIR_Datatype *dest_dt_ptr;
        MPIDI_PIP_task_t *task = (MPIDI_PIP_task_t *) MPIR_Handle_obj_alloc(&MPIDI_Task_mem);
        MPIR_Datatype_get_ptr(dest_dt, dest_dt_ptr);
        MPIR_Datatype_get_ptr(src_dt_dup, src_dt_dup_ptr);

        MPIDI_PIP_init_pack_unpack_task(task, src_buf, src_count, 0, src_dt_dup_ptr,
                                        dest_buf, dest_count, 0, dest_dt_ptr, data_sz);
        MPIDI_PIP_publish_task(MPIDI_PIP_global.task_queue, task);
        do {
            if (task->cur_offset != 0)
                MPIDI_PIP_exec_self_task(MPIDI_PIP_global.task_queue);
        } while (OPA_load_int(&task->done_data_sz) != task->orig_data_sz);

        MPIDI_PIP_cancel_task(MPIDI_PIP_global.task_queue);
        MPIR_Handle_obj_free(&MPIDI_Task_mem, task);
    }
    MPIR_Type_free_impl(&src_dt_dup);

    return;
}


MPL_STATIC_INLINE_PREFIX int MPIDI_PIP_copy_size_decision(MPIDI_PIP_task_t * task)
{
    int copy_sz;
    /* This size decision function is only for bebop machine */
    static const int PKT_16KB = 1 << 14;
    static const int PKT_32KB = 1 << 15;
    static const int PKT_64KB = 1 << 16;
    static const int PKT_96KB = PKT_32KB + PKT_64KB;
    static const int PKT_512KB = 1 << 19;
    int remain_data;
    if(task->reverse_enqueue){
        remain_data = task->cur_offset;
    }else{
        remain_data = task->orig_data_sz - task->cur_offset;
    }
    if (remain_data <= PKT_16KB)
        copy_sz = remain_data;
    else if (remain_data <= PKT_96KB)
        copy_sz = PKT_16KB;
    else if (remain_data <= PKT_512KB)
        copy_sz = PKT_32KB;
    else
        copy_sz = PKT_96KB;

    return copy_sz;
}


MPL_STATIC_INLINE_PREFIX void MPIDI_PIP_exec_memcpy_task(MPIDI_PIP_task_t * task, int copy_sz,
                                                         MPI_Aint offset)
{
    int numa_local_rank = MPIDI_PIP_global.numa_local_rank;
    void *cur_src_buf = (void *) ((uint64_t) task->src_buf + offset);
    void *cur_dest_buf = (void *) ((uint64_t) task->dest_buf + offset);

    MPIR_Memcpy(cur_dest_buf, cur_src_buf, copy_sz);
    OPA_write_barrier();

    OPA_add_int(&task->done_data_sz, copy_sz);
    return;
}

MPL_STATIC_INLINE_PREFIX void MPIDI_PIP_exec_stolen_pack_task(MPIDI_PIP_task_t * task,
                                                              int copy_sz, MPI_Aint offset)
{
    MPI_Datatype src_dt_dup;
    MPI_Aint actual_bytes;
    int numa_local_rank = MPIDI_PIP_global.numa_local_rank;
    void *cur_dest_buf = (void *) ((uint64_t) task->dest_buf + offset);

    MPIR_PIP_Type_dup(task->src_dt_ptr, &src_dt_dup);

    MPIR_Typerep_pack(task->src_buf, task->src_count, src_dt_dup,
                      task->init_src_offset + offset, cur_dest_buf, copy_sz, &actual_bytes);
    OPA_write_barrier();

    OPA_add_int(&task->done_data_sz, copy_sz);
    MPIR_Assert(actual_bytes == copy_sz);
    MPIR_Type_free_impl(&src_dt_dup);
    return;
}

MPL_STATIC_INLINE_PREFIX void MPIDI_PIP_exec_stolen_unpack_task(MPIDI_PIP_task_t * task,
                                                                int copy_sz, MPI_Aint offset)
{
    MPI_Datatype dest_dt_dup;
    MPI_Aint actual_bytes;
    int numa_local_rank = MPIDI_PIP_global.numa_local_rank;
    void *cur_src_buf = (void *) ((uint64_t) task->src_buf + offset);

    MPIR_PIP_Type_dup(task->dest_dt_ptr, &dest_dt_dup);

    MPIR_Typerep_unpack(cur_src_buf, copy_sz, task->dest_buf, task->dest_count, dest_dt_dup,
                        task->init_dest_offset + offset, &actual_bytes);
    OPA_write_barrier();

    OPA_add_int(&task->done_data_sz, copy_sz);
    MPIR_Assert(actual_bytes == copy_sz);
    MPIR_Type_free_impl(&dest_dt_dup);
    return;
}

MPL_STATIC_INLINE_PREFIX void MPIDI_PIP_exec_stolen_pack_unpack_task(MPIDI_PIP_task_t * task,
                                                                     int copy_sz, MPI_Aint offset)
{

    MPI_Datatype src_dt_dup;
    MPI_Datatype dest_dt_dup;
    MPI_Aint actual_bytes;
    int numa_local_rank = MPIDI_PIP_global.numa_local_rank;

    MPIR_PIP_Type_dup(task->src_dt_ptr, &src_dt_dup);
    MPIR_PIP_Type_dup(task->dest_dt_ptr, &dest_dt_dup);

    MPIR_Typerep_pack(task->src_buf, task->src_count, src_dt_dup,
                      task->init_src_offset + offset, MPIDI_PIP_global.pkt_load, copy_sz,
                      &actual_bytes);
    MPIR_Assert(actual_bytes == copy_sz);

    MPIR_Typerep_unpack(MPIDI_PIP_global.pkt_load, copy_sz, task->dest_buf, task->dest_count,
                        dest_dt_dup, task->init_dest_offset + offset, &actual_bytes);
    MPIR_Assert(actual_bytes == copy_sz);
    OPA_write_barrier();

    OPA_add_int(&task->done_data_sz, copy_sz);
    MPIR_Type_free_impl(&src_dt_dup);
    MPIR_Type_free_impl(&dest_dt_dup);
    return;
}

MPL_STATIC_INLINE_PREFIX void MPIDI_PIP_exec_stolen_task(MPIDI_PIP_task_queue_t * task_queue)
{
    void *src_buf, *dest_buf;
    int copy_sz, err, copy_kind;
    MPI_Aint offset;
    MPIDI_PIP_task_t *task;

    MPID_Thread_mutex_lock(&task_queue->lock, &err);
    task = task_queue->head;
    if (task) {
        if (task->reverse_enqueue) {
            if (task->cur_offset != 0) {
                copy_sz = MPIDI_PIP_copy_size_decision(task);
                task->cur_offset -= copy_sz;
                offset = task->cur_offset;
                copy_kind = task->copy_kind;
            } else {
                copy_sz = 0;
            }
        } else {
            if (task->cur_offset != task->orig_data_sz) {
                copy_sz = MPIDI_PIP_copy_size_decision(task);
                offset = task->cur_offset;
                task->cur_offset += copy_sz;
                copy_kind = task->copy_kind;
            } else {
                copy_sz = 0;
            }
        }
    } else {
        copy_sz = 0;
    }
    MPID_Thread_mutex_unlock(&task_queue->lock, &err);

    if (copy_sz) {
        switch (copy_kind) {
            case MPIDI_PIP_MEMCPY:
                MPIDI_PIP_exec_memcpy_task(task, copy_sz, offset);
                break;
            case MPIDI_PIP_PACK:
                MPIDI_PIP_exec_stolen_pack_task(task, copy_sz, offset);
                break;
            case MPIDI_PIP_UNPACK:
                MPIDI_PIP_exec_stolen_unpack_task(task, copy_sz, offset);
                break;
            case MPIDI_PIP_PACK_UNPACK:
                MPIDI_PIP_exec_stolen_pack_unpack_task(task, copy_sz, offset);
                break;
        }
    }

    return;
}


MPL_STATIC_INLINE_PREFIX int MPIDI_PIP_pack(void *src_buf, MPI_Aint src_count,
                                            MPI_Datatype src_dt, MPI_Aint inoffset,
                                            void *dest_buf, MPI_Aint max_pack_bytes,
                                            MPI_Aint * actual_pack_bytes)
{
    int mpi_errno = MPI_SUCCESS;
    MPI_Aint data_sz, orig_data_sz;

    MPIDI_Datatype_check_size(src_dt, src_count, orig_data_sz);
    orig_data_sz = orig_data_sz - inoffset;
    if (orig_data_sz > max_pack_bytes) {
        mpi_errno = MPI_ERR_TRUNCATE;
        data_sz = max_pack_bytes;
    } else {
        data_sz = orig_data_sz;
    }

    *actual_pack_bytes = data_sz;
    if (data_sz <= MPIDI_PIP_PKT_32KB) {
        MPI_Aint actual_bytes;
        MPIDI_PIP_global.local_copy_state[MPIDI_PIP_global.numa_local_rank] = 1;
        MPIR_Typerep_pack(src_buf, src_count, src_dt, inoffset, dest_buf, data_sz, &actual_bytes);
        OPA_write_barrier();
        MPIDI_PIP_global.local_copy_state[MPIDI_PIP_global.numa_local_rank] = 0;
        MPIR_Assert(actual_bytes == data_sz);
    } else {
        MPIR_Datatype *src_dt_ptr;
        MPIDI_PIP_task_t *task = (MPIDI_PIP_task_t *) MPIR_Handle_obj_alloc(&MPIDI_Task_mem);
        MPIR_Datatype_get_ptr(src_dt, src_dt_ptr);

        MPIDI_PIP_init_pack_task(task, src_buf, src_count, inoffset, src_dt_ptr, dest_buf, data_sz);
        MPIDI_PIP_publish_task(MPIDI_PIP_global.task_queue, task);
        do {
            if (task->cur_offset != 0)
                MPIDI_PIP_exec_self_task(MPIDI_PIP_global.task_queue);
        } while (OPA_load_int(&task->done_data_sz) != task->orig_data_sz);

        MPIDI_PIP_cancel_task(MPIDI_PIP_global.task_queue);
        MPIR_Handle_obj_free(&MPIDI_Task_mem, task);
    }

    return mpi_errno;
}


MPL_STATIC_INLINE_PREFIX int MPIDI_PIP_unpack(void *src_buf, MPI_Aint insize,
                                              void *dest_buf, MPI_Aint dest_count,
                                              MPI_Datatype dest_dt, MPI_Aint outoffset,
                                              MPI_Aint * actual_unpack_bytes)
{
    int mpi_errno = MPI_SUCCESS;
    MPI_Aint data_sz, orig_data_sz;

    MPIDI_Datatype_check_size(dest_dt, dest_count, orig_data_sz);
    orig_data_sz = orig_data_sz - outoffset;
    if (orig_data_sz < insize) {
        mpi_errno = MPI_ERR_TRUNCATE;
        data_sz = orig_data_sz;
    } else {
        data_sz = insize;
    }

    *actual_unpack_bytes = data_sz;
    if (data_sz <= MPIDI_PIP_PKT_32KB) {
        MPI_Aint actual_bytes;
        MPIDI_PIP_global.local_copy_state[MPIDI_PIP_global.numa_local_rank] = 1;
        MPIR_Typerep_unpack(src_buf, data_sz, dest_buf, dest_count, dest_dt, outoffset,
                            &actual_bytes);
        OPA_write_barrier();
        MPIDI_PIP_global.local_copy_state[MPIDI_PIP_global.numa_local_rank] = 0;
        MPIR_Assert(actual_bytes == data_sz);
    } else {
        MPIR_Datatype *dest_dt_ptr;
        MPIDI_PIP_task_t *task = (MPIDI_PIP_task_t *) MPIR_Handle_obj_alloc(&MPIDI_Task_mem);
        MPIR_Datatype_get_ptr(dest_dt, dest_dt_ptr);

        MPIDI_PIP_init_unpack_task(task, src_buf, dest_buf, dest_count, outoffset, dest_dt_ptr,
                                   data_sz);
        MPIDI_PIP_publish_task(MPIDI_PIP_global.task_queue, task);
        do {
            if (task->cur_offset != 0)
                MPIDI_PIP_exec_self_task(MPIDI_PIP_global.task_queue);
        } while (OPA_load_int(&task->done_data_sz) != task->orig_data_sz);

        MPIDI_PIP_cancel_task(MPIDI_PIP_global.task_queue);
        MPIR_Handle_obj_free(&MPIDI_Task_mem, task);
    }

    return mpi_errno;
}


MPL_STATIC_INLINE_PREFIX void MPIDI_PIP_Task_remote_check_and_steal(MPIDI_PIP_task_queue_t *
                                                                    task_queue, int numa_num_procs,
                                                                    MPIDI_PIP_global_t *
                                                                    victim_pip_global)
{
    int err, i;
    int cur_local_intra_copy = 0;
    int *local_copy_array = victim_pip_global->local_copy_state;
    int *local_idle_array = victim_pip_global->local_idle_state;
    for (i = 0; i < numa_num_procs; ++i) {
        /* intra local copy */
        if (local_copy_array[i] || local_idle_array[i])
            cur_local_intra_copy++;
    }
    if (cur_local_intra_copy < MPIDI_INTRA_COPY_LOCAL_PROCS_THRESHOLD) {
        MPIDI_PIP_exec_stolen_task(task_queue);
    }
    return;
}


/* Stealing procedure */
MPL_STATIC_INLINE_PREFIX void MPIDI_PIP_steal_task()
{
#ifdef MPIDI_PIP_STEALING_ENABLE
    /* local stealing */
    int numa_id = MPIDI_PIP_global.local_numa_id;
    int numa_num_procs = MPIDI_PIP_global.numa_num_procs[numa_id];
    int numa_local_rank = MPIDI_PIP_global.numa_local_rank;
    int victim = MPIDI_PIP_global.numa_cores_to_ranks[numa_id][rand() % numa_num_procs];
    MPIDI_PIP_task_t *task = NULL;

    if (victim != MPIDI_PIP_global.local_rank) {
        MPIDI_PIP_task_queue_t *victim_queue = MPIDI_PIP_global.task_queue_array[victim];
        if (victim_queue->head) {
            MPIDI_PIP_global.local_copy_state[numa_local_rank] = 1;
            MPIDI_PIP_exec_stolen_task(victim_queue);
            MPIDI_PIP_global.local_copy_state[numa_local_rank] = 0;
            return;
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
        int rmt_access = OPA_fetch_and_add_int(&MPIDI_PIP_global.numa_rmt_access[numa_id], 1);
        if (rmt_access < MPIDI_MAX_RMT_PEEKING_PROCS) {
            victim = MPIDI_PIP_global.numa_cores_to_ranks[numa_id][rand() % numa_num_procs];
            MPIDI_PIP_task_queue_t *victim_queue = MPIDI_PIP_global.task_queue_array[victim];

            if (victim_queue->head) {
                MPIDI_PIP_global_t *victim_pip_global = MPIDI_PIP_global.pip_global_array[victim];
                MPIDI_PIP_Task_remote_check_and_steal(victim_queue, numa_num_procs,
                                                      victim_pip_global);
            }
        }
        OPA_decr_int(&MPIDI_PIP_global.numa_rmt_access[numa_id]);
    }
#endif /* MPIDI_PIP_STEALING_ENABLE */
    return;
}

#endif
