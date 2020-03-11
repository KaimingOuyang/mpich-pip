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

MPL_STATIC_INLINE_PREFIX int MPIDI_PIP_exec_stolen_task(MPIDI_PIP_task_queue_t * task_queue,
                                                        int stealing_type, int victim);
MPL_STATIC_INLINE_PREFIX void MPIDI_PIP_exec_self_task(MPIDI_PIP_task_queue_t * task_queue);
MPL_STATIC_INLINE_PREFIX int MPIDI_PIP_copy_size_decision(MPIDI_PIP_task_t * task,
                                                          int stealing_type);
MPL_STATIC_INLINE_PREFIX void MPIDI_PIP_exec_memcpy_task(MPIDI_PIP_task_t * task, int copy_sz,
                                                         MPI_Aint offset);

MPL_STATIC_INLINE_PREFIX void MPIDI_PIP_publish_task(MPIDI_PIP_task_queue_t * task_queue,
                                                     MPIDI_PIP_task_t * task, uint64_t partner)
{
    int err;
    MPID_Thread_mutex_lock(&task_queue->lock, &err);
    task_queue->head = task;
    task_queue->partner = partner;
    MPID_Thread_mutex_unlock(&task_queue->lock, &err);
}


MPL_STATIC_INLINE_PREFIX void MPIDI_PIP_cancel_task(MPIDI_PIP_task_queue_t * task_queue)
{
    int err;
    MPID_Thread_mutex_lock(&task_queue->lock, &err);
    task_queue->head = NULL;
    task_queue->partner = -1;
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


MPL_STATIC_INLINE_PREFIX void MPIDI_obtain_task_info_safe(MPIDI_PIP_task_queue_t * task_queue,
                                                          MPIDI_PIP_task_t ** task_ret,
                                                          int *copy_sz_ret, MPI_Aint * offset_ret,
                                                          int *copy_kind_ret,
                                                          struct iovec **iov,
                                                          uint64_t * start_addr_ret,
                                                          MPI_Aint * start_len_ret, int *niov_ret,
                                                          int stealing_type)
{
    MPIDI_PIP_task_t *task;
    int err, copy_sz, copy_kind;
    MPI_Aint offset;
    int start_iov, end_iov;
    uint64_t start_addr;
    MPI_Aint start_len;

    MPID_Thread_mutex_lock(&task_queue->lock, &err);
    if (err) {
        printf("MPIDI_obtain_task_info_safe lock get error %d\n", err);
        fflush(stdout);
    }
    task = task_queue->head;
#ifdef ENABLE_REVERSE_TASK_ENQUEUE
    if (task && task->cur_offset != 0) {
        copy_sz = MPIDI_PIP_copy_size_decision(task, stealing_type);
        task->cur_offset -= copy_sz;
        offset = task->cur_offset;
        copy_kind = task->copy_kind;
    } else {
        copy_sz = 0;
    }
#else
    if (task && task->cur_offset != task->orig_data_sz) {
        copy_sz = MPIDI_PIP_copy_size_decision(task, stealing_type);
        offset = task->cur_offset;
        task->cur_offset += copy_sz;
        copy_kind = task->copy_kind;
    } else {
        copy_sz = 0;
    }
#endif

    if (copy_sz && copy_kind == MPIDI_PIP_ACC) {
        int rmt = copy_sz;
        start_addr = task->acc_iov->cur_addr;
        start_len = task->acc_iov->cur_len;
        start_iov = end_iov = task->acc_iov->cur_iov;
        while (1) {
            if (task->acc_iov->cur_len < rmt) {
                end_iov++;
                rmt -= task->acc_iov->cur_len;
                task->acc_iov->cur_addr = (uint64_t) task->acc_iov->iovs[end_iov].iov_base;
                task->acc_iov->cur_len = (MPI_Aint) task->acc_iov->iovs[end_iov].iov_len;
            } else if (task->acc_iov->cur_len == rmt) {
                if (end_iov < task->acc_iov->niov - 1) {
                    task->acc_iov->cur_iov = end_iov + 1;
                    task->acc_iov->cur_addr = (uint64_t) task->acc_iov->iovs[end_iov + 1].iov_base;
                    task->acc_iov->cur_len = (MPI_Aint) task->acc_iov->iovs[end_iov + 1].iov_len;
                }

                break;
            } else {
                task->acc_iov->cur_iov = end_iov;
                task->acc_iov->cur_addr += rmt;
                task->acc_iov->cur_len -= rmt;
                break;
            }
        }
    }
    MPID_Thread_mutex_unlock(&task_queue->lock, &err);
    if (err) {
        printf("MPIDI_obtain_task_info_safe lock release error %d\n", err);
        fflush(stdout);
    }

    if (copy_sz && copy_kind == MPIDI_PIP_ACC) {
        *iov = &task->acc_iov->iovs[start_iov];
        *start_addr_ret = start_addr;
        *start_len_ret = start_len;
        *niov_ret = end_iov - start_iov + 1;
    }

    *task_ret = task;
    *copy_sz_ret = copy_sz;
    *offset_ret = offset;
    *copy_kind_ret = copy_kind;
}


MPL_STATIC_INLINE_PREFIX void MPIDI_PIP_exec_acc_task(MPIDI_PIP_task_t * task, int copy_sz,
                                                      MPI_Aint offset, struct iovec *iov,
                                                      uint64_t start_addr, MPI_Aint start_len,
                                                      int niov)
{
    int i;
    int count, basic_sz;
    MPIR_Datatype_get_size_macro(task->origin_dt, basic_sz);
    uint64_t source_buf = (uint64_t) task->src_buf + offset;
    void *target_buf;
    MPI_Aint cur_work;
    int rmt = copy_sz;
    for (i = 0; i < niov; ++i) {
        if (i == 0) {
            cur_work = start_len < rmt ? start_len : rmt;
            target_buf = (void *) start_addr;
        } else {
            cur_work = iov[i].iov_len < rmt ? iov[i].iov_len : rmt;
            target_buf = iov[i].iov_base;
        }

        rmt -= cur_work;
        count = cur_work / basic_sz;

        (*task->uop) ((void *) source_buf, target_buf, &count, &task->origin_dt);
        source_buf += cur_work;
    }
    OPA_write_barrier();
    OPA_add_int(&task->done_data_sz, copy_sz);
    return;
}


MPL_STATIC_INLINE_PREFIX void MPIDI_PIP_init_acc_task(MPIDI_PIP_task_t * task,
                                                      void *src_buf, MPI_Aint src_count,
                                                      MPI_Datatype src_dtp, void *dest_buf,
                                                      MPI_Aint target_count,
                                                      MPI_Datatype target_dtp,
                                                      MPI_User_function * uop, MPI_Aint data_sz,
                                                      MPIDI_PIP_acc_iov_t * stealing_iov)
{
    /* current acc task does not support reverse stealing */
    task->copy_kind = MPIDI_PIP_ACC;

    task->src_buf = src_buf;
    task->dest_buf = dest_buf;
    task->cur_offset = 0;
    task->orig_data_sz = data_sz;
    OPA_store_int(&task->done_data_sz, 0);

    task->src_count = src_count;
    task->origin_dt = src_dtp;

    task->dest_count = target_count;
    task->target_dt = target_dtp;
    task->acc_iov = stealing_iov;
    task->uop = uop;

    return;
}

MPL_STATIC_INLINE_PREFIX int MPIDI_PIP_compute_acc_op(void *source_buf, int source_count,
                                                      MPI_Datatype source_dtp, void *target_buf,
                                                      int target_count, MPI_Datatype target_dtp,
                                                      MPI_Op acc_op, int src_kind, size_t data_sz,
                                                      MPIDI_PIP_acc_iov_t * stealing_iov)
{

    int mpi_errno = MPI_SUCCESS;
    MPI_User_function *uop = NULL;
    MPIR_FUNC_VERBOSE_STATE_DECL(MPID_STATE_MPIDIG_COMPUTE_ACC_OP);

    MPIR_FUNC_VERBOSE_ENTER(MPID_STATE_MPIDIG_COMPUTE_ACC_OP);

    /* first Judge if source buffer is empty */
    if (acc_op == MPI_NO_OP) {
        goto fn_exit;
    }

    if ((HANDLE_GET_KIND(acc_op) == HANDLE_KIND_BUILTIN)
        && ((*MPIR_OP_HDL_TO_DTYPE_FN(acc_op)) (source_dtp) == MPI_SUCCESS)) {
        /* get the function by indexing into the op table */
        uop = MPIR_OP_HDL_TO_FN(acc_op);
    } else {
        /* --BEGIN ERROR HANDLING-- */
        mpi_errno = MPIR_Err_create_code(MPI_SUCCESS, MPIR_ERR_RECOVERABLE,
                                         __func__, __LINE__, MPI_ERR_OP,
                                         "**opnotpredefined", "**opnotpredefined %d", acc_op);
        return mpi_errno;
        /* --END ERROR HANDLING-- */
    }

    MPIDI_PIP_task_t *task = (MPIDI_PIP_task_t *) MPIR_Handle_obj_alloc(&MPIDI_Task_mem);
    MPIDI_PIP_init_acc_task(task, source_buf, source_count, source_dtp,
                            target_buf, target_count, target_dtp, uop, data_sz, stealing_iov);
    MPIDI_PIP_publish_task(MPIDI_PIP_global.task_queue, task, -1);

    do {
        if (task->cur_offset != task->orig_data_sz)
            MPIDI_PIP_exec_self_task(MPIDI_PIP_global.task_queue);
    } while (OPA_load_int(&task->done_data_sz) != task->orig_data_sz);

    MPIDI_PIP_cancel_task(MPIDI_PIP_global.task_queue);
    MPIR_Handle_obj_free(&MPIDI_Task_mem, task);

  fn_exit:
    MPIR_FUNC_VERBOSE_EXIT(MPID_STATE_MPIDIG_COMPUTE_ACC_OP);
    return mpi_errno;
}

MPL_STATIC_INLINE_PREFIX int MPIDI_PIP_exec_self_task(MPIDI_PIP_task_queue_t * task_queue)
{
    void *src_buf, *dest_buf;
    int copy_sz, err, copy_kind;
    MPI_Aint offset, init_src_offset, init_dest_offset;
    MPIDI_PIP_task_t *task;
    struct iovec *iov;
    uint64_t start_addr;
    MPI_Aint start_len;
    int niov;
    int ret;
    MPIDI_obtain_task_info_safe(task_queue, &task, &copy_sz, &offset, &copy_kind,
                                &iov, &start_addr, &start_len, &niov, MPIDI_PIP_LOCAL_STEALING);

    if (copy_sz) {
        int numa_local_rank = MPIDI_PIP_global.numa_local_rank;
        // MPIDI_PIP_global.local_copy_state[numa_local_rank] = 1;
        ret = STEALING_SUCCESS;
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
            case MPIDI_PIP_ACC:
                MPIDI_PIP_exec_acc_task(task, copy_sz, offset, iov, start_addr, start_len, niov);
                break;
        }
        // MPIDI_PIP_global.local_copy_state[numa_local_rank] = 0;
    } else
        ret = STEALING_FAIL;

    return ret;
}


MPL_STATIC_INLINE_PREFIX void MPIDI_PIP_init_memcpy_task(MPIDI_PIP_task_t * task, void *src_buf,
                                                         void *dest_buf, MPI_Aint data_sz)
{
    task->copy_kind = MPIDI_PIP_MEMCPY;

    task->src_buf = src_buf;
    task->dest_buf = dest_buf;
#ifdef ENABLE_REVERSE_TASK_ENQUEUE
    task->cur_offset = data_sz;
#else
    task->cur_offset = 0;
#endif
    task->orig_data_sz = data_sz;
    OPA_store_int(&task->done_data_sz, 0);
    return;
}


MPL_STATIC_INLINE_PREFIX void MPIDI_PIP_memcpy_task_enqueue(char *src_buf,
                                                            char *dest_buf, MPI_Aint data_sz,
                                                            uint64_t partner)
{
#ifdef ENABLE_CONTIG_STEALING
    if (data_sz <= MPIDI_PIP_STEALING_THRESHOLD) {
        // MPIDI_PIP_global.local_copy_state[MPIDI_PIP_global.numa_local_rank] = 1;
        MPIR_Memcpy(dest_buf, src_buf, data_sz);
        OPA_write_barrier();
        // MPIDI_PIP_global.local_copy_state[MPIDI_PIP_global.numa_local_rank] = 0;
    } else {
        MPIDI_PIP_task_t *task = (MPIDI_PIP_task_t *) MPIR_Handle_obj_alloc(&MPIDI_Task_mem);
        MPIDI_PIP_init_memcpy_task(task, src_buf, dest_buf, data_sz);
        MPIDI_PIP_publish_task(MPIDI_PIP_global.task_queue, task, partner);
        do {
#ifdef ENABLE_REVERSE_TASK_ENQUEUE
            if (task->cur_offset != 0)
                MPIDI_PIP_exec_self_task(MPIDI_PIP_global.task_queue);
#else
            if (task->cur_offset != task->orig_data_sz)
                MPIDI_PIP_exec_self_task(MPIDI_PIP_global.task_queue);
#endif
        } while (OPA_load_int(&task->done_data_sz) != task->orig_data_sz);

        MPIDI_PIP_cancel_task(MPIDI_PIP_global.task_queue);
        MPIR_Handle_obj_free(&MPIDI_Task_mem, task);
    }
#else
    // MPIDI_PIP_global.local_copy_state[MPIDI_PIP_global.numa_local_rank] = 1;
    MPIR_Memcpy(dest_buf, src_buf, data_sz);
    OPA_write_barrier();
    // MPIDI_PIP_global.local_copy_state[MPIDI_PIP_global.numa_local_rank] = 0;
#endif
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
#ifdef ENABLE_REVERSE_TASK_ENQUEUE
    task->cur_offset = data_sz;
#else
    task->cur_offset = 0;
#endif
    task->orig_data_sz = data_sz;
    OPA_store_int(&task->done_data_sz, 0);

    task->src_count = src_count;
    task->src_dt_ptr = src_dt_ptr;
    task->init_src_offset = inoffset;
    return;
}

MPL_STATIC_INLINE_PREFIX void MPIDI_PIP_pack_task_enqueue(void *src_buf, MPI_Aint src_count,
                                                          MPIR_Datatype * src_dt_ptr,
                                                          char *dest_buf, MPI_Aint data_sz,
                                                          uint64_t partner)
{
    MPI_Datatype src_dt_dup;
    MPIR_PIP_Type_dup(src_dt_ptr, &src_dt_dup);
#ifdef ENABLE_NON_CONTIG_STEALING
    if (data_sz <= MPIDI_PIP_STEALING_THRESHOLD) {
        MPI_Aint actual_bytes;
        // MPIDI_PIP_global.local_copy_state[MPIDI_PIP_global.numa_local_rank] = 1;
        MPIR_Typerep_pack(src_buf, src_count, src_dt_dup, 0, dest_buf, data_sz, &actual_bytes);
        OPA_write_barrier();
        // MPIDI_PIP_global.local_copy_state[MPIDI_PIP_global.numa_local_rank] = 0;
        MPIR_Assert(actual_bytes == data_sz);
    } else {
        MPIR_Datatype *src_dt_dup_ptr;
        MPIDI_PIP_task_t *task = (MPIDI_PIP_task_t *) MPIR_Handle_obj_alloc(&MPIDI_Task_mem);
        MPIR_Datatype_get_ptr(src_dt_dup, src_dt_dup_ptr);

        MPIDI_PIP_init_pack_task(task, src_buf, src_count, 0, src_dt_dup_ptr, dest_buf, data_sz);
        MPIDI_PIP_publish_task(MPIDI_PIP_global.task_queue, task, partner);
        do {
#ifdef ENABLE_REVERSE_TASK_ENQUEUE
            if (task->cur_offset != 0)
                MPIDI_PIP_exec_self_task(MPIDI_PIP_global.task_queue);
#else
            if (task->cur_offset != task->orig_data_sz)
                MPIDI_PIP_exec_self_task(MPIDI_PIP_global.task_queue);
#endif /* ENABLE_REVERSE_TASK_ENQUEUE */
        } while (OPA_load_int(&task->done_data_sz) != task->orig_data_sz);

        MPIDI_PIP_cancel_task(MPIDI_PIP_global.task_queue);
        MPIR_Handle_obj_free(&MPIDI_Task_mem, task);
    }
#else /* ENABLE_NON_CONTIG_STEALING */
    MPI_Aint actual_bytes;
    // MPIDI_PIP_global.local_copy_state[MPIDI_PIP_global.numa_local_rank] = 1;
    MPIR_Typerep_pack(src_buf, src_count, src_dt_dup, 0, dest_buf, data_sz, &actual_bytes);
    OPA_write_barrier();
    // MPIDI_PIP_global.local_copy_state[MPIDI_PIP_global.numa_local_rank] = 0;
    MPIR_Assert(actual_bytes == data_sz);
#endif /* ENABLE_NON_CONTIG_STEALING */
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
#ifdef ENABLE_REVERSE_TASK_ENQUEUE
    task->cur_offset = data_sz;
#else
    task->cur_offset = 0;
#endif
    task->orig_data_sz = data_sz;
    OPA_store_int(&task->done_data_sz, 0);

    task->dest_count = dest_count;
    task->dest_dt_ptr = dest_dt_ptr;
    task->init_dest_offset = outoffset;
    return;
}

MPL_STATIC_INLINE_PREFIX void MPIDI_PIP_unpack_task_enqueue(void *src_buf,
                                                            void *dest_buf, MPI_Aint dest_count,
                                                            MPI_Datatype dest_dt, MPI_Aint data_sz,
                                                            uint64_t partner)
{
#ifdef ENABLE_NON_CONTIG_STEALING
    if (data_sz <= MPIDI_PIP_STEALING_THRESHOLD) {
        MPI_Aint actual_bytes;
        // MPIDI_PIP_global.local_copy_state[MPIDI_PIP_global.numa_local_rank] = 1;
        MPIR_Typerep_unpack(src_buf, data_sz, dest_buf, dest_count, dest_dt, 0, &actual_bytes);
        OPA_write_barrier();
        // MPIDI_PIP_global.local_copy_state[MPIDI_PIP_global.numa_local_rank] = 0;
        MPIR_Assert(actual_bytes == data_sz);
    } else {
        MPIR_Datatype *dest_dt_ptr;
        MPIDI_PIP_task_t *task = (MPIDI_PIP_task_t *) MPIR_Handle_obj_alloc(&MPIDI_Task_mem);
        MPIR_Datatype_get_ptr(dest_dt, dest_dt_ptr);

        MPIDI_PIP_init_unpack_task(task, src_buf, dest_buf, dest_count, 0, dest_dt_ptr, data_sz);
        MPIDI_PIP_publish_task(MPIDI_PIP_global.task_queue, task, partner);
        do {
#ifdef ENABLE_REVERSE_TASK_ENQUEUE
            if (task->cur_offset != 0)
                MPIDI_PIP_exec_self_task(MPIDI_PIP_global.task_queue);
#else
            if (task->cur_offset != task->orig_data_sz)
                MPIDI_PIP_exec_self_task(MPIDI_PIP_global.task_queue);
#endif
        } while (OPA_load_int(&task->done_data_sz) != task->orig_data_sz);

        MPIDI_PIP_cancel_task(MPIDI_PIP_global.task_queue);
        MPIR_Handle_obj_free(&MPIDI_Task_mem, task);
    }
#else /* ENABLE_NON_CONTIG_STEALING */
    MPI_Aint actual_bytes;
    // MPIDI_PIP_global.local_copy_state[MPIDI_PIP_global.numa_local_rank] = 1;
    MPIR_Typerep_unpack(src_buf, data_sz, dest_buf, dest_count, dest_dt, 0, &actual_bytes);
    OPA_write_barrier();
    // MPIDI_PIP_global.local_copy_state[MPIDI_PIP_global.numa_local_rank] = 0;
    MPIR_Assert(actual_bytes == data_sz);
#endif /* ENABLE_NON_CONTIG_STEALING */
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
#ifdef ENABLE_REVERSE_TASK_ENQUEUE
    task->cur_offset = data_sz;
#else
    task->cur_offset = 0;
#endif
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
                                                                 MPI_Aint data_sz, uint64_t partner)
{
    MPI_Datatype src_dt_dup;
    MPIR_PIP_Type_dup(src_dt_ptr, &src_dt_dup);
    // MPIDI_PIP_global.local_copy_state[numa_local_rank] = 1;
#ifdef ENABLE_NON_CONTIG_STEALING
    if (data_sz <= MPIDI_PIP_STEALING_THRESHOLD) {
        MPI_Aint actual_bytes;
        // MPIDI_PIP_global.local_copy_state[MPIDI_PIP_global.numa_local_rank] = 1;
        MPIR_Typerep_pack(src_buf, src_count, src_dt_dup, 0, MPIDI_PIP_global.pkt_load, data_sz,
                          &actual_bytes);
        MPIR_Assert(actual_bytes == data_sz);
        MPIR_Typerep_unpack(MPIDI_PIP_global.pkt_load, data_sz, dest_buf, dest_count, dest_dt, 0,
                            &actual_bytes);
        MPIR_Assert(actual_bytes == data_sz);
        OPA_write_barrier();
        // MPIDI_PIP_global.local_copy_state[MPIDI_PIP_global.numa_local_rank] = 0;
    } else {
        MPIR_Datatype *src_dt_dup_ptr;
        MPIR_Datatype *dest_dt_ptr;
        MPIDI_PIP_task_t *task = (MPIDI_PIP_task_t *) MPIR_Handle_obj_alloc(&MPIDI_Task_mem);
        MPIR_Datatype_get_ptr(dest_dt, dest_dt_ptr);
        MPIR_Datatype_get_ptr(src_dt_dup, src_dt_dup_ptr);

        MPIDI_PIP_init_pack_unpack_task(task, src_buf, src_count, 0, src_dt_dup_ptr,
                                        dest_buf, dest_count, 0, dest_dt_ptr, data_sz);
        MPIDI_PIP_publish_task(MPIDI_PIP_global.task_queue, task, partner);
        do {
#ifdef ENABLE_REVERSE_TASK_ENQUEUE
            if (task->cur_offset != 0)
                MPIDI_PIP_exec_self_task(MPIDI_PIP_global.task_queue);
#else
            if (task->cur_offset != task->orig_data_sz)
                MPIDI_PIP_exec_self_task(MPIDI_PIP_global.task_queue);
#endif
        } while (OPA_load_int(&task->done_data_sz) != task->orig_data_sz);

        MPIDI_PIP_cancel_task(MPIDI_PIP_global.task_queue);
        MPIR_Handle_obj_free(&MPIDI_Task_mem, task);
    }
#else /* ENABLE_NON_CONTIG_STEALING */
    MPI_Aint actual_bytes;
    void *im_buf = MPL_malloc(data_sz, MPL_MEM_OTHER);
    MPIDI_PIP_global.local_copy_state[MPIDI_PIP_global.numa_local_rank] = 1;
    MPIR_Typerep_pack(src_buf, src_count, src_dt_dup, 0, im_buf, data_sz, &actual_bytes);
    MPIR_Assert(actual_bytes == data_sz);
    MPIR_Typerep_unpack(im_buf, data_sz, dest_buf, dest_count, dest_dt, 0, &actual_bytes);
    MPIR_Assert(actual_bytes == data_sz);
    OPA_write_barrier();
    MPIDI_PIP_global.local_copy_state[MPIDI_PIP_global.numa_local_rank] = 0;
    MPL_free(im_buf);
#endif /* ENABLE_NON_CONTIG_STEALING */
    MPIR_Type_free_impl(&src_dt_dup);
    // MPIDI_PIP_global.local_copy_state[numa_local_rank] = 0;
    return;
}


MPL_STATIC_INLINE_PREFIX int MPIDI_PIP_copy_size_decision(MPIDI_PIP_task_t * task,
                                                          int stealing_type)
{
    int copy_sz;
    /* This size decision function is only for bebop machine */
    static const int PKT_16KB = 1 << 14;
    static const int PKT_32KB = 1 << 15;
    static const int PKT_64KB = 1 << 16;
    static const int PKT_96KB = PKT_32KB + PKT_64KB;
    static const int PKT_512KB = 1 << 19;

#ifdef ENABLE_REVERSE_TASK_ENQUEUE
    size_t remaining_data = task->cur_offset;
#else
    size_t remaining_data = task->orig_data_sz - task->cur_offset;
#endif

    /* 32KB task size for accumulate */
    // if (task->copy_kind == MPIDI_PIP_ACC) {
    //     if (remaining_data <= PKT_32KB)
    //         copy_sz = remaining_data;
    //     else
    //         copy_sz = PKT_32KB;
    //     goto fn_exit;
    // }
#ifdef ENABLE_DYNAMIC_CHUNK
    if (stealing_type == MPIDI_PIP_REMOTE_STEALING) {
        if (remaining_data <= PKT_96KB)
            copy_sz = 0;
        else
            copy_sz = PKT_32KB;
    } else {
        if (remaining_data <= PKT_16KB)
            copy_sz = remaining_data;
        else if (remaining_data <= PKT_96KB)
            copy_sz = PKT_16KB;
        else if (remaining_data <= PKT_512KB)
            copy_sz = PKT_32KB;
        else
            copy_sz = PKT_64KB;
    }
#else
    if (stealing_type != MPIDI_PIP_REMOTE_STEALING) {
        if (remaining_data <= PKT_64KB)
            copy_sz = remaining_data;
        else
            copy_sz = PKT_64KB;
    } else {
        if (remaining_data <= PKT_96KB)
            copy_sz = 0;
        else
            copy_sz = PKT_32KB;
    }
#endif
  fn_exit:
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


MPL_STATIC_INLINE_PREFIX int MPIDI_PIP_exec_stolen_task(MPIDI_PIP_task_queue_t * task_queue,
                                                        int stealing_type, int victim)
{
    void *src_buf, *dest_buf;
    int copy_sz, err, copy_kind;
    MPI_Aint offset;
    MPIDI_PIP_task_t *task;
    int ret = STEALING_FAIL;

    struct iovec *iov;
    uint64_t start_addr;
    MPI_Aint start_len;
    int niov;

    MPIDI_obtain_task_info_safe(task_queue, &task, &copy_sz, &offset, &copy_kind,
                                &iov, &start_addr, &start_len, &niov, stealing_type);

    if (copy_sz) {
        int partner = 0;
        if (copy_kind == MPIDI_PIP_PACK) {
            MPIDI_PIP_global.rmt_stealing_cnt++;
            if (MPIDI_PIP_global.interp_queue.head)
                partner =
                    MPIDI_PIP_global.interp_queue.head->partner + MPIDI_PIP_global.grank / 36 * 36;
            printf
                ("grank %d - steal gvictim %d ofi PACK, copy_sz %d, intraq %p, interq %p, partner %d\n",
                 MPIDI_PIP_global.grank, victim + MPIDI_PIP_global.grank / 36 * 36, copy_sz,
                 MPIDI_PIP_global.intrap_queue.head, MPIDI_PIP_global.interp_queue.head, partner);
            fflush(stdout);
        } else if (copy_kind == MPIDI_PIP_UNPACK) {
            MPIDI_PIP_global.rmt_stealing_cnt++;
            printf
                ("grank %d - steal gvictim %d ofi UNPACK, copy_sz %d, intraq %p, interq %p, partner %d\n",
                 MPIDI_PIP_global.grank, victim + MPIDI_PIP_global.grank / 36 * 36, copy_sz,
                 MPIDI_PIP_global.intrap_queue.head, MPIDI_PIP_global.interp_queue.head, partner);
            fflush(stdout);
        } else if (copy_kind == MPIDI_PIP_ACC){
            char *stype;
            if(stealing_type == MPIDI_PIP_REMOTE_STEALING){
                stype = "REMOTE";
                MPIDI_PIP_global.rmt_stealing_cnt++;
            }else{
                stype = "LOCAL";
            }
            printf("grank %d (%d) - steal gvictim %d (%d) ACC task, copy_sz %d, offset %ld, niov %d, stealing_type %s\n",
                 MPIDI_PIP_global.grank, MPIDI_PIP_global.local_rank, victim + MPIDI_PIP_global.grank / 36 * 36, victim, copy_sz,
                 offset, niov, stype);
            fflush(stdout);
        }
        MPIDI_PIP_global.total_stealing_cnt++;
        ret = STEALING_SUCCESS;
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
            case MPIDI_PIP_ACC:
                MPIDI_PIP_exec_acc_task(task, copy_sz, offset, iov, start_addr, start_len, niov);
                break;
        }
    } else
        ret = STEALING_FAIL;

    return ret;
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
    int numa_local_rank = MPIDI_PIP_global.numa_local_rank;
    MPIDI_PIP_global.local_copy_state[numa_local_rank] = 1;
    if (data_sz <= MPIDI_PIP_STEALING_THRESHOLD) {
        MPI_Aint actual_bytes;
        // MPIDI_PIP_global.local_copy_state[MPIDI_PIP_global.numa_local_rank] = 1;
        MPIR_Typerep_pack(src_buf, src_count, src_dt, inoffset, dest_buf, data_sz, &actual_bytes);
        OPA_write_barrier();
        // MPIDI_PIP_global.local_copy_state[MPIDI_PIP_global.numa_local_rank] = 0;
        MPIR_Assert(actual_bytes == data_sz);
    } else {
        MPIR_Datatype *src_dt_ptr;
        MPIDI_PIP_task_t *task = (MPIDI_PIP_task_t *) MPIR_Handle_obj_alloc(&MPIDI_Task_mem);
        MPIR_Datatype_get_ptr(src_dt, src_dt_ptr);

        MPIDI_PIP_init_pack_task(task, src_buf, src_count, inoffset, src_dt_ptr, dest_buf, data_sz);
        MPIDI_PIP_publish_task(MPIDI_PIP_global.task_queue, task, -1);
        do {
#ifdef ENABLE_REVERSE_TASK_ENQUEUE
            if (task->cur_offset != 0)
                MPIDI_PIP_exec_self_task(MPIDI_PIP_global.task_queue);
#else
            if (task->cur_offset != task->orig_data_sz)
                MPIDI_PIP_exec_self_task(MPIDI_PIP_global.task_queue);
#endif
        } while (OPA_load_int(&task->done_data_sz) != task->orig_data_sz);

        MPIDI_PIP_cancel_task(MPIDI_PIP_global.task_queue);
        MPIR_Handle_obj_free(&MPIDI_Task_mem, task);
    }
    MPIDI_PIP_global.local_copy_state[numa_local_rank] = 0;
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
    int numa_local_rank = MPIDI_PIP_global.numa_local_rank;
    MPIDI_PIP_global.local_copy_state[numa_local_rank] = 1;
    if (data_sz <= MPIDI_PIP_STEALING_THRESHOLD) {
        MPI_Aint actual_bytes;
        // MPIDI_PIP_global.local_copy_state[MPIDI_PIP_global.numa_local_rank] = 1;
        MPIR_Typerep_unpack(src_buf, data_sz, dest_buf, dest_count, dest_dt, outoffset,
                            &actual_bytes);
        OPA_write_barrier();
        // MPIDI_PIP_global.local_copy_state[MPIDI_PIP_global.numa_local_rank] = 0;
        MPIR_Assert(actual_bytes == data_sz);
    } else {
        MPIR_Datatype *dest_dt_ptr;
        MPIDI_PIP_task_t *task = (MPIDI_PIP_task_t *) MPIR_Handle_obj_alloc(&MPIDI_Task_mem);
        MPIR_Datatype_get_ptr(dest_dt, dest_dt_ptr);

        MPIDI_PIP_init_unpack_task(task, src_buf, dest_buf, dest_count, outoffset, dest_dt_ptr,
                                   data_sz);
        MPIDI_PIP_publish_task(MPIDI_PIP_global.task_queue, task, -1);
        do {
#ifdef ENABLE_REVERSE_TASK_ENQUEUE
            if (task->cur_offset != 0)
                MPIDI_PIP_exec_self_task(MPIDI_PIP_global.task_queue);
#else
            if (task->cur_offset != task->orig_data_sz)
                MPIDI_PIP_exec_self_task(MPIDI_PIP_global.task_queue);
#endif
        } while (OPA_load_int(&task->done_data_sz) != task->orig_data_sz);

        MPIDI_PIP_cancel_task(MPIDI_PIP_global.task_queue);
        MPIR_Handle_obj_free(&MPIDI_Task_mem, task);
    }
    MPIDI_PIP_global.local_copy_state[numa_local_rank] = 0;
    return mpi_errno;
}

MPL_STATIC_INLINE_PREFIX void MPIDI_PIP_Task_remote_check_and_steal(MPIDI_PIP_task_queue_t *
                                                                    task_queue, int numa_num_procs,
                                                                    MPIDI_PIP_global_t *
                                                                    victim_pip_global, int numa_id,
                                                                    int victim)
{
    int err, i;
    int cur_local_intra_copy = 0;
    int *local_copy_array = victim_pip_global->local_copy_state;
    // int *local_idle_array = victim_pip_global->local_idle_state;
    for (i = 0; i < numa_num_procs; ++i) {
        /* intra local copy */
        if (local_copy_array[i])
            cur_local_intra_copy++;
    }

    if (cur_local_intra_copy < MPIDI_PIP_MAX_NUM_LOCAL_STEALING) {
        // printf("grank %d - cur_local_intra_copy (%d) < threshold (%d)\n", MPIDI_PIP_global.grank,
        //        cur_local_intra_copy, MPIDI_PIP_MAX_NUM_LOCAL_STEALING);
        // fflush(stdout);
        MPIDI_PIP_global.allow_rmt_stealing_ptr[numa_id] = 1;
        MPIDI_PIP_exec_stolen_task(task_queue, MPIDI_PIP_REMOTE_STEALING, victim);
        MPIDI_PIP_global.allow_rmt_stealing_ptr[numa_id] = 0;
    }
    return;
}


/* Stealing procedure */
MPL_STATIC_INLINE_PREFIX void MPIDI_PIP_steal_task()
{
#ifdef MPIDI_PIP_STEALING_ENABLE
    int victim, ret = STEALING_FAIL;
    int numa_local_rank = MPIDI_PIP_global.numa_local_rank;
    // MPIDI_PIP_task_t *task = NULL;
#ifdef ENABLE_PARTNER_STEALING
    MPIDI_PIP_partner_t *curp = MPIDI_PIP_global.intrap_queue.head;
    while (curp != NULL) {
        victim = curp->partner;
        MPIDI_PIP_task_queue_t *victim_queue = MPIDI_PIP_global.task_queue_array[victim];
        if (victim_queue->head && victim_queue->partner == (uint64_t) curp) {
            ret = MPIDI_PIP_exec_stolen_task(victim_queue, MPIDI_PIP_LOCAL_STEALING, victim);
            if (ret == STEALING_SUCCESS) {
                MPIDI_PIP_global.local_try = 0;
                return;
            }
        }
        curp = curp->next;
    }
#endif
    /* local stealing */
    int numa_id = MPIDI_PIP_global.local_numa_id;
    int numa_num_procs = MPIDI_PIP_global.numa_num_procs[numa_id];
    victim = MPIDI_PIP_global.numa_cores_to_ranks[numa_id][rand() % numa_num_procs];
    if (victim != MPIDI_PIP_global.local_rank) {
        MPIDI_PIP_task_queue_t *victim_queue = MPIDI_PIP_global.task_queue_array[victim];
        if (victim_queue->head) {
            ret = MPIDI_PIP_exec_stolen_task(victim_queue, MPIDI_PIP_LOCAL_STEALING, victim);
            if (ret == STEALING_SUCCESS) {
                MPIDI_PIP_global.local_try = 0;
                return;
            }
        }
    }
#ifdef ENABLE_PARTNER_STEALING
    curp = MPIDI_PIP_global.interp_queue.head;
    while (curp != NULL) {
        victim = curp->partner;
        MPIDI_PIP_task_queue_t *victim_queue = MPIDI_PIP_global.task_queue_array[victim];
        if (victim_queue->partner == (uint64_t) curp) {
            // numa_num_procs = MPIDI_PIP_global.numa_num_procs[curp->partner_numa_id];
            // MPIDI_PIP_global.local_copy_state[numa_local_rank] = 1;
            ret =
                MPIDI_PIP_exec_stolen_task(victim_queue, MPIDI_PIP_REMOTE_PARTNER_STEALING, victim);
            if (ret == STEALING_SUCCESS) {
                MPIDI_PIP_global.local_try = 0;
                return;
            }
            // MPIDI_PIP_global.local_copy_state[numa_local_rank] = 0;
        }

        curp = curp->next;
    }
#endif
    if (MPIDI_PIP_global.local_try < CORES_PER_NUMA_NODE) {
        ++MPIDI_PIP_global.local_try;
        return;
    } else {
        MPIDI_PIP_global.local_try = 0;
    }
    /* check whether local tasks exists */
    // int i, j;
    // for (i = 0; i < numa_num_procs; ++i) {
    //     j = MPIDI_PIP_global.numa_cores_to_ranks[numa_id][i];
    //     if (MPIDI_PIP_global.task_queue_array[j]->head)
    //         return;
    // }

    /* remote stealing */
    numa_id = MPIDI_PIP_global.partner_numa;
    numa_num_procs = MPIDI_PIP_global.numa_num_procs[numa_id];
    if (numa_num_procs != 0) {
        if (OPA_cas_int(&MPIDI_PIP_global.bdw_checking_ptr[numa_id], 0, 1) == 0) {
            victim = MPIDI_PIP_global.numa_cores_to_ranks[numa_id][rand() % numa_num_procs];
            MPIDI_PIP_task_queue_t *victim_queue = MPIDI_PIP_global.task_queue_array[victim];
            if (victim_queue->head) {
                MPIDI_PIP_Task_remote_check_and_steal(victim_queue, numa_num_procs,
                                                      MPIDI_PIP_global.pip_global_array[victim],
                                                      numa_id, victim);
            }
            OPA_store_int(&MPIDI_PIP_global.bdw_checking_ptr[numa_id], 0);
        } else if (MPIDI_PIP_global.allow_rmt_stealing_ptr[numa_id]) {
            victim = MPIDI_PIP_global.numa_cores_to_ranks[numa_id][rand() % numa_num_procs];
            MPIDI_PIP_task_queue_t *victim_queue = MPIDI_PIP_global.task_queue_array[victim];

            if (victim_queue->head) {
                MPIDI_PIP_exec_stolen_task(victim_queue, MPIDI_PIP_REMOTE_STEALING, victim);
                return;
            }
        }
    }
#endif /* MPIDI_PIP_STEALING_ENABLE */
    return;
}

MPL_STATIC_INLINE_PREFIX int MPIDI_SHM_PIP_rma_lock(MPL_proc_mutex_t * mutex_ptr)
{
    while (pthread_mutex_trylock(mutex_ptr))
        MPIDI_PIP_steal_task();
}

#endif
