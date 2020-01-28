/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 * (C) 2019 by Argonne National Laboratory.
 *     See COPYRIGHT in top-level directory.
 */

#ifndef PIP_RECV_H_INCLUDED
#define PIP_RECV_H_INCLUDED

#include "ch4_impl.h"
#include "shm_control.h"
#include "pip_pre.h"
#include "pip_impl.h"

MPL_STATIC_INLINE_PREFIX void MPIDI_PIP_init_memcpy_task(MPIDI_PIP_task_t * task, void *src_buf,
                                                         size_t copy_sz, void *dest_buf,
                                                         int task_kind)
{
    task->compl_flag = MPIDI_PIP_NOT_COMPLETE;
    task->task_next = NULL;
    task->compl_next = NULL;

    task->task_kind = task_kind;
    task->copy_kind = MPIDI_PIP_MEMCPY;

    task->src_buf = src_buf;
    task->dest_buf = dest_buf;
    task->data_sz = copy_sz;
    return;
}

MPL_STATIC_INLINE_PREFIX void MPIDI_PIP_memcpy_task_enqueue(char *src_buf,
                                                            char *dest_buf, MPI_Aint data_sz,
                                                            int task_kind)
{
    MPI_Aint copy_sz;
    char *revs_src_buf = src_buf + data_sz;
    char *revs_dest_buf = dest_buf + data_sz;
    int numa_local_rank = MPIDI_PIP_global.numa_local_rank;
    do {
        if (data_sz <= MPIDI_PIP_LAST_PKT_THRESHOLD) {
            /* Last packet, I need to copy it myself. */
            copy_sz = data_sz;
            MPIDI_PIP_global.local_copy_state[numa_local_rank] = 1;
            MPIR_Memcpy((void *) dest_buf, (void *) src_buf, copy_sz);
            MPIDI_PIP_global.local_copy_state[numa_local_rank] = 0;
            MPIDI_PIP_fflush_task();
            while (MPIDI_PIP_global.compl_queue->head)
                MPIDI_PIP_fflush_compl_task(MPIDI_PIP_global.compl_queue);
        } else {
            /* I have a lot of tasks to do, enqueue tasks and hope others steal them. */
            MPIDI_PIP_task_t *task = (MPIDI_PIP_task_t *) MPIR_Handle_obj_alloc(&MPIDI_Task_mem);

            copy_sz = MPIDI_PIP_PKT_SIZE;
            revs_src_buf -= copy_sz;
            revs_dest_buf -= copy_sz;
            MPIDI_PIP_init_memcpy_task(task, revs_src_buf, copy_sz, revs_dest_buf, task_kind);
            MPIDI_PIP_Task_safe_enqueue(MPIDI_PIP_global.task_queue, task);
            MPIDI_PIP_Compl_task_enqueue(MPIDI_PIP_global.compl_queue, task);

            if (MPIDI_PIP_global.compl_queue->task_num >= MPIDI_MAX_TASK_THRESHOLD)
                MPIDI_PIP_exec_one_task(MPIDI_PIP_global.task_queue, MPIDI_PIP_global.compl_queue);
        }

        data_sz -= copy_sz;
    } while (data_sz > 0);
    return;
}

MPL_STATIC_INLINE_PREFIX void MPIDI_PIP_init_pack_task(MPIDI_PIP_task_t * task, void *src_buf,
                                                       MPI_Aint src_count, MPI_Aint inoffset,
                                                       MPIR_Datatype * src_dt_ptr, void *dest_buf,
                                                       MPI_Aint max_pack_bytes, int task_kind)
{
    task->compl_flag = MPIDI_PIP_NOT_COMPLETE;
    task->task_next = NULL;
    task->compl_next = NULL;

    task->task_kind = task_kind;
    task->copy_kind = MPIDI_PIP_PACK;

    task->src_buf = src_buf;
    task->dest_buf = dest_buf;
    task->data_sz = max_pack_bytes;

    task->src_count = src_count;
    task->src_dt_ptr = src_dt_ptr;
    task->src_offset = inoffset;
    return;
}

MPL_STATIC_INLINE_PREFIX void MPIDI_PIP_pack_task_enqueue(void *src_buf,
                                                          MPI_Aint src_count,
                                                          MPIR_Datatype * src_dt_ptr,
                                                          char *dest_buf, MPI_Aint data_sz,
                                                          int task_kind)
{
    MPI_Aint copy_sz;
    MPI_Aint inoffset = data_sz;
    char *revs_dest_buf = dest_buf + data_sz;
    MPI_Datatype src_dt_dup;
    MPIR_Datatype *src_dt_dup_ptr;
    // printf("rank %d - pack src_dt_ptr->typerep %p\n", MPIDI_PIP_global.local_rank, src_dt_ptr->typerep);
    // fflush(stdout);
    MPIR_PIP_Type_dup(src_dt_ptr, &src_dt_dup);
    MPIR_Datatype_get_ptr(src_dt_dup, src_dt_dup_ptr);
    // printf("rank %d - END pack enqueue routine, src_dt_dup_ptr->typerep %p\n", MPIDI_PIP_global.local_rank, src_dt_dup_ptr->typerep);
    // fflush(stdout);
    do {
        if (data_sz <= MPIDI_PIP_LAST_PKT_THRESHOLD) {
            MPI_Aint actual_bytes;
            copy_sz = data_sz;
            MPIR_Typerep_pack(src_buf, src_count, src_dt_dup, 0, dest_buf, copy_sz, &actual_bytes);
            MPIR_Assert(actual_bytes == copy_sz);

            MPIDI_PIP_fflush_task();

            while (MPIDI_PIP_global.compl_queue->head)
                MPIDI_PIP_fflush_compl_task(MPIDI_PIP_global.compl_queue);
            MPIR_Type_free_impl(&src_dt_dup);
        } else {
            MPIDI_PIP_task_t *task = (MPIDI_PIP_task_t *) MPIR_Handle_obj_alloc(&MPIDI_Task_mem);
            copy_sz = MPIDI_PIP_PKT_SIZE;
            inoffset -= copy_sz;
            revs_dest_buf -= copy_sz;
            MPIDI_PIP_init_pack_task(task, src_buf, src_count, inoffset, src_dt_dup_ptr,
                                     revs_dest_buf, copy_sz, task_kind);
            MPIDI_PIP_Task_safe_enqueue(MPIDI_PIP_global.task_queue, task);
            MPIDI_PIP_Compl_task_enqueue(MPIDI_PIP_global.compl_queue, task);

            if (MPIDI_PIP_global.compl_queue->task_num >= MPIDI_MAX_TASK_THRESHOLD)
                MPIDI_PIP_exec_one_task(MPIDI_PIP_global.task_queue, MPIDI_PIP_global.compl_queue);
        }
        data_sz -= copy_sz;
    } while (data_sz > 0);
    return;
}

MPL_STATIC_INLINE_PREFIX void MPIDI_PIP_init_unpack_task(MPIDI_PIP_task_t * task, void *src_buf,
                                                         MPI_Aint insize, void *dest_buf,
                                                         MPI_Aint dest_count, MPI_Aint outoffset,
                                                         MPIR_Datatype * dest_dt_ptr, int task_kind)
{
    task->compl_flag = MPIDI_PIP_NOT_COMPLETE;
    task->task_next = NULL;
    task->compl_next = NULL;

    task->task_kind = task_kind;
    task->copy_kind = MPIDI_PIP_UNPACK;

    task->src_buf = src_buf;
    task->dest_buf = dest_buf;
    task->data_sz = insize;

    task->dest_count = dest_count;
    task->dest_dt_ptr = dest_dt_ptr;
    task->dest_offset = outoffset;
    return;
}

MPL_STATIC_INLINE_PREFIX void MPIDI_PIP_unpack_task_enqueue(char *src_buf,
                                                            void *dest_buf, MPI_Aint dest_count,
                                                            MPI_Datatype dest_dt, MPI_Aint data_sz,
                                                            int task_kind)
{
    MPI_Aint copy_sz;
    MPI_Aint outoffset = data_sz;
    char *revs_src_buf = src_buf + data_sz;
    MPIR_Datatype *dest_dt_ptr;
    MPIR_Datatype_get_ptr(dest_dt, dest_dt_ptr);
    do {
        if (data_sz <= MPIDI_PIP_LAST_PKT_THRESHOLD) {
            MPI_Aint actual_bytes;
            copy_sz = data_sz;
            MPIR_Typerep_unpack(src_buf, copy_sz, dest_buf, dest_count, dest_dt, 0, &actual_bytes);
            MPIR_Assert(actual_bytes == copy_sz);

            MPIDI_PIP_fflush_task();

            while (MPIDI_PIP_global.compl_queue->head)
                MPIDI_PIP_fflush_compl_task(MPIDI_PIP_global.compl_queue);
        } else {
            MPIDI_PIP_task_t *task = (MPIDI_PIP_task_t *) MPIR_Handle_obj_alloc(&MPIDI_Task_mem);
            copy_sz = MPIDI_PIP_PKT_SIZE;
            revs_src_buf -= copy_sz;
            outoffset -= copy_sz;
            MPIDI_PIP_init_unpack_task(task, revs_src_buf, copy_sz, dest_buf, dest_count, outoffset,
                                       dest_dt_ptr, task_kind);
            MPIDI_PIP_Task_safe_enqueue(MPIDI_PIP_global.task_queue, task);
            MPIDI_PIP_Compl_task_enqueue(MPIDI_PIP_global.compl_queue, task);

            if (MPIDI_PIP_global.compl_queue->task_num >= MPIDI_MAX_TASK_THRESHOLD)
                MPIDI_PIP_exec_one_task(MPIDI_PIP_global.task_queue, MPIDI_PIP_global.compl_queue);
        }
        data_sz -= copy_sz;
    } while (data_sz > 0);
    return;
}

MPL_STATIC_INLINE_PREFIX void MPIDI_PIP_init_pack_unpack_task(MPIDI_PIP_task_t * task,
                                                              void *src_buf, MPI_Aint src_count,
                                                              MPI_Aint inoffset,
                                                              MPIR_Datatype * src_dt_ptr,
                                                              void *dest_buf, MPI_Aint dest_count,
                                                              MPI_Aint outoffset,
                                                              MPIR_Datatype * dest_dt_ptr,
                                                              MPI_Aint max_in_out_bytes,
                                                              int task_kind)
{
    task->compl_flag = MPIDI_PIP_NOT_COMPLETE;
    task->task_next = NULL;
    task->compl_next = NULL;

    task->task_kind = task_kind;
    task->copy_kind = MPIDI_PIP_PACK_UNPACK;

    task->src_buf = src_buf;
    task->dest_buf = dest_buf;
    task->data_sz = max_in_out_bytes;

    task->src_count = src_count;
    task->src_dt_ptr = src_dt_ptr;
    task->src_offset = inoffset;

    task->dest_count = dest_count;
    task->dest_dt_ptr = dest_dt_ptr;
    task->dest_offset = outoffset;
    return;
}

MPL_STATIC_INLINE_PREFIX void MPIDI_PIP_pack_unpack_task_enqueue(void *src_buf,
                                                                 MPI_Aint src_count,
                                                                 MPIR_Datatype * src_dt_ptr,
                                                                 void *dest_buf,
                                                                 MPI_Aint dest_count,
                                                                 MPI_Datatype dest_dt,
                                                                 MPI_Aint data_sz, int task_kind)
{
    MPI_Aint copy_sz;
    MPI_Aint inoffset = data_sz;
    MPI_Aint outoffset = data_sz;
    char *revs_src_buf = src_buf + data_sz;
    MPI_Datatype src_dt_dup;
    MPIR_Datatype *src_dt_dup_ptr;
    MPIR_Datatype *dest_dt_ptr;
    // printf("rank %d - pack/unpack src_dt_ptr->typerep %p\n", MPIDI_PIP_global.local_rank, src_dt_ptr->typerep);
    // fflush(stdout);
    MPIR_PIP_Type_dup(src_dt_ptr, &src_dt_dup);

    MPIR_Datatype_get_ptr(src_dt_dup, src_dt_dup_ptr);
    MPIR_Datatype_get_ptr(dest_dt, dest_dt_ptr);

    // printf("rank %d - END pack/unpack enqueue routine, src_dup->typerep %p\n", MPIDI_PIP_global.local_rank, src_dt_dup_ptr->typerep);
    // fflush(stdout);
    /* pack enqueue */
    do {
        if (data_sz <= MPIDI_PIP_LAST_PKT_THRESHOLD) {
            MPI_Aint actual_bytes;

            copy_sz = data_sz;
            MPIR_Typerep_pack(src_buf, src_count, src_dt_dup, 0, MPIDI_PIP_global.pkt_load, copy_sz,
                              &actual_bytes);
            MPIR_Assert(actual_bytes == copy_sz);

            MPIR_Typerep_unpack(MPIDI_PIP_global.pkt_load, copy_sz, dest_buf, dest_count, dest_dt,
                                0, &actual_bytes);
            MPIR_Assert(actual_bytes == copy_sz);

            MPIDI_PIP_fflush_task();
            while (MPIDI_PIP_global.compl_queue->head)
                MPIDI_PIP_fflush_compl_task(MPIDI_PIP_global.compl_queue);
            MPIR_Type_free_impl(&src_dt_dup);
        } else {
            copy_sz = MPIDI_PIP_PKT_SIZE;
            MPIDI_PIP_task_t *task = (MPIDI_PIP_task_t *) MPIR_Handle_obj_alloc(&MPIDI_Task_mem);

            inoffset -= copy_sz;
            outoffset -= copy_sz;

            MPIDI_PIP_init_pack_unpack_task(task, src_buf, src_count, inoffset, src_dt_dup_ptr,
                                            dest_buf, dest_count, outoffset, dest_dt_ptr, copy_sz,
                                            task_kind);

            MPIDI_PIP_Task_safe_enqueue(MPIDI_PIP_global.task_queue, task);
            MPIDI_PIP_Compl_task_enqueue(MPIDI_PIP_global.compl_queue, task);

            if (MPIDI_PIP_global.compl_queue->task_num >= MPIDI_MAX_TASK_THRESHOLD) {
                MPIDI_PIP_exec_one_task(MPIDI_PIP_global.task_queue, MPIDI_PIP_global.compl_queue);
            }
        }

        data_sz -= copy_sz;
    } while (data_sz > 0);


    return;
}

MPL_STATIC_INLINE_PREFIX int MPIDI_PIP_handle_lmt_rts_recv(uint64_t src_offset, MPI_Aint src_count,
                                                           uint64_t src_data_sz, uint64_t sreq_ptr,
                                                           int src_is_contig,
                                                           MPIR_Datatype * src_dt_ptr,
                                                           int src_lrank, uint64_t partner,
                                                           int partner_queue, MPIR_Request * rreq)
{
    int mpi_errno = MPI_SUCCESS;
    MPI_Aint data_sz, recv_data_sz;
    int dest_dt_contig, true_lb;
    MPIDI_SHM_ctrl_hdr_t ack_ctrl_hdr;
    MPIDI_SHM_ctrl_pip_send_lmt_send_fin_t *slmt_fin_hdr = &ack_ctrl_hdr.pip_slmt_fin;

    MPIR_FUNC_VERBOSE_STATE_DECL(MPID_STATE_PIP_HANDLE_LMT_RECV);
    MPIR_FUNC_VERBOSE_ENTER(MPID_STATE_PIP_HANDLE_LMT_RECV);

    MPIDI_Datatype_check_contig_size_lb(MPIDIG_REQUEST(rreq, datatype), MPIDIG_REQUEST(rreq, count),
                                        dest_dt_contig, data_sz, true_lb);
    if (src_data_sz > data_sz)
        rreq->status.MPI_ERROR = MPI_ERR_TRUNCATE;

    /* Copy data to receive buffer */
    recv_data_sz = MPL_MIN(src_data_sz, data_sz);
    int task_kind =
        MPIDI_PIP_global.local_numa_id ==
        MPIDI_PIP_global.pip_global_array[src_lrank]->local_numa_id ? MPIDI_PIP_INTRA_TASK :
        MPIDI_PIP_INTER_TASK;

    int copy_kind;
    if (src_is_contig && dest_dt_contig) {
        /* both are contiguous */
        MPIDI_PIP_memcpy_task_enqueue((char *) src_offset,
                                      (char *) MPIDIG_REQUEST(rreq, buffer) + true_lb,
                                      recv_data_sz, task_kind);
    } else if (!src_is_contig && dest_dt_contig) {
        /* src data is non-contig */
        MPIDI_PIP_pack_task_enqueue((void *) src_offset, src_count, src_dt_ptr,
                                    (char *) MPIDIG_REQUEST(rreq, buffer) + true_lb, recv_data_sz,
                                    task_kind);
    } else if (src_is_contig && !dest_dt_contig) {
        /* dest data is non-contig */
        MPIDI_PIP_unpack_task_enqueue((char *) src_offset,
                                      MPIDIG_REQUEST(rreq, buffer), MPIDIG_REQUEST(rreq, count),
                                      MPIDIG_REQUEST(rreq, datatype), recv_data_sz, task_kind);
    } else {
        /* both are non-contig */
        MPIDI_PIP_pack_unpack_task_enqueue((void *) src_offset,
                                           src_count, src_dt_ptr, MPIDIG_REQUEST(rreq, buffer),
                                           MPIDIG_REQUEST(rreq, count), MPIDIG_REQUEST(rreq,
                                                                                       datatype),
                                           recv_data_sz, task_kind);
    }

    PIP_TRACE("handle_lmt_recv: handle matched rreq %p [source %d, tag %d, context_id 0x%x],"
              " copy dst %p, src %p, bytes %ld\n", rreq, MPIDIG_REQUEST(rreq, rank),
              MPIDIG_REQUEST(rreq, tag), MPIDIG_REQUEST(rreq, context_id),
              (char *) MPIDIG_REQUEST(rreq, buffer), (void *) src_offset, recv_data_sz);

    /* Set receive status */
    MPIR_STATUS_SET_COUNT(rreq->status, recv_data_sz);
    rreq->status.MPI_SOURCE = MPIDIG_REQUEST(rreq, rank);
    rreq->status.MPI_TAG = MPIDIG_REQUEST(rreq, tag);

    /* Send ack to sender */
    slmt_fin_hdr->req_ptr = sreq_ptr;
    slmt_fin_hdr->partner = partner;
    slmt_fin_hdr->partner_queue = partner_queue;
    mpi_errno =
        MPIDI_SHM_do_ctrl_send(MPIDIG_REQUEST(rreq, rank),
                               MPIDIG_context_id_to_comm(MPIDIG_REQUEST(rreq, context_id)),
                               MPIDI_SHM_PIP_SEND_LMT_SEND_ACK, &ack_ctrl_hdr);
    MPIR_ERR_CHECK(mpi_errno);

    MPIR_Datatype_release_if_not_builtin(MPIDIG_REQUEST(rreq, datatype));
    MPID_Request_complete(rreq);

  fn_exit:
    MPIR_FUNC_VERBOSE_EXIT(MPID_STATE_PIP_HANDLE_LMT_RECV);
    return mpi_errno;
  fn_fail:
    goto fn_exit;
}


#endif /* PIP_RECV_H_INCLUDED */
