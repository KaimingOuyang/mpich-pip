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

MPL_STATIC_INLINE_PREFIX void MPIDI_PIP_init_task(MPIDI_PIP_task_t * task, void *src_buf,
                                                  size_t copy_sz, void *dest_buf)
{
    task->compl_flag = 0;
    task->src_buf = src_buf;
    task->dest_buf = dest_buf;

    task->data_sz = copy_sz;
    task->task_next = NULL;
    task->compl_next = NULL;
    return;
}

MPL_STATIC_INLINE_PREFIX void MPIDI_PIP_lmt_rts_recv_enqueue_tasks(char *src_buf,
                                                                   uint64_t data_sz, char *dest_buf)
{
    uint64_t copy_sz;
    do {
        if (data_sz < MPIDI_PIP_LAST_PKT_THRESHOLD) {
            /* Last packet, I need to copy it myself. */
            copy_sz = data_sz;
            MPIR_Memcpy((void *) dest_buf, (void *) src_buf, copy_sz);

        } else if (data_sz < MPIDI_PIP_SEC_LAST_PKT_THRESHOLD) {
            /* Second last packet, I need to copy it myself and flush all previous tasks. */
            copy_sz = MPIDI_PIP_PKT_SIZE;
            MPIR_Memcpy((void *) dest_buf, (void *) src_buf, copy_sz);
            MPIDI_PIP_fflush_task();
            while (MPIDI_PIP_global.compl_queue->head)
                MPIDI_PIP_fflush_compl_task(MPIDI_PIP_global.compl_queue);
        } else {
            /* I have a lot of tasks to do, enqueue tasks and hope others steal them. */
            MPIDI_PIP_task_t *task = (MPIDI_PIP_task_t *) MPIR_Handle_obj_alloc(&MPIDI_Task_mem);

            copy_sz = MPIDI_PIP_PKT_SIZE;
            MPIDI_PIP_init_task(task, src_buf, copy_sz, dest_buf);
            MPIDI_PIP_Task_safe_enqueue(MPIDI_PIP_global.task_queue, task);
            MPIDI_PIP_Compl_task_enqueue(MPIDI_PIP_global.compl_queue, task);

            if (MPIDI_PIP_global.compl_queue->task_num >= MPIDI_MAX_TASK_THRESHOLD)
                MPIDI_PIP_exec_one_task(MPIDI_PIP_global.task_queue, MPIDI_PIP_global.compl_queue);
        }

        src_buf += copy_sz;
        dest_buf += copy_sz;
        data_sz -= copy_sz;
    } while (data_sz);
    return;
}

MPL_STATIC_INLINE_PREFIX int MPIDI_PIP_handle_lmt_rts_recv(uint64_t src_offset,
                                                           uint64_t src_data_sz, uint64_t sreq_ptr,
                                                           int src_lrank, MPIR_Request * rreq)
{
    int mpi_errno = MPI_SUCCESS;
    size_t data_sz, recv_data_sz;
    int dt_contig, true_lb;
    MPIDI_SHM_ctrl_hdr_t ack_ctrl_hdr;
    MPIDI_SHM_ctrl_pip_send_lmt_send_fin_t *slmt_fin_hdr = &ack_ctrl_hdr.pip_slmt_fin;

    MPIR_FUNC_VERBOSE_STATE_DECL(MPID_STATE_PIP_HANDLE_LMT_RECV);
    MPIR_FUNC_VERBOSE_ENTER(MPID_STATE_PIP_HANDLE_LMT_RECV);

    MPIDI_Datatype_check_contig_size_lb(MPIDIG_REQUEST(rreq, datatype), MPIDIG_REQUEST(rreq, count),
                                        dt_contig, data_sz, true_lb);
    if (src_data_sz > data_sz)
        rreq->status.MPI_ERROR = MPI_ERR_TRUNCATE;

    /* Copy data to receive buffer */
    recv_data_sz = MPL_MIN(src_data_sz, data_sz);
#ifdef MPIDI_PIP_STEALING_ENABLE
    if (dt_contig) {
        /* Note: for now, just consider contiguous stealing [Need to fix this issue] */
        MPIDI_PIP_lmt_rts_recv_enqueue_tasks((char *) src_offset, recv_data_sz,
                                             (char *) MPIDIG_REQUEST(rreq, buffer) + true_lb);
        MPIR_ERR_CHECK(mpi_errno);
    } else {
        mpi_errno = MPIR_Localcopy((char *) src_offset, recv_data_sz,
                                   MPI_BYTE, (char *) MPIDIG_REQUEST(rreq, buffer),
                                   MPIDIG_REQUEST(rreq, count), MPIDIG_REQUEST(rreq, datatype));
    }
#else
    mpi_errno = MPIR_Localcopy((char *) src_offset, recv_data_sz,
                               MPI_BYTE, (char *) MPIDIG_REQUEST(rreq, buffer),
                               MPIDIG_REQUEST(rreq, count), MPIDIG_REQUEST(rreq, datatype));
#endif

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
