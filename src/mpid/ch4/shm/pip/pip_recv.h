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
    // int task_kind =
    //     MPIDI_PIP_global.local_numa_id ==
    //     MPIDI_PIP_global.pip_global_array[src_lrank]->local_numa_id ? MPIDI_PIP_INTRA_TASK :
    //     MPIDI_PIP_INTER_TASK;
    // if (partner_queue == MPIDI_PIP_INTRA_QUEUE) {
    //     /* reply CTS to set up partner task_flag */
    //     MPIDI_SHM_ctrl_hdr_t cts_ctrl_hdr;
    //     MPIDI_SHM_ctrl_pip_send_lmt_cts_t *slmt_cts_hdr = &cts_ctrl_hdr.pip_slmt_cts;
    //     slmt_cts_hdr->partner = partner;

    //     mpi_errno =
    //         MPIDI_SHM_do_ctrl_send(MPIDIG_REQUEST(rreq, rank),
    //                                MPIDIG_context_id_to_comm(MPIDIG_REQUEST(rreq, context_id)),
    //                                MPIDI_SHM_PIP_SEND_LMT_CTS, &cts_ctrl_hdr);
    //     MPIR_ERR_CHECK(mpi_errno);
    // }

    uint64_t partner_post = -1;
    if (partner_queue == MPIDI_PIP_INTRA_QUEUE)
        partner_post = partner;

    int copy_kind;
    if (src_is_contig && dest_dt_contig) {
        /* both are contiguous */
        MPIDI_PIP_memcpy_task_enqueue((char *) src_offset,
                                      (char *) MPIDIG_REQUEST(rreq, buffer) + true_lb,
                                      recv_data_sz, partner_post);
    } else if (!src_is_contig && dest_dt_contig) {
        /* src data is non-contig */
        MPIDI_PIP_pack_task_enqueue((void *) src_offset, src_count, src_dt_ptr,
                                    (char *) MPIDIG_REQUEST(rreq, buffer) + true_lb, recv_data_sz,
                                    partner_post);
    } else if (src_is_contig && !dest_dt_contig) {
        /* dest data is non-contig */
        MPIDI_PIP_unpack_task_enqueue((void *) src_offset,
                                      MPIDIG_REQUEST(rreq, buffer), MPIDIG_REQUEST(rreq, count),
                                      MPIDIG_REQUEST(rreq, datatype), recv_data_sz, partner_post);
    } else {
        // if (partner_queue == MPIDI_PIP_INTER_QUEUE) {
        //     /* reply CTS to set up partner task_flag */
        //     MPIDI_SHM_ctrl_hdr_t cts_ctrl_hdr;
        //     MPIDI_SHM_ctrl_pip_send_lmt_cts_t *slmt_cts_hdr = &cts_ctrl_hdr.pip_slmt_cts;
        //     slmt_cts_hdr->partner = partner;

        //     mpi_errno =
        //         MPIDI_SHM_do_ctrl_send(MPIDIG_REQUEST(rreq, rank),
        //                                MPIDIG_context_id_to_comm(MPIDIG_REQUEST(rreq, context_id)),
        //                                MPIDI_SHM_PIP_SEND_LMT_CTS, &cts_ctrl_hdr);
        //     MPIR_ERR_CHECK(mpi_errno);
        // }
        /* both are non-contig */
        if (partner_queue == MPIDI_PIP_INTER_QUEUE)
            partner_post = partner;
        MPIDI_PIP_pack_unpack_task_enqueue((void *) src_offset,
                                           src_count, src_dt_ptr, MPIDIG_REQUEST(rreq, buffer),
                                           MPIDIG_REQUEST(rreq, count), MPIDIG_REQUEST(rreq,
                                                                                       datatype),
                                           recv_data_sz, partner_post);
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
#ifdef ENABLE_PARTNER_STEALING
    slmt_fin_hdr->partner = partner;
    slmt_fin_hdr->partner_queue = partner_queue;
#endif
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
