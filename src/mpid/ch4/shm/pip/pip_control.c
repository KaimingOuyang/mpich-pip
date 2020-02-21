/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 * (C) 2019 by Argonne National Laboratory.
 *     See COPYRIGHT in top-level directory.
 */

#include "mpidimpl.h"
#include "pip_recv.h"

int MPIDI_PIP_ctrl_send_lmt_send_fin_cb(MPIDI_SHM_ctrl_hdr_t * ctrl_hdr)
{
    int mpi_errno = MPI_SUCCESS;
    MPIR_Request *sreq = (MPIR_Request *) ctrl_hdr->pip_slmt_fin.req_ptr;
    MPIDI_PIP_partner_t *partner = (MPIDI_PIP_partner_t *) ctrl_hdr->pip_slmt_fin.partner;
    int partner_queue = ctrl_hdr->pip_slmt_fin.partner_queue;
    MPIR_FUNC_VERBOSE_STATE_DECL(MPID_STATE_XPMEM_CTRL_SEND_LMT_ACK_CB);
    MPIR_FUNC_VERBOSE_ENTER(MPID_STATE_XPMEM_CTRL_SEND_LMT_ACK_CB);

    PIP_TRACE("send_lmt_ack_cb: complete sreq %p\n", sreq);
    if (partner_queue == MPIDI_PIP_INTRA_QUEUE)
        MPIDI_PIP_PARTNER_DEQUEUE(partner, &MPIDI_PIP_global.intrap_queue);
    else
        MPIDI_PIP_PARTNER_DEQUEUE(partner, &MPIDI_PIP_global.interp_queue);
    MPIR_Handle_obj_free(&MPIDI_Partner_mem, partner);
    MPID_Request_complete(sreq);

    MPIR_FUNC_VERBOSE_EXIT(MPID_STATE_XPMEM_CTRL_SEND_LMT_ACK_CB);
    return mpi_errno;
}

int MPIDI_PIP_ctrl_send_lmt_rts_cb(MPIDI_SHM_ctrl_hdr_t * ctrl_hdr)
{
    int mpi_errno = MPI_SUCCESS;
    MPIDI_SHM_ctrl_pip_send_lmt_rts_t *slmt_rts_hdr = &ctrl_hdr->pip_slmt_rts;
    MPIR_Request *rreq = NULL;
    MPIR_Comm *root_comm;
    MPIR_Request *anysource_partner;

    MPIR_FUNC_VERBOSE_STATE_DECL(MPID_STATE_PIP_CTRL_SEND_LMT_REQ_CB);
    MPIR_FUNC_VERBOSE_ENTER(MPID_STATE_PIP_CTRL_SEND_LMT_REQ_CB);

    PIP_TRACE("send_lmt_req_cb: received src_offset 0x%lx, data_sz 0x%lx, sreq_ptr 0x%lx, "
              "src_lrank %d, match info[src_rank %d, tag %d, context_id 0x%x]\n",
              slmt_rts_hdr->src_offset, slmt_rts_hdr->data_sz, slmt_rts_hdr->sreq_ptr,
              slmt_rts_hdr->src_lrank, slmt_rts_hdr->src_rank, slmt_rts_hdr->tag,
              slmt_rts_hdr->context_id);

    /* Try to match a posted receive request.
     * root_comm cannot be NULL if a posted receive request exists, because
     * we increase its refcount at enqueue time. */
    root_comm = MPIDIG_context_id_to_comm(slmt_rts_hdr->context_id);
    if (root_comm) {
        int continue_matching = 1;
        while (continue_matching) {
            anysource_partner = NULL;

            rreq = MPIDIG_dequeue_posted(slmt_rts_hdr->src_rank, slmt_rts_hdr->tag,
                                         slmt_rts_hdr->context_id,
                                         &MPIDIG_COMM(root_comm, posted_list));

            if (rreq && MPIDI_REQUEST_ANYSOURCE_PARTNER(rreq)) {
                /* Try to cancel NM parter request */
                anysource_partner = MPIDI_REQUEST_ANYSOURCE_PARTNER(rreq);
                mpi_errno = MPIDI_anysource_matched(anysource_partner,
                                                    MPIDI_SHM, &continue_matching);
                MPIR_ERR_CHECK(mpi_errno);

                if (continue_matching) {
                    /* NM partner request has already been matched, we need to continue until
                     * no matching rreq. This SHM rreq will be cancelled by NM. */
                    MPIR_Comm_release(root_comm);       /* -1 for posted_list */
                    MPIR_Datatype_release_if_not_builtin(MPIDIG_REQUEST(rreq, datatype));
                    continue;
                }

                /* Release cancelled NM partner request (only SHM request is returned to user) */
                MPIDI_REQUEST_ANYSOURCE_PARTNER(rreq) = NULL;
                MPIDI_REQUEST_ANYSOURCE_PARTNER(anysource_partner) = NULL;
                MPIR_Request_free(anysource_partner);
            }
            break;
        }
    }

    if (rreq) {
        /* Matching receive was posted */
        MPIR_Comm_release(root_comm);   /* -1 for posted_list */
        MPIDIG_REQUEST(rreq, rank) = slmt_rts_hdr->src_rank;
        MPIDIG_REQUEST(rreq, tag) = slmt_rts_hdr->tag;
        MPIDIG_REQUEST(rreq, context_id) = slmt_rts_hdr->context_id;

        /* Complete XPMEM receive */
        mpi_errno = MPIDI_PIP_handle_lmt_rts_recv(slmt_rts_hdr->src_offset, slmt_rts_hdr->src_count,
                                                  slmt_rts_hdr->data_sz,
                                                  slmt_rts_hdr->sreq_ptr, slmt_rts_hdr->is_contig,
                                                  slmt_rts_hdr->src_dt_ptr,
                                                  slmt_rts_hdr->src_lrank,
                                                  slmt_rts_hdr->partner,
                                                  slmt_rts_hdr->partner_queue,
                                                  slmt_rts_hdr->inter_flag, rreq);
        MPIR_ERR_CHECK(mpi_errno);
    } else {
        /* Enqueue unexpected receive request */
        rreq = MPIDIG_request_create(MPIR_REQUEST_KIND__RECV, 2);
        MPIR_ERR_CHKANDSTMT(rreq == NULL, mpi_errno, MPIX_ERR_NOREQ, goto fn_fail, "**nomemreq");

        /* store CH4 am rreq info */
        MPIDIG_REQUEST(rreq, buffer) = NULL;
        MPIDIG_REQUEST(rreq, datatype) = MPI_BYTE;
        MPIDIG_REQUEST(rreq, count) = slmt_rts_hdr->data_sz;
        MPIDIG_REQUEST(rreq, rank) = slmt_rts_hdr->src_rank;
        MPIDIG_REQUEST(rreq, tag) = slmt_rts_hdr->tag;
        MPIDIG_REQUEST(rreq, context_id) = slmt_rts_hdr->context_id;
        MPIDI_REQUEST(rreq, is_local) = 1;

        /* store XPMEM internal info */
        MPIDI_PIP_REQUEST(rreq, unexp_rreq).src_offset = slmt_rts_hdr->src_offset;
        MPIDI_PIP_REQUEST(rreq, unexp_rreq).data_sz = slmt_rts_hdr->data_sz;
        MPIDI_PIP_REQUEST(rreq, unexp_rreq).sreq_ptr = slmt_rts_hdr->sreq_ptr;
        MPIDI_PIP_REQUEST(rreq, unexp_rreq).src_lrank = slmt_rts_hdr->src_lrank;
        MPIDI_PIP_REQUEST(rreq, unexp_rreq).is_contig = slmt_rts_hdr->is_contig;
        MPIDI_PIP_REQUEST(rreq, unexp_rreq).src_dt_ptr = slmt_rts_hdr->src_dt_ptr;
        MPIDI_PIP_REQUEST(rreq, unexp_rreq).src_count = slmt_rts_hdr->src_count;
        MPIDI_PIP_REQUEST(rreq, unexp_rreq).partner = slmt_rts_hdr->partner;
        MPIDI_PIP_REQUEST(rreq, unexp_rreq).partner_queue = slmt_rts_hdr->partner_queue;
        MPIDI_PIP_REQUEST(rreq, unexp_rreq).inter_flag = slmt_rts_hdr->inter_flag;

        MPIDI_SHM_REQUEST(rreq, status) |= MPIDI_SHM_REQ_PIP_SEND_LMT;

        if (root_comm) {
            MPIR_Comm_add_ref(root_comm);       /* +1 for unexp_list */
            MPIDIG_enqueue_unexp(rreq, &MPIDIG_COMM(root_comm, unexp_list));
        } else {
            MPIDIG_enqueue_unexp(rreq,
                                 MPIDIG_context_id_to_uelist(MPIDIG_REQUEST(rreq, context_id)));
        }

        PIP_TRACE("send_lmt_req_cb: enqueue unexpected, rreq=%p\n", rreq);
    }

  fn_exit:
    MPIR_FUNC_VERBOSE_EXIT(MPID_STATE_PIP_CTRL_SEND_LMT_REQ_CB);
    return mpi_errno;
  fn_fail:
    goto fn_exit;
}

int MPIDI_PIP_ctrl_send_lmt_cts_cb(MPIDI_SHM_ctrl_hdr_t * ctrl_hdr)
{
    MPIDI_SHM_ctrl_pip_send_lmt_cts_t *slmt_cts_hdr = &ctrl_hdr->pip_slmt_cts;
    MPIR_Request *sreq = (MPIR_Request *) slmt_cts_hdr->sreq_ptr;
    MPI_Aint remain_data = slmt_cts_hdr->remain_data;
    MPI_Aint actual_bytes;
    MPI_Aint copy_sz;
    int mpi_errno = MPI_SUCCESS;
    void *src_buf = MPIDIG_REQUEST(sreq, buffer);
    MPI_Aint src_count = MPIDIG_REQUEST(sreq, count);
    MPI_Datatype src_dt = MPIDIG_REQUEST(sreq, datatype);
    int buffer_index = MPIDI_PIP_global.buffer_index;
    MPI_Aint send_data = 0;
    int start_index, i, send_cnt = 0;
    // int remain_cnt = remain_data / MPIDI_PIP_CELL_SIZE + (remain_data % MPIDI_PIP_CELL_SIZE) ? 0 : 1;

    start_index = buffer_index;
    while (MPIDI_PIP_global.cells[buffer_index].full == 0 && send_data < remain_data) {
        send_data += MPIDI_PIP_CELL_SIZE;
        send_cnt++;
        buffer_index = (buffer_index + 1) % MPIDI_PIP_CELL_NUM;
        if (buffer_index == start_index)
            break;
    }
    MPIDI_PIP_global.buffer_index = buffer_index;
    if (send_data > remain_data)
        send_data = remain_data;

    MPIDI_SHM_ctrl_hdr_t ctrl_hdr_pkt;
    MPIDI_SHM_ctrl_pip_send_lmt_pkt_t *pip_slmt_pkt = &ctrl_hdr_pkt.pip_slmt_pkt;
    pip_slmt_pkt->data_sz = send_data;
    pip_slmt_pkt->rreq_ptr = slmt_cts_hdr->rreq_ptr;
    pip_slmt_pkt->sreq_ptr = slmt_cts_hdr->sreq_ptr;
    pip_slmt_pkt->start_index = start_index;
    pip_slmt_pkt->cells = (uint64_t) MPIDI_PIP_global.cells;

    mpi_errno =
        MPIDI_SHM_do_ctrl_send(MPIDIG_REQUEST(sreq, rank),
                               MPIDIG_context_id_to_comm(MPIDIG_REQUEST(sreq, context_id)),
                               MPIDI_SHM_PIP_SEND_LMT_PKT, &ctrl_hdr_pkt);
    MPIR_ERR_CHECK(mpi_errno);

    int cur_index = start_index;
    MPIR_Datatype *src_dt_ptr;
    MPIR_Datatype_get_ptr(src_dt, src_dt_ptr);
    MPIDI_PIP_task_t *task_head = NULL;
    MPIDI_PIP_task_t *task_tail = NULL;
    while (send_data) {
        if (remain_data < MPIDI_PIP_CELL_SIZE)
            copy_sz = remain_data;
        else
            copy_sz = MPIDI_PIP_CELL_SIZE;
        MPIDI_PIP_task_t *task = (MPIDI_PIP_task_t *) MPIR_Handle_obj_alloc(&MPIDI_Task_mem);
        MPIDI_PIP_init_knl_pack_task(task, src_buf, src_count, MPIDI_PIP_REQUEST(sreq, offset),
                                     src_dt_ptr, &MPIDI_PIP_global.cells[cur_index], copy_sz,
                                     MPIDI_PIP_INTER_TASK);

        if (task_head == NULL)
            task_tail = task_head = task;
        else {
            task_tail->task_next = task;
            task_tail->compl_next = task;
            task_tail = task;
        }

        MPIDI_PIP_REQUEST(sreq, offset) += copy_sz;
        remain_data -= copy_sz;
        send_data -= copy_sz;
        cur_index = (cur_index + 1) % MPIDI_PIP_CELL_NUM;
    }

    MPIDI_PIP_Task_safe_enqueue_knl(MPIDI_PIP_global.task_queue, task_head, task_tail, send_cnt);
    MPIDI_PIP_Compl_task_enqueue_knl(MPIDI_PIP_global.compl_queue, task_head, task_tail, send_cnt);
    MPIDI_PIP_fflush_task();
    while (MPIDI_PIP_global.compl_queue->head)
        MPIDI_PIP_fflush_compl_task(MPIDI_PIP_global.compl_queue);

    if (remain_data == 0)
        MPID_Request_complete(sreq);
  fn_exit:
    return mpi_errno;
  fn_fail:
    goto fn_exit;
}

int MPIDI_PIP_ctrl_send_lmt_pkt_cb(MPIDI_SHM_ctrl_hdr_t * ctrl_hdr)
{
    MPIDI_SHM_ctrl_pip_send_lmt_pkt_t *slmt_pkt_hdr = &ctrl_hdr->pip_slmt_pkt;
    MPIR_Request *rreq = (MPIR_Request *) slmt_pkt_hdr->rreq_ptr;
    MPI_Aint send_data = slmt_pkt_hdr->data_sz;
    MPI_Aint cur_index = slmt_pkt_hdr->start_index;
    MPI_Aint copy_sz;
    MPIDI_PIP_cell_t *cells = (MPIDI_PIP_cell_t *) slmt_pkt_hdr->cells;
    MPI_Aint actual_bytes;
    int mpi_errno = MPI_SUCCESS;

    MPI_Aint remain_data = MPIDI_PIP_REQUEST(rreq, remain_data);
    void *dest_buf = MPIDIG_REQUEST(rreq, buffer);
    MPI_Aint dest_count = MPIDIG_REQUEST(rreq, count);
    int dest_dt = MPIDIG_REQUEST(rreq, datatype);
    MPIR_Datatype *dest_dt_ptr;
    MPIR_Datatype_get_ptr(dest_dt, dest_dt_ptr);
    int send_cnt = send_data / MPIDI_PIP_CELL_SIZE + (send_data % MPIDI_PIP_CELL_SIZE == 0 ? 0 : 1);
    MPIDI_PIP_task_t *task_head = NULL;
    MPIDI_PIP_task_t *task_tail = NULL;
    while (send_data) {
        if (send_data < MPIDI_PIP_CELL_SIZE)
            copy_sz = send_data;
        else
            copy_sz = MPIDI_PIP_CELL_SIZE;

        MPIDI_PIP_task_t *task = (MPIDI_PIP_task_t *) MPIR_Handle_obj_alloc(&MPIDI_Task_mem);
        MPIDI_PIP_init_knl_unpack_task(task, &cells[cur_index], copy_sz, dest_buf, dest_count,
                                       MPIDI_PIP_REQUEST(rreq, offset), dest_dt_ptr,
                                       MPIDI_PIP_INTER_TASK);

        if (task_head == NULL)
            task_tail = task_head = task;
        else {
            task_tail->task_next = task;
            task_tail->compl_next = task;
            task_tail = task;
        }

        // MPIDI_PIP_Task_safe_enqueue(MPIDI_PIP_global.task_queue, task);
        // MPIDI_PIP_Compl_task_enqueue(MPIDI_PIP_global.compl_queue, task);

        // if (MPIDI_PIP_global.compl_queue->task_num >= MPIDI_MAX_TASK_THRESHOLD)
        //     MPIDI_PIP_exec_one_task(MPIDI_PIP_global.task_queue, MPIDI_PIP_global.compl_queue);

        MPIDI_PIP_REQUEST(rreq, offset) += copy_sz;
        send_data -= copy_sz;
        cur_index = (cur_index + 1) % MPIDI_PIP_CELL_NUM;
    }

    MPIDI_PIP_Task_safe_enqueue_knl(MPIDI_PIP_global.task_queue, task_head, task_tail, send_cnt);
    MPIDI_PIP_Compl_task_enqueue_knl(MPIDI_PIP_global.compl_queue, task_head, task_tail, send_cnt);
    MPIDI_PIP_fflush_task();
    while (MPIDI_PIP_global.compl_queue->head)
        MPIDI_PIP_fflush_compl_task(MPIDI_PIP_global.compl_queue);

    MPIDI_PIP_REQUEST(rreq, remain_data) -= slmt_pkt_hdr->data_sz;

    if (MPIDI_PIP_REQUEST(rreq, remain_data) == 0) {
        MPIR_Datatype_release_if_not_builtin(MPIDIG_REQUEST(rreq, datatype));
        MPID_Request_complete(rreq);
    } else {
        MPIDI_SHM_ctrl_hdr_t cts_ctrl_hdr;
        MPIDI_SHM_ctrl_pip_send_lmt_cts_t *slmt_cts_hdr = &cts_ctrl_hdr.pip_slmt_cts;
        slmt_cts_hdr->remain_data = MPIDI_PIP_REQUEST(rreq, remain_data);
        slmt_cts_hdr->sreq_ptr = slmt_pkt_hdr->sreq_ptr;
        slmt_cts_hdr->rreq_ptr = (uint64_t) rreq;
        mpi_errno =
            MPIDI_SHM_do_ctrl_send(MPIDIG_REQUEST(rreq, rank),
                                   MPIDIG_context_id_to_comm(MPIDIG_REQUEST(rreq, context_id)),
                                   MPIDI_SHM_PIP_SEND_LMT_CTS, &cts_ctrl_hdr);
    }
    return mpi_errno;
}
