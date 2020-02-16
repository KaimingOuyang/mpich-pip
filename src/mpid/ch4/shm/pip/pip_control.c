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

    MPIR_FUNC_VERBOSE_STATE_DECL(MPID_STATE_XPMEM_CTRL_SEND_LMT_ACK_CB);
    MPIR_FUNC_VERBOSE_ENTER(MPID_STATE_XPMEM_CTRL_SEND_LMT_ACK_CB);

    PIP_TRACE("send_lmt_ack_cb: complete sreq %p\n", sreq);
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
                                                  slmt_rts_hdr->src_lrank, rreq);
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

    while (MPIDI_PIP_global.cells[buffer_index].full);
    MPIR_Assert(MPIDI_PIP_global.cells[buffer_index].full == 0);

    if (remain_data >= MPIDI_PIP_CELL_SIZE)
        copy_sz = MPIDI_PIP_CELL_SIZE;
    else
        copy_sz = remain_data;
    MPIR_Typerep_pack(src_buf, src_count, src_dt, MPIDI_PIP_REQUEST(sreq, offset),
                      MPIDI_PIP_global.cells[buffer_index].load, copy_sz, &actual_bytes);
    OPA_write_barrier();
    MPIR_Assert(actual_bytes == copy_sz);
    MPIDI_PIP_global.cells[buffer_index].full = 1;
    MPIDI_PIP_REQUEST(sreq, offset) += copy_sz;

    MPIDI_SHM_ctrl_hdr_t ctrl_hdr_pkt;
    MPIDI_SHM_ctrl_pip_send_lmt_pkt_t *pip_slmt_pkt = &ctrl_hdr_pkt.pip_slmt_pkt;
    pip_slmt_pkt->data_sz = copy_sz;
    pip_slmt_pkt->rreq_ptr = slmt_cts_hdr->rreq_ptr;
    pip_slmt_pkt->sreq_ptr = slmt_cts_hdr->sreq_ptr;
    pip_slmt_pkt->cell = (uint64_t) & MPIDI_PIP_global.cells[buffer_index];

    mpi_errno =
        MPIDI_SHM_do_ctrl_send(MPIDIG_REQUEST(sreq, rank),
                               MPIDIG_context_id_to_comm(MPIDIG_REQUEST(sreq, context_id)),
                               MPIDI_SHM_PIP_SEND_LMT_PKT, &ctrl_hdr_pkt);
    MPIR_ERR_CHECK(mpi_errno);
    MPIDI_PIP_global.buffer_index = (MPIDI_PIP_global.buffer_index + 1) % MPIDI_PIP_CELL_NUM;

    if (remain_data - copy_sz == 0)
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
    MPI_Aint copy_sz = slmt_pkt_hdr->data_sz;
    MPIDI_PIP_cell_t *cell = (MPIDI_PIP_cell_t *) slmt_pkt_hdr->cell;
    MPI_Aint actual_bytes;
    int mpi_errno = MPI_SUCCESS;
    MPIR_Typerep_unpack(cell->load, copy_sz, MPIDIG_REQUEST(rreq, buffer),
                        MPIDIG_REQUEST(rreq, count), MPIDIG_REQUEST(rreq, datatype),
                        MPIDI_PIP_REQUEST(rreq, offset), &actual_bytes);
    OPA_write_barrier();
    MPIR_Assert(actual_bytes == copy_sz);
    cell->full = 0;
    MPIDI_PIP_REQUEST(rreq, offset) += copy_sz;

    if (MPIDI_PIP_REQUEST(rreq, offset) == MPIDI_PIP_REQUEST(rreq, target_data_sz)) {
        MPIR_Datatype_release_if_not_builtin(MPIDIG_REQUEST(rreq, datatype));
        MPID_Request_complete(rreq);
    } else if (MPIDI_PIP_REQUEST(rreq, remain_data) != 0) {

        MPIDI_SHM_ctrl_hdr_t cts_ctrl_hdr;
        MPIDI_SHM_ctrl_pip_send_lmt_cts_t *slmt_cts_hdr = &cts_ctrl_hdr.pip_slmt_cts;
        slmt_cts_hdr->remain_data = MPIDI_PIP_REQUEST(rreq, remain_data);
        slmt_cts_hdr->sreq_ptr = slmt_pkt_hdr->sreq_ptr;
        slmt_cts_hdr->rreq_ptr = (uint64_t) rreq;
        mpi_errno =
            MPIDI_SHM_do_ctrl_send(MPIDIG_REQUEST(rreq, rank),
                                   MPIDIG_context_id_to_comm(MPIDIG_REQUEST(rreq, context_id)),
                                   MPIDI_SHM_PIP_SEND_LMT_CTS, &cts_ctrl_hdr);
        if (MPIDI_PIP_REQUEST(rreq, remain_data) > MPIDI_PIP_CELL_SIZE) {
            MPIDI_PIP_REQUEST(rreq, remain_data) -= MPIDI_PIP_CELL_SIZE;
        } else {
            MPIDI_PIP_REQUEST(rreq, remain_data) = 0;
        }
    }
    return mpi_errno;
}
