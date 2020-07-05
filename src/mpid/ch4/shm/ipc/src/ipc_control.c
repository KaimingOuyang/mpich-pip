/*
 * Copyright (C) by Argonne National Laboratory
 *     See COPYRIGHT in top-level directory
 */

#include "mpidimpl.h"
#include "ipc_pre.h"
#include "ipc_types.h"
#include "ipc_recv.h"
#include "ipc_control.h"
#include "mpl_shm.h"
#include "mpidu_shm_seg.h"

int MPIDI_IPCI_send_contig_lmt_fin_cb(MPIDI_SHMI_ctrl_hdr_t * ctrl_hdr)
{
    int mpi_errno = MPI_SUCCESS;
    MPIR_Request *sreq = ctrl_hdr->ipc_contig_slmt_fin.req_ptr;

    MPIR_FUNC_VERBOSE_STATE_DECL(MPID_STATE_MPIDI_IPCI_SEND_CONTIG_LMT_FIN_CB);
    MPIR_FUNC_VERBOSE_ENTER(MPID_STATE_MPIDI_IPCI_SEND_CONTIG_LMT_FIN_CB);

    IPC_TRACE("send_contig_lmt_fin_cb: complete sreq %p\n", sreq);

    if (MPIDIG_REQUEST(sreq, memory)) {
        int mpl_err;
        MPIDU_shm_seg_t *memory = (MPIDU_shm_seg_t *) MPIDIG_REQUEST(sreq, memory);
        mpl_err = MPL_shm_seg_remove(memory->hnd);
        MPIR_ERR_CHKANDJUMP(mpl_err, mpi_errno, MPI_ERR_OTHER, "**remove_shar_mem");

        MPL_shm_hnd_finalize(&(memory->hnd));
        MPL_free(memory);
    }

    MPIR_Datatype_release_if_not_builtin(MPIDIG_REQUEST(sreq, datatype));
    MPID_Request_complete(sreq);

  fn_exit:
    MPIR_FUNC_VERBOSE_EXIT(MPID_STATE_MPIDI_IPCI_SEND_CONTIG_LMT_FIN_CB);
    return mpi_errno;
  fn_fail:
    goto fn_exit;
}

int MPIDI_IPCI_send_contig_lmt_rts_cb(MPIDI_SHMI_ctrl_hdr_t * ctrl_hdr)
{
    int mpi_errno = MPI_SUCCESS;
    MPIDI_IPC_ctrl_send_contig_lmt_rts_t *slmt_rts_hdr = &ctrl_hdr->ipc_contig_slmt_rts;
    MPIR_Request *rreq = NULL;
    MPIR_Comm *root_comm;
    MPIR_FUNC_VERBOSE_STATE_DECL(MPID_STATE_MPIDI_IPCI_SEND_CONTIG_LMT_RTS_CB);
    MPIR_FUNC_VERBOSE_ENTER(MPID_STATE_MPIDI_IPCI_SEND_CONTIG_LMT_RTS_CB);

    IPC_TRACE("send_contig_lmt_rts_cb: received data_sz 0x%" PRIu64 ", sreq_ptr 0x%p, "
              "src_lrank %d, match info[src_rank %d, tag %d, context_id 0x%x]\n",
              slmt_rts_hdr->data_sz, slmt_rts_hdr->sreq_ptr,
              slmt_rts_hdr->src_lrank, slmt_rts_hdr->src_rank, slmt_rts_hdr->tag,
              slmt_rts_hdr->context_id);

    /* Try to match a posted receive request.
     * root_comm cannot be NULL if a posted receive request exists, because
     * we increase its refcount at enqueue time. */
    root_comm = MPIDIG_context_id_to_comm(slmt_rts_hdr->context_id);
    if (root_comm) {
        while (TRUE) {
            rreq = MPIDIG_dequeue_posted(slmt_rts_hdr->src_rank, slmt_rts_hdr->tag,
                                         slmt_rts_hdr->context_id, 1,
                                         &MPIDIG_COMM(root_comm, posted_list));
#ifndef MPIDI_CH4_DIRECT_NETMOD
            if (rreq) {
                int is_cancelled;
                MPIDI_anysrc_try_cancel_partner(rreq, &is_cancelled);
                if (!is_cancelled) {
                    MPIR_Comm_release(root_comm);       /* -1 for posted_list */
                    MPIR_Datatype_release_if_not_builtin(MPIDIG_REQUEST(rreq, datatype));
                    continue;
                }
                /* NOTE: NM partner is freed at MPIDI_anysrc_try_cancel_partner,
                 * no need to call MPIDI_anysrc_free_partner at completions
                 */
            }
#endif
            break;
        }
    }

    if (rreq) {
        void *flattened_type = NULL;

        /* Matching receive was posted */
        MPIR_Comm_release(root_comm);   /* -1 for posted_list */
        MPIDIG_REQUEST(rreq, rank) = slmt_rts_hdr->src_rank;
        MPIDIG_REQUEST(rreq, tag) = slmt_rts_hdr->tag;
        MPIDIG_REQUEST(rreq, context_id) = slmt_rts_hdr->context_id;

        if (slmt_rts_hdr->flattened_type_size)
            flattened_type = slmt_rts_hdr->flattened_type;

        /* Complete IPC receive */
        mpi_errno = MPIDI_IPCI_handle_lmt_recv(slmt_rts_hdr->ipc_type,
                                               slmt_rts_hdr->ipc_handle,
                                               slmt_rts_hdr->data_sz, slmt_rts_hdr->sreq_ptr,
                                               flattened_type, rreq);
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

        /* store IPC internal info */
        MPIDI_SHM_REQUEST(rreq, status) |= MPIDI_SHM_REQ_IPC_SEND_LMT;
        MPIDI_IPCI_REQUEST(rreq, ipc_type) = slmt_rts_hdr->ipc_type;
        MPIDI_IPCI_REQUEST(rreq, unexp_rreq).ipc_handle = slmt_rts_hdr->ipc_handle;
        MPIDI_IPCI_REQUEST(rreq, unexp_rreq).data_sz = slmt_rts_hdr->data_sz;
        MPIDI_IPCI_REQUEST(rreq, unexp_rreq).src_lrank = slmt_rts_hdr->src_lrank;
        MPIDI_IPCI_REQUEST(rreq, unexp_rreq).sreq_ptr = slmt_rts_hdr->sreq_ptr;
        if (slmt_rts_hdr->flattened_type_size) {
            MPIDI_IPCI_REQUEST(rreq, unexp_rreq).flattened_type =
                MPL_malloc(slmt_rts_hdr->flattened_type_size, MPL_MEM_OTHER);
            memcpy(MPIDI_IPCI_REQUEST(rreq, unexp_rreq).flattened_type,
                   slmt_rts_hdr->flattened_type, slmt_rts_hdr->flattened_type_size);
        } else {
            MPIDI_IPCI_REQUEST(rreq, unexp_rreq).flattened_type = NULL;
        }

        if (root_comm) {
            MPIR_Comm_add_ref(root_comm);       /* +1 for unexp_list */
            MPIDIG_enqueue_unexp(rreq, &MPIDIG_COMM(root_comm, unexp_list));
        } else {
            MPIDIG_enqueue_unexp(rreq,
                                 MPIDIG_context_id_to_uelist(MPIDIG_REQUEST(rreq, context_id)));
        }

        IPC_TRACE("send_contig_lmt_rts_cb: enqueue unexpected, rreq=%p\n", rreq);
    }

  fn_exit:
    MPIR_FUNC_VERBOSE_EXIT(MPID_STATE_MPIDI_IPCI_SEND_CONTIG_LMT_RTS_CB);
    return mpi_errno;
  fn_fail:
    goto fn_exit;
}

int MPIDI_IPCI_send_lmt_cts_cb(MPIDI_SHMI_ctrl_hdr_t * ctrl_hdr)
{
    int mpi_errno = MPI_SUCCESS;
    MPIDI_IPC_ctrl_send_contig_lmt_cts_t *ipc_slmt_cts = &ctrl_hdr->ipc_slmt_cts;
    void *flattened_type;

    MPIR_FUNC_VERBOSE_STATE_DECL(MPID_STATE_MPIDI_IPCI_SEND_LMT_CTS_CB);
    MPIR_FUNC_VERBOSE_ENTER(MPID_STATE_MPIDI_IPCI_SEND_LMT_CTS_CB);

    if (ipc_slmt_cts->flattened_type_size)
        flattened_type = ipc_slmt_cts->flattened_type;
    else
        flattened_type = NULL;

    mpi_errno = MPIDI_IPCI_handle_lmt_cts_recv(ipc_slmt_cts->ipc_type, ipc_slmt_cts->ipc_handle,
                                               ipc_slmt_cts->data_sz,
                                               ipc_slmt_cts->rreq_ptr,
                                               flattened_type, ipc_slmt_cts->sreq_ptr);

  fn_exit:
    MPIR_FUNC_VERBOSE_EXIT(MPID_STATE_MPIDI_IPCI_SEND_LMT_CTS_CB);
    return mpi_errno;
  fn_fail:
    goto fn_exit;
}

int MPIDI_IPCI_send_lmt_ctrl_hdr_rts_cb(MPIDI_SHMI_ctrl_hdr_t * ctrl_hdr)
{
    int mpi_errno = MPI_SUCCESS, mpl_err;
    MPIDI_IPC_ctrl_send_lmt_ctrl_hdr_rts_t *slmt_ctrl_hdr = &ctrl_hdr->ipc_slmt_hdr_rts;
    MPIDU_shm_seg_t *memory = NULL;

    MPIR_FUNC_VERBOSE_STATE_DECL(MPID_STATE_MPIDI_IPCI_SEND_LMT_CTRL_HDR_RTS_CB);
    MPIR_FUNC_VERBOSE_ENTER(MPID_STATE_MPIDI_IPCI_SEND_LMT_CTRL_HDR_RTS_CB);

    memory = MPL_malloc(sizeof(MPIDU_shm_seg_t), MPL_MEM_OTHER);
    MPIR_Assert(memory);

    mpl_err = MPL_shm_hnd_init(&(memory->hnd));
    MPIR_ERR_CHKANDJUMP(mpl_err, mpi_errno, MPI_ERR_OTHER, "**alloc_shar_mem");

    memory->segment_len = slmt_ctrl_hdr->ctrl_hdr_size;
    mpl_err =
        MPL_shm_hnd_deserialize(memory->hnd, slmt_ctrl_hdr->serialized_hnd,
                                strlen(slmt_ctrl_hdr->serialized_hnd));
    MPIR_ERR_CHKANDJUMP(mpl_err, mpi_errno, MPI_ERR_OTHER, "**alloc_shar_mem");

    mpl_err = MPL_shm_seg_attach(memory->hnd, memory->segment_len, (void **) &memory->base_addr, 0);
    MPIR_ERR_CHKANDJUMP(mpl_err, mpi_errno, MPI_ERR_OTHER, "**attach_shar_mem");

    /* call actual call back */
    mpi_errno =
        MPIDI_global.shm.ctrl_cbs[slmt_ctrl_hdr->
                                  ctrl_id] ((MPIDI_SHMI_ctrl_hdr_t *) memory->base_addr);
    MPIR_ERR_CHECK(mpi_errno);

    mpl_err = MPL_shm_seg_detach(memory->hnd, (void **) &(memory->base_addr), memory->segment_len);
    MPIR_ERR_CHKANDJUMP(mpl_err, mpi_errno, MPI_ERR_OTHER, "**detach_shar_mem");

  fn_exit:
    MPL_shm_hnd_finalize(&(memory->hnd));
    MPL_free(memory);
    MPIR_FUNC_VERBOSE_EXIT(MPID_STATE_MPIDI_IPCI_SEND_LMT_CTRL_HDR_RTS_CB);
    return mpi_errno;
  fn_fail:
    goto fn_exit;
}
