/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil ; -*- */
/*
 *  (C) 2019 by Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#ifndef PIP_SEND_H_INCLUDED
#define PIP_SEND_H_INCLUDED

#include "ch4_impl.h"
#include "shm_control.h"
#include "pip_pre.h"

MPL_STATIC_INLINE_PREFIX int MPIDI_PIP_lmt_rts_isend(const void *buf, MPI_Aint count,
                                                     MPI_Datatype datatype, int rank, int tag,
                                                     MPIR_Comm * comm, int context_offset,
                                                     MPIDI_av_entry_t * addr,
                                                     MPIR_Request ** request)
{
    int mpi_errno = MPI_SUCCESS;
    MPIR_Request *sreq = NULL;
    size_t data_sz;
    MPI_Aint true_lb;
    bool is_contig;
    MPIR_Datatype *src_datatype = NULL;
    MPIDI_SHM_ctrl_hdr_t ctrl_hdr;
    MPIDI_SHM_ctrl_pip_send_lmt_rts_t *slmt_rts_hdr = &ctrl_hdr.pip_slmt_rts;

    MPIR_FUNC_VERBOSE_STATE_DECL(MPID_STATE_MPIDI_PIP_LMT_ISEND);
    MPIR_FUNC_VERBOSE_ENTER(MPID_STATE_MPIDI_PIP_LMT_ISEND);

    sreq = MPIDIG_request_create(MPIR_REQUEST_KIND__SEND, 2);
    MPIR_ERR_CHKANDSTMT((sreq) == NULL, mpi_errno, MPIX_ERR_NOREQ, goto fn_fail, "**nomemreq");
    *request = sreq;

    MPIDI_Datatype_check_contig_size_lb(datatype, count, is_contig, data_sz, true_lb);
    MPIR_Datatype_get_ptr(datatype, src_datatype);

    MPIR_Assert(data_sz > MPIR_CVAR_CH4_PIP_LMT_MSG_SIZE);

    /* pip internal info */
    if (is_contig)
        slmt_rts_hdr->src_offset = (uint64_t) buf + true_lb;
    else {
        MPIDIG_REQUEST(sreq, buffer) = buf;
        MPIDIG_REQUEST(sreq, count) = count;
        MPIDIG_REQUEST(sreq, datatype) = datatype;
        MPIDIG_REQUEST(sreq, rank) = rank;
        MPIDIG_REQUEST(sreq, context_id) = comm->context_id + context_offset;
        MPIDI_PIP_REQUEST(sreq, offset) = 0;
        slmt_rts_hdr->src_offset = (uint64_t) buf;
    }
    slmt_rts_hdr->data_sz = data_sz;
    slmt_rts_hdr->sreq_ptr = (uint64_t) sreq;
    slmt_rts_hdr->src_lrank = MPIDI_PIP_global.local_rank;
    slmt_rts_hdr->is_contig = is_contig;
    slmt_rts_hdr->src_dt_ptr = src_datatype;
    slmt_rts_hdr->src_count = count;


    /* message matching info */
    slmt_rts_hdr->src_rank = comm->rank;
    slmt_rts_hdr->tag = tag;
    slmt_rts_hdr->context_id = comm->context_id + context_offset;

    /* enqueue partner */
    int src_numa_id = MPIDI_PIP_global.local_numa_id;
    const int grank = MPIDIU_rank_to_lpid(rank, comm);
    int dest_lrank = MPIDI_PIP_global.grank_to_lrank[grank];
    // printf("src_lrank %d sends to dest_lrank %d\n", MPIDI_PIP_global.local_rank, dest_lrank);
    int dest_numa_id = MPIDI_PIP_global.numa_lrank_to_nid[dest_lrank];

    MPIDI_PIP_partner_t *partner =
        (MPIDI_PIP_partner_t *) MPIR_Handle_obj_alloc(&MPIDI_Partner_mem);
    partner->partner = dest_lrank;
    if (src_numa_id == dest_numa_id) {
        MPIDI_PIP_PARTNER_ENQUEUE(partner, &MPIDI_PIP_global.intrap_queue);
        slmt_rts_hdr->partner_queue = MPIDI_PIP_INTRA_QUEUE;
    } else {
        MPIDI_PIP_PARTNER_ENQUEUE(partner, &MPIDI_PIP_global.interp_queue);
        slmt_rts_hdr->partner_queue = MPIDI_PIP_INTER_QUEUE;
    }
    slmt_rts_hdr->partner = (uint64_t) partner;

    PIP_TRACE("pip_lmt_isend: shm ctrl_id %d, src_offset 0x%lx, data_sz 0x%lx, sreq_ptr 0x%lx, "
              "src_lrank %d, match info[dest %d, src_rank %d, tag %d, context_id 0x%x]\n",
              MPIDI_SHM_PIP_SEND_LMT_RTS, slmt_rts_hdr->src_offset,
              slmt_rts_hdr->data_sz, slmt_rts_hdr->sreq_ptr, slmt_rts_hdr->src_lrank,
              rank, slmt_rts_hdr->src_rank, slmt_rts_hdr->tag, slmt_rts_hdr->context_id);

    mpi_errno = MPIDI_SHM_do_ctrl_send(rank, comm, MPIDI_SHM_PIP_SEND_LMT_RTS, &ctrl_hdr);
    MPIR_ERR_CHECK(mpi_errno);

  fn_exit:
    MPIR_FUNC_VERBOSE_EXIT(MPID_STATE_MPIDI_PIP_LMT_ISEND);
    return mpi_errno;
  fn_fail:
    goto fn_exit;
}


MPL_STATIC_INLINE_PREFIX int MPIDI_PIP_lmt_cts_isend(const void *buf, MPI_Aint count,
                                                     MPI_Datatype datatype, int rank, int tag,
                                                     MPIR_Comm * comm, int context_offset,
                                                     MPIDI_av_entry_t * addr,
                                                     MPIR_Request ** request)
{
    int mpi_errno = MPI_SUCCESS;
    MPIR_FUNC_VERBOSE_STATE_DECL(MPID_STATE_MPIDI_PIP_LMT_ISEND);
    MPIR_FUNC_VERBOSE_ENTER(MPID_STATE_MPIDI_PIP_LMT_ISEND);

  fn_exit:
    MPIR_FUNC_VERBOSE_EXIT(MPID_STATE_MPIDI_PIP_LMT_ISEND);
    return mpi_errno;
  fn_fail:
    goto fn_exit;
}

#endif /* PIP_SEND_H_INCLUDED */
