/*
 * Copyright (C) by Argonne National Laboratory
 *     See COPYRIGHT in top-level directory
 */

#ifndef IPC_P2P_H_INCLUDED
#define IPC_P2P_H_INCLUDED

/*
=== BEGIN_MPI_T_CVAR_INFO_BLOCK ===
cvars:
    - name        : MPIR_CVAR_CH4_IPC_NON_CONTIG_CHUNK_SIZE
      category    : CH4
      type        : int
      default     : 262144
      class       : none
      verbosity   : MPI_T_VERBOSITY_USER_BASIC
      scope       : MPI_T_SCOPE_ALL_EQ
      description : >-
        this parameter sets the chunk size for the case where both source and
        destination datatype are non-contiguous; it indicates the amount of data
        to be copied each time
=== END_MPI_T_CVAR_INFO_BLOCK ===
*/

#include "ch4_impl.h"
#include "mpidimpl.h"
#include "shm_control.h"
#include "ipc_pre.h"
#include "ipc_types.h"
#include "ipc_mem.h"

/* Generic IPC protocols for P2P. */

/* Generic sender-initialized LMT routine with contig send buffer.
 *
 * If the send buffer is noncontiguous the submodule can first pack the
 * data into a temporary buffer and use the temporary buffer as the send
 * buffer with this call. The sender gets the memory attributes of the
 * specified buffer (which include IPC type and memory handle), and sends
 * to the receiver. The receiver will then open the remote memory handle
 * and perform direct data transfer.
 */
MPL_STATIC_INLINE_PREFIX int MPIDI_IPCI_send_contig_lmt(const void *buf, MPI_Aint count,
                                                        MPI_Datatype datatype, uintptr_t data_sz,
                                                        int rank, int tag, MPIR_Comm * comm,
                                                        int context_offset, MPIDI_av_entry_t * addr,
                                                        MPIDI_IPCI_ipc_attr_t ipc_attr,
                                                        MPIR_Request ** request)
{
    int mpi_errno = MPI_SUCCESS;
    MPIR_Request *sreq = NULL;
    MPIDI_SHMI_ctrl_hdr_t *ctrl_hdr;
    MPIDI_IPC_ctrl_send_contig_lmt_rts_t *slmt_req_hdr;
    int flattened_type_size, ctrl_hdr_size, dt_contig;
    void *flattened_type_ptr;

    MPIR_FUNC_VERBOSE_STATE_DECL(MPID_STATE_MPIDI_IPCI_SEND_CONTIG_LMT);
    MPIR_FUNC_VERBOSE_ENTER(MPID_STATE_MPIDI_IPCI_SEND_CONTIG_LMT);

    MPIDI_Datatype_check_contig(datatype, dt_contig);

    /* Allocate full memory for control header */
    if (!MPIR_DATATYPE_IS_PREDEFINED(datatype) && !dt_contig) {
        MPIR_Datatype_get_flattened(datatype, &flattened_type_ptr, &flattened_type_size);
    } else {
        flattened_type_size = 0;
    }

    ctrl_hdr_size = sizeof(MPIDI_SHMI_ctrl_hdr_t) + flattened_type_size;
    ctrl_hdr = (MPIDI_SHMI_ctrl_hdr_t *) MPL_malloc(ctrl_hdr_size, MPL_MEM_OTHER);
    MPIR_Assert(ctrl_hdr);

    slmt_req_hdr = &ctrl_hdr->ipc_contig_slmt_rts;
    slmt_req_hdr->flattened_type_size = flattened_type_size;
    if (flattened_type_size)
        memcpy(slmt_req_hdr->flattened_type, flattened_type_ptr, flattened_type_size);

    /* Create send request */
    MPIR_Datatype_add_ref_if_not_builtin(datatype);
    sreq = MPIDIG_request_create(MPIR_REQUEST_KIND__SEND, 2);
    MPIR_ERR_CHKANDSTMT((sreq) == NULL, mpi_errno, MPIX_ERR_NOREQ, goto fn_fail, "**nomemreq");
    *request = sreq;
    MPIDIG_REQUEST(sreq, buffer) = (void *) buf;
    MPIDIG_REQUEST(sreq, datatype) = datatype;
    MPIDIG_REQUEST(sreq, rank) = rank;
    MPIDIG_REQUEST(sreq, count) = count;
    MPIDIG_REQUEST(sreq, context_id) = comm->context_id + context_offset;
    MPIDIG_REQUEST(sreq, memory) = NULL;

    slmt_req_hdr->src_lrank = MPIR_Process.local_rank;
    slmt_req_hdr->data_sz = data_sz;
    slmt_req_hdr->sreq_ptr = sreq;
    slmt_req_hdr->ipc_type = ipc_attr.ipc_type;
    slmt_req_hdr->ipc_handle = ipc_attr.ipc_handle;

    /* message matching info */
    slmt_req_hdr->src_rank = comm->rank;
    slmt_req_hdr->tag = tag;
    slmt_req_hdr->context_id = comm->context_id + context_offset;

    if (ipc_attr.gpu_attr.type == MPL_GPU_POINTER_DEV) {
        mpi_errno = MPIDI_GPU_ipc_handle_cache_insert(rank, comm, ipc_attr.ipc_handle.gpu);
        MPIR_ERR_CHECK(mpi_errno);
    }

    IPC_TRACE("send_contig_lmt: shm ctrl_id %d, data_sz 0x%" PRIu64 ", sreq_ptr 0x%p, "
              "src_lrank %d, match info[dest %d, src_rank %d, tag %d, context_id 0x%x]\n",
              MPIDI_IPC_SEND_CONTIG_LMT_RTS, slmt_req_hdr->data_sz, slmt_req_hdr->sreq_ptr,
              slmt_req_hdr->src_lrank, rank, slmt_req_hdr->src_rank, slmt_req_hdr->tag,
              slmt_req_hdr->context_id);

    if (flattened_type_size) {
        mpi_errno =
            MPIDI_SHM_do_lmt_ctrl_send(rank, comm, MPIDI_IPC_SEND_CONTIG_LMT_RTS,
                                       ctrl_hdr_size, ctrl_hdr, sreq);
    } else {
        mpi_errno = MPIDI_SHM_do_ctrl_send(rank, comm, MPIDI_IPC_SEND_CONTIG_LMT_RTS, ctrl_hdr);
    }

  fn_exit:
    MPL_free(ctrl_hdr);
    MPIR_FUNC_VERBOSE_EXIT(MPID_STATE_MPIDI_IPCI_SEND_CONTIG_LMT);
    return mpi_errno;
  fn_fail:
    goto fn_exit;
}

/* Generic receiver side handler for sender-initialized LMT with contig send buffer.
 *
 * The receiver opens the memory handle issued by sender and then performs unpack
 * to its recv buffer. It closes the memory handle after unpack and finally issues
 * LMT_FIN ack to the sender.
 */
MPL_STATIC_INLINE_PREFIX int MPIDI_IPCI_handle_lmt_recv(MPIDI_IPCI_type_t ipc_type,
                                                        MPIDI_IPCI_ipc_handle_t ipc_handle,
                                                        size_t src_data_sz,
                                                        MPIR_Request * sreq_ptr,
                                                        void *flattened_type, MPIR_Request * rreq)
{
    int mpi_errno = MPI_SUCCESS;
    void *src_buf = NULL, *copy_src_buf;
    uintptr_t data_sz, recv_data_sz;
    MPIDI_SHMI_ctrl_hdr_t ack_ctrl_hdr;
    MPI_Datatype src_datatype;
    MPIR_Datatype *src_datatype_ptr;
    int src_dt_contig, dest_dt_contig, src_true_lb, dest_true_lb;
    uintptr_t src_dt_size, src_count;

    MPIR_FUNC_VERBOSE_STATE_DECL(MPID_STATE_MPIDI_IPCI_HANDLE_LMT_RECV);
    MPIR_FUNC_VERBOSE_ENTER(MPID_STATE_MPIDI_IPCI_HANDLE_LMT_RECV);

    MPIDI_Datatype_check_contig_size_lb(MPIDIG_REQUEST(rreq, datatype), MPIDIG_REQUEST(rreq, count),
                                        dest_dt_contig, data_sz, dest_true_lb);

    /* recover src datatype */
    if (flattened_type) {
        src_datatype_ptr = (MPIR_Datatype *) MPIR_Handle_obj_alloc(&MPIR_Datatype_mem);
        MPIR_Assert(src_datatype_ptr);

        MPIR_Object_set_ref(src_datatype_ptr, 1);
        MPIR_Typerep_unflatten(src_datatype_ptr, flattened_type);
        src_datatype = src_datatype_ptr->handle;
        MPIDI_Datatype_check_contig_size_lb(src_datatype, 1, src_dt_contig, src_dt_size,
                                            src_true_lb);
        src_count = src_data_sz / src_dt_size;
    } else {
        src_datatype_ptr = NULL;
        src_datatype = MPI_BYTE;
        src_dt_contig = 1;
        src_count = src_data_sz;
        src_true_lb = 0;
    }

    /* Data truncation checking */
    recv_data_sz = MPL_MIN(src_data_sz, data_sz);
    if (src_data_sz > data_sz)
        rreq->status.MPI_ERROR = MPI_ERR_TRUNCATE;

    /* Set receive status */
    MPIR_STATUS_SET_COUNT(rreq->status, recv_data_sz);
    rreq->status.MPI_SOURCE = MPIDIG_REQUEST(rreq, rank);
    rreq->status.MPI_TAG = MPIDIG_REQUEST(rreq, tag);

    /* start send cts packet */
    void *vaddr;
    MPIDI_SHMI_ctrl_hdr_t *ctrl_hdr;
    MPIDI_IPCI_ipc_attr_t ipc_attr;
    int flattened_type_size, ctrl_hdr_size;
    void *flattened_type_ptr;
    MPIDI_IPC_ctrl_send_contig_lmt_cts_t *slmt_cts;

    if (!MPIR_DATATYPE_IS_PREDEFINED(MPIDIG_REQUEST(rreq, datatype)) && !dest_dt_contig) {
        MPIR_Datatype_get_flattened(MPIDIG_REQUEST(rreq, datatype), &flattened_type_ptr,
                                    &flattened_type_size);
    } else {
        flattened_type_size = 0;
    }

    ctrl_hdr_size = sizeof(MPIDI_SHMI_ctrl_hdr_t) + flattened_type_size;
    ctrl_hdr = (MPIDI_SHMI_ctrl_hdr_t *) MPL_malloc(ctrl_hdr_size, MPL_MEM_OTHER);
    MPIR_Assert(ctrl_hdr);

    slmt_cts = &ctrl_hdr->ipc_slmt_cts;
    slmt_cts->flattened_type_size = flattened_type_size;
    if (flattened_type_size)
        memcpy(slmt_cts->flattened_type, flattened_type_ptr, flattened_type_size);

    vaddr = (void *) ((char *) MPIDIG_REQUEST(rreq, buffer) + dest_true_lb);

    MPL_pointer_attr_t attr;
    MPIR_Comm *comm = MPIDIG_context_id_to_comm(MPIDIG_REQUEST(rreq, context_id));

    MPIR_GPU_query_pointer_attr(vaddr, &attr);

    if (attr.type == MPL_GPU_POINTER_DEV) {
        mpi_errno = MPIDI_GPU_get_ipc_attr(vaddr, MPIDIG_REQUEST(rreq, rank), comm, &ipc_attr);
        MPIR_ERR_CHECK(mpi_errno);
    } else {
        MPI_Aint dtp_extent, buf_extent;
        MPIR_Datatype_get_extent_macro(MPIDIG_REQUEST(rreq, datatype), dtp_extent);
        buf_extent = dtp_extent * MPIDIG_REQUEST(rreq, count);
        /* datatype extent could be negative, so we need to assure buffer
         * extent is positive for attachment. */
        if (buf_extent < 0)
            buf_extent = buf_extent * (MPI_Aint) - 1;

        mpi_errno = MPIDI_XPMEM_get_ipc_attr(vaddr, buf_extent, &ipc_attr);
        MPIR_ERR_CHECK(mpi_errno);
    }

    slmt_cts->data_sz = recv_data_sz;
    slmt_cts->sreq_ptr = sreq_ptr;
    slmt_cts->rreq_ptr = rreq;
    slmt_cts->ipc_type = ipc_attr.ipc_type;
    slmt_cts->ipc_handle = ipc_attr.ipc_handle;

    mpi_errno =
        MPIDI_SHM_do_lmt_ctrl_send(MPIDIG_REQUEST(rreq, rank), comm,
                                   MPIDI_IPC_SEND_LMT_CTS, ctrl_hdr_size, ctrl_hdr, rreq);
    MPIR_ERR_CHECK(mpi_errno);

    MPL_free(ctrl_hdr);
    /* end send cts packet */

    /* attach remote buffer */
    switch (ipc_type) {
        case MPIDI_IPCI_TYPE__XPMEM:
            mpi_errno = MPIDI_XPMEM_ipc_handle_map(ipc_handle.xpmem, &src_buf);
            break;
        case MPIDI_IPCI_TYPE__GPU:
            mpi_errno =
                MPIDI_GPU_ipc_handle_map(ipc_handle.gpu, attr.device,
                                         MPIDIG_REQUEST(rreq, datatype), &src_buf);
            break;
        case MPIDI_IPCI_TYPE__NONE:
            /* no-op */
            break;
        default:
            /* Unknown IPC type */
            MPIR_Assert(0);
            break;
    }

    IPC_TRACE("handle_lmt_recv: handle matched rreq %p [source %d, tag %d, "
              " context_id 0x%x], copy dst %p, bytes %ld\n", rreq,
              MPIDIG_REQUEST(rreq, rank), MPIDIG_REQUEST(rreq, tag),
              MPIDIG_REQUEST(rreq, context_id), (char *) MPIDIG_REQUEST(rreq, buffer),
              recv_data_sz);

    /* Copy data to receive buffer */
    MPI_Aint actual_unpack_bytes;
    MPI_Aint actual_pack_bytes;
    MPI_Aint offset, copy_data_sz;

    copy_src_buf = (void *) ((uintptr_t) src_buf - src_true_lb);
    offset = 0;
    copy_data_sz = recv_data_sz >> 1;

    if (!src_dt_contig && dest_dt_contig) {
        /* source datatype is non-contiguous and destination datatype is contiguous */
        mpi_errno = MPIR_Typerep_pack((const void *) copy_src_buf, src_count, src_datatype,
                                      offset, vaddr, copy_data_sz, &actual_pack_bytes);
        MPIR_ERR_CHECK(mpi_errno);
        MPIR_Assert(actual_pack_bytes <= recv_data_sz);
    } else if (src_dt_contig) {
        /* source datatype is contiguous */
        mpi_errno =
            MPIR_Typerep_unpack((const void *) ((char *) copy_src_buf + src_true_lb),
                                copy_data_sz, MPIDIG_REQUEST(rreq, buffer), MPIDIG_REQUEST(rreq,
                                                                                           count),
                                MPIDIG_REQUEST(rreq, datatype), offset, &actual_unpack_bytes);
        MPIR_ERR_CHECK(mpi_errno);
        MPIR_Assert(actual_unpack_bytes <= recv_data_sz);
    } else {
        /* both datatype are non-contiguous */
        void *tmp_buf = MPL_malloc(copy_data_sz, MPL_MEM_OTHER);
        mpi_errno = MPIR_Typerep_pack((const void *) copy_src_buf, src_count, src_datatype,
                                      offset, tmp_buf, copy_data_sz, &actual_pack_bytes);
        MPIR_ERR_CHECK(mpi_errno);
        MPIR_Assert(actual_pack_bytes <= copy_data_sz);

        mpi_errno = MPIR_Typerep_unpack((const void *) tmp_buf, actual_pack_bytes,
                                        MPIDIG_REQUEST(rreq, buffer), MPIDIG_REQUEST(rreq,
                                                                                     count),
                                        MPIDIG_REQUEST(rreq, datatype), offset,
                                        &actual_unpack_bytes);
        MPIR_ERR_CHECK(mpi_errno);
        MPIR_Assert(actual_unpack_bytes <= copy_data_sz);

        MPL_free(tmp_buf);
    }

    mpi_errno = MPIDI_IPCI_handle_unmap(ipc_type, src_buf, ipc_handle);
    MPIR_ERR_CHECK(mpi_errno);

    ack_ctrl_hdr.ipc_contig_slmt_fin.ipc_type = ipc_type;
    ack_ctrl_hdr.ipc_contig_slmt_fin.req_ptr = sreq_ptr;
    mpi_errno = MPIDI_SHM_do_ctrl_send(MPIDIG_REQUEST(rreq, rank),
                                       MPIDIG_context_id_to_comm(MPIDIG_REQUEST
                                                                 (rreq, context_id)),
                                       MPIDI_IPC_SEND_CONTIG_LMT_FIN, &ack_ctrl_hdr);
    MPIR_ERR_CHECK(mpi_errno);

    if (src_datatype_ptr)
        MPIR_Datatype_ptr_release(src_datatype_ptr);

  fn_exit:
    MPIR_FUNC_VERBOSE_EXIT(MPID_STATE_MPIDI_IPCI_HANDLE_LMT_RECV);
    return mpi_errno;
  fn_fail:
    goto fn_exit;
}

MPL_STATIC_INLINE_PREFIX int MPIDI_IPCI_handle_lmt_cts_recv(MPIDI_IPCI_type_t ipc_type,
                                                            MPIDI_IPCI_ipc_handle_t ipc_handle,
                                                            size_t dest_data_sz,
                                                            MPIR_Request * rreq_ptr,
                                                            void *flattened_type,
                                                            MPIR_Request * sreq)
{
    int mpi_errno = MPI_SUCCESS;
    size_t data_sz;
    void *dest_buf = NULL;
    MPIDI_SHMI_ctrl_hdr_t ack_ctrl_hdr;
    MPI_Datatype dest_datatype;
    MPIR_Datatype *dest_datatype_ptr;
    int dest_dt_contig, src_dt_contig, dest_true_lb, src_true_lb;
    uintptr_t dest_dt_size, dest_count;

    MPIR_FUNC_VERBOSE_STATE_DECL(MPID_STATE_MPIDI_IPCI_HANDLE_LMT_CTS_RECV);
    MPIR_FUNC_VERBOSE_ENTER(MPID_STATE_MPIDI_IPCI_HANDLE_LMT_CTS_RECV);

    MPIDI_Datatype_check_contig_size_lb(MPIDIG_REQUEST(sreq, datatype), MPIDIG_REQUEST(sreq, count),
                                        src_dt_contig, data_sz, src_true_lb);
    if (data_sz > dest_data_sz)
        sreq->status.MPI_ERROR = MPI_ERR_TRUNCATE;

    /* recover src datatype */
    if (flattened_type) {
        dest_datatype_ptr = (MPIR_Datatype *) MPIR_Handle_obj_alloc(&MPIR_Datatype_mem);
        MPIR_Assert(dest_datatype_ptr);

        MPIR_Object_set_ref(dest_datatype_ptr, 1);
        MPIR_Typerep_unflatten(dest_datatype_ptr, flattened_type);
        dest_datatype = dest_datatype_ptr->handle;
        MPIDI_Datatype_check_contig_size_lb(dest_datatype, 1, dest_dt_contig, dest_dt_size,
                                            dest_true_lb);
        dest_count = dest_data_sz / dest_dt_size;
    } else {
        dest_datatype_ptr = NULL;
        dest_datatype = MPI_BYTE;
        dest_dt_contig = 1;
        dest_count = dest_data_sz;
        dest_true_lb = 0;
    }

    MPL_pointer_attr_t attr;
    MPIR_GPU_query_pointer_attr(MPIDIG_REQUEST(sreq, buffer), &attr);

    /* attach remote buffer */
    switch (ipc_type) {
        case MPIDI_IPCI_TYPE__XPMEM:
            mpi_errno = MPIDI_XPMEM_ipc_handle_map(ipc_handle.xpmem, &dest_buf);
            break;
        case MPIDI_IPCI_TYPE__GPU:
            mpi_errno =
                MPIDI_GPU_ipc_handle_map(ipc_handle.gpu, attr.device,
                                         MPIDIG_REQUEST(sreq, datatype), &dest_buf);
            break;
        case MPIDI_IPCI_TYPE__NONE:
            /* no-op */
            break;
        default:
            /* Unknown IPC type */
            MPIR_Assert(0);
            break;
    }

    IPC_TRACE("handle_lmt_cts_recv: handle matched rreq %p [source %d, tag %d, "
              " context_id 0x%x], copy dst %p, bytes %ld\n", sreq,
              MPIDIG_REQUEST(sreq, rank), MPIDIG_REQUEST(sreq, tag),
              MPIDIG_REQUEST(sreq, context_id), (char *) MPIDIG_REQUEST(sreq, buffer),
              dest_data_sz);

    /* Copy data to receive buffer */
    MPI_Aint actual_unpack_bytes;
    MPI_Aint actual_pack_bytes;
    MPI_Aint offset, copy_data_sz;
    void *copy_dest_buf;

    copy_dest_buf = (void *) ((uintptr_t) dest_buf - dest_true_lb);

    offset = dest_data_sz >> 1;
    copy_data_sz = dest_data_sz - offset;

    if (!src_dt_contig && dest_dt_contig) {
        /* source datatype is non-contiguous and destination datatype is contiguous */
        mpi_errno =
            MPIR_Typerep_pack(MPIDIG_REQUEST(sreq, buffer), MPIDIG_REQUEST(sreq, count),
                              MPIDIG_REQUEST(sreq, datatype), offset,
                              (void *) ((uintptr_t) copy_dest_buf + dest_true_lb + offset),
                              copy_data_sz, &actual_pack_bytes);
        MPIR_ERR_CHECK(mpi_errno);
        MPIR_Assert(actual_pack_bytes <= dest_data_sz);
    } else if (src_dt_contig) {
        /* source datatype is contiguous */
        mpi_errno =
            MPIR_Typerep_unpack((void *) ((char *) MPIDIG_REQUEST(sreq, buffer) + src_true_lb +
                                          offset), copy_data_sz, copy_dest_buf, dest_count,
                                dest_datatype, offset, &actual_unpack_bytes);
        MPIR_ERR_CHECK(mpi_errno);
        MPIR_Assert(actual_unpack_bytes <= dest_data_sz);
    } else {
        /* both datatype are non-contiguous */
        void *tmp_buf = MPL_malloc(copy_data_sz, MPL_MEM_OTHER);
        mpi_errno =
            MPIR_Typerep_pack(MPIDIG_REQUEST(sreq, buffer), MPIDIG_REQUEST(sreq, count),
                              MPIDIG_REQUEST(sreq, datatype), offset, tmp_buf, copy_data_sz,
                              &actual_pack_bytes);
        MPIR_ERR_CHECK(mpi_errno);
        MPIR_Assert(actual_pack_bytes <= copy_data_sz);

        mpi_errno = MPIR_Typerep_unpack((const void *) tmp_buf, actual_pack_bytes,
                                        copy_dest_buf, dest_count, dest_datatype, offset,
                                        &actual_unpack_bytes);
        MPIR_ERR_CHECK(mpi_errno);
        MPIR_Assert(actual_unpack_bytes <= copy_data_sz);

        MPL_free(tmp_buf);
    }

    mpi_errno = MPIDI_IPCI_handle_unmap(ipc_type, dest_buf, ipc_handle);
    MPIR_ERR_CHECK(mpi_errno);

    ack_ctrl_hdr.ipc_contig_slmt_fin.ipc_type = ipc_type;
    ack_ctrl_hdr.ipc_contig_slmt_fin.req_ptr = rreq_ptr;
    mpi_errno = MPIDI_SHM_do_ctrl_send(MPIDIG_REQUEST(sreq, rank),
                                       MPIDIG_context_id_to_comm(MPIDIG_REQUEST
                                                                 (sreq, context_id)),
                                       MPIDI_IPC_SEND_CONTIG_LMT_FIN, &ack_ctrl_hdr);
    MPIR_ERR_CHECK(mpi_errno);

    if (dest_datatype_ptr)
        MPIR_Datatype_ptr_release(dest_datatype_ptr);

  fn_exit:
    MPIR_FUNC_VERBOSE_EXIT(MPID_STATE_MPIDI_IPCI_HANDLE_LMT_CTS_RECV);
    return mpi_errno;
  fn_fail:
    goto fn_exit;
}

#endif /* IPC_P2P_H_INCLUDED */
