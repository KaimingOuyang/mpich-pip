/*
 * Copyright (C) by Argonne National Laboratory
 *     See COPYRIGHT in top-level directory
 */

#ifndef SHM_CONTROL_H_INCLUDED
#define SHM_CONTROL_H_INCLUDED

#include "shm_types.h"
#include "../posix/posix_am.h"
#include "mpl_shm.h"
#include "mpidu_shm_seg.h"

MPL_STATIC_INLINE_PREFIX int MPIDI_SHM_do_ctrl_send(int rank, MPIR_Comm * comm,
                                                    int ctrl_id, void *ctrl_hdr)
{
    int ret;

    MPIR_FUNC_VERBOSE_STATE_DECL(MPID_STATE_MPIDI_SHM_DO_CTRL_SEND);
    MPIR_FUNC_VERBOSE_ENTER(MPID_STATE_MPIDI_SHM_DO_CTRL_SEND);

    ret = MPIDI_POSIX_am_send_hdr(rank, comm, MPIDI_POSIX_AM_HDR_SHM,
                                  ctrl_id, ctrl_hdr, sizeof(MPIDI_SHMI_ctrl_hdr_t));

    MPIR_FUNC_VERBOSE_EXIT(MPID_STATE_MPIDI_SHM_DO_CTRL_SEND);
    return ret;
}

/* MPIDI_SHM_do_lmt_ctrl_send uses XPMEM to transfer large ctrl header. Caller should keep
 * ctrl_hdr until receiving fin packet. */
MPL_STATIC_INLINE_PREFIX int MPIDI_SHM_do_lmt_ctrl_send(int rank, MPIR_Comm * comm,
                                                        int ctrl_id, int ctrl_hdr_size,
                                                        void *ctrl_hdr, MPIR_Request * req)
{
    int mpl_err, mpi_errno = MPI_SUCCESS;
    MPIDI_SHMI_ctrl_hdr_t *lmt_ctrl_hdr = NULL;
    MPIDU_shm_seg_t *memory = NULL;
    char *serialized_hnd = NULL;
    int serialized_hnd_size;

    MPIR_FUNC_VERBOSE_STATE_DECL(MPID_STATE_MPIDI_SHM_DO_CTRL_SEND);
    MPIR_FUNC_VERBOSE_ENTER(MPID_STATE_MPIDI_SHM_DO_CTRL_SEND);

    memory = MPL_malloc(sizeof(MPIDU_shm_seg_t), MPL_MEM_OTHER);
    MPIR_Assert(memory);
    MPIDIG_REQUEST(req, memory) = memory;

    mpl_err = MPL_shm_hnd_init(&(memory->hnd));
    MPIR_ERR_CHKANDJUMP(mpl_err, mpi_errno, MPI_ERR_OTHER, "**alloc_shar_mem");

    memory->segment_len = ctrl_hdr_size;
    mpl_err = MPL_shm_seg_create_and_attach(memory->hnd, memory->segment_len,
                                            (void **) &(memory->base_addr), 0);
    MPIR_ERR_CHKANDJUMP(mpl_err, mpi_errno, MPI_ERR_OTHER, "**alloc_shar_mem");

    mpl_err = MPL_shm_hnd_get_serialized_by_ref(memory->hnd, &serialized_hnd);
    MPIR_ERR_CHKANDJUMP(mpl_err, mpi_errno, MPI_ERR_OTHER, "**alloc_shar_mem");
    serialized_hnd_size = strlen(serialized_hnd) + 1;

    lmt_ctrl_hdr =
        (MPIDI_SHMI_ctrl_hdr_t *) MPL_malloc(sizeof(MPIDI_IPC_ctrl_send_lmt_ctrl_hdr_rts_t) +
                                             serialized_hnd_size, MPL_MEM_OTHER);
    lmt_ctrl_hdr->ipc_slmt_hdr_rts.ctrl_id = ctrl_id;
    lmt_ctrl_hdr->ipc_slmt_hdr_rts.ctrl_hdr_size = ctrl_hdr_size;
    memcpy(lmt_ctrl_hdr->ipc_slmt_hdr_rts.serialized_hnd, serialized_hnd, serialized_hnd_size);

    /* copy ctrl hdr into shared memory */
    memcpy(memory->base_addr, ctrl_hdr, ctrl_hdr_size);

    mpi_errno = MPIDI_POSIX_am_send_hdr(rank, comm, MPIDI_POSIX_AM_HDR_SHM,
                                        MPIDI_IPC_SEND_CTRL_HDR_LMT_RTS, lmt_ctrl_hdr,
                                        sizeof(MPIDI_IPC_ctrl_send_lmt_ctrl_hdr_rts_t) +
                                        serialized_hnd_size);

  fn_exit:
    MPL_free(lmt_ctrl_hdr);
    MPIR_FUNC_VERBOSE_EXIT(MPID_STATE_MPIDI_SHM_DO_CTRL_SEND);
    return mpi_errno;
  fn_fail:
    goto fn_exit;
}

void MPIDI_SHMI_ctrl_reg_cb(int ctrl_id, MPIDI_SHMI_ctrl_cb cb);
int MPIDI_SHMI_ctrl_dispatch(int ctrl_id, void *ctrl_hdr);

#endif /* SHM_CONTROL_H_INCLUDED */
