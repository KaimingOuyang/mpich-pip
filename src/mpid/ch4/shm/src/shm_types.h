/*
 * Copyright (C) by Argonne National Laboratory
 *     See COPYRIGHT in top-level directory
 */

#ifndef SHM_TYPES_H_INCLUDED
#define SHM_TYPES_H_INCLUDED

#include "../ipc/src/ipc_pre.h"

typedef enum {
    MPIDI_IPC_SEND_CONTIG_LMT_RTS,      /* issued by sender to initialize IPC with contig sbuf */
    MPIDI_IPC_SEND_LMT_CTS,     /* issued by sender to initialize IPC with contig sbuf */
    MPIDI_IPC_SEND_CTRL_HDR_LMT_RTS,    /* issued by sender to send large ctrl header */
    MPIDI_IPC_SEND_CONTIG_LMT_FIN,      /* issued by receiver to notify completion of sender-initialized contig IPC */
    MPIDI_SHMI_CTRL_IDS_MAX
} MPIDI_SHMI_ctrl_id_t;

typedef union {
    MPIDI_IPC_ctrl_send_contig_lmt_rts_t ipc_contig_slmt_rts;
    MPIDI_IPC_ctrl_send_contig_lmt_fin_t ipc_contig_slmt_fin;
    MPIDI_IPC_ctrl_send_lmt_ctrl_hdr_rts_t ipc_slmt_hdr_rts;
    MPIDI_IPC_ctrl_send_contig_lmt_cts_t ipc_slmt_cts;
} MPIDI_SHMI_ctrl_hdr_t;

typedef int (*MPIDI_SHMI_ctrl_cb) (MPIDI_SHMI_ctrl_hdr_t * ctrl_hdr);

#endif /* SHM_TYPES_H_INCLUDED */
