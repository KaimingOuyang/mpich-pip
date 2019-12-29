/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil ; -*- */
/*
 *  (C) 2006 by Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 *
 *  Portions of this code were written by Intel Corporation.
 *  Copyright (C) 2011-2016 Intel Corporation.  Intel provides this material
 *  to Argonne National Laboratory subject to Software Grant and Corporate
 *  Contributor License Agreement dated February 8, 2012.
 */

#include "mpidimpl.h"
#include "pip_pre.h"

#ifdef MPL_USE_DBG_LOGGING
MPL_dbg_class MPIDI_CH4_SHM_PIP_GENERAL;
#endif

/* array 0 is intra threshold, 1 is inter */
const int MPIDI_PIP_upperbound_threshold[MPIDI_STEALING_CASE] =
    { MPIDI_INTRA_COPY_LOCAL_PROCS_THRESHOLD, MPIDI_INTER_COPY_LOCAL_PROCS_THRESHOLD };

/* intra map is at 0 position */
const int MPIDI_PIP_thp_map[MPIDI_STEALING_CASE][MPIDI_NUM_COPY_LOCAL_PROCS_ARRAY] =
    { {MPIDI_RMT_COPY_PROCS_THRESHOLD, 4, 3, 2, 1, 0, 0, 0}, {MPIDI_RMT_COPY_PROCS_THRESHOLD,
                                                              MPIDI_RMT_COPY_PROCS_THRESHOLD, 4, 4,
                                                              3, 2, 2, 1}
};

MPIDI_PIP_global_t MPIDI_PIP_global;

MPIDI_PIP_task_t MPIDI_Task_direct[MPIDI_TASK_PREALLOC] = { 0 };
MPIDI_PIP_cell_t MPIDI_Cell_direct[MPIDI_CELL_PREALLOC] = { 0 };

MPIR_Object_alloc_t MPIDI_Task_mem = {
    0, 0, 0, 0, MPIDI_TASK, sizeof(MPIDI_PIP_task_t), MPIDI_Task_direct,
    MPIDI_TASK_PREALLOC
};

MPIR_Object_alloc_t MPIDI_Cell_mem = {
    0, 0, 0, 0, MPIDI_TASK, sizeof(MPIDI_PIP_cell_t), MPIDI_Cell_direct,
    MPIDI_CELL_PREALLOC
};
