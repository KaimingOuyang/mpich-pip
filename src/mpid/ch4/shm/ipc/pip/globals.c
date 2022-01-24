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

MPIDI_PIP_global_t MPIDI_PIP_global;

MPIDI_PIP_task_t MPIDI_Task_direct[1] = { 0 };
MPIDI_PIP_Coll_task_t MPIDI_Coll_task_direct[1] = { 0 };        /* +1 to avoid dynamic allocation */
MPIDI_PIP_Coll_easy_task_t MPIDI_Coll_easy_task_direct[MPIDI_COLL_EASY_TASK_PREALLOC + 1] = { 0 };

MPIR_Object_alloc_t MPIDI_Task_mem = {
    0, 0, 0, 0, 0, 0, MPIR_INTERNAL, sizeof(MPIDI_PIP_task_t), MPIDI_Task_direct,
    1
};

MPIR_Object_alloc_t MPIDI_Coll_task_mem = {
    0, 0, 0, 0, 0, 0, MPIR_INTERNAL, sizeof(MPIDI_PIP_Coll_task_t), MPIDI_Coll_task_direct,
    1, NULL
};

MPIR_Object_alloc_t MPIDI_Coll_easy_task_mem = {
    0, 0, 0, 0, 0, 0, MPIR_INTERNAL, sizeof(MPIDI_PIP_Coll_easy_task_t),
    MPIDI_Coll_easy_task_direct,
    MPIDI_COLL_EASY_TASK_PREALLOC, NULL
};
