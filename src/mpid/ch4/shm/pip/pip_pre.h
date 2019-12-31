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

#ifndef PIP_PRE_H_INCLUDED
#define PIP_PRE_H_INCLUDED

extern MPL_dbg_class MPIDI_CH4_SHM_PIP_GENERAL;
#define PIP_TRACE(...) \
    MPL_DBG_MSG_FMT(MPIDI_CH4_SHM_PIP_GENERAL,VERBOSE,(MPL_DBG_FDEST, "PIP "__VA_ARGS__))

#define MPIDI_TASK_PREALLOC 64
#define MPIDI_MAX_TASK_THRESHOLD 60
#define MPIDI_PIP_SEC_LAST_PKT_THRESHOLD (MPIDI_PIP_PKT_SIZE << 1)      /* 64KB * 2 */
#define MPIDI_PIP_LAST_PKT_THRESHOLD MPIDI_PIP_PKT_SIZE /* 64KB */
#define MPIDI_PIP_PKT_SIZE 65536        /* 64KB */

typedef struct MPIDI_PIP_task {
    MPIR_OBJECT_HEADER;
    int compl_flag;
    int locality;

    void *src_buf;
    void *dest_buf;
    size_t data_sz;
    struct MPIDI_PIP_task *task_next;
    struct MPIDI_PIP_task *compl_next;
} MPIDI_PIP_task_t;

typedef struct MPIDI_PIP_task_queue {
    MPIDI_PIP_task_t *head;
    MPIDI_PIP_task_t *tail;
    MPID_Thread_mutex_t lock;

    /* Info structures */
    int task_num;
} MPIDI_PIP_task_queue_t;

typedef struct MPIDI_PIP_global {
    uint32_t num_local;
    uint32_t local_rank;
    uint32_t rank;
    uint32_t num_numa_node;
    uint32_t local_numa_id;

    MPIDI_PIP_task_queue_t *task_queue;
    MPIDI_PIP_task_queue_t **task_queue_array;
    MPIDI_PIP_task_queue_t *compl_queue;

    /* Info structures */
    struct MPIDI_PIP_global **pip_global_array;

    /* NUMA info */
    int **numa_cores_to_ranks;
    int *numa_num_procs;

    /* finalized procs cnt */
    OPA_int_t fin_procs;

    size_t pkt_size;
} MPIDI_PIP_global_t;

typedef struct {
    uint64_t src_offset;
    uint64_t data_sz;
    uint64_t sreq_ptr;
    int src_lrank;
} MPIDI_PIP_am_unexp_rreq_t;

typedef struct {
    MPIDI_PIP_am_unexp_rreq_t unexp_rreq;
} MPIDI_PIP_am_request_t;

extern MPIDI_PIP_global_t MPIDI_PIP_global;
extern MPIR_Object_alloc_t MPIDI_Task_mem;

#define MPIDI_PIP_REQUEST(req, field)      ((req)->dev.ch4.am.shm_am.pip.field)


#endif /* PIP_PRE_H_INCLUDED */
