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

#define MPIDI_INTRA_COPY_LOCAL_PROCS_THRESHOLD 5        /* #local process threshold for intra-NUMA copy on bebop */
#define MPIDI_INTER_COPY_LOCAL_PROCS_THRESHOLD 8        /* #local process threshold for inter-NUMA copy on bebop */
#define MPIDI_NUM_COPY_LOCAL_PROCS_ARRAY MPIDI_INTER_COPY_LOCAL_PROCS_THRESHOLD
#define MPIDI_RMT_COPY_PROCS_THRESHOLD 5        /* max #remote process in stealing on bebop */
#define MPIDI_PROC_COPY 1
#define MPIDI_PROC_NOT_COPY 0

/* Task kind */
#define MPIDI_STEALING_CASE 2
#define MPIDI_PIP_INTRA_TASK 0
#define MPIDI_PIP_INTER_TASK 1

typedef struct MPIDI_PIP_task {
    MPIR_OBJECT_HEADER;
    int compl_flag;
    int task_kind;

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
    int **numa_cores_to_ranks;  /* map between core id to rank */
    int *numa_num_procs;        /* #processes in each NUMA node */
    int numa_root_rank;         /* rank of root process in my NUMA node */
    int numa_local_rank;

    /* finalized procs cnt */
    OPA_int_t fin_procs;
    OPA_int_t *fin_procs_ptr;

    /* current #remote stealing processes */
    OPA_int_t rmt_steal_procs;  /* #remote stealing processes, valid only in root process */
    OPA_int_t *rmt_steal_procs_ptr;

    /* copy state */
    int *local_copy_state[MPIDI_STEALING_CASE]; /* copy state of processes in eahc NUMA node */
    /* idle state */
    int *local_idle_state;

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
extern const int MPIDI_PIP_upperbound_threshold[MPIDI_STEALING_CASE];
extern const int MPIDI_PIP_thp_map[MPIDI_STEALING_CASE][MPIDI_NUM_COPY_LOCAL_PROCS_ARRAY];

#define MPIDI_PIP_REQUEST(req, field)      ((req)->dev.ch4.am.shm_am.pip.field)


#endif /* PIP_PRE_H_INCLUDED */
