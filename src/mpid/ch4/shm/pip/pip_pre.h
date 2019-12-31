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
#define MPIDI_PIP_PKT_SIZE 65536        /* 64KB */
#define MPIDI_PIP_L2_CACHE_THRESHOLD 131072     /* 64KB * 2 this size has two considerations, one is keeping head data in L2 cache in receiver, the other is reducing the chances of remote process stealing, lock contention and remote data access overhead that will slow down the copy due to small data_sz. */
#define MPIDI_PIP_LAST_PKT_THRESHOLD MPIDI_PIP_PKT_SIZE /* 64KB */
#define MPIDI_PIP_CELL_SIZE 65536

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

/* Copy kind */
#define MPIDI_PIP_MEMCPY 0
#define MPIDI_PIP_PACK 1
#define MPIDI_PIP_UNPACK 2
#define MPIDI_PIP_PACK_UNPACK 3

/* Complete status */
#define MPIDI_PIP_NOT_COMPLETE  0
#define MPIDI_PIP_COMPLETE      1

typedef struct MPIDI_PIP_cell {
    MPIR_OBJECT_HEADER;
    char load[MPIDI_PIP_CELL_SIZE];
} MPIDI_PIP_cell_t;

typedef struct MPIDI_PIP_task {
    MPIR_OBJECT_HEADER;
    /* task header info */
    int compl_flag;
    struct MPIDI_PIP_task *task_next;
    struct MPIDI_PIP_task *compl_next;

    /* kind info */
    int task_kind;              /* describe inter-NUMA or intra-NUMA copy task */
    int copy_kind;              /* describe pack or unpack operations */

    /* basic buffer and size */
    void *src_buf;
    void *dest_buf;
    size_t data_sz;

    /* non-contig copy attributes */
    MPI_Aint src_count;
    MPIR_Datatype *src_dt_ptr;
    size_t src_offset;
    MPI_Aint dest_count;
    MPIR_Datatype *dest_dt_ptr;
    size_t dest_offset;
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

    /* pack/unpack load for stealing */
    char pkt_load[MPIDI_PIP_PKT_SIZE];
    size_t pkt_size;
} MPIDI_PIP_global_t;

typedef struct {
    uint64_t src_offset;
    uint64_t data_sz;
    uint64_t sreq_ptr;
    int src_lrank;
    int is_contig;
    MPIR_Datatype *src_dt_ptr;
    MPI_Aint src_count;
} MPIDI_PIP_am_unexp_rreq_t;

typedef struct {
    MPIDI_PIP_am_unexp_rreq_t unexp_rreq;
} MPIDI_PIP_am_request_t;

extern MPIDI_PIP_global_t MPIDI_PIP_global;
extern MPIR_Object_alloc_t MPIDI_Task_mem;
extern MPIR_Object_alloc_t MPIDI_Cell_mem;
extern const int MPIDI_PIP_upperbound_threshold[MPIDI_STEALING_CASE];
extern const int MPIDI_PIP_thp_map[MPIDI_STEALING_CASE][MPIDI_NUM_COPY_LOCAL_PROCS_ARRAY];

#define MPIDI_PIP_REQUEST(req, field)      ((req)->dev.ch4.am.shm_am.pip.field)


#endif /* PIP_PRE_H_INCLUDED */
