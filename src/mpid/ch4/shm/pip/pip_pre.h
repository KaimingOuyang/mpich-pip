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

#define MPIDI_TASK_PREALLOC 96
#define MPIDI_PARTNER_PREALLOC 64
#define MPIDI_MAX_TASK_THRESHOLD 60
#define MPIDI_PIP_PKT_SIZE 65536        /* 64KB */
#define MPIDI_PIP_L2_CACHE_THRESHOLD 131072     /* 64KB * 2 this size has two considerations, one is keeping head data in L2 cache in receiver, the other is reducing the chances of remote process stealing, lock contention and remote data access overhead that will slow down the copy due to small data_sz. */
#define MPIDI_PIP_LAST_PKT_THRESHOLD MPIDI_PIP_PKT_SIZE /* 64KB */
#define MPIDI_PIP_CELL_SIZE 65536
#define MPIDI_PIP_CELL_NUM 72

#define MPIDI_PROC_COPY 1
#define MPIDI_PROC_NOT_COPY 0

#define MPIDI_PIP_THRESHOLD_CASE 5
extern const int MPIDI_PIP_local_stealing_num[MPIDI_PIP_THRESHOLD_CASE];        // #local stealing threshold
extern const int MPIDI_PIP_local_stealing_map[MPIDI_PIP_THRESHOLD_CASE];        // max remote stealing map
/* Task kind */
#define MPIDI_STEALING_CASE 2
#define MPIDI_PIP_INTRA_TASK 0
#define MPIDI_PIP_INTER_TASK 1
// #define MPIDI_STEALING_CASE 2
#define MPIDI_PIP_INTRA_QUEUE 0
#define MPIDI_PIP_INTER_QUEUE 1

/* Local or remote stealing */
#define MPIDI_PIP_LOCAL_STEALING 0
#define MPIDI_PIP_REMOTE_STEALING 1

/* Copy kind */
#define MPIDI_PIP_MEMCPY 0
#define MPIDI_PIP_PACK 1
#define MPIDI_PIP_UNPACK 2
#define MPIDI_PIP_PACK_UNPACK 3
#define MPIDI_PIP_KNL_PACK 4
#define MPIDI_PIP_KNL_UNPACK 5

/* Complete status */
#define MPIDI_PIP_NOT_COMPLETE  0
#define MPIDI_PIP_COMPLETE      1

typedef struct MPIDI_PIP_cell {
    volatile int full;
    char load[MPIDI_PIP_CELL_SIZE];
} MPIDI_PIP_cell_t;

#ifdef BEBOP
#define CORES_PER_NUMA_NODE 18
#define MPIDI_PIP_MAX_NUM_LOCAL_STEALING 4
#define TOTAL_CORES 36
#elif KNL
#define CORES_PER_NUMA_NODE 16
#define MPIDI_PIP_MAX_NUM_LOCAL_STEALING 12
#define TOTAL_CORES 64
#else
#define CORES_PER_NUMA_NODE 18
#define MPIDI_PIP_MAX_NUM_LOCAL_STEALING 4
#define TOTAL_CORES 36
#endif

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

typedef struct MPIDI_PIP_partner {
    MPIR_OBJECT_HEADER;
    int partner;
    struct MPIDI_PIP_partner *prev, *next;
} MPIDI_PIP_partner_t;

typedef struct MPIDI_PIP_partner_queue {
    MPIDI_PIP_partner_t *head, *tail;
} MPIDI_PIP_partner_queue_t;

typedef struct MPIDI_PIP_global {
    uint32_t num_local;
    uint32_t local_rank;
    uint32_t rank;
    uint32_t num_numa_node;
    uint32_t local_numa_id;
    uint32_t local_try;
    MPIDI_PIP_task_queue_t *task_queue;
    MPIDI_PIP_task_queue_t **task_queue_array;
    MPIDI_PIP_task_queue_t *compl_queue;

    /* Info structures */
    struct MPIDI_PIP_global **pip_global_array;

    /* NUMA info */
    int **numa_cores_to_ranks;  /* map between core id to rank */
    int *numa_num_procs;        /* #processes in each NUMA node */
    int *numa_lrank_to_nid;     /* local rank to numa id */
    int numa_root_rank;         /* rank of root process in my NUMA node */
    int numa_local_rank;
    int numa_partner;

    /* finalized procs cnt */
    OPA_int_t fin_procs;
    OPA_int_t *fin_procs_ptr;

    /* copy state */
    int *local_copy_state;      /* copy state of processes in eahc NUMA node */
    /* idle state */
    int *local_idle_state;

    /* pack/unpack load for stealing */
    char pkt_load[MPIDI_PIP_PKT_SIZE];

    MPIDI_PIP_partner_queue_t intrap_queue;
    MPIDI_PIP_partner_queue_t interp_queue;
    int *grank_to_lrank;

    int *allow_rmt_stealing;
    int *allow_rmt_stealing_ptr;
    OPA_int_t *bdw_checking;
    OPA_int_t *bdw_checking_ptr;
    int buffer_index;
    MPIDI_PIP_cell_t cells[MPIDI_PIP_CELL_NUM];
} MPIDI_PIP_global_t;

typedef struct {
    uint64_t src_offset;
    uint64_t data_sz;
    uint64_t sreq_ptr;
    uint64_t partner;
    int partner_queue;
    int src_lrank;
    int is_contig;
    int inter_flag;
    MPIR_Datatype *src_dt_ptr;
    MPI_Aint src_count;
} MPIDI_PIP_am_unexp_rreq_t;

typedef struct {
    MPIDI_PIP_am_unexp_rreq_t unexp_rreq;
    // MPI_Aint target_data_sz;
    MPI_Aint remain_data;
    MPI_Aint offset;
} MPIDI_PIP_am_request_t;

static inline void MPIDI_PIP_PARTNER_ENQUEUE(MPIDI_PIP_partner_t * partner_ptr,
                                             MPIDI_PIP_partner_queue_t * queue_ptr)
{
    if (queue_ptr->head) {
        partner_ptr->prev = queue_ptr->tail;
        queue_ptr->tail->next = partner_ptr;
        partner_ptr->next = NULL;
        queue_ptr->tail = partner_ptr;
    } else {
        queue_ptr->head = queue_ptr->tail = partner_ptr;
        partner_ptr->prev = partner_ptr->next = NULL;
    }
}

static inline void MPIDI_PIP_PARTNER_DEQUEUE(MPIDI_PIP_partner_t * partner_ptr,
                                             MPIDI_PIP_partner_queue_t * queue_ptr)
{
    if (partner_ptr->prev == NULL)
        queue_ptr->head = partner_ptr->next;
    else
        partner_ptr->prev->next = partner_ptr->next;
    if (partner_ptr->next == NULL)
        queue_ptr->tail = partner_ptr->prev;
    else
        partner_ptr->next->prev = partner_ptr->prev;
}

extern MPIDI_PIP_global_t MPIDI_PIP_global;
extern MPIR_Object_alloc_t MPIDI_Task_mem;
extern MPIR_Object_alloc_t MPIDI_Cell_mem;
extern MPIR_Object_alloc_t MPIDI_Partner_mem;
// extern MPIDI_POSIX_global_t MPIDI_POSIX_global;
// extern MPIR_Object_alloc_t MPIDI_Cell_mem;
// extern const int MPIDI_PIP_upperbound_threshold[MPIDI_STEALING_CASE];
// extern const int MPIDI_PIP_thp_map[MPIDI_STEALING_CASE][MPIDI_NUM_COPY_LOCAL_PROCS_ARRAY];

#define MPIDI_PIP_REQUEST(req, field)      ((req)->dev.ch4.am.shm_am.pip.field)


#endif /* PIP_PRE_H_INCLUDED */
