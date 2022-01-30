/*
 * Copyright (C) by Argonne National Laboratory
 *     See COPYRIGHT in top-level directory
 */

#include "mpiimpl.h"
#include "mpicomm.h"
#include "mpir_info.h"  /* MPIR_Info_free */

#include "utlist.h"
#include "uthash.h"

/* This is the utility file for comm that contains the basic comm items
   and storage management */
#ifndef MPID_COMM_PREALLOC
#define MPID_COMM_PREALLOC 8
#endif

/* Preallocated comm objects */
/* initialized in initthread.c */
MPIR_Comm MPIR_Comm_builtin[MPIR_COMM_N_BUILTIN];
MPIR_Comm MPIR_Comm_direct[MPID_COMM_PREALLOC];

MPIR_Object_alloc_t MPIR_Comm_mem = {
    0,
    0,
    0,
    0,
    0,
    0,
    MPIR_COMM,
    sizeof(MPIR_Comm),
    MPIR_Comm_direct,
    MPID_COMM_PREALLOC,
    NULL
};

/* Communicator creation functions */
struct MPIR_Commops *MPIR_Comm_fns = NULL;
static int MPIR_Comm_commit_internal(MPIR_Comm * comm);

/* Communicator hint functions */
/* For balance of simplicity and feature, we'll internally use integers for both keys
 * and values, and provide facilities to translate from and to string-based infos.
 */

struct MPIR_HINT {
    const char *key;
    MPIR_Comm_hint_fn_t fn;
    int type;
    int attr;                   /* e.g. whether this key is local */
};
static struct MPIR_HINT MPIR_comm_hint_list[MPIR_COMM_HINT_MAX];
static int next_comm_hint_index = MPIR_COMM_HINT_PREDEFINED_COUNT;

int MPIR_Comm_register_hint(int idx, const char *hint_key, MPIR_Comm_hint_fn_t fn,
                            int type, int attr)
{
    if (idx == 0) {
        idx = next_comm_hint_index;
        next_comm_hint_index++;
        MPIR_Assert(idx < MPIR_COMM_HINT_MAX);
    } else {
        MPIR_Assert(idx > 0 && idx < MPIR_COMM_HINT_PREDEFINED_COUNT);
    }
    MPIR_comm_hint_list[idx] = (struct MPIR_HINT) {
        hint_key, fn, type, attr
    };
    return idx;
}

static int parse_string_value(const char *s, int type, int *val)
{
    if (type == MPIR_COMM_HINT_TYPE_BOOL) {
        if (strcmp(s, "true") == 0) {
            *val = 1;
        } else if (strcmp(s, "false") == 0) {
            *val = 0;
        } else {
            *val = atoi(s);
        }
    } else if (type == MPIR_COMM_HINT_TYPE_INT) {
        *val = atoi(s);
    } else {
        return -1;
    }
    return 0;
}

static int get_string_value(char *s, int type, int val)
{
    if (type == MPIR_COMM_HINT_TYPE_BOOL) {
        strncpy(s, val ? "true" : "false", MPI_MAX_INFO_VAL);
    } else if (type == MPIR_COMM_HINT_TYPE_INT) {
        MPL_snprintf(s, MPI_MAX_INFO_VAL, "%d", val);
    } else {
        return -1;
    }
    return 0;
}

/* Hints are stored as hints array inside MPIR_Comm.
 * All hints are initialized to zero. Communitcator creation hook can be used to
 * to customize initialization value (make sure only do that when the value is zero
 * or risk resetting user hints).
 * If the hint is registered with callback function, it can be used for customization
 * at both creation time and run-time.
 */
int MPII_Comm_set_hints(MPIR_Comm * comm_ptr, MPIR_Info * info)
{
    MPIR_Info *curr_info;
    LL_FOREACH(info, curr_info) {
        if (curr_info->key == NULL)
            continue;
        for (int i = 0; i < next_comm_hint_index; i++) {
            if (MPIR_comm_hint_list[i].key &&
                strcmp(curr_info->key, MPIR_comm_hint_list[i].key) == 0) {
                int val;
                int ret = parse_string_value(curr_info->value, MPIR_comm_hint_list[i].type, &val);
                if (ret == 0) {
                    if (MPIR_comm_hint_list[i].fn) {
                        MPIR_comm_hint_list[i].fn(comm_ptr, i, val);
                    } else {
                        comm_ptr->hints[i] = val;
                    }
                }
            }
        }
    }
    /* FIXME: run collective to ensure hints consistency */
    return MPI_SUCCESS;
}

int MPII_Comm_get_hints(MPIR_Comm * comm_ptr, MPIR_Info * info)
{
    int mpi_errno = MPI_SUCCESS;

    char hint_val_str[MPI_MAX_INFO_VAL];
    for (int i = 0; i < next_comm_hint_index; i++) {
        if (MPIR_comm_hint_list[i].key) {
            get_string_value(hint_val_str, MPIR_comm_hint_list[i].type, comm_ptr->hints[i]);
            mpi_errno = MPIR_Info_set_impl(info, MPIR_comm_hint_list[i].key, hint_val_str);
            MPIR_ERR_CHECK(mpi_errno);
        }
    }

  fn_exit:
    return mpi_errno;
  fn_fail:
    goto fn_exit;
}

int MPII_Comm_check_hints(MPIR_Comm * comm)
{
    /* for all non-local hints and non-zero hint values, run collective
     * to check whether they are equal across the communicator */
    /* TODO */
    return MPI_SUCCESS;
}

void MPIR_Comm_hint_init(void)
{
    MPIR_Comm_register_hint(MPIR_COMM_HINT_NO_ANY_TAG, "mpi_assert_no_any_tag",
                            NULL, MPIR_COMM_HINT_TYPE_BOOL, 0);
    MPIR_Comm_register_hint(MPIR_COMM_HINT_NO_ANY_SOURCE, "mpi_assert_no_any_source",
                            NULL, MPIR_COMM_HINT_TYPE_BOOL, 0);
    MPIR_Comm_register_hint(MPIR_COMM_HINT_EXACT_LENGTH, "mpi_assert_exact_length",
                            NULL, MPIR_COMM_HINT_TYPE_BOOL, 0);
    MPIR_Comm_register_hint(MPIR_COMM_HINT_ALLOW_OVERTAKING, "mpi_assert_allow_overtaking",
                            NULL, MPIR_COMM_HINT_TYPE_BOOL, 0);
}

/* FIXME :
   Reusing context ids can lead to a race condition if (as is desirable)
   MPI_Comm_free does not include a barrier.  Consider the following:
   Process A frees the communicator.
   Process A creates a new communicator, reusing the just released id
   Process B sends a message to A on the old communicator.
   Process A receives the message, and believes that it belongs to the
   new communicator.
   Process B then cancels the message, and frees the communicator.

   The likelihood of this happening can be reduced by introducing a gap
   between when a context id is released and when it is reused.  An alternative
   is to use an explicit message (in the implementation of MPI_Comm_free)
   to indicate that a communicator is being freed; this will often require
   less communication than a barrier in MPI_Comm_free, and will ensure that
   no messages are later sent to the same communicator (we may also want to
   have a similar check when building fault-tolerant versions of MPI).
 */

/* Zeroes most non-handle fields in a communicator, as well as initializing any
 * other special fields, such as a per-object mutex.  Also defaults the
 * reference count to 1, under the assumption that the caller holds a reference
 * to it.
 *
 * !!! The resulting struct is _not_ ready for communication !!! */
int MPII_Comm_init(MPIR_Comm * comm_p)
{
    int mpi_errno = MPI_SUCCESS;

    MPIR_Object_set_ref(comm_p, 1);

    /* initialize local and remote sizes to -1 to allow other parts of
     * the stack to detect errors more easily */
    comm_p->local_size = -1;
    comm_p->remote_size = -1;

    /* Clear many items (empty means to use the default; some of these
     * may be overridden within the upper-level communicator initialization) */
    comm_p->errhandler = NULL;
    comm_p->attributes = NULL;
    comm_p->remote_group = NULL;
    comm_p->local_group = NULL;
    comm_p->topo_fns = NULL;
    comm_p->name[0] = '\0';
    comm_p->seq = 0;    /* default to 0, to be updated at Comm_commit */
    comm_p->tainted = 0;
    memset(comm_p->hints, 0, sizeof(comm_p->hints));

    comm_p->hierarchy_kind = MPIR_COMM_HIERARCHY_KIND__FLAT;
    comm_p->node_comm = NULL;
    comm_p->node_roots_comm = NULL;
    comm_p->pip_roots_comm = NULL;
    comm_p->intranode_table = NULL;
    comm_p->internode_table = NULL;

    /* abstractions bleed a bit here... :(*/
    comm_p->next_sched_tag = MPIR_FIRST_NBC_TAG;

    /* Initialize the revoked flag as false */
    comm_p->revoked = 0;
    comm_p->mapper_head = NULL;
    comm_p->mapper_tail = NULL;

#if MPICH_THREAD_GRANULARITY == MPICH_THREAD_GRANULARITY__POBJ
    {
        int thr_err;
        MPID_Thread_mutex_create(&MPIR_THREAD_POBJ_COMM_MUTEX(comm_p), &thr_err);
        MPIR_Assert(thr_err == 0);
    }
#endif
    /* Fields not set include context_id, remote and local size, and
     * kind, since different communicator construction routines need
     * different values */
    return mpi_errno;
}


/*
    Create a communicator structure and perform basic initialization
    (mostly clearing fields and updating the reference count).
 */
int MPIR_Comm_create(MPIR_Comm ** newcomm_ptr)
{
    int mpi_errno = MPI_SUCCESS;
    MPIR_Comm *newptr;
    MPIR_FUNC_TERSE_STATE_DECL(MPID_STATE_MPIR_COMM_CREATE);

    MPIR_FUNC_TERSE_ENTER(MPID_STATE_MPIR_COMM_CREATE);

    newptr = (MPIR_Comm *) MPIR_Handle_obj_alloc(&MPIR_Comm_mem);
    MPIR_ERR_CHKANDJUMP(!newptr, mpi_errno, MPI_ERR_OTHER, "**nomem");

    *newcomm_ptr = newptr;

    mpi_errno = MPII_Comm_init(newptr);
    MPIR_ERR_CHECK(mpi_errno);

    /* Insert this new communicator into the list of known communicators.
     * Make this conditional on debugger support to match the test in
     * MPIR_Comm_release . */
    MPII_COMML_REMEMBER(newptr);

  fn_fail:
    MPIR_FUNC_TERSE_EXIT(MPID_STATE_MPIR_COMM_CREATE);

    return mpi_errno;
}

/* Create a local intra communicator from the local group of the
   specified intercomm. */
/* FIXME this is an alternative constructor that doesn't use MPIR_Comm_create! */
int MPII_Setup_intercomm_localcomm(MPIR_Comm * intercomm_ptr)
{
    MPIR_Comm *localcomm_ptr;
    int mpi_errno = MPI_SUCCESS;
    MPIR_FUNC_TERSE_STATE_DECL(MPID_STATE_MPIR_SETUP_INTERCOMM_LOCALCOMM);

    MPIR_FUNC_TERSE_ENTER(MPID_STATE_MPIR_SETUP_INTERCOMM_LOCALCOMM);

    localcomm_ptr = (MPIR_Comm *) MPIR_Handle_obj_alloc(&MPIR_Comm_mem);
    MPIR_ERR_CHKANDJUMP(!localcomm_ptr, mpi_errno, MPI_ERR_OTHER, "**nomem");

    /* get sensible default values for most fields (usually zeros) */
    mpi_errno = MPII_Comm_init(localcomm_ptr);
    MPIR_ERR_CHECK(mpi_errno);

    /* use the parent intercomm's recv ctx as the basis for our ctx */
    localcomm_ptr->recvcontext_id =
        MPIR_CONTEXT_SET_FIELD(IS_LOCALCOMM, intercomm_ptr->recvcontext_id, 1);
    localcomm_ptr->context_id = localcomm_ptr->recvcontext_id;

    MPL_DBG_MSG_FMT(MPIR_DBG_COMM, TYPICAL,
                    (MPL_DBG_FDEST,
                     "setup_intercomm_localcomm ic=%p ic->context_id=%d ic->recvcontext_id=%d lc->recvcontext_id=%d",
                     intercomm_ptr, intercomm_ptr->context_id, intercomm_ptr->recvcontext_id,
                     localcomm_ptr->recvcontext_id));

    /* Save the kind of the communicator */
    localcomm_ptr->comm_kind = MPIR_COMM_KIND__INTRACOMM;

    /* Set the sizes and ranks */
    localcomm_ptr->remote_size = intercomm_ptr->local_size;
    localcomm_ptr->local_size = intercomm_ptr->local_size;
    localcomm_ptr->rank = intercomm_ptr->rank;

    MPIR_Comm_map_dup(localcomm_ptr, intercomm_ptr, MPIR_COMM_MAP_DIR__L2L);

    /* TODO More advanced version: if the group is available, dup it by
     * increasing the reference count instead of recreating it later */
    /* FIXME  : No local functions for the topology routines */

    intercomm_ptr->local_comm = localcomm_ptr;

    /* sets up the SMP-aware sub-communicators and tables */
    /* This routine maybe used inside MPI_Comm_idup, so we can't synchronize
     * seq using blocking collectives, thus mark as tainted. */
    localcomm_ptr->tainted = 1;
    mpi_errno = MPIR_Comm_commit(localcomm_ptr);
    MPIR_ERR_CHECK(mpi_errno);

  fn_fail:
    MPIR_FUNC_TERSE_EXIT(MPID_STATE_MPIR_SETUP_INTERCOMM_LOCALCOMM);

    return mpi_errno;
}

int MPIR_Comm_map_irregular(MPIR_Comm * newcomm, MPIR_Comm * src_comm,
                            int *src_mapping, int src_mapping_size,
                            MPIR_Comm_map_dir_t dir, MPIR_Comm_map_t ** map)
{
    int mpi_errno = MPI_SUCCESS;
    MPIR_Comm_map_t *mapper;
    MPIR_CHKPMEM_DECL(3);
    MPIR_FUNC_TERSE_STATE_DECL(MPID_STATE_MPIR_COMM_MAP_TYPE__IRREGULAR);

    MPIR_FUNC_TERSE_ENTER(MPID_STATE_MPIR_COMM_MAP_TYPE__IRREGULAR);

    MPIR_CHKPMEM_MALLOC(mapper, MPIR_Comm_map_t *, sizeof(MPIR_Comm_map_t), mpi_errno, "mapper",
                        MPL_MEM_COMM);

    mapper->type = MPIR_COMM_MAP_TYPE__IRREGULAR;
    mapper->src_comm = src_comm;
    mapper->dir = dir;
    mapper->src_mapping_size = src_mapping_size;

    if (src_mapping) {
        mapper->src_mapping = src_mapping;
        mapper->free_mapping = 0;
    } else {
        MPIR_CHKPMEM_MALLOC(mapper->src_mapping, int *,
                            src_mapping_size * sizeof(int), mpi_errno, "mapper mapping",
                            MPL_MEM_COMM);
        mapper->free_mapping = 1;
    }

    mapper->next = NULL;

    LL_APPEND(newcomm->mapper_head, newcomm->mapper_tail, mapper);

    if (map)
        *map = mapper;

  fn_exit:
    MPIR_CHKPMEM_COMMIT();
    MPIR_FUNC_TERSE_EXIT(MPID_STATE_MPIR_COMM_MAP_TYPE__IRREGULAR);
    return mpi_errno;
  fn_fail:
    MPIR_CHKPMEM_REAP();
    goto fn_exit;
}

int MPIR_Comm_map_dup(MPIR_Comm * newcomm, MPIR_Comm * src_comm, MPIR_Comm_map_dir_t dir)
{
    int mpi_errno = MPI_SUCCESS;
    MPIR_Comm_map_t *mapper;
    MPIR_CHKPMEM_DECL(1);
    MPIR_FUNC_TERSE_STATE_DECL(MPID_STATE_MPIR_COMM_MAP_TYPE__DUP);

    MPIR_FUNC_TERSE_ENTER(MPID_STATE_MPIR_COMM_MAP_TYPE__DUP);

    MPIR_CHKPMEM_MALLOC(mapper, MPIR_Comm_map_t *, sizeof(MPIR_Comm_map_t), mpi_errno, "mapper",
                        MPL_MEM_COMM);

    mapper->type = MPIR_COMM_MAP_TYPE__DUP;
    mapper->src_comm = src_comm;
    mapper->dir = dir;

    mapper->next = NULL;

    LL_APPEND(newcomm->mapper_head, newcomm->mapper_tail, mapper);

  fn_exit:
    MPIR_CHKPMEM_COMMIT();
    MPIR_FUNC_TERSE_EXIT(MPID_STATE_MPIR_COMM_MAP_TYPE__DUP);
    return mpi_errno;
  fn_fail:
    MPIR_CHKPMEM_REAP();
    goto fn_exit;
}


int MPIR_Comm_map_free(MPIR_Comm * comm)
{
    int mpi_errno = MPI_SUCCESS;
    MPIR_Comm_map_t *mapper, *tmp;
    MPIR_FUNC_TERSE_STATE_DECL(MPID_STATE_MPIR_COMM_MAP_FREE);

    MPIR_FUNC_TERSE_ENTER(MPID_STATE_MPIR_COMM_MAP_FREE);

    for (mapper = comm->mapper_head; mapper;) {
        tmp = mapper->next;
        if (mapper->type == MPIR_COMM_MAP_TYPE__IRREGULAR && mapper->free_mapping)
            MPL_free(mapper->src_mapping);
        MPL_free(mapper);
        mapper = tmp;
    }
    comm->mapper_head = NULL;

    MPIR_FUNC_TERSE_EXIT(MPID_STATE_MPIR_COMM_MAP_FREE);
    return mpi_errno;
}

static int get_node_count(MPIR_Comm * comm, int *node_count)
{
    int mpi_errno = MPI_SUCCESS;
    struct uniq_nodes {
        int id;
        UT_hash_handle hh;
    } *node_list = NULL;
    struct uniq_nodes *s, *tmp;

    if (comm->comm_kind != MPIR_COMM_KIND__INTRACOMM) {
        *node_count = comm->local_size;
        goto fn_exit;
    } else if (comm->hierarchy_kind == MPIR_COMM_HIERARCHY_KIND__NODE) {
        *node_count = 1;
        goto fn_exit;
    }
#ifndef MPIDI_CH4_SHM_ENABLE_PIP
    else if (comm->hierarchy_kind == MPIR_COMM_HIERARCHY_KIND__NODE_ROOTS) {
        *node_count = comm->local_size;
        goto fn_exit;
    }
#endif
    /* go through the list of ranks and add the unique ones to the
     * node_list array */
    for (int i = 0; i < comm->local_size; i++) {
        int node;

        mpi_errno = MPID_Get_node_id(comm, i, &node);
        MPIR_ERR_CHECK(mpi_errno);

        HASH_FIND_INT(node_list, &node, s);
        if (s == NULL) {
            s = (struct uniq_nodes *) MPL_malloc(sizeof(struct uniq_nodes), MPL_MEM_COLL);
            MPIR_Assert(s);
            s->id = node;
            HASH_ADD_INT(node_list, id, s, MPL_MEM_COLL);
        }
    }

    /* the final size of our hash table is our node count */
    *node_count = HASH_COUNT(node_list);

    /* free up everything */
    HASH_ITER(hh, node_list, s, tmp) {
        HASH_DEL(node_list, s);
        MPL_free(s);
    }

  fn_exit:
    return mpi_errno;
  fn_fail:
    goto fn_exit;
}

static int MPIR_Comm_commit_internal(MPIR_Comm * comm)
{
    int mpi_errno = MPI_SUCCESS;
    MPIR_FUNC_TERSE_STATE_DECL(MPID_STATE_MPIR_COMM_COMMIT_INTERNAL);
    MPIR_FUNC_TERSE_ENTER(MPID_STATE_MPIR_COMM_COMMIT_INTERNAL);

    /* Notify device of communicator creation */
    mpi_errno = MPID_Comm_commit_pre_hook(comm);
    MPIR_ERR_CHECK(mpi_errno);

    mpi_errno = get_node_count(comm, &comm->node_count);
    MPIR_ERR_CHECK(mpi_errno);

    MPIR_Comm_map_free(comm);

  fn_exit:
    MPIR_FUNC_TERSE_EXIT(MPID_STATE_MPIR_COMM_COMMIT_INTERNAL);
    return mpi_errno;
  fn_fail:
    goto fn_exit;
}

// int MPIR_PIP_Comm_barrier(MPIR_Comm * comm)
// {
//     int mpi_errno = MPI_SUCCESS;
//     int local_size = comm->node_procs_min;
//     MPIDI_Comm_shm_barrier_t *barrier = comm->barrier;
//     int sense = MPL_atomic_load_int(&barrier->wait);

//     if (local_size == 1)
//         goto fn_exit;

//     if (MPL_atomic_fetch_add_int(&barrier->val, 1) == local_size - 1) {
//         MPL_atomic_store_int(&barrier->val, 0);
//         MPL_atomic_store_int(&barrier->wait, 1 - sense);
//     } else {
//         /* wait */
//         while (MPL_atomic_load_int(&barrier->wait) == sense)
//             MPL_sched_yield();  /* skip */
//     }

//   fn_exit:
//     return mpi_errno;
// }

#define CACHE_LINE 64
MPIDI_PIP_Coll_task_t *MPIR_PIP_Comm_get_task(MPIDI_PIP_Coll_task_t * volatile *task_array,
                                              int round)
{
    while (task_array[round] == NULL)
        MPL_sched_yield();
    return task_array[round];
}

MPIDI_PIP_Coll_task_t *MPIR_PIP_Comm_post_task(MPIDI_PIP_Coll_task_t ** task_array, int round,
                                               int cnt, void *buf)
{
    MPIDI_PIP_Coll_task_t *shared_addr =
        (MPIDI_PIP_Coll_task_t *) MPIR_Handle_obj_alloc(&MPIDI_Coll_task_mem);
    shared_addr->addr = buf;
    shared_addr->cnt = cnt;
    __sync_synchronize();
    task_array[round] = (void *) shared_addr;
    return shared_addr;
}

void MPIR_PIP_Comm_reclaim_all_tasks(MPIDI_PIP_Coll_task_t ** task_array, int target_cnt,
                                     int length)
{
    for (int i = 0; i < length; ++i) {
        if (task_array[i] != NULL) {
            while (task_array[i]->cnt != target_cnt)
                MPL_sched_yield();
            MPIR_Handle_obj_free(&MPIDI_Coll_task_mem, (void *) task_array[i]);
            task_array[i] = NULL;
        }
    }
}

void MPIR_PIP_Comm_opt_intra_barrier(MPIR_Comm * comm, int local_size)
{
    int local_rank = comm->local_rank;
    int val;
    int barrier_round = comm->barrier_round;
    MPIDI_Comm_intra_barrier_t *barrier = &comm->barrier[barrier_round];

    if (local_size == 1)
        return;

    if (local_rank == 0) {
        while (barrier->val != local_size - 1)
            MPL_sched_yield();
        comm->barrier[barrier_round ^ 1].val = 0;
    }
    __sync_fetch_and_add(&barrier->val, 1);
    while (barrier->val != local_size)
        MPL_sched_yield();

    comm->barrier_round = barrier_round ^ 1;
    return;
}


MPIDI_PIP_Coll_easy_task_t *MPIR_Comm_post_easy_task(void *addr, MPIDI_PIP_Coll_task_type_t type,
                                                     int data_sz, int free_flag, int target_cmpl,
                                                     MPIR_Comm * comm)
{
    MPIDI_PIP_Coll_easy_task_t *local_task =
        (MPIDI_PIP_Coll_easy_task_t *) MPIR_Handle_obj_alloc(&MPIDI_Coll_easy_task_mem);
    local_task->addr = addr;
    local_task->type = type;
    local_task->data_sz = data_sz;
    local_task->free = free_flag;
    local_task->complete = 0;
    local_task->root_complete = 0;
    local_task->target_cmpl = target_cmpl;

    switch (type) {
        case TMPI_Scatter:{
                int next_index = (comm->scatter_post_index + 1) % MPIDI_COLL_TASK_PREALLOC;
                if (comm->scatter_queue[next_index] != NULL) {
                    /* clean up scatter */
                    MPIDI_PIP_Coll_easy_task_t *reclaim_task;
                    int tmp_next_index = next_index;
                    while (reclaim_task = comm->scatter_queue[tmp_next_index]) {
                        while (reclaim_task->complete != reclaim_task->target_cmpl)
                            MPL_sched_yield();
                        if (reclaim_task->free == 1)
                            free(reclaim_task->addr);
                        MPIR_Handle_obj_free(&MPIDI_Coll_easy_task_mem, (void *) reclaim_task);
                        comm->scatter_queue[tmp_next_index] = NULL;
                        tmp_next_index = (tmp_next_index + 1) % MPIDI_COLL_TASK_PREALLOC;
                    }
                }

                __sync_synchronize();
                comm->scatter_queue[comm->scatter_post_index] = local_task;
                comm->scatter_post_index = next_index;
                break;
            }

        case TMPI_Allgather:{
                int next_index = (comm->allgather_post_index + 1) % MPIDI_COLL_TASK_PREALLOC;
                if (comm->allgather_queue[next_index] != NULL) {
                    MPIDI_PIP_Coll_easy_task_t *reclaim_task;
                    int tmp_next_index = next_index;
                    while (reclaim_task = comm->allgather_queue[tmp_next_index]) {
                        while (reclaim_task->complete != reclaim_task->target_cmpl)
                            MPL_sched_yield();
                        if (reclaim_task->free == 1)
                            free(reclaim_task->addr);
                        MPIR_Handle_obj_free(&MPIDI_Coll_easy_task_mem, (void *) reclaim_task);
                        comm->allgather_queue[tmp_next_index] = NULL;
                        tmp_next_index = (tmp_next_index + 1) % MPIDI_COLL_TASK_PREALLOC;
                    }
                }

                __sync_synchronize();
                comm->allgather_queue[comm->allgather_post_index] = local_task;
                comm->allgather_post_index = next_index;
                break;
            }

        case TMPI_Bcast:{
                int next_index = (comm->bcast_post_index + 1) % MPIDI_COLL_TASK_PREALLOC;
                if (comm->bcast_queue[next_index] != NULL) {
                    MPIDI_PIP_Coll_easy_task_t *reclaim_task;
                    int tmp_next_index = next_index;
                    while (reclaim_task = comm->bcast_queue[tmp_next_index]) {
                        while (reclaim_task->complete != reclaim_task->target_cmpl)
                            MPL_sched_yield();
                        if (reclaim_task->free == 1)
                            free(reclaim_task->addr);
                        MPIR_Handle_obj_free(&MPIDI_Coll_easy_task_mem, (void *) reclaim_task);
                        comm->bcast_queue[tmp_next_index] = NULL;
                        tmp_next_index = (tmp_next_index + 1) % MPIDI_COLL_TASK_PREALLOC;
                    }
                }

                __sync_synchronize();
                comm->bcast_queue[comm->bcast_post_index] = local_task;
                comm->bcast_post_index = next_index;
                break;
            }

        case TMPI_Allreduce:{
                int next_index = (comm->allreduce_post_index + 1) % MPIDI_COLL_TASK_PREALLOC;
                if (comm->allreduce_queue[next_index] != NULL) {
                    MPIDI_PIP_Coll_easy_task_t *reclaim_task;
                    int tmp_next_index = next_index;
                    while (reclaim_task = comm->allreduce_queue[tmp_next_index]) {
                        while (reclaim_task->complete != reclaim_task->target_cmpl)
                            MPL_sched_yield();
                        if (reclaim_task->free == 1)
                            free(reclaim_task->addr);
                        MPIR_Handle_obj_free(&MPIDI_Coll_easy_task_mem, (void *) reclaim_task);
                        comm->allreduce_queue[tmp_next_index] = NULL;
                        tmp_next_index = (tmp_next_index + 1) % MPIDI_COLL_TASK_PREALLOC;
                    }
                }

                __sync_synchronize();
                comm->allreduce_queue[comm->allreduce_post_index] = local_task;
                comm->allreduce_post_index = next_index;
                break;
            }

        case TMPI_Reduce:{
                int next_index = (comm->reduce_post_index + 1) % MPIDI_COLL_TASK_PREALLOC;
                if (comm->reduce_queue[next_index] != NULL) {
                    MPIDI_PIP_Coll_easy_task_t *reclaim_task;
                    int tmp_next_index = next_index;
                    while (reclaim_task = comm->reduce_queue[tmp_next_index]) {
                        while (reclaim_task->complete != reclaim_task->target_cmpl)
                            MPL_sched_yield();
                        if (reclaim_task->free == 1)
                            free(reclaim_task->addr);
                        MPIR_Handle_obj_free(&MPIDI_Coll_easy_task_mem, (void *) reclaim_task);
                        comm->reduce_queue[tmp_next_index] = NULL;
                        tmp_next_index = (tmp_next_index + 1) % MPIDI_COLL_TASK_PREALLOC;
                    }
                }

                __sync_synchronize();
                comm->reduce_queue[comm->reduce_post_index] = local_task;
                comm->reduce_post_index = next_index;
                break;
            }

        case TMPI_Rem:{
                int next_index = (comm->rem_post_index + 1) % MPIDI_COLL_TASK_PREALLOC;
                if (comm->rem_queue[next_index] != NULL) {
                    MPIDI_PIP_Coll_easy_task_t *reclaim_task;
                    int tmp_next_index = next_index;
                    int buf_cnt;
                    struct rem_bufs *tmp_rem_buf;
                    while (reclaim_task = comm->rem_queue[tmp_next_index]) {
                        while (reclaim_task->complete != reclaim_task->target_cmpl)
                            MPL_sched_yield();
                        if (reclaim_task->free == 1) {
                            buf_cnt = reclaim_task->data_sz;
                            tmp_rem_buf = (struct rem_bufs *) reclaim_task->addr;
                            for (int i = 0; i < buf_cnt; ++i)
                                free(tmp_rem_buf[i].rem_buf);
                            free(tmp_rem_buf);
                        }
                        MPIR_Handle_obj_free(&MPIDI_Coll_easy_task_mem, (void *) reclaim_task);
                        comm->rem_queue[tmp_next_index] = NULL;
                        tmp_next_index = (tmp_next_index + 1) % MPIDI_COLL_TASK_PREALLOC;
                    }
                }

                __sync_synchronize();
                comm->rem_queue[comm->rem_post_index] = local_task;
                comm->rem_post_index = next_index;
                break;
            }
        default:
            MPIR_Assert(0);
    }
    return local_task;
}

MPIDI_PIP_Coll_easy_task_t *MPIR_Comm_get_easy_task(MPIR_Comm * comm, int target, int type)
{
    MPIR_Comm *target_comm = comm->comms_array[target];
    MPIDI_PIP_Coll_easy_task_t *target_task = NULL;
    switch (type) {
        case TMPI_Scatter:{
                int target_get_index = comm->scatter_get_index[target];
                while (target_comm->scatter_queue[target_get_index] == NULL)
                    MPL_sched_yield();
                target_task = target_comm->scatter_queue[target_get_index];
                comm->scatter_get_index[target] = (target_get_index + 1) % MPIDI_COLL_TASK_PREALLOC;
                break;
            }
        case TMPI_Allgather:{
                int target_get_index = comm->allgather_get_index[target];
                while (target_comm->allgather_queue[target_get_index] == NULL)
                    MPL_sched_yield();
                target_task = target_comm->allgather_queue[target_get_index];
                comm->allgather_get_index[target] =
                    (target_get_index + 1) % MPIDI_COLL_TASK_PREALLOC;
                break;
            }
        case TMPI_Bcast:{
                int target_get_index = comm->bcast_get_index[target];
                while (target_comm->bcast_queue[target_get_index] == NULL)
                    MPL_sched_yield();
                target_task = target_comm->bcast_queue[target_get_index];
                comm->bcast_get_index[target] = (target_get_index + 1) % MPIDI_COLL_TASK_PREALLOC;
                break;
            }
        case TMPI_Allreduce:{
                int target_get_index = comm->allreduce_get_index[target];
                while (target_comm->allreduce_queue[target_get_index] == NULL)
                    MPL_sched_yield();
                target_task = target_comm->allreduce_queue[target_get_index];
                comm->allreduce_get_index[target] =
                    (target_get_index + 1) % MPIDI_COLL_TASK_PREALLOC;
                break;
            }
        case TMPI_Reduce:{
                int target_get_index = comm->reduce_get_index[target];
                while (target_comm->reduce_queue[target_get_index] == NULL)
                    MPL_sched_yield();
                target_task = target_comm->reduce_queue[target_get_index];
                comm->reduce_get_index[target] = (target_get_index + 1) % MPIDI_COLL_TASK_PREALLOC;
                break;
            }
        case TMPI_Rem:{
                int target_get_index = comm->rem_get_index[target];
                while (target_comm->rem_queue[target_get_index] == NULL)
                    MPL_sched_yield();
                target_task = target_comm->rem_queue[target_get_index];
                comm->rem_get_index[target] = (target_get_index + 1) % MPIDI_COLL_TASK_PREALLOC;
                break;
            }
        default:
            MPIR_Assert(0);
    }
    return target_task;
}

int MPIR_Comm_create_subcomms(MPIR_Comm * comm)
{
    int mpi_errno = MPI_SUCCESS;
    int num_local = -1, num_external = -1;
    int local_rank = -1, external_rank = -1;
    int *local_procs = NULL, *external_procs = NULL, *roots_map = NULL;
    int root_rank;
    int leader_num, step, mask;
    MPIDI_PIP_Coll_task_t ***tcoll_queue;

    MPIR_FUNC_TERSE_STATE_DECL(MPID_STATE_MPIR_COMM_CREATE_SUBCOMMS);
    MPIR_FUNC_TERSE_ENTER(MPID_STATE_MPIR_COMM_CREATE_SUBCOMMS);

    MPIR_CHKPMEM_DECL(4);
    MPIR_Assert(comm->node_comm == NULL);
    MPIR_Assert(comm->node_roots_comm == NULL);

    mpi_errno = MPIR_Find_local(comm, &num_local, &local_rank, &local_procs,
                                &comm->intranode_table);
    /* --BEGIN ERROR HANDLING-- */
    if (mpi_errno) {
        if (MPIR_Err_is_fatal(mpi_errno))
            MPIR_ERR_POP(mpi_errno);

        /* Non-fatal errors simply mean that this communicator will not have
         * any node awareness.  Node-aware collectives are an optimization. */
        MPL_DBG_MSG_P(MPIR_DBG_COMM, VERBOSE, "MPIR_Find_local failed for comm_ptr=%p", comm);
        MPL_free(comm->intranode_table);

        mpi_errno = MPI_SUCCESS;
        goto fn_exit;
    }
    /* --END ERROR HANDLING-- */

    /* init shared queue */
    MPIDU_Init_shm_put(&comm, sizeof(struct MPIR_Comm *));
    MPIDU_Init_shm_barrier();
    MPIR_CHKPMEM_MALLOC(comm->comms_array, struct MPIR_Comm **,
                        sizeof(struct MPIR_Comm *) * num_local,
                        mpi_errno, "pip task queue array", MPL_MEM_SHM);
    for (int i = 0; i < num_local; i++)
        MPIDU_Init_shm_get(i, sizeof(struct MPIR_Comm *), &comm->comms_array[i]);
    MPIDU_Init_shm_barrier();




    mpi_errno = MPIR_Find_external(comm, &num_external, &external_rank, &external_procs,
                                   &comm->internode_table);
    /* --BEGIN ERROR HANDLING-- */
    if (mpi_errno) {
        if (MPIR_Err_is_fatal(mpi_errno))
            MPIR_ERR_POP(mpi_errno);

        /* Non-fatal errors simply mean that this communicator will not have
         * any node awareness.  Node-aware collectives are an optimization. */
        MPL_DBG_MSG_P(MPIR_DBG_COMM, VERBOSE, "MPIR_Find_external failed for comm_ptr=%p", comm);
        MPL_free(comm->internode_table);

        mpi_errno = MPI_SUCCESS;
        goto fn_exit;
    }
    /* --END ERROR HANDLING-- */

    /* defensive checks */
    MPIR_Assert(num_local > 0);
    MPIR_Assert(num_local > 1 || external_rank >= 0);
    MPIR_Assert(external_rank < 0 || external_procs != NULL);

#ifndef MPIDI_CH4_SHM_ENABLE_PIP
    /* if the node_roots_comm and comm would be the same size, then creating
     * the second communicator is useless and wasteful. */
    if (num_external == comm->remote_size) {
        MPIR_Assert(num_local == 1);
        goto fn_exit;
    }
#else
    if (comm->local_size == 1) {
        comm->node_procs_min = 1;
        MPIR_Assert(num_local == 1);
        goto fn_exit;
    }
#endif

    mpi_errno =
        MPIR_Find_node_procs_sum_min(comm, num_external, &comm->node_id, &root_rank,
                                     &comm->node_procs_min, &comm->node_procs_sum, &roots_map);
    if (mpi_errno) {
        if (MPIR_Err_is_fatal(mpi_errno))
            MPIR_ERR_POP(mpi_errno);

        /* Non-fatal errors simply mean that this communicator will not have
         * any node awareness.  Node-aware collectives are an optimization. */
        MPL_DBG_MSG_P(MPIR_DBG_COMM, VERBOSE, "MPIR_Find_node_procs_sum_min failed for comm_ptr=%p",
                      comm);
        MPL_free(comm->internode_table);

        mpi_errno = MPI_SUCCESS;
        goto fn_exit;
    }

    leader_num = comm->node_procs_min;
    if (num_local > 0) {
        mpi_errno = MPIR_Comm_create(&comm->node_comm);
        MPIR_ERR_CHECK(mpi_errno);

        comm->node_comm->context_id = comm->context_id + MPIR_CONTEXT_INTRANODE_OFFSET;
        comm->node_comm->recvcontext_id = comm->node_comm->context_id;
        comm->node_comm->rank = local_rank;
        comm->node_comm->comm_kind = MPIR_COMM_KIND__INTRACOMM;
        comm->node_comm->hierarchy_kind = MPIR_COMM_HIERARCHY_KIND__NODE;
        comm->node_comm->local_comm = NULL;
        MPL_DBG_MSG_D(MPIR_DBG_COMM, VERBOSE, "Create node_comm=%p\n", comm->node_comm);

        comm->node_comm->local_size = num_local;
        comm->node_comm->node_count = 1;
        comm->node_comm->local_rank = local_rank;
        comm->node_comm->remote_size = num_local;
        comm->node_comm->node_procs_min = leader_num;
        comm->node_comm->node_procs_sum = NULL;
        comm->node_comm->comms_array = comm->comms_array;
        comm->node_comm->max_depth = 0;
        memset((void *) comm->node_comm->scatter_queue, 0,
               sizeof(MPIDI_PIP_Coll_easy_task_t *) * MPIDI_COLL_TASK_PREALLOC);
        comm->node_comm->scatter_post_index = 0;
        comm->node_comm->scatter_get_index =
            (int *) calloc(comm->node_comm->local_size, sizeof(int));

        memset((void *) comm->node_comm->allgather_queue, 0,
               sizeof(MPIDI_PIP_Coll_easy_task_t *) * MPIDI_COLL_TASK_PREALLOC);
        comm->node_comm->allgather_post_index = 0;
        comm->node_comm->allgather_get_index =
            (int *) calloc(comm->node_comm->local_size, sizeof(int));

        memset((void *) comm->node_comm->rem_queue, 0,
               sizeof(MPIDI_PIP_Coll_easy_task_t *) * MPIDI_COLL_TASK_PREALLOC);
        comm->node_comm->rem_post_index = 0;
        comm->node_comm->rem_get_index = (int *) calloc(comm->node_comm->local_size, sizeof(int));

        memset((void *) comm->node_comm->bcast_queue, 0,
               sizeof(MPIDI_PIP_Coll_easy_task_t *) * MPIDI_COLL_TASK_PREALLOC);
        comm->node_comm->bcast_post_index = 0;
        comm->node_comm->bcast_get_index = (int *) calloc(comm->node_comm->local_size, sizeof(int));

        memset((void *) comm->node_comm->allreduce_queue, 0,
               sizeof(MPIDI_PIP_Coll_easy_task_t *) * MPIDI_COLL_TASK_PREALLOC);
        comm->node_comm->allreduce_post_index = 0;
        comm->node_comm->allreduce_get_index =
            (int *) calloc(comm->node_comm->local_size, sizeof(int));

        memset((void *) comm->node_comm->reduce_queue, 0,
               sizeof(MPIDI_PIP_Coll_easy_task_t *) * MPIDI_COLL_TASK_PREALLOC);
        comm->node_comm->reduce_post_index = 0;
        comm->node_comm->reduce_get_index =
            (int *) calloc(comm->node_comm->local_size, sizeof(int));

        comm->node_comm->comms_array =
            (MPIR_Comm **) malloc(comm->node_comm->local_size * sizeof(MPIR_Comm *));
        MPIDU_Init_shm_put(&comm->node_comm, sizeof(MPIR_Comm *));
        MPIDU_Init_shm_barrier();
        for (int i = 0; i < comm->node_comm->local_size; i++)
            MPIDU_Init_shm_get(i, sizeof(MPIR_Comm *), &comm->node_comm->comms_array[i]);
        MPIDU_Init_shm_barrier();

        step = 0;
        mask = 1;
        while (mask <= comm->node_comm->local_size) {
            ++step;
            mask <<= 1;
        }
        MPIR_Assert(step < MPIDI_COLL_TASK_PREALLOC);
        comm->node_comm->max_pof2_step = step;
        if (comm->node_comm->rank == 0) {
            comm->node_comm->barrier =
                (MPIDI_Comm_intra_barrier_t *) calloc(2, sizeof(MPIDI_Comm_intra_barrier_t));
            MPIDU_Init_shm_put(&comm->node_comm->barrier, sizeof(MPIDI_Comm_intra_barrier_t *));
            MPIDU_Init_shm_barrier();
        } else {
            MPIDU_Init_shm_barrier();
            MPIDU_Init_shm_get(0, sizeof(MPIDI_Comm_intra_barrier_t *), &comm->node_comm->barrier);
        }
        MPIDU_Init_shm_barrier();
        comm->node_comm->barrier_round = 0;
        // comm->node_comm->node_barrier = NULL;
        // comm->node_comm->pip_roots_barrier = NULL;

        MPIR_Comm_map_irregular(comm->node_comm, comm, local_procs, num_local,
                                MPIR_COMM_MAP_DIR__L2L, NULL);
        mpi_errno = MPIR_Comm_commit_internal(comm->node_comm);
        MPIR_ERR_CHECK(mpi_errno);
    }

    if (local_rank == 0) {
        mpi_errno = MPIR_Comm_create(&comm->node_roots_comm);
        MPIR_ERR_CHECK(mpi_errno);

        comm->node_roots_comm->context_id = comm->context_id + MPIR_CONTEXT_INTERNODE_OFFSET;
        comm->node_roots_comm->recvcontext_id = comm->node_roots_comm->context_id;
        comm->node_roots_comm->rank = external_rank;
        comm->node_roots_comm->comm_kind = MPIR_COMM_KIND__INTRACOMM;
        comm->node_roots_comm->hierarchy_kind = MPIR_COMM_HIERARCHY_KIND__NODE_ROOTS;
        comm->node_roots_comm->local_comm = NULL;
        MPL_DBG_MSG_D(MPIR_DBG_COMM, VERBOSE, "Create node_roots_comm=%p\n", comm->node_roots_comm);

        comm->node_roots_comm->local_size = num_external;
        comm->node_roots_comm->remote_size = num_external;

        MPIR_Comm_map_irregular(comm->node_roots_comm, comm, external_procs, num_external,
                                MPIR_COMM_MAP_DIR__L2L, NULL);
        mpi_errno = MPIR_Comm_commit_internal(comm->node_roots_comm);
        MPIR_ERR_CHECK(mpi_errno);
    }

    /* this process may not be a member of the node_roots_comm */
    if (local_rank < leader_num && num_external > 1) {
        mpi_errno = MPIR_Comm_create(&comm->pip_roots_comm);
        MPIR_ERR_CHECK(mpi_errno);

        comm->pip_roots_comm->context_id =
            comm->context_id + MPIR_CONTEXT_INTERNODE_OFFSET + MPIR_CONTEXT_INTRANODE_OFFSET;
        comm->pip_roots_comm->recvcontext_id = comm->pip_roots_comm->context_id;
        comm->pip_roots_comm->rank = root_rank;
        comm->pip_roots_comm->local_rank = root_rank % leader_num;
        comm->pip_roots_comm->comm_kind = MPIR_COMM_KIND__INTRACOMM;
        comm->pip_roots_comm->hierarchy_kind = MPIR_COMM_HIERARCHY_KIND__NODE_ROOTS;
        comm->pip_roots_comm->local_comm = NULL;
        MPL_DBG_MSG_D(MPIR_DBG_COMM, VERBOSE, "Create pip_roots_comm=%p\n", comm->pip_roots_comm);

        comm->pip_roots_comm->local_size = num_external * leader_num;
        comm->pip_roots_comm->remote_size = num_external * leader_num;
        comm->pip_roots_comm->node_procs_min = leader_num;
        comm->pip_roots_comm->local_rank = root_rank % leader_num;
        comm->pip_roots_comm->node_procs_sum = comm->node_procs_sum;
        comm->pip_roots_comm->node_id = comm->node_id;
        comm->pip_roots_comm->node_count = comm->node_count;

        comm->pip_roots_comm->intranode_size = num_local;
        if (comm->node_comm) {
            comm->pip_roots_comm->node_comm = comm->node_comm;
            comm->node_comm->node_procs_min = leader_num;
        }

        memset((void *) comm->pip_roots_comm->scatter_queue, 0,
               sizeof(MPIDI_PIP_Coll_easy_task_t *) * MPIDI_COLL_TASK_PREALLOC);
        comm->pip_roots_comm->scatter_get_index = calloc(comm->node_procs_min, sizeof(int));
        comm->pip_roots_comm->scatter_post_index = 0;

        memset((void *) comm->pip_roots_comm->allgather_queue, 0,
               sizeof(MPIDI_PIP_Coll_easy_task_t *) * MPIDI_COLL_TASK_PREALLOC);
        comm->pip_roots_comm->allgather_post_index = 0;
        comm->pip_roots_comm->allgather_get_index = (int *) calloc(leader_num, sizeof(int));

        memset((void *) comm->pip_roots_comm->bcast_queue, 0,
               sizeof(MPIDI_PIP_Coll_easy_task_t *) * MPIDI_COLL_TASK_PREALLOC);
        comm->pip_roots_comm->bcast_post_index = 0;
        comm->pip_roots_comm->bcast_get_index = (int *) calloc(leader_num, sizeof(int));

        memset((void *) comm->pip_roots_comm->allreduce_queue, 0,
               sizeof(MPIDI_PIP_Coll_easy_task_t *) * MPIDI_COLL_TASK_PREALLOC);
        comm->pip_roots_comm->allreduce_post_index = 0;
        comm->pip_roots_comm->allreduce_get_index = (int *) calloc(leader_num, sizeof(int));

        memset((void *) comm->pip_roots_comm->reduce_queue, 0,
               sizeof(MPIDI_PIP_Coll_easy_task_t *) * MPIDI_COLL_TASK_PREALLOC);
        comm->pip_roots_comm->reduce_post_index = 0;
        comm->pip_roots_comm->reduce_get_index = (int *) calloc(leader_num, sizeof(int));

        memset((void *) comm->pip_roots_comm->rem_queue, 0,
               sizeof(MPIDI_PIP_Coll_easy_task_t *) * MPIDI_COLL_TASK_PREALLOC);
        comm->pip_roots_comm->rem_post_index = 0;
        comm->pip_roots_comm->rem_get_index = (int *) calloc(leader_num, sizeof(int));

        comm->pip_roots_comm->comms_array =
            (MPIR_Comm **) malloc(comm->node_procs_min * sizeof(MPIR_Comm *));
        MPIDU_Init_shm_put(&comm->pip_roots_comm, sizeof(MPIR_Comm *));
        MPIDU_Init_shm_barrier();
        for (int i = 0; i < comm->node_procs_min; i++)
            MPIDU_Init_shm_get(i, sizeof(MPIR_Comm *), &comm->pip_roots_comm->comms_array[i]);
        MPIDU_Init_shm_barrier();

        step = 0;
        mask = 1;
        while (mask <= leader_num) {
            ++step;
            mask <<= 1;
        }

        comm->pip_roots_comm->max_pof2_step = step;
        if (comm->pip_roots_comm->local_rank == 0) {
            comm->pip_roots_comm->barrier =
                (MPIDI_Comm_intra_barrier_t *) calloc(2, sizeof(MPIDI_Comm_intra_barrier_t));
            MPIDU_Init_shm_put(&comm->pip_roots_comm->barrier,
                               sizeof(MPIDI_Comm_intra_barrier_t *));
            MPIDU_Init_shm_barrier();
        } else {
            MPIDU_Init_shm_barrier();
            MPIDU_Init_shm_get(0, sizeof(MPIDI_Comm_intra_barrier_t *),
                               &comm->pip_roots_comm->barrier);
        }
        MPIDU_Init_shm_barrier();
        comm->pip_roots_comm->barrier_round = 0;

        MPIR_Comm_map_irregular(comm->pip_roots_comm, comm, roots_map, num_external * leader_num,
                                MPIR_COMM_MAP_DIR__L2L, NULL);
        mpi_errno = MPIR_Comm_commit_internal(comm->pip_roots_comm);
        MPIR_ERR_CHECK(mpi_errno);
    } else {
        MPIDU_Init_shm_barrier();
        MPIDU_Init_shm_barrier();
        MPIDU_Init_shm_barrier();
        MPIDU_Init_shm_barrier();
    }

    comm->hierarchy_kind = MPIR_COMM_HIERARCHY_KIND__PARENT;

  fn_exit:
    MPL_free(roots_map);
    MPL_free(local_procs);
    MPL_free(external_procs);
    MPIR_FUNC_TERSE_EXIT(MPID_STATE_MPIR_COMM_CREATE_SUBCOMMS);
    return mpi_errno;
  fn_fail:
    goto fn_exit;
}

/* static routines for MPIR_Comm_commit */
static int init_comm_seq(MPIR_Comm * comm)
{
    int mpi_errno = MPI_SUCCESS;

    /* Every user-level communicator gets a sequence number, which can be
     * used, for example, to hash vci.
     * Builtin-comm, e.g. MPI_COMM_WORLD, always have seq at 0 */
    if (!HANDLE_IS_BUILTIN(comm->handle)) {
        static int vci_seq = 0;
        vci_seq++;

        int tmp = vci_seq;
        /* Bcast seq over vci 0 */
        MPIR_Assert(comm->seq == 0);

        /* Every rank need share the same seq from root. NOTE: it is possible for
         * different communicators to have the same seq. It is only used as an
         * opportunistic optimization */
        MPIR_Errflag_t errflag = MPIR_ERR_NONE;
        mpi_errno = MPIR_Bcast_allcomm_auto(&tmp, 1, MPI_INT, 0, comm, &errflag);
        MPIR_ERR_CHECK(mpi_errno);

        comm->seq = tmp;
    }

    if (comm->node_comm) {
        comm->node_comm->seq = comm->seq;
    }

    if (comm->node_roots_comm) {
        comm->node_roots_comm->seq = comm->seq;
    }

    if (comm->pip_roots_comm) {
        comm->pip_roots_comm->seq = comm->seq;
    }

  fn_exit:
    return mpi_errno;
  fn_fail:
    goto fn_exit;
}

/* Provides a hook for the top level functions to perform some manipulation on a
   communicator just before it is given to the application level.

   For example, we create sub-communicators for SMP-aware collectives at this
   step. */
int MPIR_Comm_commit(MPIR_Comm * comm)
{
    int mpi_errno = MPI_SUCCESS;
    int depth = 0, tmp_cnt;
    MPIR_FUNC_TERSE_STATE_DECL(MPID_STATE_MPIR_COMM_COMMIT);

    MPIR_FUNC_TERSE_ENTER(MPID_STATE_MPIR_COMM_COMMIT);

    /* It's OK to relax these assertions, but we should do so very
     * intentionally.  For now this function is the only place that we create
     * our hierarchy of communicators */
    MPIR_Assert(comm->node_comm == NULL);
    MPIR_Assert(comm->node_roots_comm == NULL);

    /* Notify device of communicator creation */
    mpi_errno = MPIR_Comm_commit_internal(comm);
    MPIR_ERR_CHECK(mpi_errno);

    if (comm->comm_kind == MPIR_COMM_KIND__INTRACOMM && !MPIR_CONTEXT_READ_FIELD(SUBCOMM, comm->context_id)) {  /*make sure this is not a subcomm */
        mpi_errno = MPIR_Comm_create_subcomms(comm);
        MPIR_ERR_CHECK(mpi_errno);
    }

    /* Create collectives-specific infrastructure */
    mpi_errno = MPIR_Coll_comm_init(comm);
    MPIR_ERR_CHECK(mpi_errno);

    if (comm->node_comm) {
        mpi_errno = MPIR_Coll_comm_init(comm->node_comm);
        MPIR_ERR_CHECK(mpi_errno);
    }

    if (comm->node_roots_comm) {
        mpi_errno = MPIR_Coll_comm_init(comm->node_roots_comm);
        MPIR_ERR_CHECK(mpi_errno);
    }

    if (comm->pip_roots_comm) {
        mpi_errno = MPIR_Coll_comm_init(comm->pip_roots_comm);
        MPIR_ERR_CHECK(mpi_errno);
    }

    /* call post commit hooks */
    mpi_errno = MPID_Comm_commit_post_hook(comm);
    MPIR_ERR_CHECK(mpi_errno);

    if (comm->node_comm) {
        mpi_errno = MPID_Comm_commit_post_hook(comm->node_comm);
        MPIR_ERR_CHECK(mpi_errno);
    }

    if (comm->node_roots_comm) {
        mpi_errno = MPID_Comm_commit_post_hook(comm->node_roots_comm);
        MPIR_ERR_CHECK(mpi_errno);
    }

    if (comm->pip_roots_comm) {
        mpi_errno = MPID_Comm_commit_post_hook(comm->pip_roots_comm);
        MPIR_ERR_CHECK(mpi_errno);
    }

    if (comm->comm_kind == MPIR_COMM_KIND__INTRACOMM && !comm->tainted) {
        mpi_errno = init_comm_seq(comm);
        MPIR_ERR_CHECK(mpi_errno);
    }

    tmp_cnt = comm->node_count;
    while (tmp_cnt > 0) {
        depth++;
        tmp_cnt /= (comm->node_procs_min + 1);
    }

    comm->max_depth = depth;
    if (comm->node_comm) {
        comm->node_comm->max_depth = depth;
    }

    if (comm->pip_roots_comm) {
        comm->pip_roots_comm->max_depth = depth;
    }

  fn_exit:
    MPIR_FUNC_TERSE_EXIT(MPID_STATE_MPIR_COMM_COMMIT);
    return mpi_errno;
  fn_fail:
    goto fn_exit;
}

/* Returns true if the given communicator is aware of node topology information,
   false otherwise.  Such information could be used to implement more efficient
   collective communication, for example. */
int MPIR_Comm_is_parent_comm(MPIR_Comm * comm)
{
    return (comm->hierarchy_kind == MPIR_COMM_HIERARCHY_KIND__PARENT);
}

/* Returns true if the communicator is node-aware and processes in all the nodes
   are consecutive. For example, if node 0 contains "0, 1, 2, 3", node 1
   contains "4, 5, 6", and node 2 contains "7", we shall return true. */
int MPII_Comm_is_node_consecutive(MPIR_Comm * comm)
{
    int i = 0, curr_nodeidx = 0;
    int *internode_table = comm->internode_table;

    if (!MPIR_Comm_is_parent_comm(comm))
        return 0;

    for (; i < comm->local_size; i++) {
        if (internode_table[i] == curr_nodeidx + 1)
            curr_nodeidx++;
        else if (internode_table[i] != curr_nodeidx)
            return 0;
    }

    return 1;
}

/*
 * Copy a communicator, including creating a new context and copying the
 * virtual connection tables and clearing the various fields.
 * Does *not* copy attributes.  If size is < the size of the local group
 * in the input communicator, copy only the first size elements.
 * If this process is not a member, return a null pointer in outcomm_ptr.
 * This is only supported in the case where the communicator is in
 * Intracomm (not an Intercomm).  Note that this is all that is required
 * for cart_create and graph_create.
 *
 * Used by cart_create, graph_create, and dup_create
 */
int MPII_Comm_copy(MPIR_Comm * comm_ptr, int size, MPIR_Info * info, MPIR_Comm ** outcomm_ptr)
{
    int mpi_errno = MPI_SUCCESS;
    MPIR_Context_id_t new_context_id, new_recvcontext_id;
    MPIR_Comm *newcomm_ptr = NULL;
    MPIR_Comm_map_t *map = NULL;
    MPIR_FUNC_TERSE_STATE_DECL(MPID_STATE_MPIR_COMM_COPY);

    MPIR_FUNC_TERSE_ENTER(MPID_STATE_MPIR_COMM_COPY);

    /* Get a new context first.  We need this to be collective over the
     * input communicator */
    /* If there is a context id cache in oldcomm, use it here.  Otherwise,
     * use the appropriate algorithm to get a new context id.  Be careful
     * of intercomms here */
    if (comm_ptr->comm_kind == MPIR_COMM_KIND__INTERCOMM) {
        mpi_errno = MPIR_Get_intercomm_contextid(comm_ptr, &new_context_id, &new_recvcontext_id);
        MPIR_ERR_CHECK(mpi_errno);
    } else {
        mpi_errno = MPIR_Get_contextid_sparse(comm_ptr, &new_context_id, FALSE);
        new_recvcontext_id = new_context_id;
        MPIR_ERR_CHECK(mpi_errno);
        MPIR_Assert(new_context_id != 0);
    }

    /* This is the local size, not the remote size, in the case of
     * an intercomm */
    if (comm_ptr->rank >= size) {
        *outcomm_ptr = 0;
        /* always free the recvcontext ID, never the "send" ID */
        MPIR_Free_contextid(new_recvcontext_id);
        goto fn_exit;
    }

    /* We're left with the processes that will have a non-null communicator.
     * Create the object, initialize the data, and return the result */

    mpi_errno = MPIR_Comm_create(&newcomm_ptr);
    if (mpi_errno)
        goto fn_fail;

    newcomm_ptr->context_id = new_context_id;
    newcomm_ptr->recvcontext_id = new_recvcontext_id;

    /* Save the kind of the communicator */
    newcomm_ptr->comm_kind = comm_ptr->comm_kind;
    newcomm_ptr->local_comm = 0;

    /* There are two cases here - size is the same as the old communicator,
     * or it is smaller.  If the size is the same, we can just add a reference.
     * Otherwise, we need to create a new network address mapping.  Note that this is the
     * test that matches the test on rank above. */
    if (size == comm_ptr->local_size) {
        /* Duplicate the network address mapping */
        if (comm_ptr->comm_kind == MPIR_COMM_KIND__INTRACOMM)
            MPIR_Comm_map_dup(newcomm_ptr, comm_ptr, MPIR_COMM_MAP_DIR__L2L);
        else
            MPIR_Comm_map_dup(newcomm_ptr, comm_ptr, MPIR_COMM_MAP_DIR__R2R);
    } else {
        int i;

        if (comm_ptr->comm_kind == MPIR_COMM_KIND__INTRACOMM)
            MPIR_Comm_map_irregular(newcomm_ptr, comm_ptr, NULL, size, MPIR_COMM_MAP_DIR__L2L,
                                    &map);
        else
            MPIR_Comm_map_irregular(newcomm_ptr, comm_ptr, NULL, size, MPIR_COMM_MAP_DIR__R2R,
                                    &map);
        for (i = 0; i < size; i++) {
            /* For rank i in the new communicator, find the corresponding
             * rank in the input communicator */
            map->src_mapping[i] = i;
        }
    }

    /* If it is an intercomm, duplicate the local network address references */
    if (comm_ptr->comm_kind == MPIR_COMM_KIND__INTERCOMM) {
        MPIR_Comm_map_dup(newcomm_ptr, comm_ptr, MPIR_COMM_MAP_DIR__L2L);
    }

    /* Set the sizes and ranks */
    newcomm_ptr->rank = comm_ptr->rank;
    if (comm_ptr->comm_kind == MPIR_COMM_KIND__INTERCOMM) {
        newcomm_ptr->local_size = comm_ptr->local_size;
        newcomm_ptr->remote_size = comm_ptr->remote_size;
        newcomm_ptr->is_low_group = comm_ptr->is_low_group;
    } else {
        newcomm_ptr->local_size = size;
        newcomm_ptr->remote_size = size;
    }

    /* Inherit the error handler (if any) */
    MPID_THREAD_CS_ENTER(POBJ, MPIR_THREAD_POBJ_COMM_MUTEX(comm_ptr));
    newcomm_ptr->errhandler = comm_ptr->errhandler;
    if (comm_ptr->errhandler) {
        MPIR_Errhandler_add_ref(comm_ptr->errhandler);
    }
    MPID_THREAD_CS_EXIT(POBJ, MPIR_THREAD_POBJ_COMM_MUTEX(comm_ptr));

    if (info) {
        MPII_Comm_set_hints(newcomm_ptr, info);
    }

    newcomm_ptr->tainted = comm_ptr->tainted;
    mpi_errno = MPIR_Comm_commit(newcomm_ptr);
    MPIR_ERR_CHECK(mpi_errno);

    /* Start with no attributes on this communicator */
    newcomm_ptr->attributes = 0;

    *outcomm_ptr = newcomm_ptr;

  fn_fail:
  fn_exit:

    MPIR_FUNC_TERSE_EXIT(MPID_STATE_MPIR_COMM_COPY);

    return mpi_errno;
}

/* Copy a communicator, including copying the virtual connection tables and
 * clearing the various fields.  Does *not* allocate a context ID or commit the
 * communicator.  Does *not* copy attributes.
 *
 * Used by comm_idup.
 */
int MPII_Comm_copy_data(MPIR_Comm * comm_ptr, MPIR_Info * info, MPIR_Comm ** outcomm_ptr)
{
    int mpi_errno = MPI_SUCCESS;
    MPIR_Comm *newcomm_ptr = NULL;
    MPIR_FUNC_TERSE_STATE_DECL(MPID_STATE_MPIR_COMM_COPY_DATA);

    MPIR_FUNC_TERSE_ENTER(MPID_STATE_MPIR_COMM_COPY_DATA);

    mpi_errno = MPIR_Comm_create(&newcomm_ptr);
    if (mpi_errno)
        goto fn_fail;

    /* use a large garbage value to ensure errors are caught more easily */
    newcomm_ptr->context_id = 32767;
    newcomm_ptr->recvcontext_id = 32767;

    /* Save the kind of the communicator */
    newcomm_ptr->comm_kind = comm_ptr->comm_kind;
    newcomm_ptr->local_comm = 0;

    if (comm_ptr->comm_kind == MPIR_COMM_KIND__INTRACOMM)
        MPIR_Comm_map_dup(newcomm_ptr, comm_ptr, MPIR_COMM_MAP_DIR__L2L);
    else
        MPIR_Comm_map_dup(newcomm_ptr, comm_ptr, MPIR_COMM_MAP_DIR__R2R);

    /* If it is an intercomm, duplicate the network address mapping */
    if (comm_ptr->comm_kind == MPIR_COMM_KIND__INTERCOMM) {
        MPIR_Comm_map_dup(newcomm_ptr, comm_ptr, MPIR_COMM_MAP_DIR__L2L);
    }

    /* Set the sizes and ranks */
    newcomm_ptr->rank = comm_ptr->rank;
    newcomm_ptr->local_size = comm_ptr->local_size;
    newcomm_ptr->remote_size = comm_ptr->remote_size;
    newcomm_ptr->is_low_group = comm_ptr->is_low_group; /* only relevant for intercomms */

    /* Inherit the error handler (if any) */
    MPID_THREAD_CS_ENTER(POBJ, MPIR_THREAD_POBJ_COMM_MUTEX(comm_ptr));
    newcomm_ptr->errhandler = comm_ptr->errhandler;
    if (comm_ptr->errhandler) {
        MPIR_Errhandler_add_ref(comm_ptr->errhandler);
    }
    MPID_THREAD_CS_EXIT(POBJ, MPIR_THREAD_POBJ_COMM_MUTEX(comm_ptr));

    if (info) {
        MPII_Comm_set_hints(newcomm_ptr, info);
    }

    /* Start with no attributes on this communicator */
    newcomm_ptr->attributes = 0;
    *outcomm_ptr = newcomm_ptr;

    /* inherit tainted flag */
    newcomm_ptr->tainted = comm_ptr->tainted;

  fn_fail:
    MPIR_FUNC_TERSE_EXIT(MPID_STATE_MPIR_COMM_COPY_DATA);
    return mpi_errno;
}

/* Common body between MPIR_Comm_release and MPIR_comm_release_always.  This
 * helper function frees the actual MPIR_Comm structure and any associated
 * storage.  It also releases any references to other objects.
 * This function should only be called when the communicator's reference count
 * has dropped to 0.
 *
 * !!! This routine should *never* be called outside of MPIR_Comm_release{,_always} !!!
 */
int MPIR_Comm_delete_internal(MPIR_Comm * comm_ptr)
{
    int in_use;
    int mpi_errno = MPI_SUCCESS;
    MPIR_FUNC_TERSE_STATE_DECL(MPID_STATE_COMM_DELETE_INTERNAL);

    MPIR_FUNC_TERSE_ENTER(MPID_STATE_COMM_DELETE_INTERNAL);

    MPIR_Assert(MPIR_Object_get_ref(comm_ptr) == 0);    /* sanity check */

    /* Remove the attributes, executing the attribute delete routine.
     * Do this only if the attribute functions are defined.
     * This must be done first, because if freeing the attributes
     * returns an error, the communicator is not freed */
    if (MPIR_Process.attr_free && comm_ptr->attributes) {
        /* Temporarily add a reference to this communicator because
         * the attr_free code requires a valid communicator */
        MPIR_Object_add_ref(comm_ptr);
        mpi_errno = MPIR_Process.attr_free(comm_ptr->handle, &comm_ptr->attributes);
        /* Release the temporary reference added before the call to
         * attr_free */
        MPIR_Object_release_ref(comm_ptr, &in_use);
    }

    /* If the attribute delete functions return failure, the
     * communicator must not be freed.  That is the reason for the
     * test on mpi_errno here. */
    if (mpi_errno == MPI_SUCCESS) {
        /* If this communicator is our parent, and we're disconnecting
         * from the parent, mark that fact */
        if (MPIR_Process.comm_parent == comm_ptr)
            MPIR_Process.comm_parent = NULL;

        /* Cleanup collectives-specific infrastructure */
        mpi_errno = MPII_Coll_comm_cleanup(comm_ptr);
        MPIR_ERR_CHECK(mpi_errno);

        /* Notify the device that the communicator is about to be
         * destroyed */
        mpi_errno = MPID_Comm_free_hook(comm_ptr);
        MPIR_ERR_CHECK(mpi_errno);

        if (comm_ptr->comm_kind == MPIR_COMM_KIND__INTERCOMM && comm_ptr->local_comm)
            MPIR_Comm_release(comm_ptr->local_comm);

        /* Free the local and remote groups, if they exist */
        if (comm_ptr->local_group)
            MPIR_Group_release(comm_ptr->local_group);
        if (comm_ptr->remote_group)
            MPIR_Group_release(comm_ptr->remote_group);

        /* free the intra/inter-node communicators, if they exist */
        if (comm_ptr->node_comm) {
            if (comm_ptr->node_comm->rank == 0) {
                MPL_free(comm_ptr->node_comm->barrier);
                MPL_free(comm_ptr->node_comm->scatter_get_index);
                MPL_free(comm_ptr->node_comm->allgather_get_index);
                MPL_free(comm_ptr->node_comm->bcast_get_index);
                MPL_free(comm_ptr->node_comm->allreduce_get_index);
                MPL_free(comm_ptr->node_comm->reduce_get_index);
                MPL_free(comm_ptr->node_comm->rem_get_index);
            }
            MPIR_Comm_release(comm_ptr->node_comm);
            MPL_free(comm_ptr->node_procs_sum);
            MPL_free(comm_ptr->comms_array);
        }
        if (comm_ptr->node_roots_comm)
            MPIR_Comm_release(comm_ptr->node_roots_comm);
        if (comm_ptr->pip_roots_comm) {
            if (comm_ptr->pip_roots_comm->local_rank == 0) {
                MPL_free(comm_ptr->pip_roots_comm->barrier);
                MPL_free(comm_ptr->pip_roots_comm->scatter_get_index);
                MPL_free(comm_ptr->pip_roots_comm->allgather_get_index);
                MPL_free(comm_ptr->pip_roots_comm->bcast_get_index);
                MPL_free(comm_ptr->pip_roots_comm->allreduce_get_index);
                MPL_free(comm_ptr->pip_roots_comm->reduce_get_index);
                MPL_free(comm_ptr->pip_roots_comm->rem_get_index);
            }
            comm_ptr->pip_roots_comm->node_comm = NULL;
            MPIR_Comm_release(comm_ptr->pip_roots_comm);
        }
        MPL_free(comm_ptr->intranode_table);
        MPL_free(comm_ptr->internode_table);

        /* Free the context value.  This should come after freeing the
         * intra/inter-node communicators since those free calls won't
         * release this context ID and releasing this before then could lead
         * to races once we make threading finer grained. */
        /* This must be the recvcontext_id (i.e. not the (send)context_id)
         * because in the case of intercommunicators the send context ID is
         * allocated out of the remote group's bit vector, not ours. */
        MPIR_Free_contextid(comm_ptr->recvcontext_id);

#if MPICH_THREAD_GRANULARITY == MPICH_THREAD_GRANULARITY__POBJ
        {
            int thr_err;
            MPID_Thread_mutex_destroy(&MPIR_THREAD_POBJ_COMM_MUTEX(comm_ptr), &thr_err);
            MPIR_Assert(thr_err == 0);
        }
#endif
        /* We need to release the error handler */
        if (comm_ptr->errhandler && !(HANDLE_IS_BUILTIN(comm_ptr->errhandler->handle))) {
            int errhInuse;
            MPIR_Errhandler_release_ref(comm_ptr->errhandler, &errhInuse);
            if (!errhInuse) {
                MPIR_Handle_obj_free(&MPIR_Errhandler_mem, comm_ptr->errhandler);
            }
        }

        /* Remove from the list of active communicators if
         * we are supporting message-queue debugging.  We make this
         * conditional on having debugger support since the
         * operation is not constant-time */
        MPII_COMML_FORGET(comm_ptr);

        /* Check for predefined communicators - these should not
         * be freed */
        if (!(HANDLE_IS_BUILTIN(comm_ptr->handle)))
            MPIR_Handle_obj_free(&MPIR_Comm_mem, comm_ptr);
    } else {
        /* If the user attribute free function returns an error,
         * then do not free the communicator */
        MPIR_Comm_add_ref(comm_ptr);
    }

  fn_exit:
    MPIR_FUNC_TERSE_EXIT(MPID_STATE_COMM_DELETE_INTERNAL);
    return mpi_errno;
  fn_fail:
    goto fn_exit;
}

/* Release a reference to a communicator.  If there are no pending
   references, delete the communicator and recover all storage and
   context ids.  This version of the function always manipulates the reference
   counts, even for predefined objects. */
int MPIR_Comm_release_always(MPIR_Comm * comm_ptr)
{
    int mpi_errno = MPI_SUCCESS;
    int in_use;
    MPIR_FUNC_TERSE_STATE_DECL(MPID_STATE_MPIR_COMM_RELEASE_ALWAYS);

    MPIR_FUNC_TERSE_ENTER(MPID_STATE_MPIR_COMM_RELEASE_ALWAYS);

    /* we want to short-circuit any optimization that avoids reference counting
     * predefined communicators, such as MPI_COMM_WORLD or MPI_COMM_SELF. */
    MPIR_Object_release_ref_always(comm_ptr, &in_use);
    if (!in_use) {
        mpi_errno = MPIR_Comm_delete_internal(comm_ptr);
        MPIR_ERR_CHECK(mpi_errno);
    }

  fn_exit:
    MPIR_FUNC_TERSE_EXIT(MPID_STATE_MPIR_COMM_RELEASE_ALWAYS);
    return mpi_errno;
  fn_fail:
    goto fn_exit;
}
