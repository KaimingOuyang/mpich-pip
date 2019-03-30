/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil ; -*- */
/*
 *  (C) 2001 by Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

/*
=== BEGIN_MPI_T_CVAR_INFO_BLOCK ===

categories:
    - name        : MEMORY
      description : affects memory allocation and usage, including MPI object handles

cvars:
    - name        : MPIR_CVAR_ABORT_ON_LEAKED_HANDLES
      category    : MEMORY
      type        : boolean
      default     : false
      class       : device
      verbosity   : MPI_T_VERBOSITY_USER_BASIC
      scope       : MPI_T_SCOPE_ALL_EQ
      description : >-
        If true, MPI will call MPI_Abort at MPI_Finalize if any MPI object
        handles have been leaked.  For example, if MPI_Comm_dup is called
        without calling a corresponding MPI_Comm_free.  For uninteresting
        reasons, enabling this option may prevent all known object leaks from
        being reported.  MPICH must have been configure with
        "--enable-g=handlealloc" or better in order for this functionality to
        work.

=== END_MPI_T_CVAR_INFO_BLOCK ===
*/

#include "mpiimpl.h"
#include <stdio.h>
// #define HANDLE_SHM_NUM_BLOCKS 64
/*
  MPIR_Handlemem_init should be called after MPID_Init and MPID_Coll_Init
  in order to use MPIR_Bcast.
*/

#undef FUNCNAME
#define FUNCNAME MPIR_Handlemem_shm_obj_init
#undef FCNAME
#define FCNAME MPL_QUOTE(FUNCNAME)
static inline int MPIR_Handlemem_shm_obj_init(MPIR_Object_alloc_t * objmem)
{
    int rank = MPIR_Process.comm_world->node_comm->rank;
    int mpi_errno = MPI_SUCCESS;
    MPIR_FUNC_VERBOSE_STATE_DECL(MPIR_STATE_MPIR_HANDLEMEM_SHM_OBJ_INIT);
    MPIR_FUNC_VERBOSE_ENTER(MPIR_STATE_MPIR_HANDLEMEM_SHM_OBJ_INIT);

    if (MPL_proc_mutex_enabled()) {
        // printf("rank %d - Init start\n", rank);
        // fflush(stdout);

        objmem->shm_base = (void **) MPL_calloc(HANDLE_NUM_BLOCKS, sizeof(void *), MPL_MEM_OBJECT);
        MPIR_ERR_CHKANDJUMP(!objmem->shm_base, mpi_errno, MPI_ERR_OTHER, "**alloc_shar_mem");

        objmem->shm_avail =
            (uint64_t **) MPL_calloc(HANDLE_NUM_BLOCKS, sizeof(uint64_t *), MPL_MEM_OBJECT);
        MPIR_ERR_CHKANDJUMP(!objmem->shm_avail, mpi_errno, MPI_ERR_OTHER, "**alloc_shar_mem");

        objmem->shm_lock =
            (MPL_proc_mutex_t **) MPL_calloc(HANDLE_NUM_BLOCKS, sizeof(MPL_proc_mutex_t *),
                                             MPL_MEM_OBJECT);
        MPIR_ERR_CHKANDJUMP(!objmem->shm_lock, mpi_errno, MPI_ERR_OTHER, "**alloc_shar_mem");
        objmem->shm_size = 0;

        if (!rank) {
            mpi_errno = MPIR_Handle_shm_obj_seg_create(objmem);
            if (mpi_errno)
                MPIR_ERR_POP(mpi_errno);
        } else {
            mpi_errno = MPIR_Handle_shm_obj_seg_attach(objmem);
            if (mpi_errno)
                MPIR_ERR_POP(mpi_errno);
        }


    } else {
        /* inter-process lock is not available, so it's not possible to use
         * shared pool */
        objmem->shm_base = NULL;
        objmem->shm_lock = NULL;
        objmem->shm_avail = 0;
    }

    OPA_compiler_barrier();
    objmem->shm_initialized = 1;
    // printf("rank %d - next str %s, avail %ld, header size %ld, objsize %x\n", rank,
    //        (char *) objmem->shm_base[0], *objmem->shm_avail,
    //        sizeof(MPL_proc_mutex_t) + sizeof(uint64_t), objmem->size);
    // fflush(stdout);

  fn_exit:
    MPIR_FUNC_VERBOSE_EXIT(MPIR_STATE_MPIR_HANDLEMEM_SHM_OBJ_INIT);
    return mpi_errno;
  fn_fail:
    goto fn_exit;

}


#undef FUNCNAME
#define FUNCNAME MPIR_Handlemem_shm_obj_pool_init
#undef FCNAME
#define FCNAME MPL_QUOTE(FUNCNAME)
int MPIR_Handle_shm_obj_seg_create(MPIR_Object_alloc_t * objmem)
{
    MPL_shm_hnd_t hnd;
    MPIR_Errflag_t errflag = MPIR_ERR_NONE;
    // int rank = MPIR_Process.comm_world->node_comm->rank;
    int i;
    int mpl_err = 0;
    int mpi_errno = MPI_SUCCESS;
    MPIR_Handle_common *hptr = 0;
    char *ptr;
    void *shm_indirect;
    char *serialized_hnd = NULL;
    char *val = NULL;
    MPIR_FUNC_VERBOSE_STATE_DECL(MPIR_STATE_MPIR_HANDLEMEM_SHM_OBJ_POOL_INIT);
    MPIR_FUNC_VERBOSE_ENTER(MPIR_STATE_MPIR_HANDLEMEM_SHM_OBJ_POOL_INIT);

    mpl_err = MPL_shm_hnd_init(&hnd);
    MPIR_ERR_CHKANDJUMP(mpl_err, mpi_errno, MPI_ERR_OTHER, "**alloc_shar_mem");

    /* Create a seg and broadcast to others */
    /* TODO: remember to delete this attached shared memory after program completes */
    mpl_err =
        MPL_shm_seg_create_and_attach(hnd,
                                      sizeof(MPL_proc_mutex_t) +
                                      sizeof(uint64_t) +
                                      MPLI_SHM_GHND_SZ +
                                      HANDLE_NUM_INDICES * objmem->size,
                                      (void **) &objmem->shm_base[objmem->shm_size], 0);
    MPIR_ERR_CHKANDJUMP(mpl_err, mpi_errno, MPI_ERR_OTHER, "**alloc_shar_mem");

    objmem->shm_lock[objmem->shm_size] = (MPL_proc_mutex_t *) objmem->shm_base[objmem->shm_size];
    MPL_proc_mutex_create(objmem->shm_lock[objmem->shm_size], &mpl_err);
    /* TODO: remember to change error message **alloc_shar_mem */
    MPIR_ERR_CHKANDJUMP(mpl_err, mpi_errno, MPI_ERR_OTHER, "**alloc_shar_mem");

    ptr = shm_indirect =
        (void *) ((size_t) objmem->shm_base[objmem->shm_size] + sizeof(MPL_proc_mutex_t) +
                  sizeof(uint64_t) + MPLI_SHM_GHND_SZ);
    for (i = 0; i < HANDLE_NUM_INDICES; i++) {
        hptr = (MPIR_Handle_common *) (void *) ptr;
        ptr = ptr + objmem->size;
        /* offset to the shared memory base addr */
        hptr->next = (void *) ((objmem->shm_size << HANDLE_INDIRECT_SHIFT) | (i + 1));
        hptr->handle =
            (HANDLE_KIND_SHARED << HANDLE_KIND_SHARED_SHIFT) | (objmem->kind <<
                                                                HANDLE_MPI_KIND_SHIFT)
            | (HANDLE_KIND_INDIRECT << HANDLE_KIND_SHIFT) | (objmem->shm_size <<
                                                             HANDLE_INDIRECT_SHIFT) | i;
    }

    if (hptr)
        hptr->next = (void *) -1;

    objmem->shm_avail[objmem->shm_size] =
        (uint64_t *) ((uint64_t) objmem->shm_base[objmem->shm_size] + sizeof(MPL_proc_mutex_t));
    *objmem->shm_avail[objmem->shm_size] = (uint64_t) (objmem->shm_size << HANDLE_INDIRECT_SHIFT);

    val = (char *) objmem->shm_base[objmem->shm_size] + sizeof(MPL_proc_mutex_t) + sizeof(uint64_t);
    /* TODO: do we need to consider portability of strcpy? */
    strcpy(val, "MPIR_SHM_OBJ_NULL");
    // printf("rank %d - copy str %s\n", rank, val);
    // fflush(stdout);
    mpl_err = MPL_shm_hnd_get_serialized_by_ref(hnd, &serialized_hnd);
    MPIR_ERR_CHKANDJUMP(mpl_err, mpi_errno, MPI_ERR_OTHER, "**alloc_shar_mem");
    if (objmem->shm_size)
        strcpy((char *) objmem->shm_base[objmem->shm_size - 1], serialized_hnd);
    else {
        mpi_errno = MPIR_Bcast(serialized_hnd, MPLI_SHM_GHND_SZ, MPI_CHAR, 0,
                               MPIR_Process.comm_world->node_comm, &errflag);
        if (mpi_errno)
            MPIR_ERR_POP(mpi_errno);
    }


    /* Calibrate shm_base to the beginning of string "MPIR_SHM_OBJ_NULL" */
    objmem->shm_base[objmem->shm_size] =
        (void *) ((uint64_t) objmem->shm_base[objmem->shm_size] + sizeof(MPL_proc_mutex_t) +
                  sizeof(uint64_t));
    /* Local shm_size update */
    ++objmem->shm_size;

  fn_exit:
    MPIR_FUNC_VERBOSE_EXIT(MPIR_STATE_MPIR_HANDLEMEM_SHM_OBJ_POOL_INIT);
    return mpi_errno;
  fn_fail:
    goto fn_exit;

}


#undef FUNCNAME
#define FUNCNAME MPIR_Handlemem_shm_obj_pool_init
#undef FCNAME
#define FCNAME MPL_QUOTE(FUNCNAME)
int MPIR_Handle_shm_obj_seg_attach(MPIR_Object_alloc_t * objmem)
{
    MPL_shm_hnd_t hnd;
    MPIR_Errflag_t errflag = MPIR_ERR_NONE;
    // int rank = MPIR_Process.comm_world->node_comm->rank;
    int i;
    int mpl_err = 0;
    int mpi_errno = MPI_SUCCESS;
    MPIR_Handle_common *hptr = 0;
    char *ptr;
    void *shm_indirect;
    char *serialized_hnd = NULL;
    char *val = NULL;
    MPIR_FUNC_VERBOSE_STATE_DECL(MPIR_STATE_MPIR_HANDLEMEM_SHM_OBJ_POOL_INIT);
    MPIR_FUNC_VERBOSE_ENTER(MPIR_STATE_MPIR_HANDLEMEM_SHM_OBJ_POOL_INIT);
    MPIR_CHKLMEM_DECL(1);

    mpl_err = MPL_shm_hnd_init(&hnd);
    MPIR_ERR_CHKANDJUMP(mpl_err, mpi_errno, MPI_ERR_OTHER, "**alloc_shar_mem");

    MPIR_CHKLMEM_MALLOC(val, char *, MPLI_SHM_GHND_SZ, mpi_errno, "val", MPL_MEM_SHM);
    if (objmem->shm_size) {
        strcpy(val, (char *) objmem->shm_base[objmem->shm_size - 1]);
    } else {
        mpi_errno =
            MPIR_Bcast(val, MPLI_SHM_GHND_SZ, MPI_CHAR, 0, MPIR_Process.comm_world->node_comm,
                       &errflag);
        if (mpi_errno)
            MPIR_ERR_POP(mpi_errno);
    }

    mpl_err = MPL_shm_hnd_deserialize(hnd, val, strlen(val));
    MPIR_ERR_CHKANDJUMP(mpl_err, mpi_errno, MPI_ERR_OTHER, "**alloc_shar_mem");


    mpl_err = MPL_shm_seg_attach(hnd, sizeof(MPL_proc_mutex_t) +
                                 sizeof(uint64_t) +
                                 MPLI_SHM_GHND_SZ +
                                 HANDLE_NUM_INDICES * objmem->size,
                                 (void **) &objmem->shm_base[objmem->shm_size], 0);
    MPIR_ERR_CHKANDJUMP(mpl_err, mpi_errno, MPI_ERR_OTHER, "**attach_shar_mem");
    // printf("rank %d - attach segment %s\n", rank, (char *) ((uint64_t) objmem->shm_base[objmem->shm_size] + sizeof(MPL_proc_mutex_t) + sizeof(uint64_t)));
    // fflush(stdout);
    objmem->shm_lock[objmem->shm_size] = (MPL_proc_mutex_t *) objmem->shm_base[objmem->shm_size];
    objmem->shm_avail[objmem->shm_size] =
        (uint64_t *) ((uint64_t) objmem->shm_base[objmem->shm_size] + sizeof(MPL_proc_mutex_t));


    /* Calibrate shm_base to the beginning of string "MPIR_SHM_OBJ_NULL" */
    objmem->shm_base[objmem->shm_size] =
        (void *) ((uint64_t) objmem->shm_base[objmem->shm_size] + sizeof(MPL_proc_mutex_t) +
                  sizeof(uint64_t));
    /* Local shm_size update */
    ++objmem->shm_size;

  fn_exit:
    MPIR_CHKLMEM_FREEALL();
    MPIR_FUNC_VERBOSE_EXIT(MPIR_STATE_MPIR_HANDLEMEM_SHM_OBJ_POOL_INIT);
    return mpi_errno;
  fn_fail:
    goto fn_exit;

}


#undef FUNCNAME
#define FUNCNAME MPIR_Handlemem_shm_obj_pool_init
#undef FCNAME
#define FCNAME MPL_QUOTE(FUNCNAME)
int MPIR_Handlemem_shm_obj_pool_init()
{

    int mpi_errno = MPI_SUCCESS;
    MPIR_FUNC_VERBOSE_STATE_DECL(MPIR_STATE_MPIR_HANDLEMEM_SHM_OBJ_POOL_INIT);
    MPIR_FUNC_VERBOSE_ENTER(MPIR_STATE_MPIR_HANDLEMEM_SHM_OBJ_POOL_INIT);

    /* List all MPIR_Object_alloc_t type objects that need shared pool */
    MPIR_Handlemem_shm_obj_init(&MPIR_Request_mem);

  fn_exit:
    MPIR_FUNC_VERBOSE_EXIT(MPIR_STATE_MPIR_HANDLEMEM_SHM_OBJ_POOL_INIT);
    return mpi_errno;
  fn_fail:
    goto fn_exit;
}

#undef FUNCNAME
#define FUNCNAME MPIR_Handle_get_ptr_shared
#undef FCNAME
#define FCNAME MPL_QUOTE(FUNCNAME)
void *MPIR_Handle_get_ptr_shared(uint64_t handle, MPIR_Object_alloc_t * objmem)
{
    int block = HANDLE_SHARED_BLOCK(handle);
    int index = HANDLE_SHARED_INDEX(handle);
    return (void *) ((char *) objmem->shm_base[block] + MPLI_SHM_GHND_SZ + index * objmem->size);
}

/* style: allow:printf:5 sig:0 */
#ifdef MPICH_DEBUG_HANDLEALLOC
/* The following is a handler that may be added to finalize to test whether
   handles remain allocated, including those from the direct blocks.

   When adding memory checking, this routine should be invoked as

   MPIR_Add_finalize(MPIR_check_handles_on_finalize, objmem, 1);

   as part of the object intialization.

   The algorithm follows the following approach:

   The memory allocation approach manages a list of available objects.
   These objects are allocated from several places:
      "direct" - this is a block of preallocated space
      "indirect" - this is a block of blocks that are allocated as necessary.
                   E.g., objmem_ptr->indirect[0..objmem_ptr->indirect_size-1]
                   are pointers (or null) to a block of memory.  This block is
                   then divided into objects that are added to the avail list.

   To provide information on the handles that are still in use, we must
   "repatriate" all of the free objects, at least virtually.  To give
   the best information, for each free item, we determine to which block
   it belongs.
*/
int MPIR_check_handles_on_finalize(void *objmem_ptr)
{
    MPIR_Object_alloc_t *objmem = (MPIR_Object_alloc_t *) objmem_ptr;
    int i;
    MPIR_Handle_common *ptr;
    int leaked_handles = FALSE;
    int directSize = objmem->direct_size;
    char *direct = (char *) objmem->direct;
    char *directEnd = (char *) direct + directSize * objmem->size - 1;
    int nDirect = 0;
    int *nIndirect = 0;

    /* Return immediately if this object has not allocated any space */
    if (!objmem->initialized) {
        return 0;
    }

    if (objmem->indirect_size > 0) {
        nIndirect = (int *) MPL_calloc(objmem->indirect_size, sizeof(int), MPL_MEM_OBJECT);
    }
    /* Count the number of items in the avail list.  These include
     * all objects, whether direct or indirect allocation */
    ptr = objmem->avail;
    while (ptr) {
        /* printf("Looking at %p\n", ptr); */
        /* Find where this object belongs */
        if ((char *) ptr >= direct && (char *) ptr < directEnd) {
            nDirect++;
        } else {
            void **indirect = (void **) objmem->indirect;
            for (i = 0; i < objmem->indirect_size; i++) {
                char *start = indirect[i];
                char *end = start + HANDLE_NUM_INDICES * objmem->size;
                if ((char *) ptr >= start && (char *) ptr < end) {
                    nIndirect[i]++;
                    break;
                }
            }
            if (i == objmem->indirect_size) {
                /* Error - could not find the owning memory */
                /* Temp */
                printf("Could not place object at %p in handle memory for type %s\n", ptr,
                       MPIR_Handle_get_kind_str(objmem->kind));
                printf("direct block is [%p,%p]\n", direct, directEnd);
                if (objmem->indirect_size) {
                    printf("indirect block is [%p,%p]\n", indirect[0],
                           (char *) indirect[0] + HANDLE_NUM_INDICES * objmem->size);
                }
            }
        }
        ptr = ptr->next;
    }

    if (0) {
        /* Produce a report */
        printf("Object handles:\n\ttype  \t%s\n\tsize  \t%d\n\tdirect size\t%d\n\
\tindirect size\t%d\n", MPIR_Handle_get_kind_str(objmem->kind), objmem->size, objmem->direct_size, objmem->indirect_size);
    }
    if (nDirect != directSize) {
        leaked_handles = TRUE;
        printf("In direct memory block for handle type %s, %d handles are still allocated\n",
               MPIR_Handle_get_kind_str(objmem->kind), directSize - nDirect);
    }
    for (i = 0; i < objmem->indirect_size; i++) {
        if (nIndirect[i] != HANDLE_NUM_INDICES) {
            leaked_handles = TRUE;
            printf
                ("In indirect memory block %d for handle type %s, %d handles are still allocated\n",
                 i, MPIR_Handle_get_kind_str(objmem->kind), HANDLE_NUM_INDICES - nIndirect[i]);
        }
    }

    if (nIndirect) {
        MPL_free(nIndirect);
    }

    if (leaked_handles && MPIR_CVAR_ABORT_ON_LEAKED_HANDLES) {
        /* comm_world has been (or should have been) destroyed by this point,
         * pass comm=NULL */
        MPID_Abort(NULL, MPI_ERR_OTHER, 1, "ERROR: leaked handles detected, aborting");
        MPIR_Assert(0);
    }

    return 0;
}
#endif

/* returns the name of the handle kind for debugging/logging purposes */
const char *MPIR_Handle_get_kind_str(int kind)
{
#define mpiu_name_case_(name_) case MPIR_##name_: return (#name_)
    switch (kind) {
            mpiu_name_case_(COMM);
            mpiu_name_case_(GROUP);
            mpiu_name_case_(DATATYPE);
            mpiu_name_case_(FILE);
            mpiu_name_case_(ERRHANDLER);
            mpiu_name_case_(OP);
            mpiu_name_case_(INFO);
            mpiu_name_case_(WIN);
            mpiu_name_case_(KEYVAL);
            mpiu_name_case_(ATTR);
            mpiu_name_case_(REQUEST);
            mpiu_name_case_(PROCGROUP);
            mpiu_name_case_(VCONN);
            mpiu_name_case_(WORKQ_ELEM);
            mpiu_name_case_(GREQ_CLASS);
        default:
            return "unknown";
    }
#undef mpiu_name_case_
}
