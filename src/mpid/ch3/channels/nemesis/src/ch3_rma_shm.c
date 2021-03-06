/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil ; -*- */
/*
 *  (C) 2001 by Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#include "mpidi_ch3_impl.h"
#include "mpidrma.h"


#ifdef HAVE_PIP
/* TODO: This optimization should be moved to MPL.*/
static int free_shm_segment(MPIR_Comm * node_comm_ptr, intptr_t shm_segment_len,
                            MPL_shm_hnd_t * shm_segment_handle, char **shm_seg_ptr)
{
    int mpi_errno = MPI_SUCCESS;
    if (node_comm_ptr->rank == 0 && *shm_seg_ptr != NULL) {
        MPL_free(*shm_seg_ptr);
        *shm_seg_ptr = NULL;
    }
    return mpi_errno;
}
#else
static int free_shm_segment(MPIR_Comm * node_comm_ptr ATTRIBUTE((unused)),
                            intptr_t shm_segment_len, MPL_shm_hnd_t * shm_segment_handle,
                            char **shm_seg_ptr)
{
    int mpi_errno = MPI_SUCCESS;
    mpi_errno = MPL_shm_seg_detach(*shm_segment_handle, shm_seg_ptr, shm_segment_len);
    if (mpi_errno)
        return mpi_errno;

    MPL_shm_hnd_finalize(shm_segment_handle);
    return mpi_errno;
}
#endif

#undef FUNCNAME
#define FUNCNAME MPIDI_CH3_Win_shared_query
#undef FCNAME
#define FCNAME MPL_QUOTE(FUNCNAME)
int MPIDI_CH3_SHM_Win_shared_query(MPIR_Win * win_ptr, int target_rank, MPI_Aint * size,
                                   int *disp_unit, void *baseptr)
{
    int comm_size;
    int mpi_errno = MPI_SUCCESS;
    MPIR_FUNC_VERBOSE_STATE_DECL(MPID_STATE_MPIDI_CH3_WIN_SHARED_QUERY);

    MPIR_FUNC_VERBOSE_RMA_ENTER(MPID_STATE_MPIDI_CH3_WIN_SHARED_QUERY);

    if (win_ptr->comm_ptr->node_comm == NULL) {
        mpi_errno = MPIDI_CH3U_Win_shared_query(win_ptr, target_rank, size, disp_unit, baseptr);
        if (mpi_errno != MPI_SUCCESS) {
            MPIR_ERR_POP(mpi_errno);
        }
        goto fn_exit;
    }

    comm_size = win_ptr->comm_ptr->local_size;

    /* Scan the sizes to locate the first process that allocated a nonzero
     * amount of space */
    if (target_rank == MPI_PROC_NULL) {
        int i;

        /* Default, if no processes have size > 0. */
        *size = 0;
        *disp_unit = 0;
        *((void **) baseptr) = NULL;

        for (i = 0; i < comm_size; i++) {
            if (win_ptr->basic_info_table[i].size > 0) {
                int local_i = win_ptr->comm_ptr->intranode_table[i];
                MPIR_Assert(local_i >= 0 && local_i < win_ptr->comm_ptr->node_comm->local_size);
                *size = win_ptr->basic_info_table[i].size;
                *disp_unit = win_ptr->basic_info_table[i].disp_unit;
                *((void **) baseptr) = win_ptr->shm_base_offsets[local_i] + win_ptr->shm_base_addr;
                break;
            }
        }

    }
    else {
        int local_target_rank = win_ptr->comm_ptr->intranode_table[target_rank];
        MPIR_Assert(local_target_rank >= 0 &&
                    local_target_rank < win_ptr->comm_ptr->node_comm->local_size);
        *size = win_ptr->basic_info_table[target_rank].size;
        *disp_unit = win_ptr->basic_info_table[target_rank].disp_unit;
        *((void **) baseptr) = win_ptr->shm_base_offsets[local_target_rank] + win_ptr->shm_base_addr;
    }

  fn_exit:
    MPIR_FUNC_VERBOSE_RMA_EXIT(MPID_STATE_MPIDI_CH3_WIN_SHARED_QUERY);
    return mpi_errno;

  fn_fail:
    goto fn_exit;
}


#undef FUNCNAME
#define FUNCNAME MPIDI_CH3_SHM_Win_free
#undef FCNAME
#define FCNAME MPL_QUOTE(FUNCNAME)
int MPIDI_CH3_SHM_Win_free(MPIR_Win ** win_ptr)
{
    int mpi_errno = MPI_SUCCESS;
    MPIR_FUNC_VERBOSE_STATE_DECL(MPID_STATE_MPIDI_CH3_SHM_WIN_FREE);

    MPIR_FUNC_VERBOSE_RMA_ENTER(MPID_STATE_MPIDI_CH3_SHM_WIN_FREE);

    if ((*win_ptr)->comm_ptr->node_comm == NULL) {
        goto fn_exit;
    }

    /* Free shared memory region */
    if ((*win_ptr)->shm_allocated) {
        /* free shm_base_offsets that's only used for shared memory windows */
        mpi_errno = free_shm_segment((*win_ptr)->comm_ptr->node_comm,
                                     (*win_ptr)->shm_base_offsets_segment_len,
                                     &(*win_ptr)->shm_base_offsets_segment_handle,
                                     (char **) &(*win_ptr)->shm_base_offsets);
        if (mpi_errno)
        MPIR_ERR_POP(mpi_errno);

        /* Only allocate and allocate_shared allocate new shared segments */
        if (((*win_ptr)->create_flavor == MPI_WIN_FLAVOR_SHARED ||
             (*win_ptr)->create_flavor == MPI_WIN_FLAVOR_ALLOCATE) &&
            (*win_ptr)->shm_segment_len > 0) {
            /* detach from shared memory segment */
            mpi_errno = free_shm_segment((*win_ptr)->comm_ptr->node_comm,
                                         (*win_ptr)->shm_segment_len,
                                         &(*win_ptr)->shm_segment_handle,
                                         (char **) &(*win_ptr)->shm_base_addr);
            if (mpi_errno)
                MPIR_ERR_POP(mpi_errno);
        }
    }

    /* Free shared process mutex memory region */
    /* Only allocate and allocate_shared allocate new shared mutex.
     * FIXME: it causes unnecessary synchronization when using the same mutex.  */
    if (((*win_ptr)->create_flavor == MPI_WIN_FLAVOR_SHARED ||
         (*win_ptr)->create_flavor == MPI_WIN_FLAVOR_ALLOCATE) &&
        (*win_ptr)->shm_mutex && (*win_ptr)->shm_segment_len > 0) {
        MPIR_Comm *node_comm_ptr = NULL;

        /* When allocating shared memory region segment, we need comm of processes
         * that are on the same node as this process (node_comm).
         * If node_comm == NULL, this process is the only one on this node, therefore
         * we use comm_self as node comm. */
        node_comm_ptr = (*win_ptr)->comm_ptr->node_comm;
        MPIR_Assert(node_comm_ptr != NULL);

        if (node_comm_ptr->rank == 0) {
            MPIDI_CH3I_SHM_MUTEX_DESTROY(*win_ptr);
        }

        /* detach from shared memory segment */
        mpi_errno = free_shm_segment((*win_ptr)->comm_ptr->node_comm,
                                     sizeof(MPIDI_CH3I_SHM_MUTEX),
                                     &(*win_ptr)->shm_mutex_segment_handle,
                                     (char **) &(*win_ptr)->shm_mutex);
        if (mpi_errno)
            MPIR_ERR_POP(mpi_errno);
    }

    /* Free shared memory region for window info */
    if ((*win_ptr)->info_shm_base_addr != NULL) {
        mpi_errno = free_shm_segment((*win_ptr)->comm_ptr->node_comm,
                                     (*win_ptr)->info_shm_segment_len,
                                     &(*win_ptr)->info_shm_segment_handle,
                                     (char **) &(*win_ptr)->info_shm_base_addr);
        if (mpi_errno)
            MPIR_ERR_POP(mpi_errno);

        (*win_ptr)->basic_info_table = NULL;
    }

    /* Unlink from global SHM window list if it is original shared window */
    if ((*win_ptr)->create_flavor == MPI_WIN_FLAVOR_SHARED ||
        (*win_ptr)->create_flavor == MPI_WIN_FLAVOR_ALLOCATE) {
        MPIDI_CH3I_SHM_Wins_unlink(&shm_wins_list, (*win_ptr));
    }

  fn_exit:
    MPIR_FUNC_VERBOSE_RMA_EXIT(MPID_STATE_MPIDI_CH3_SHM_WIN_FREE);
    return mpi_errno;

  fn_fail:
    goto fn_exit;
}
