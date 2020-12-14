/*
 * Copyright (C) by Argonne National Laboratory
 *     See COPYRIGHT in top-level directory
 */

#include "mpiimpl.h"
#include "pip_pre.h"

int MPIDI_PIP_ipc_handle_map(MPIDI_PIP_ipc_handle_t handle, void **vaddr)
{
    int mpi_errno = MPI_SUCCESS;
    MPIR_FUNC_VERBOSE_STATE_DECL(MPID_STATE_MPIDI_PIP_IPC_HANDLE_MAP);
    MPIR_FUNC_VERBOSE_ENTER(MPID_STATE_MPIDI_PIP_IPC_HANDLE_MAP);

    *vaddr = (void *) handle.src_offset;

    MPIR_FUNC_VERBOSE_EXIT(MPID_STATE_MPIDI_PIP_IPC_HANDLE_MAP);
    return mpi_errno;
}
