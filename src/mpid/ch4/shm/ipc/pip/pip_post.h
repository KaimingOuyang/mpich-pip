/*
 * Copyright (C) by Argonne National Laboratory
 *     See COPYRIGHT in top-level directory
 */
#ifndef PIP_POST_H_INCLUDED
#define PIP_POST_H_INCLUDED

#include "ch4_impl.h"
#include "ipc_types.h"

/*
=== BEGIN_MPI_T_CVAR_INFO_BLOCK ===

cvars:
    - name        : MPIR_CVAR_CH4_PIP_ENABLE
      category    : CH4
      type        : int
      default     : 1
      class       : none
      verbosity   : MPI_T_VERBOSITY_USER_BASIC
      scope       : MPI_T_SCOPE_ALL_EQ
      description : >-
        To manually disable PIP set to 0. The environment variable is valid only when the PIP
        submodule is enabled.

    - name        : MPIR_CVAR_CH4_IPC_PIP_P2P_THRESHOLD
      category    : CH4
      type        : int
      default     : 4096
      class       : none
      verbosity   : MPI_T_VERBOSITY_USER_BASIC
      scope       : MPI_T_SCOPE_ALL_EQ
      description : >-
        If a send message size is greater than or equal to MPIR_CVAR_CH4_IPC_PIP_P2P_THRESHOLD (in
        bytes), then enable PIP-based single copy protocol for intranode communication. The
        environment variable is valid only when the PIP submodule is enabled.

=== END_MPI_T_CVAR_INFO_BLOCK ===
*/

MPL_STATIC_INLINE_PREFIX int MPIDI_PIP_get_ipc_attr(const void *vaddr, uintptr_t data_sz,
                                                    MPIDI_IPCI_ipc_attr_t * ipc_attr)
{
    MPIR_FUNC_VERBOSE_STATE_DECL(MPID_STATE_MPIDI_PIP_GET_IPC_ATTR);
    MPIR_FUNC_VERBOSE_ENTER(MPID_STATE_MPIDI_PIP_GET_IPC_ATTR);

    memset(&ipc_attr->ipc_handle, 0, sizeof(MPIDI_IPCI_ipc_handle_t));

#ifdef MPIDI_CH4_SHM_ENABLE_PIP
    ipc_attr->ipc_type = MPIDI_IPCI_TYPE__PIP;
    ipc_attr->ipc_handle.pip.src_offset = (uint64_t) vaddr;
    ipc_attr->ipc_handle.pip.data_sz = data_sz;
    ipc_attr->ipc_handle.pip.src_lrank = MPIR_Process.local_rank;
    if (MPIR_CVAR_CH4_PIP_ENABLE)
        ipc_attr->threshold.send_lmt_sz = MPIR_CVAR_CH4_IPC_PIP_P2P_THRESHOLD;
    else
        ipc_attr->threshold.send_lmt_sz = MPIR_AINT_MAX;
#else
    ipc_attr->ipc_type = MPIDI_IPCI_TYPE__NONE;
    ipc_attr->threshold.send_lmt_sz = MPIR_AINT_MAX;
#endif

    MPIR_FUNC_VERBOSE_EXIT(MPID_STATE_MPIDI_PIP_GET_IPC_ATTR);
    return MPI_SUCCESS;
}

int MPIDI_PIP_mpi_init_hook(int rank, int size);
int MPIDI_PIP_mpi_finalize_hook(void);
int MPIDI_PIP_ipc_handle_map(MPIDI_PIP_ipc_handle_t mem_handle, void **vaddr);

#endif /* PIP_POST_H_INCLUDED */