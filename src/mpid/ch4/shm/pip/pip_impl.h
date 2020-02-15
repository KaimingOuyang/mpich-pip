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

#ifndef PIP_IMPL_H_INCLUDED
#define PIP_IMPL_H_INCLUDED

#include "pip_pre.h"

MPL_STATIC_INLINE_PREFIX int MPIR_PIP_Type_dup(MPIR_Datatype * old_dtp, MPI_Datatype * newtype)
{
    int mpi_errno = MPI_SUCCESS;
    MPIR_Datatype *new_dtp;
    MPIR_FUNC_TERSE_STATE_DECL(MPID_STATE_MPI_PIP_TYPE_DUP);

    MPIR_ERRTEST_INITIALIZED_ORDIE();

    MPIR_FUNC_TERSE_ENTER(MPID_STATE_MPI_PIP_TYPE_DUP);
    MPID_THREAD_CS_ENTER(GLOBAL, MPIR_THREAD_GLOBAL_ALLFUNC_MUTEX);

    /* allocate new datatype object and handle */
    new_dtp = (MPIR_Datatype *) MPIR_Handle_obj_alloc(&MPIR_Datatype_mem);
    if (!new_dtp) {
        /* --BEGIN ERROR HANDLING-- */
        mpi_errno = MPIR_Err_create_code(MPI_SUCCESS, MPIR_ERR_RECOVERABLE,
                                         "MPIR_Type_dup", __LINE__, MPI_ERR_OTHER, "**nomem", 0);
        goto fn_fail;
        /* --END ERROR HANDLING-- */
    }

    /* fill in datatype */
    MPIR_Object_set_ref(new_dtp, 1);
    /* new_dtp->handle is filled in by MPIR_Handle_obj_alloc() */
    new_dtp->is_contig = old_dtp->is_contig;
    new_dtp->size = old_dtp->size;
    new_dtp->extent = old_dtp->extent;
    new_dtp->ub = old_dtp->ub;
    new_dtp->lb = old_dtp->lb;
    new_dtp->true_ub = old_dtp->true_ub;
    new_dtp->true_lb = old_dtp->true_lb;
    new_dtp->alignsize = old_dtp->alignsize;
    new_dtp->has_sticky_ub = old_dtp->has_sticky_ub;
    new_dtp->has_sticky_lb = old_dtp->has_sticky_lb;
    new_dtp->is_committed = old_dtp->is_committed;

    new_dtp->attributes = NULL; /* Attributes are copied in the
                                 * top-level MPI_Type_dup routine */
    new_dtp->name[0] = 0;       /* The Object name is not copied on
                                 * a dup */
    new_dtp->n_builtin_elements = old_dtp->n_builtin_elements;
    new_dtp->builtin_element_size = old_dtp->builtin_element_size;
    new_dtp->basic_type = old_dtp->basic_type;

    new_dtp->max_contig_blocks = old_dtp->max_contig_blocks;

    new_dtp->typerep = NULL;
    *newtype = new_dtp->handle;

    new_dtp->contents = NULL;

    if (old_dtp->is_committed) {
        MPIR_Assert(old_dtp->typerep != NULL);
        MPIR_Typerep_dup(old_dtp->typerep, &new_dtp->typerep);
        MPID_Type_commit_hook(new_dtp);
    } else {
        mpi_errno = MPI_ERR_OTHER;
        goto fn_fail;
    }

  fn_exit:
    MPIR_FUNC_TERSE_EXIT(MPID_STATE_MPI_PIP_TYPE_DUP);
    MPID_THREAD_CS_EXIT(GLOBAL, MPIR_THREAD_GLOBAL_ALLFUNC_MUTEX);
    return mpi_errno;

  fn_fail:
    /* --BEGIN ERROR HANDLING-- */
    *newtype = MPI_DATATYPE_NULL;
    mpi_errno = MPIR_Err_return_comm(NULL, __func__, mpi_errno);
    goto fn_exit;
    /* --END ERROR HANDLING-- */
}

#endif
