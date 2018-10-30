#ifndef XPMEM_PROGRESS_INCLUDED
#define XPMEM_PROGRESS_INCLUDED

#include "./include/xpmem.h"

int MPIR_Wait_impl(MPIR_Request * request_ptr, MPI_Status * status);

MPL_STATIC_INLINE_PREFIX int MPID_XPMEM_Wait(MPIR_Request *request_ptr) {
	int mpi_errno = MPI_SUCCESS;
	if (request_ptr == NULL) {
		goto fn_exit;
	}

	mpi_errno = MPIR_Wait_impl(request_ptr, MPI_STATUS_IGNORE);
	if (mpi_errno != MPI_SUCCESS) {
		goto fn_fail;
	}

	mpi_errno = request_ptr->status.MPI_ERROR;
	MPIR_Request_free(request_ptr);
	if (mpi_errno != MPI_SUCCESS) {
		goto fn_fail;
	}

fn_exit:
fn_fail:
	return mpi_errno;
}

#endif