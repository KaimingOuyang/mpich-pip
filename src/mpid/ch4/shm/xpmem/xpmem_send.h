#ifndef XPMEM_SEND_H_INCLUDED
#define XPMEM_SEND_H_INCLUDED

#include <xpmem.h>
#include "posix_impl.h"
#include "ch4_impl.h"
#include <../mpi/pt2pt/bsendutil.h>

/* ---------------------------------------------------- */
/* MPIDI_XPMEM_do_send                                  */
/* ---------------------------------------------------- */

#undef FCNAME
#define FCNAME DECL_FUNC(MPIDI_XPMEM_SEND)
MPL_STATIC_INLINE_PREFIX int MPIDI_XPMEM_mpi_send(const void *buf,
        int count,
        MPI_Datatype datatype,
        int rank,
        int tag,
        MPIR_Comm * comm, int context_offset,
        MPIR_Request ** request) {

	int dt_contig __attribute__ ((__unused__)), mpi_errno = MPI_SUCCESS;
	MPI_Aint dt_true_lb;
	size_t data_sz;
	MPIR_Datatype *dt_ptr;
	const int permitValue = 0x0440;


	MPIDI_Datatype_get_info(count, datatype, dt_contig, data_sz, dt_ptr, dt_true_lb);
	char* sdbuffer;
	/* Now we only test continuous data segment */
	if (data_sz <= MPIDI_POSIX_EAGER_THRESHOLD) {
		// Maybe I should change malloc function to MPI internal malloc function
		// Static memory does not work here since the following send/recv may overwrite data
		sdbuffer = (char*) malloc(MPIDI_POSIX_EAGER_THRESHOLD);
		MPIR_Memcpy(sdbuffer, buf + dt_true_lb, data_sz);
	} else
		sdbuffer = buf;


	/* Expose memory and get handler */
	xpmem_segid_t dtHandler = xpmem_make(sdbuffer, data_sz, XPMEM_PERMIT_MODE, &permitValue);

	/* SYNC */
	// MPIR_Request *syncReq = NULL;
	mpi_errno = MPIDI_POSIX_mpi_send(&dtHandler, 1, MPI_LONG, rank, tag, comm, context_offset, request);
	if (mpi_errno != MPI_SUCCESS) {
		goto fn_fail;
	}

	/* Wait */
	int ack;
	MPI_Status ackStatus;
	if (data_sz > MPIDI_POSIX_EAGER_THRESHOLD) {
		mpi_errno = MPIDI_POSIX_mpi_recv(&ack, 1, MPI_INT, rank, tag, comm, context_offset, &ackStatus, request);
		if (mpi_errno != MPI_SUCCESS) {
			goto fn_fail;
		}

		MPIR_Request *request_ptr = *request;
		if (request_ptr == NULL) {
			goto fn_exit;
		}

		mpi_errno = MPID_Wait(request_ptr, MPI_STATUS_IGNORE);
		if (mpi_errno != MPI_SUCCESS)
			goto fn_fail;

		mpi_errno = request_ptr->status.MPI_ERROR;
		MPIR_Request_free(request_ptr);
		if (mpi_errno != MPI_SUCCESS)
			goto fn_fail;
	}


fn_exit:
fn_fail:
	*request = NULL;
	return mpi_errno;
}

#endif