#ifndef XPMEM_RECV_H_INCLUDED
#define XPMEM_RECV_H_INCLUDED

#include <xpmem.h>
#include "posix_impl.h"
#include "ch4_impl.h"

/* ---------------------------------------------------- */
/* general queues                                       */
/* ---------------------------------------------------- */
extern MPIDI_POSIX_request_queue_t MPIDI_POSIX_recvq_posted;
extern MPIDI_POSIX_request_queue_t MPIDI_POSIX_recvq_unexpected;

/* ---------------------------------------------------- */
/* MPIDI_POSIX_do_irecv                                             */
/* ---------------------------------------------------- */
#undef FCNAME
#define FCNAME MPL_QUOTE(MPIDI_POSIX_mpi_recv)
MPL_STATIC_INLINE_PREFIX int MPIDI_XPMEM_mpi_recv(void *buf,
        MPI_Aint count,
        MPI_Datatype datatype,
        int rank,
        int tag,
        MPIR_Comm * comm,
        int context_offset, MPI_Status * status,
        MPIR_Request ** request) {

	int mpi_errno = MPI_SUCCESS, dt_contig __attribute__ ((__unused__));
	size_t data_sz __attribute__ ((__unused__));

	/* create a request */
	mpi_errno = MPIDI_POSIX_do_irecv(buf, count, datatype, rank, tag, comm, context_offset, request);

	return mpi_errno;
}

#endif