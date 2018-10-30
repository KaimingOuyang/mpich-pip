#ifndef XPMEM_RECV_H_INCLUDED
#define XPMEM_RECV_H_INCLUDED

#include "./include/xpmem.h"
#include "xpmem_progress.h"

/* ---------------------------------------------------- */
/* MPIDI_XPMEM_mpi_recv                                             */
/* ---------------------------------------------------- */
// static char ackBuffer[MPIDI_POSIX_EAGER_THRESHOLD];

#undef FCNAME
#define FCNAME MPL_QUOTE(MPIDI_XPMEM_mpi_recv)
MPL_STATIC_INLINE_PREFIX int MPIDI_XPMEM_mpi_recv(void *buf,
        MPI_Aint count,
        MPI_Datatype datatype,
        int rank,
        int tag,
        MPIR_Comm * comm,
        int context_offset, MPI_Status * status,
        MPIR_Request ** request) {

	int mpi_errno = MPI_SUCCESS, dt_contig __attribute__ ((__unused__));
	int errLine;
	size_t data_sz __attribute__ ((__unused__));



	/* Get data handler in order to attach memory page from source process */
	ackHeader header;
	mpi_errno = MPIDI_POSIX_mpi_recv(&header.dataSz, 4, MPI_LONG_LONG, rank, tag, comm, context_offset, status, request);
	if (mpi_errno != MPI_SUCCESS) {
		errLine = __LINE__;
		goto fn_fail;
	}
	mpi_errno = MPID_XPMEM_Wait(*request);
	if (mpi_errno != MPI_SUCCESS) {
		errLine = __LINE__;
		goto fn_fail;
	}
	if (status != MPI_STATUS_IGNORE) {
		MPIR_STATUS_SET_COUNT(*status, header.dataSz);
		status->MPI_SOURCE = rank;
		status->MPI_TAG = tag;
	}
	// printf("Receiver receiving handler: header.dataSz %lld, header pagesize: %lld, header offset: %lld, header.dtHandler %llX\n", header.dataSz, header.pageSz, header.offset, header.dtHandler);
	// fflush(stdout);
	/* Attach memory page */
	void *permitValue = (void*) 00666;
	struct xpmem_addr addr;

	xpmem_apid_t apid = xpmem_get(header.dtHandler, XPMEM_RDWR, XPMEM_PERMIT_MODE, permitValue);

	if (apid == -1) {
		errLine = __LINE__;
		goto fn_fail;
	}
	// printf("Receiver xpmem_get completes, apid: %llX\n", apid);
	// fflush(stdout);

	addr.apid = apid;
	addr.offset = 0;
	char *data = (char*) xpmem_attach(addr, header.pageSz, NULL);
	if (data == -1) {
		errLine = __LINE__;
		goto fn_fail;
	}

	char *cpdata = data + header.offset;
	// printf("Receiver xpmem_attach completes, data: %llX\n", cpdata);
	// fflush(stdout);

	/* Copy data by dataSz bytes */
	MPIR_Memcpy(buf, cpdata, header.dataSz);
	// if (mpi_errno == -1) {
	// 	errLine = __LINE__;
	// 	goto fn_fail;
	// }

	// printf("Receiver complete copying data\n");
	// fflush(stdout);
	/* Release resources */
	mpi_errno = xpmem_detach(data);
	if (mpi_errno == -1) {
		errLine = __LINE__;
		goto fn_fail;
	}

	mpi_errno = xpmem_release(apid);
	if (mpi_errno == -1) {
		errLine = __LINE__;
		goto fn_fail;
	}

	int ack;
	mpi_errno = MPIDI_POSIX_mpi_send(&ack, 1, MPI_INT, rank, tag, comm, context_offset, NULL, request);
	if (mpi_errno != MPI_SUCCESS) {
		goto fn_fail;
	}
	// printf("Receiver send ACK back\n");
	// fflush(stdout);
	goto fn_exit;

fn_fail:
	printf("[%s-%d] Error with mpi_errno (%d)\n", __FUNCTION__, errLine, mpi_errno);
fn_exit:
	*request = NULL;
	return mpi_errno;
}

#endif