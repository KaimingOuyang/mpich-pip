#ifndef XPMEM_SEND_H_INCLUDED
#define XPMEM_SEND_H_INCLUDED

#include "mpir_datatype.h"
#include "xpmem_progress.h"
#include <xpmem.h>
#include "../posix/posix_send.h"
#include "../posix/posix_recv.h"

/* ---------------------------------------------------- */
/* MPIDI_XPMEM_do_send                                  */
/* ---------------------------------------------------- */

#undef FCNAME
#define FCNAME DECL_FUNC(MPIDI_XPMEM_SEND)
MPL_STATIC_INLINE_PREFIX int MPIDI_XPMEM_mpi_send(const void *buf, MPI_Aint count,
        MPI_Datatype datatype, int rank, int tag,
        MPIR_Comm * comm, int context_offset,
        MPIDI_av_entry_t * addr, MPIR_Request ** request)
{

	int mpi_errno = MPI_SUCCESS;
	size_t dataSz;
	int errLine;
	ackHeader my_header;
	ackHeader recv_header;
	// printf("xpmem send\n");

	if (count == 0) {
		my_header.dataSz = 0;
		mpi_errno = MPIDI_POSIX_mpi_send(&my_header.dataSz, 5, MPI_LONG_LONG, rank, tag, comm, context_offset, NULL, request);
		if (mpi_errno != MPI_SUCCESS) {
			errLine = __LINE__;
			goto fn_fail;
		}
		/* Dummy receive */
		mpi_errno = MPIDI_POSIX_mpi_recv(&recv_header.dataSz, 5, MPI_LONG_LONG, rank, tag, comm, context_offset, MPI_STATUS_IGNORE, request);
		if (mpi_errno != MPI_SUCCESS) {
			errLine = __LINE__;
			goto fn_fail;
		}

		if (*request != NULL) {
			mpi_errno = MPID_XPMEM_Wait(*request);
			if (mpi_errno != MPI_SUCCESS) {
				errLine = __LINE__;
				goto fn_fail;
			}
		}
		return mpi_errno;
	}

	dataSz = MPIR_Datatype_get_basic_size(datatype) * count;
	/* Expose memory and get handler */
	mpi_errno = xpmemExposeMem(buf, dataSz, &my_header);
	if (mpi_errno != MPI_SUCCESS) {
		errLine = __LINE__;
		goto fn_fail;
	}


#ifdef XPMEM_PROFILE_MISS
	int myrank = comm->rank;
	char buffer[8];
	char file[64] = "XPMEM-send_";

	sprintf(buffer, "%d_", myrank);
	strcat(file, buffer);
	sprintf(buffer, "%d", dataSz);
	strcat(file, buffer);
	strcat(file, ".log");
	FILE *fp = fopen(file, "a");
	fprintf(fp, "0 0\n");
	fclose(fp);
#endif


#ifndef XPMEM_SYNC
	mpi_errno = MPIDI_POSIX_mpi_send(&my_header.dataSz, 5, MPI_LONG_LONG, rank, tag, comm, context_offset, NULL, request);
	if (mpi_errno != MPI_SUCCESS) {
		errLine = __LINE__;
		goto fn_fail;
	}
	/* From receiver, get receive header */
	mpi_errno = MPIDI_POSIX_mpi_recv(&recv_header.dataSz, 5, MPI_LONG_LONG, rank, tag, comm, context_offset, MPI_STATUS_IGNORE, request);
	if (mpi_errno != MPI_SUCCESS) {
		errLine = __LINE__;
		goto fn_fail;
	}

	if (*request != NULL) {
		mpi_errno = MPID_XPMEM_Wait(*request);
		if (mpi_errno != MPI_SUCCESS) {
			errLine = __LINE__;
			goto fn_fail;
		}
	}
#endif

	void *dataBuffer, *realBuffer;
	xpmem_apid_t apid;

#ifdef XPMEM_WO_SYSCALL
	static ackHeader recheader = {.dataSz = -1, .dtHandler = -1, .pageSz = -1, .offset = -1};
	static void *recdatabuf = NULL;
	static void *recrealbuf = NULL;
	static xpmem_apid_t recapid = -1;
	if (recheader.dtHandler == recv_header.dtHandler && recheader.dataSz == recv_header.dataSz) {
		// printf("Rank: %d, recv the same handler size= %lld, handler= %lld\n", comm->rank, header.dataSz, header.dtHandler);
		// fflush(stdout);
		dataBuffer = recdatabuf;
		realBuffer = recrealbuf;
		apid = recapid;
	} else {
		mpi_errno = xpmemAttachMem(&recv_header, &dataBuffer, &realBuffer, &apid);
		if (mpi_errno != MPI_SUCCESS) {
			errLine = __LINE__;
			goto fn_fail;
		}
		recheader = recv_header;
		recdatabuf = dataBuffer;
		recrealbuf = realBuffer;
		recapid = apid;
	}
#else
#ifndef XPMEM_SYSCALL
	mpi_errno = xpmemAttachMem(&recv_header, &dataBuffer, &realBuffer, &apid);
	if (mpi_errno != MPI_SUCCESS) {
		errLine = __LINE__;
		goto fn_fail;
	}
#endif
#endif


#ifndef XPMEM_MEMCOPY
	long long offset = my_header.dataSz / 2L;
	long long ssize = my_header.dataSz - offset;
	// printf("Send copy size= %lld\n", ssize);
	// fflush(stdout);
	memcpy((char*)dataBuffer + offset, (char*)buf + offset, ssize);
#endif


#ifndef XPMEM_SYNC
	/* Wait */
	int ack;
	mpi_errno = MPIDI_POSIX_mpi_send(&ack, 1, MPI_INT, rank, 0, comm, context_offset, NULL, request);
	if (mpi_errno != MPI_SUCCESS) {
		errLine = __LINE__;
		goto fn_fail;
	}
	mpi_errno = MPIDI_POSIX_mpi_recv(&ack, 1, MPI_INT, rank, 0, comm, context_offset, MPI_STATUS_IGNORE, request);
	if (mpi_errno != MPI_SUCCESS) {
		errLine = __LINE__;
		goto fn_fail;
	}

	if (*request != NULL) {
		mpi_errno = MPID_XPMEM_Wait(*request);
		if (mpi_errno != MPI_SUCCESS) {
			errLine = __LINE__;
			goto fn_fail;
		}
	}
#endif

#ifndef XPMEM_WO_SYSCALL
#ifndef XPMEM_SYSCALL
	/* Release resources */
	// printf("Send Detach mem\n");
	// fflush(stdout);
	mpi_errno = xpmemDetachMem(realBuffer, &apid);
	if (mpi_errno != MPI_SUCCESS) {
		errLine = __LINE__;
		goto fn_fail;
	}
#endif
#endif

	goto fn_exit;
fn_fail:
	printf("[%s-%d] Error with mpi_errno (%d)\n", __FUNCTION__, errLine, mpi_errno);
	fflush(stdout);
fn_exit:
	*request = NULL;
	return mpi_errno;
}

#endif