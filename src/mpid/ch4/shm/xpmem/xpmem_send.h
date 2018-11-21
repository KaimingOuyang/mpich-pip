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
        MPIDI_av_entry_t * addr, MPIR_Request ** request) {

	int mpi_errno = MPI_SUCCESS;
	size_t dataSz;
	int errLine;
	ackHeader header;
	// printf("xpmem send\n");

	if (count == 0) {
		header.dataSz = 0;
		mpi_errno = MPIDI_POSIX_mpi_send(&header.dataSz, 4, MPI_LONG_LONG, rank, tag, comm, context_offset, NULL, request);
		if (mpi_errno != MPI_SUCCESS) {
			errLine = __LINE__;
			goto fn_fail;
		}
		return mpi_errno;
	}

	dataSz = MPIR_Datatype_get_basic_size(datatype) * count;
	// printf("Sender dataSz= %lld\n", dataSz);
	// fflush(stdout);
	/* Expose memory and get handler */
// #ifdef XPMEM_PROFILE
// 	double synctime = 0.0, systime = 0.0, copytime = 0.0;
// 	int events[2] = {PAPI_L3_TCM, PAPI_TLB_DM};
// 	long long values[2];
// 	int myrank = comm->rank;
// 	char buffer[8];
// 	char file[64] = "xpmem-send_";

// 	sprintf(buffer, "%d_", myrank);
// 	strcat(file, buffer);
// 	sprintf(buffer, "%ld", count);
// 	strcat(file, buffer);
// 	strcat(file, ".log");
// 	FILE *fp = fopen(file, "a");
// 	systime -= MPI_Wtime();
// #endif
#ifdef XPMEM_WO_SYSCALL
	/* Current reuse is simple, need to deal with more complicated cases */
	static long long recaddr = -1;
	static ackHeader recheader = {.dataSz = -1, .dtHandler = -1, .pageSz = -1, .offset = -1};
	if (recaddr == (long long) buf && recheader.dataSz == dataSz) {
		// printf("Rank: %d, I am the same\n", comm->rank);
		// fflush(stdout);
		header = recheader;
	} else {
		mpi_errno = xpmemExposeMem(buf, dataSz, &header);
		if (mpi_errno != MPI_SUCCESS) {
			errLine = __LINE__;
			goto fn_fail;
		}
		recaddr = (long long)buf;
		recheader = header;
	}
#else
	mpi_errno = xpmemExposeMem(buf, dataSz, &header);
	if (mpi_errno != MPI_SUCCESS) {
		errLine = __LINE__;
		goto fn_fail;
	}
#endif


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
	mpi_errno = MPIDI_POSIX_mpi_send(&header.dataSz, 4, MPI_LONG_LONG, rank, tag, comm, context_offset, NULL, request);
	if (mpi_errno != MPI_SUCCESS) {
		errLine = __LINE__;
		goto fn_fail;
	}

// #ifdef XPMEM_PROFILE
// 	synctime += MPI_Wtime();
// #endif

	/* Wait */
	int ack;
	// MPI_Status ackStatus;
// #ifdef XPMEM_PROFILE
// 	synctime -= MPI_Wtime();
// #endif
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
// #ifdef XPMEM_PROFILE
// 	synctime += MPI_Wtime();
// 	fprintf(fp, "%.8lf %.8lf 0.0 0 0\n", synctime, systime);
// 	fclose(fp);
// #endif
// #ifndef XPMEM_WO_SYSCALL
// 	mpi_errno = xpmemRemoveMem(&header);
// 	if (mpi_errno != MPI_SUCCESS) {
// 		errLine = __LINE__;
// 		goto fn_fail;
// 	}
// #endif
	// printf("I finish remove mem\n");
	// fflush(stdout);

	goto fn_exit;
fn_fail:
	printf("[%s-%d] Error with mpi_errno (%d)\n", __FUNCTION__, errLine, mpi_errno);
	fflush(stdout);
fn_exit:
	*request = NULL;
	return mpi_errno;
}

#endif