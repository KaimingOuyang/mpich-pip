#ifndef XPMEM_RECV_H_INCLUDED
#define XPMEM_RECV_H_INCLUDED

#include <xpmem.h>
#include "xpmem_progress.h"
#include "../posix/posix_send.h"
#include "../posix/posix_recv.h"

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

	int mpi_errno = MPI_SUCCESS;
	int errLine;
	ackHeader header;

// #ifdef XPMEM_PROFILE
// 	int events[2] = {PAPI_L3_TCM, PAPI_TLB_DM};
// 	long long values[2];
// 	int myrank = comm->rank;
// 	char buffer[8];
// 	char file[64] = "xpmem-recv_";
// 	double synctime = 0.0, systime = 0.0, copytime = 0.0;
// 	synctime -= MPI_Wtime();
// #endif
	/* Get data handler in order to attach memory page from source process */
#ifndef XPMEM_SYNC
	mpi_errno = MPIDI_POSIX_mpi_recv(&header.dataSz, 4, MPI_LONG_LONG, rank, tag, comm, context_offset, status, request);
	if (mpi_errno != MPI_SUCCESS) {
		errLine = __LINE__;
		goto fn_fail;
	}

	if (*request != NULL) {
		// printf("Recv wait\n");
		// fflush(stdout);
		mpi_errno = MPID_XPMEM_Wait(*request);
		if (mpi_errno != MPI_SUCCESS) {
			errLine = __LINE__;
			goto fn_fail;
		}
	}
#endif
// #ifdef XPMEM_PROFILE
// 	synctime += MPI_Wtime();
// 	sprintf(buffer, "%d_", myrank);
// 	strcat(file, buffer);
// 	sprintf(buffer, "%lld", header.dataSz);
// 	strcat(file, buffer);
// 	strcat(file, ".log");
// 	FILE *fp = fopen(file, "a");
// #endif
	// printf("Receiver dataSz= %lld\n", header.dataSz);
	// fflush(stdout);
	if (header.dataSz == 0) {
		MPIR_STATUS_SET_COUNT(*status, 0);
		status->MPI_SOURCE = rank;
		status->MPI_TAG = tag;
		return mpi_errno;
	}
	// printf("Recv header.dtHandler %llX\n", header.dtHandler);
	// fflush(stdout);
	void *dataBuffer, *realBuffer;
	xpmem_apid_t apid;
	// double time = MPI_Wtime();
// #ifdef XPMEM_PROFILE
// 	systime -= MPI_Wtime();
// #endif
#ifndef XPMEM_SYSCALL
	// printf("define XPMEM_SYSCALL\n");
	mpi_errno = xpmemAttachMem(&header, &dataBuffer, &realBuffer, &apid);
#endif
// #ifdef XPMEM_PROFILE
// 	systime += MPI_Wtime();
// #endif

	// time = MPI_Wtime() - time;
	// printf("xpmemAttachMem time= %.6lf\n", time);
	// fflush(stdout);
	if (mpi_errno != MPI_SUCCESS) {
		errLine = __LINE__;
		goto fn_fail;
	}

	/* Copy data by dataSz bytes */
	// time = MPI_Wtime();
// #ifdef XPMEM_PROFILE
// 	copytime -= MPI_Wtime();
// #endif
#ifndef XPMEM_MEMCOPY
	// printf("define XPMEM_MEMCOPY\n");
	MPIR_Memcpy(buf, dataBuffer, header.dataSz);
#endif
// #ifdef XPMEM_PROFILE
// 	copytime += MPI_Wtime();
// #endif
	// time = MPI_Wtime() - time;
	// printf("copy time= %.6lf\n", time);
	// fflush(stdout);
	// printf("Receiver enter infinite loop\n");
	// fflush(stdout);
	// sleep(10);
	if (status != MPI_STATUS_IGNORE) {
		MPIR_STATUS_SET_COUNT(*status, header.dataSz);
		status->MPI_SOURCE = rank;
		status->MPI_TAG = tag;
	}
#ifndef XPMEM_SYSCALL
	/* Release resources */
	mpi_errno = xpmemDetachMem(realBuffer, &apid);
	if (mpi_errno != MPI_SUCCESS) {
		errLine = __LINE__;
		goto fn_fail;
	}
#endif

#ifndef XPMEM_SYNC
	int ack;
// #ifdef XPMEM_PROFILE
// 	synctime -= MPI_Wtime();
// #endif
	mpi_errno = MPIDI_POSIX_mpi_send(&ack, 1, MPI_INT, rank, 0, comm, context_offset, NULL, request);
// #ifdef XPMEM_PROFILE
// 	synctime += MPI_Wtime();
// 	fprintf(fp, "%.8lf %.8lf %.8lf %lld %lld\n", synctime, systime, copytime, values[0], values[1]);
// 	fclose(fp);
// #endif
	if (mpi_errno != MPI_SUCCESS) {
		errLine = __LINE__;
		goto fn_fail;
	}
#endif
	goto fn_exit;

fn_fail:
	printf("[%s-%d] Error with mpi_errno (%d)\n", __FUNCTION__, errLine, mpi_errno);
fn_exit:
	*request = NULL;
	return mpi_errno;
}

#endif