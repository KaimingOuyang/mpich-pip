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
	ackHeader sender_header;
	ackHeader my_header;
// #ifdef XPMEM_PROFILE
// 	int events[2] = {PAPI_L3_TCM, PAPI_TLB_DM};
// 	long long values[2];
// 	int myrank = comm->rank;
// 	char buffer[8];
// 	char file[64] = "xpmem-recv_";
// 	double synctime = 0.0, systime = 0.0, copytime = 0.0;
// 	synctime -= MPI_Wtime();
// #endif
	long long dataSz = MPIR_Datatype_get_basic_size(datatype) * count;
	mpi_errno = xpmemExposeMem(buf, dataSz, &my_header);
	if (mpi_errno != MPI_SUCCESS) {
		errLine = __LINE__;
		goto fn_fail;
	}
	/* Get data handler in order to attach memory page from source process */
#ifndef XPMEM_SYNC
	mpi_errno = MPIDI_POSIX_mpi_send(&my_header.dataSz, 5, MPI_LONG_LONG, rank, tag, comm, context_offset, NULL, request);
	if (mpi_errno != MPI_SUCCESS) {
		errLine = __LINE__;
		goto fn_fail;
	}

	mpi_errno = MPIDI_POSIX_mpi_recv(&sender_header.dataSz, 5, MPI_LONG_LONG, rank, tag, comm, context_offset, MPI_STATUS_IGNORE, request);
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

	if (sender_header.dataSz == 0) {
		MPIR_STATUS_SET_COUNT(*status, 0);
		status->MPI_SOURCE = rank;
		status->MPI_TAG = tag;
		return mpi_errno;
	}
#endif
	// printf("Receiver dataSz= %lld, pageSz= %lld, offset= %lld, attoffset= %lld\n", header.dataSz, header.pageSz, header.offset, header.attoffset);
	// fflush(stdout);
	
	// printf("Recv header.dtHandler %llX\n", header.dtHandler);
	// fflush(stdout);
	void *dataBuffer, *realBuffer;
	xpmem_apid_t apid;
	// double time = MPI_Wtime();
// #ifdef XPMEM_PROFILE
// 	systime -= MPI_Wtime();
// #endif
#ifdef XPMEM_WO_SYSCALL
	static ackHeader recheader = {.dataSz = -1, .dtHandler = -1, .pageSz = -1, .offset = -1};
	static void *recdatabuf = NULL;
	static void *recrealbuf = NULL;
	static xpmem_apid_t recapid = -1;
	if (recheader.dtHandler == sender_header.dtHandler && recheader.dataSz == sender_header.dataSz) {
		// printf("Rank: %d, recv the same handler size= %lld, handler= %lld\n", comm->rank, header.dataSz, header.dtHandler);
		// fflush(stdout);
		dataBuffer = recdatabuf;
		realBuffer = recrealbuf;
		apid = recapid;
	} else {
		mpi_errno = xpmemAttachMem(&sender_header, &dataBuffer, &realBuffer, &apid);
		if (mpi_errno != MPI_SUCCESS) {
			errLine = __LINE__;
			goto fn_fail;
		}
		recheader = sender_header;
		recdatabuf = dataBuffer;
		recrealbuf = realBuffer;
		recapid = apid;
	}
#else
#ifndef XPMEM_SYSCALL
	// printf("define XPMEM_SYSCALL\n");
	mpi_errno = xpmemAttachMem(&sender_header, &dataBuffer, &realBuffer, &apid);
	if (mpi_errno != MPI_SUCCESS) {
		errLine = __LINE__;
		goto fn_fail;
	}
#endif
#endif


#ifdef XPMEM_PROFILE_MISS
	long long sumv[2] = {0, 0};
#ifdef XPMEM_COMBINE_MISS
	int EventSet = PAPI_NULL;
	int *events = NULL;
	long long values[4] = {0, 0, 0, 0};
	int retval;
	if ((retval = PAPI_create_eventset(&EventSet)) != PAPI_OK) {
		fprintf(stderr, "PAPI_create_eventset error %d\n", retval);
		exit(1);
	}
	retval = PAPI_add_named_event( EventSet, "PAGE_WALKER_LOADS:DTLB_L1" );
	if ( retval != PAPI_OK ) {
		printf("Error : %s\n", PAPI_strerror(retval));
		return -1;
	}

	retval = PAPI_add_named_event( EventSet, "PAGE_WALKER_LOADS:DTLB_L2" );
	if ( retval != PAPI_OK ) {
		printf("Error : %s\n", PAPI_strerror(retval));
		return -1;
	}

	retval = PAPI_add_named_event( EventSet, "PAGE_WALKER_LOADS:DTLB_MEMORY" );
	if ( retval != PAPI_OK ) {
		printf("Error : %s\n", PAPI_strerror(retval));
		return -1;
	}

	retval = PAPI_add_named_event( EventSet, "OFFCORE_RESPONSE_0:L3_MISS" );
	if ( retval != PAPI_OK ) {
		printf("Error : %s\n", PAPI_strerror(retval));
		return -1;
	}
#else
	const int vnum = 2;
	long long values[2] = {0, 0};
#ifdef TLB_MISS
	int events[2] = {PAPI_PRF_DM, PAPI_TLB_DM};
#else
	int events[2] = {PAPI_PRF_DM, PAPI_L3_TCM};
#endif
#endif

	FILE *fp;
	mpi_errno = papiStart(events, "XPMEM-recv_", comm->rank, sender_header.dataSz, &fp, &EventSet);
	if (mpi_errno != MPI_SUCCESS) {
		errLine = __LINE__;
		goto fn_fail;
	}
#endif


#ifndef XPMEM_MEMCOPY
	long long ssize = sender_header.dataSz / 2L;
	// printf("Recv copy size= %lld\n", ssize);
	// fflush(stdout);
	memcpy(buf, dataBuffer, sender_header.dataSz);
#endif


#ifdef XPMEM_PROFILE_MISS
#ifdef XPMEM_COMBINE_MISS
	if ((retval = PAPI_stop(EventSet, values)) != PAPI_OK) {
		printf("Error : %s\n", PAPI_strerror(retval));
		errLine = __LINE__;
		goto fn_fail;
	}
	sumv[0] = values[0] + values[1] + values[2];
	sumv[1] = values[3];
	PAPI_cleanup_eventset(EventSet);
	PAPI_destroy_eventset(&EventSet);
#else
	if ((retval = PAPI_stop_counters(values, vnum)) != PAPI_OK) {
		printf("Error : %s\n", PAPI_strerror(retval));
		mpi_errno = MPI_ERR_OTHER;
		errLine = __LINE__;
		goto fn_fail;
	}
	sumv[0] = values[0];
	sumv[1] = values[1];
#endif
	fprintf(fp, "%lld %lld\n", sumv[0], sumv[1]);
	fclose(fp);
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
		MPIR_STATUS_SET_COUNT(*status, sender_header.dataSz);
		status->MPI_SOURCE = rank;
		status->MPI_TAG = tag;
	}

#ifndef XPMEM_WO_SYSCALL
#ifndef XPMEM_SYSCALL
	/* Release resources */
	// printf("Recv Detach mem\n");
	// fflush(stdout);
	mpi_errno = xpmemDetachMem(realBuffer, &apid);
	if (mpi_errno != MPI_SUCCESS) {
		errLine = __LINE__;
		goto fn_fail;
	}
#endif
#endif


#ifndef XPMEM_SYNC
	int ack;
// #ifdef XPMEM_PROFILE
// 	synctime -= MPI_Wtime();
// #endif
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
	goto fn_exit;

fn_fail:
	printf("[%s-%d] Error with mpi_errno (%d)\n", __FUNCTION__, errLine, mpi_errno);
fn_exit:
	*request = NULL;
	return mpi_errno;
}

#endif