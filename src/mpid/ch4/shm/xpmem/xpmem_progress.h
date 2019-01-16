#ifndef XPMEM_PROGRESS_INCLUDED
#define XPMEM_PROGRESS_INCLUDED

#include <xpmem.h>
#ifdef XPMEM_PROFILE_MISS
#include <papi.h>
#endif

typedef struct ackHeader {
	__s64 dataSz;
	xpmem_segid_t dtHandler;
	__s64 pageSz;
	__s64 offset;
	__s64 attoffset;
} ackHeader;

// #define XPMEM_PROFILE
#define PAGE_SIZE (1L << 12)
#define PAGE_MASK (PAGE_SIZE - 1L)
#define PAGE_ALIGN_ADDR_LOW(addr) (addr & (~PAGE_MASK))
#define PAGE_ALIGN_ADDR_HIGH(addr) ((addr) & PAGE_MASK ? (((addr) & ~PAGE_MASK) + PAGE_SIZE) : (addr))
int MPIR_Wait_impl(MPIR_Request * request_ptr, MPI_Status * status);

MPL_STATIC_INLINE_PREFIX int MPID_XPMEM_Wait(MPIR_Request *request_ptr) {
	int mpi_errno = MPI_SUCCESS;
	int errLine;
	if (request_ptr == NULL) {
		goto fn_exit;
	}

	mpi_errno = MPIR_Wait_impl(request_ptr, MPI_STATUS_IGNORE);
	if (mpi_errno != MPI_SUCCESS) {
		errLine = __LINE__;
		goto fn_fail;
	}

	mpi_errno = request_ptr->status.MPI_ERROR;
	MPIR_Request_free(request_ptr);
	if (mpi_errno != MPI_SUCCESS) {
		errLine = __LINE__;
		goto fn_fail;
	}
	request_ptr = NULL;
	goto fn_exit;
fn_fail:
	printf("[%s-%d] Error with mpi_errno (%d)\n", __FUNCTION__, errLine, mpi_errno);
fn_exit:
	return mpi_errno;
}


MPL_STATIC_INLINE_PREFIX int xpmemExposeMem(const void *buf, size_t dataSz, ackHeader *header) {
	extern xpmem_segid_t dtHandler;
	long long lowAddr = PAGE_ALIGN_ADDR_LOW((long long) buf);
	long long highAddr = PAGE_ALIGN_ADDR_HIGH((long long) buf + dataSz);
	size_t newSize = highAddr - lowAddr;
	long long offset = (long long) buf - lowAddr;
	int errLine;
	int mpi_errno = MPI_SUCCESS;
	/* Expose memory and get handler */
	header->dataSz = (__s64) dataSz;
	header->pageSz = (__s64) newSize;
	header->offset = offset;
	header->attoffset = (long long) lowAddr;
	header->dtHandler = dtHandler;
	// printf("lowAddr=%llX, highAddr=%llX, newSize=%lld, offset=%llX, dataSz=%lld\n", lowAddr, highAddr, newSize, offset, dataSz);
	// fflush(stdout);
	goto fn_exit;
fn_fail:
	printf("[%s-%d] Error with mpi_errno (%d)\n", __FUNCTION__, errLine, mpi_errno);
fn_exit:
	return mpi_errno;
}


MPL_STATIC_INLINE_PREFIX int xpmemAttachMem(ackHeader *header, void **dtbuf, void **realbuf, xpmem_apid_t *apid) {
	int mpi_errno = MPI_SUCCESS;
	int errLine;
	/* Attach memory page */
	// void *permitValue = (void*) 0600;
	struct xpmem_addr addr;
	char *realdata;
	// do {
	*apid = xpmem_get(header->dtHandler, XPMEM_RDWR, XPMEM_PERMIT_MODE, NULL);

	if (*apid == -1) {
		mpi_errno = MPI_ERR_OTHER;
		errLine = __LINE__;
		goto fn_fail;
	}
	// printf("Get apid: %lld\n", *apid);
	// fflush(stdout);
	addr.apid = *apid;
	addr.offset = header->attoffset;

	// int times = 0;
	// do {
	realdata = (char*) xpmem_attach(addr, header->pageSz, NULL);
	// times++;
	// } while ((long long)realdata == -1L && times < 100000);
	// if ((long long)realdata == -1L) {
	// 	mpi_errno = xpmem_release(*apid);
	// 	if (mpi_errno == -1) {
	// 		errLine = __LINE__;
	// 		goto fn_fail;
	// 	}
	// }
	// } while ((long long)realdata == -1L);
	if ((long long)realdata == -1L) {
		mpi_errno = MPI_ERR_OTHER;
		errLine = __LINE__;
		goto fn_fail;
	}

	*dtbuf = realdata + header->offset;
	*realbuf = realdata;
	goto fn_exit;
fn_fail:
	printf("[%s-%d] Error with mpi_errno (%d)\n", __FUNCTION__, errLine, mpi_errno);
fn_exit:
	return mpi_errno;
}


MPL_STATIC_INLINE_PREFIX int xpmemDetachMem(void *realbuf, xpmem_apid_t *apid) {
	int errLine;
	int mpi_errno = MPI_SUCCESS;
	mpi_errno = xpmem_detach(realbuf);
	if (mpi_errno == -1) {
		errLine = __LINE__;
		goto fn_fail;
	}

	mpi_errno = xpmem_release(*apid);
	if (mpi_errno == -1) {
		errLine = __LINE__;
		goto fn_fail;
	}
	goto fn_exit;
fn_fail:
	printf("[%s-%d] Error with mpi_errno (%d)\n", __FUNCTION__, errLine, mpi_errno);
fn_exit:
	return mpi_errno;
}


MPL_STATIC_INLINE_PREFIX int xpmemRemoveMem(ackHeader *header) {
	int errLine;
	int mpi_errno = MPI_SUCCESS;
#ifndef XPMEM_SYSCALL
	mpi_errno = xpmem_remove(header->dtHandler);
	if (mpi_errno != MPI_SUCCESS) {
		errLine = __LINE__;
		goto fn_fail;
	}
#endif
	goto fn_exit;
fn_fail:
	printf("[%s-%d] Error with mpi_errno (%d)\n", __FUNCTION__, errLine, mpi_errno);
fn_exit:
	return mpi_errno;
}


#ifdef XPMEM_PROFILE_MISS
MPL_STATIC_INLINE_PREFIX int papiStart(int *events, char *prefix, int myrank, int dataSz, FILE **fp, int *eventset) {
	char buffer[8];
	char file[64];
	int errLine, mpi_errno = MPI_SUCCESS;

	strcpy(file, prefix);
	sprintf(buffer, "%d_", myrank);
	strcat(file, buffer);
	sprintf(buffer, "%ld", dataSz);
	strcat(file, buffer);
	strcat(file, ".log");
	*fp = fopen(file, "a");
	if (events != NULL) {
		if (PAPI_start_counters(events, 2) != PAPI_OK) {
			mpi_errno = MPI_ERR_OTHER;
			errLine = __LINE__;
			goto fn_fail;
		}
	} else {
		if (PAPI_start(*eventset) != PAPI_OK) {
			printf("Error PAPI_start\n");
			return -1;
		}
	}

	goto fn_exit;
fn_fail:
	printf("[%s-%d] Error with mpi_errno (%d)\n", __FUNCTION__, errLine, mpi_errno);
fn_exit:
	return mpi_errno;
}
#endif

#endif


