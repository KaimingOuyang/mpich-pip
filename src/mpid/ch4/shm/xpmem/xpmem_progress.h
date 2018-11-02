#ifndef XPMEM_PROGRESS_INCLUDED
#define XPMEM_PROGRESS_INCLUDED

#include "./include/xpmem.h"

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
	goto fn_exit;
fn_fail:
	printf("[%s-%d] Error with mpi_errno (%d)\n", __FUNCTION__, errLine, mpi_errno);
fn_exit:
	return mpi_errno;
}


MPL_STATIC_INLINE_PREFIX int xpmemExposeMem(const void *buf, size_t dataSz, ackHeader *header) {
	void *permitValue = (void*) 00666;
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
	// printf("lowAddr=%llX, highAddr=%llX, newSize=%lld, offset=%llX, dataSz=%lld\n", lowAddr, highAddr, newSize, offset, dataSz);

	header->dtHandler = xpmem_make((void*) lowAddr, newSize, XPMEM_PERMIT_MODE, permitValue);

	if (header->dtHandler == -1) {
		mpi_errno = MPI_ERR_UNKNOWN;
		errLine = __LINE__;
		goto fn_fail;
	}

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
	void *permitValue = (void*) 00666;
	struct xpmem_addr addr;
	char *realdata;
	do {
		*apid = xpmem_get(header->dtHandler, XPMEM_RDWR, XPMEM_PERMIT_MODE, permitValue);

		if (*apid == -1) {
			mpi_errno = MPI_ERR_OTHER;
			errLine = __LINE__;
			goto fn_fail;
		}
		// printf("Get apid: %lld\n", *apid);

		addr.apid = *apid;
		addr.offset = 0;

		int times = 0;
		// do {
		realdata = (char*) xpmem_attach(addr, header->pageSz, NULL);
		// times++;
		// } while ((long long)realdata == -1L && times < 100000);
		if ((long long)realdata == -1L) {
			mpi_errno = xpmem_release(*apid);
			if (mpi_errno == -1) {
				errLine = __LINE__;
				goto fn_fail;
			}
		}
	} while ((long long)realdata == -1L);
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
	mpi_errno = xpmem_remove(header->dtHandler);
	if (mpi_errno != MPI_SUCCESS) {
		errLine = __LINE__;
		goto fn_fail;
	}
	goto fn_exit;
fn_fail:
	printf("[%s-%d] Error with mpi_errno (%d)\n", __FUNCTION__, errLine, mpi_errno);
fn_exit:
	return mpi_errno;
}

#endif