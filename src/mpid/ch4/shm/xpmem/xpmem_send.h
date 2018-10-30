#ifndef XPMEM_SEND_H_INCLUDED
#define XPMEM_SEND_H_INCLUDED

#include "./include/xpmem.h"
#include "xpmem_progress.h"
#include "ch4_impl.h"


#define PAGE_SIZE (1L << 12)
#define PAGE_MASK (PAGE_SIZE - 1L)
#define PAGE_ALIGN_ADDR_LOW(addr) (addr & (~PAGE_MASK))
#define PAGE_ALIGN_ADDR_HIGH(addr) ((addr) & PAGE_MASK ? (((addr) & ~PAGE_MASK) + PAGE_SIZE) : (addr))

size_t getPageUpperValue(size_t size) {

}


/* ---------------------------------------------------- */
/* MPIDI_XPMEM_do_send                                  */
/* ---------------------------------------------------- */

#undef FCNAME
#define FCNAME DECL_FUNC(MPIDI_XPMEM_SEND)
MPL_STATIC_INLINE_PREFIX int MPIDI_XPMEM_mpi_send(const void *buf, MPI_Aint count,
        MPI_Datatype datatype, int rank, int tag,
        MPIR_Comm * comm, int context_offset,
        MPIDI_av_entry_t * addr, MPIR_Request ** request) {

	int dt_contig __attribute__ ((__unused__)), mpi_errno = MPI_SUCCESS;
	MPI_Aint dt_true_lb;
	size_t data_sz;
	MPIR_Datatype *dt_ptr;
	void *permitValue = (void*) 00666;


	MPIDI_Datatype_get_info(count, datatype, dt_contig, data_sz, dt_ptr, dt_true_lb);
	long long lowAddr = PAGE_ALIGN_ADDR_LOW((long long) buf);
	long long highAddr = PAGE_ALIGN_ADDR_HIGH((long long) buf + data_sz);
	size_t newSize = highAddr - lowAddr;
	long long offset = (long long) buf - lowAddr;
	// printf("Sender buf: %llX, datasz: %lu, lowAddr: %llX, highAddr: %llX, newSize: %lu, offset: %llX, permit: %o\n", (long long) buf, data_sz, lowAddr, highAddr, newSize, offset, (int) permitValue);
	// fflush(stdout);
	/* Let's first try synchronous communication */

	// char* sdbuffer;
	// /* Now we only test continuous data segment */
	// if (data_sz <= MPIDI_POSIX_EAGER_THRESHOLD) {
	// 	// Maybe I should change malloc function to MPI internal malloc function
	// 	// Static memory does not work here since the following send/recv may overwrite data
	// 	sdbuffer = (char*) malloc(MPIDI_POSIX_EAGER_THRESHOLD);
	// 	MPIR_Memcpy(sdbuffer, buf + dt_true_lb, data_sz);
	// } else
	// 	sdbuffer = buf;


	/* Expose memory and get handler */
	ackHeader header;
	header.dataSz = (__s64) data_sz;
	header.pageSz = (__s64) newSize;
	header.offset = offset;
	header.dtHandler = xpmem_make((void*) lowAddr, newSize, XPMEM_PERMIT_MODE, permitValue);

	/* SYNC */
	// MPIR_Request *syncReq = NULL;
	// printf("Sender: header.dataSz: %lld, header.dtHandler: %llX\n", header.dataSz, header.dtHandler);
	// fflush(stdout);
	if (header.dtHandler == -1) {
		mpi_errno = MPI_ERR_UNKNOWN;
		goto fn_fail;
	}

	mpi_errno = MPIDI_POSIX_mpi_send(&header.dataSz, 4, MPI_LONG_LONG, rank, tag, comm, context_offset, NULL, request);
	if (mpi_errno != MPI_SUCCESS) {
		goto fn_fail;
	}
	// printf("Sender complete sending handler\n");
	// fflush(stdout);
	/* Wait */
	int ack;
	MPI_Status ackStatus;
	// if (data_sz > MPIDI_POSIX_EAGER_THRESHOLD) {
	mpi_errno = MPIDI_POSIX_mpi_recv(&ack, 1, MPI_INT, rank, tag, comm, context_offset, &ackStatus, request);
	if (mpi_errno != MPI_SUCCESS) {
		goto fn_fail;
	}
	// printf("Sender submits recv cmd and wait for response\n");
	// fflush(stdout);
	mpi_errno = MPID_XPMEM_Wait(*request);
	if (mpi_errno != MPI_SUCCESS) {
		goto fn_fail;
	}


	mpi_errno = xpmem_remove(header.dtHandler);
	if (mpi_errno != MPI_SUCCESS) {
		goto fn_fail;
	}
	// printf("Sender remove xpmem exposed page\n");
	// fflush(stdout);

	// }
	goto fn_exit;
fn_fail:
fn_exit:
	*request = NULL;
	return mpi_errno;
}

#endif