#ifndef XPMEM_REDUCE_INCLUDED
#define XPMEM_REDUCE_INCLUDED

#include "shm.h"
#include "xpmem_progress.h"

static inline void printArray(int rank, int* array, int count)
{
	printf("rank %d array: ", rank);
	for (int i = 0; i < count; ++i) {
		printf("%d ", array[i]);
	}
	printf("\n");
	fflush(stdout);
}

#undef FUNCNAME
#define FUNCNAME MPIDI_XPMEM_mpi_reduce
#undef FCNAME
#define FCNAME MPL_QUOTE(FUNCNAME)
static inline int MPIDI_XPMEM_mpi_reduce(const void *sendbuf, void *recvbuf, int count,
        MPI_Datatype datatype, MPI_Op op, int root,
        MPIR_Comm * comm, MPIR_Errflag_t * errflag,
        const void *ch4_algo_parameters_container_in __attribute__((unused)))
{
	/*
		Current version does not consider the inter sockets communications.
		optimized implementation should add this feature which needs to
		modify the comm structure and split function.
	*/

	int mpi_errno = MPI_SUCCESS;
	int myrank = comm->rank;

	int psize = comm->local_size;
	int errLine;

	ackHeader destheader;
	void *dest_dataBuf = NULL;
	void *dest_realBuf = NULL;

	ackHeader srcHeader;
	void *srcdataBuf = NULL;
	void *srcrealBuf = NULL;

	size_t dataSz, typesize;
	if (count == 0)
		goto fn_exit;

	ackHeader *header_array = (ackHeader*) MPL_malloc(sizeof(ackHeader) * psize, MPL_MEM_OTHER);
	// if (myrank == root) {
	// 	// printf("rank %d sendbuf: ", myrank);
	// 	if (sendbuf != MPI_IN_PLACE) {
	// 		printArray(myrank, sendbuf, count);
	// 	} else {
	// 		printArray(myrank, recvbuf, count);
	// 	}
	// 	// printf("\n");
	// 	// fflush(stdout);
	// } else {
	// 	printArray(myrank, sendbuf, count);
	// }
	// COLL_SHMEM_MODULE = POSIX_MODULE;
	// MPIDI_POSIX_mpi_barrier(comm, errflag, NULL);
	// COLL_SHMEM_MODULE = XPMEM_MODULE;

	// if (myrank != root) {
	// 	printArray(myrank, sendbuf, count);
	// }
	typesize =  MPIR_Datatype_get_basic_size(datatype);
	dataSz = typesize * count;
	/* Attach destination buffer located on root process */
	if (myrank == root) {
		// 	*(__s64*)(destheader + 1) = 1L;
		// 	*(__s64*)(destheader + 1) = 0L;
		// destdataBuf = recvbuf;
		// printf("Rank: %d, enter MPIDI_XPMEM_mpi_reduce with recvbuf[9] %d\n", myrank, ((int*)recvbuf)[9]);
		// fflush(stdout);
		// printf("dataSz=%d, typesize=%d\n", dataSz, typesize);
		if (sendbuf != MPI_IN_PLACE) {
			memcpy(recvbuf, sendbuf, dataSz);
		}

		mpi_errno = xpmemExposeMem(recvbuf, dataSz, &destheader, xpmem_local_rank);
		if (mpi_errno != MPI_SUCCESS) {
			errLine = __LINE__;
			goto fn_fail;
		}
		// printf("Rank: %d, root %d expose dest buffer with handler %llX\n", myrank, root, destheader.dtHandler);
		// fflush(stdout);
	} else {

		mpi_errno = xpmemExposeMem(sendbuf, dataSz, &destheader, xpmem_local_rank);
		if (mpi_errno != MPI_SUCCESS) {
			errLine = __LINE__;
			goto fn_fail;
		}
	}

	/* Exchange mem segment info */
	mpi_errno = MPIR_Allgather(&destheader, sizeof(ackHeader), MPI_BYTE, header_array, sizeof(ackHeader), MPI_BYTE, comm, errflag);
	if (mpi_errno != MPI_SUCCESS) {
		errLine = __LINE__;
		goto fn_fail;
	}
	// COLL_SHMEM_MODULE = POSIX_MODULE;
	// mpi_errno = MPIDI_POSIX_mpi_bcast(&destheader, 4, MPI_LONG_LONG, root, comm, errflag, NULL);
	// if (mpi_errno != MPI_SUCCESS) {
	// 	errLine = __LINE__;
	// 	goto fn_fail;
	// }
	// COLL_SHMEM_MODULE = XPMEM_MODULE;
	// printf("Rank: %d completes bcast\n", myrank);
	// fflush(stdout);



	// if (myrank != root) {
	// 	mpi_errno = xpmemAttachMem(&destheader, &destdataBuf, &destrealBuf, &destapid);
	// 	if (mpi_errno != MPI_SUCCESS) {
	// 		errLine = __LINE__;
	// 		goto fn_fail;
	// 	}
	// 	// printArray(myrank, destdataBuf, count);
	// 	// printf("Rank: %d, attach dest buffer with handler %llX\n", myrank, destheader.dtHandler);
	// 	// fflush(stdout);
	// }
	// printf("Rank: %d, attach dest buffer with handler %llX, and %d\n", myrank, destheader.dtHandler, ((int*)destdataBuf)[0]);
	// fflush(stdout);

	
	mpi_errno = xpmemAttachMem(&header_array[root], &dest_dataBuf, &dest_realBuf);
	if (mpi_errno != MPI_SUCCESS) {
		errLine = __LINE__;
		goto fn_fail;
	}

	size_t sindex = (size_t) (myrank * count / psize);
	size_t len = (size_t) ((myrank + 1) * count / psize) - sindex;
	void *insrc;
	void *outdest = (void*) ((char*) dest_dataBuf + sindex * typesize);

	/* Attach src data from each other within a for loop */
	for (int i = 0; i < psize; ++i) {
		if (i == root) {
			continue;
		}

		mpi_errno = xpmemAttachMem(&header_array[i], &srcdataBuf, &srcrealBuf);
		if (mpi_errno != MPI_SUCCESS) {
			errLine = __LINE__;
			goto fn_fail;
		}

		insrc = (void*) ((char*) srcdataBuf + sindex * typesize);
		MPIR_Reduce_local(insrc, outdest, len, datatype, op);

		// if (myrank != i) {
		mpi_errno = xpmemDetachMem(srcrealBuf);
		if (mpi_errno != MPI_SUCCESS) {
			errLine = __LINE__;
			goto fn_fail;
		}

	}

	mpi_errno = xpmemDetachMem(dest_realBuf);
	if (mpi_errno != MPI_SUCCESS) {
		errLine = __LINE__;
		goto fn_fail;
	}

	MPL_free(header_array);
	// COLL_SHMEM_MODULE = POSIX_MODULE;
	MPIDI_POSIX_mpi_barrier(comm, errflag, NULL);
	// COLL_SHMEM_MODULE = XPMEM_MODULE;
	// if (myrank == root) {
	// 	mpi_errno = xpmemRemoveMem(&destheader);
	// 	if (mpi_errno != MPI_SUCCESS) {
	// 		errLine = __LINE__;
	// 		goto fn_fail;
	// 	}
	// }

	// free(destheader);

	goto fn_exit;
fn_fail :
	printf("[%s-%d] Error with mpi_errno (%d)\n", __FUNCTION__, errLine, mpi_errno);
fn_exit :
	return mpi_errno;
}

#endif