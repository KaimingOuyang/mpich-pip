#ifndef XPMEM_REDUCE_INCLUDED
#define XPMEM_REDUCE_INCLUDED

#include "mpl.h"
#include "xpmem_progress.h"
extern int xpmem_local_rank;


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
#ifdef XPMEM_REUSE
	// static ackHeader *header_reuse = NULL;
	static uint64_t *src_addr = NULL;
	static ackHeader *prev_header = NULL;
	if (src_addr == NULL) {
		// header_reuse = (ackHeader*) MPL_malloc(sizeof(ackHeader) * psize, MPL_MEM_OTHER);
		src_addr = MPL_malloc(sizeof(uint64_t) * psize, MPL_MEM_OTHER);
		prev_header = MPL_malloc(sizeof(ackHeader) * psize, MPL_MEM_OTHER);
		memset(src_addr, 0, sizeof(uint64_t) * psize);
		memset(prev_header, 0, sizeof(ackHeader) * psize);
		// printf("psize %d\n", psize);
	}
#endif


	typesize =  MPIR_Datatype_get_basic_size(datatype);
	dataSz = typesize * count;
	/* Attach destination buffer located on root process */
	if (myrank == root) {

#ifndef NO_XPMEM_REDUCE_LOCAL
		if (sendbuf != MPI_IN_PLACE) {
			memcpy(recvbuf, sendbuf, dataSz);
		}
#endif
		if (psize == 1)
			goto fn_exit;

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
#ifdef XPMEM_REUSE


	if (prev_header[root].data_size == header_array[root].data_size && prev_header[root].exp_offset == header_array[root].exp_offset && prev_header[root].data_offset == header_array[root].data_offset) {

		dest_dataBuf = (void*) src_addr[root];
		// if (comm->rank == 0) {
		// printf("Rank %d - DEST Reuse exp_offset %X, data_offset %X, data_size %lld, attached addr %X\n", comm->rank, header_array[root].exp_offset, header_array[root].data_offset, header_array[root].data_size, dest_dataBuf);
		// fflush(stdout);
		// }
	} else {

#ifndef NO_XPMEM_SYSCALL
		mpi_errno = xpmemAttachMem(&header_array[root], &dest_dataBuf, &dest_realBuf);
		if (mpi_errno != MPI_SUCCESS) {
			errLine = __LINE__;
			goto fn_fail;
		}
		// if (comm->rank == 0) {

		// }
		src_addr[root] = (uint64_t) dest_dataBuf;
		prev_header[root] = header_array[root];
		// printf("Rank %d - DEST First attach exp_offset %X, data_offset %X, data_size %lld, attached addr %X\n", comm->rank, prev_header[root].exp_offset, prev_header[root].data_offset, prev_header[root].data_size, src_addr[root]);
		// fflush(stdout);
#endif
	}

#else // Root XPMEM_REUSE

#ifndef NO_XPMEM_SYSCALL
	mpi_errno = xpmemAttachMem(&header_array[root], &dest_dataBuf, &dest_realBuf);
	if (mpi_errno != MPI_SUCCESS) {
		errLine = __LINE__;
		goto fn_fail;
	}
#endif

#endif // Root XPMEM_REUSE
	size_t sindex = (size_t) (myrank * count / psize);
	size_t len = (size_t) ((myrank + 1) * count / psize) - sindex;
	void *insrc;
	void *outdest = (void*) ((char*) dest_dataBuf + sindex * typesize);

	/* Attach src data from each other within a for loop */
	for (int i = 0; i < psize; ++i) {
		if (i == root) {
			continue;
		}

#ifdef XPMEM_REUSE
		// if (comm->rank == 0) {
		// 	printf("Ready to attach src mem %X\n", header_array[i].exp_offset);
		// 	fflush(stdout);
		// }

		if (prev_header[i].data_size == header_array[i].data_size && prev_header[i].exp_offset == header_array[i].exp_offset && prev_header[i].data_offset == header_array[i].data_offset) {
			srcdataBuf = (void*) src_addr[i];
			// if (comm->rank == 0) {
			// printf("Rank %d - SRC Reuse exp_offset %X, data_offset %X, data_size %lld, attached addr %X\n", comm->rank, header_array[i].exp_offset, header_array[i].data_offset, header_array[i].data_size, srcdataBuf);
			// fflush(stdout);
			// }
		} else {

#ifndef NO_XPMEM_SYSCALL
			mpi_errno = xpmemAttachMem(&header_array[i], &srcdataBuf, &srcrealBuf);
			if (mpi_errno != MPI_SUCCESS) {
				errLine = __LINE__;
				goto fn_fail;
			}
			src_addr[i] = (uint64_t) srcdataBuf;
			prev_header[i] = header_array[i];
#endif
			// if (comm->rank == 0) {
			// printf("Rank %d - SRC First attach exp_offset %X, data_offset %X, data_size %lld, attached addr %X\n", comm->rank, prev_header[i].exp_offset, prev_header[i].data_offset, prev_header[i].data_size, src_addr[i]);
			// fflush(stdout);
			// }
		}

#else // Root XPMEM_REUSE

#ifndef NO_XPMEM_SYSCALL
		mpi_errno = xpmemAttachMem(&header_array[i], &srcdataBuf, &srcrealBuf);
		if (mpi_errno != MPI_SUCCESS) {
			errLine = __LINE__;
			goto fn_fail;
		}
#endif

#endif
		insrc = (void*) ((char*) srcdataBuf + sindex * typesize);

#ifndef NO_XPMEM_REDUCE_LOCAL

		// printf("Rank %d - Before reduce local\n", comm->rank);
		// fflush(stdout);
		MPIR_Reduce_local(insrc, outdest, len, datatype, op);
		// printf("Rank %d - Complete reduce local\n", comm->rank);
		// fflush(stdout);
#endif

#ifndef XPMEM_REUSE
#ifndef NO_XPMEM_SYSCALL
		// printf("How dare you have detach?\n");
		// fflush(stdout);
		// if (myrank != i) {
		mpi_errno = xpmemDetachMem(srcrealBuf);
		if (mpi_errno != MPI_SUCCESS) {
			errLine = __LINE__;
			goto fn_fail;
		}
#endif
#endif // XPMEM_REUSE
	}

#ifndef XPMEM_REUSE
#ifndef NO_XPMEM_SYSCALL
	// printf("How dare you have detach?\n");
	// fflush(stdout);
	mpi_errno = xpmemDetachMem(dest_realBuf);
	if (mpi_errno != MPI_SUCCESS) {
		errLine = __LINE__;
		goto fn_fail;
	}
#endif
#endif // XPMEM_REUSE

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


#undef FCNAME
#define FCNAME MPL_QUOTE(MPIDI_PIP_mpi_tree_based_reduce)
static inline int MPIDI_XPMEM_mpi_tree_based_reduce(const void *sendbuf, void *recvbuf, int count,
        MPI_Datatype datatype, MPI_Op op, int root,
        MPIR_Comm * comm, MPIR_Errflag_t * errflag,
        const void *ch4_algo_parameters_container_in __attribute__((unused)))
{
	/* Current tree base algorithm only deal with power of 2 sockets */
	int mpi_errno = MPI_SUCCESS;
	int myrank = comm->rank;
	int psize = comm->local_size;
	int errLine;
	void *dest = NULL;
	void *src = NULL;
	int step = 1;
	int new_rank = myrank;
	MPIR_Request *request = NULL;
	int size = MPIR_Datatype_get_basic_size(datatype) * count;

	void* local_buffer;
	int ack;

	if ((myrank & 1) == 0) {
		if (myrank != root) {

			local_buffer = MPL_malloc(size, MPL_MEM_OTHER);
#ifndef NO_XPMEM_REDUCE_LOCAL
			MPIR_Memcpy(local_buffer, sendbuf, size);
#endif
		} else {
			/* root sendbuf should be MPI_IN_PLACE */
			local_buffer = recvbuf;
		}
	} else {
		local_buffer = sendbuf;
	}

	void* data_buf;
	void* real_buf;
	ackHeader header;
	while (1) {
		if (new_rank & 1) {
			src = local_buffer;
			mpi_errno = xpmemExposeMem(src, size, &header, xpmem_local_rank);
			if (unlikely(mpi_errno != MPI_SUCCESS))
				goto fn_fail;

			mpi_errno = MPIDI_POSIX_mpi_send(&header, sizeof(ackHeader), MPI_BYTE, myrank - step, 0, comm, MPIR_CONTEXT_INTRA_COLL, NULL, &request);
			if (unlikely(mpi_errno != MPI_SUCCESS))
				goto fn_fail;
			mpi_errno = MPID_XPMEM_Wait(request);
			if (unlikely(mpi_errno != MPI_SUCCESS))
				goto fn_fail;

			// printf("inter socket rank %d waits for ack (local_rank %d)\n", myrank, xpmem_local_rank);
			// fflush(stdout);
			mpi_errno = MPIDI_POSIX_mpi_recv(&ack, 1, MPI_INT, myrank - step, 0, comm, MPIR_CONTEXT_INTRA_COLL, MPI_STATUS_IGNORE, &request);
			if (unlikely(mpi_errno != MPI_SUCCESS))
				goto fn_fail;
			mpi_errno = MPID_XPMEM_Wait(request);
			if (unlikely(mpi_errno != MPI_SUCCESS))
				goto fn_fail;
			// printf("inter socket rank %d receives ack and break out (local_rank %d)\n", myrank, xpmem_local_rank);
			// fflush(stdout);
			break;
		} else {
			static uint64_t src_addr = 0;
			static ackHeader prev_header = {.data_size = 0, .exp_offset = 0};
			mpi_errno = MPIDI_POSIX_mpi_recv(&header, sizeof(ackHeader), MPI_BYTE, myrank + step, 0, comm, MPIR_CONTEXT_INTRA_COLL, MPI_STATUS_IGNORE, &request);
			if (unlikely(mpi_errno != MPI_SUCCESS))
				goto fn_fail;
			mpi_errno = MPID_XPMEM_Wait(request);
			if (unlikely(mpi_errno != MPI_SUCCESS))
				goto fn_fail;
			// printf("inter socket rank %d receives header %p (local_rank %d)\n", myrank, &header, xpmem_local_rank);
			// fflush(stdout);
#ifdef XPMEM_REUSE
			// if (comm->rank == 0) {
			// 	printf("Ready to attach src mem %X\n", header_array[i].exp_offset);
			// 	fflush(stdout);
			// }

			if (prev_header.data_size == header.data_size && prev_header.exp_offset == header.exp_offset && prev_header.data_offset == header.data_offset) {
				data_buf = (void*) src_addr;
			} else {
#ifndef NO_XPMEM_SYSCALL
				mpi_errno = xpmemAttachMem(&header, &data_buf, &real_buf);
				if (mpi_errno != MPI_SUCCESS) {
					errLine = __LINE__;
					goto fn_fail;
				}
				src_addr = (uint64_t) data_buf;
				prev_header = header;
#endif
			}

#else // Root XPMEM_REUSE

#ifndef NO_XPMEM_SYSCALL
			mpi_errno = xpmemAttachMem(&header, &data_buf, &real_buf);
			if (mpi_errno != MPI_SUCCESS) {
				errLine = __LINE__;
				goto fn_fail;
			}
#endif

#endif


#ifndef NO_XPMEM_REDUCE_LOCAL
			MPIR_Reduce_local(data_buf, local_buffer, count, datatype, op);
#endif
			// printf("inter socket rank %d reduce (local_rank %d)\n", myrank, xpmem_local_rank);
			// fflush(stdout);
#ifndef XPMEM_REUSE
#ifndef NO_XPMEM_SYSCALL
			mpi_errno = xpmemDetachMem(real_buf);
			if (mpi_errno != MPI_SUCCESS) {
				errLine = __LINE__;
				goto fn_fail;
			}
#endif
#endif
			// printf("inter socket rank %d send back ack (local_rank %d)\n", myrank, xpmem_local_rank);
			// fflush(stdout);
			mpi_errno = MPIDI_POSIX_mpi_send(&ack, 1, MPI_INT, myrank + step, 0, comm, MPIR_CONTEXT_INTRA_COLL, NULL, &request);
			if (unlikely(mpi_errno != MPI_SUCCESS))
				goto fn_fail;
			mpi_errno = MPID_XPMEM_Wait(request);
			if (unlikely(mpi_errno != MPI_SUCCESS))
				goto fn_fail;
			// printf("inter socket rank %d finish (local_rank %d)\n", myrank, xpmem_local_rank);
			// fflush(stdout);
			new_rank = new_rank >> 1;
			step = step << 1;
			if (step >= psize)
				break;
		}
	}

	if ((myrank & 1) == 0 && myrank != root) {
		MPL_free(local_buffer);
	}


fn_exit:
	return mpi_errno;
fn_fail:
	printf("[%s-%d] Error with mpi_errno (%d)\n", __FUNCTION__, errLine, mpi_errno);
	goto fn_exit;

}

#endif