#ifndef PIP_PROGRESS_INCLUDED
#define PIP_PROGRESS_INCLUDED
#include <papi.h>

#define COOP_COPY_DATA_THRESHOLD 4096

#ifdef PROFILE_MISS
extern long long values[2];
#endif

extern char *COLL_SHMEM_MODULE;
int MPIR_Wait_impl(MPIR_Request * request_ptr, MPI_Status * status);

typedef struct pipHeader {
	long long addr;
	long long dataSz;
} pipHeader;

#undef FCNAME
#define FCNAME MPL_QUOTE(MPID_PIP_Wait)
MPL_STATIC_INLINE_PREFIX int MPID_PIP_Wait(MPIR_Request *request_ptr) {
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

// MPL_STATIC_INLINE_PREFIX int initNativeEventSet() {
// 	int retval;
// 	int EventSet = PAPI_NULL;
// 	// long long *values;
// 	int code;

// 	int r, i;
// 	const PAPI_component_info_t *cmpinfo = NULL;
// 	PAPI_event_info_t evinfo;

// 	retval = PAPI_library_init(PAPI_VER_CURRENT);
// 	if (retval != PAPI_VER_CURRENT) {
// 		printf("%s: PAPI library init error [line %s].\n", __FUNCTION__, __LINE__);
// 		return -1;
// 	}

// 	/* Create EventSet */
// 	retval = PAPI_create_eventset(&EventSet);
// 	if (retval != PAPI_OK) {
// 		printf("%s: PAPI_create_eventset() [line %s].", __FILE__, __LINE__);
// 		return -1;
// 	}

// 	/* Add all events */

// 	code = PAPI_NATIVE_MASK;

// 	// r = PAPI_enum_cmp_event( &code, PAPI_ENUM_FIRST, rapl_cid );
// 	retval = PAPI_event_name_to_code("rapl:::PACKAGE_ENERGY:PACKAGE0", &code);
// 	if (retval != PAPI_OK) {
// 		printf("Error translating %#x\n", code);
// 		return -1;
// 	}
// 	retval = PAPI_add_event(EventSet, code);
// 	if (retval != PAPI_OK) {
// 		printf("Error adding rapl:::PACKAGE_ENERGY:PACKAGE0\n");
// 		return -1;
// 	}
// 	retval = PAPI_event_name_to_code("rapl:::PACKAGE_ENERGY:PACKAGE1", &code);
// 	if (retval != PAPI_OK) {
// 		printf("Error translating %#x\n", code);
// 		return -1;
// 	}
// 	retval = PAPI_add_event(EventSet, code);
// 	if (retval != PAPI_OK) {
// 		printf("Error adding rapl:::PACKAGE_ENERGY:PACKAGE1\n");
// 		return -1;
// 	}

// 	return EventSet;
// }

#endif