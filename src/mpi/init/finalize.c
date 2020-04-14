/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil ; -*- */
/*
 *  (C) 2001 by Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */
/* style: allow:fprintf:1 sig:0 */

#include "mpiimpl.h"
#include "mpi_init.h"

/* -- Begin Profiling Symbol Block for routine MPI_Finalize */
#if defined(HAVE_PRAGMA_WEAK)
#pragma weak MPI_Finalize = PMPI_Finalize
#elif defined(HAVE_PRAGMA_HP_SEC_DEF)
#pragma _HP_SECONDARY_DEF PMPI_Finalize  MPI_Finalize
#elif defined(HAVE_PRAGMA_CRI_DUP)
#pragma _CRI duplicate MPI_Finalize as PMPI_Finalize
#elif defined(HAVE_WEAK_ATTRIBUTE)
int MPI_Finalize(void) __attribute__ ((weak, alias("PMPI_Finalize")));
#endif
/* -- End Profiling Symbol Block */

/* Define MPICH_MPI_FROM_PMPI if weak symbols are not supported to build
   the MPI routines */
#ifndef MPICH_MPI_FROM_PMPI
#undef MPI_Finalize
#define MPI_Finalize PMPI_Finalize

/* Any internal routines can go here.  Make them static if possible */

/* The following routines provide a callback facility for modules that need
   some code called on exit.  This method allows us to avoid forcing
   MPI_Finalize to know the routine names a priori.  Any module that wants to
   have a callback calls MPIR_Add_finalize(routine, extra, priority).

 */
PMPI_LOCAL void MPIR_Call_finalize_callbacks(int, int);
typedef struct Finalize_func_t {
    int (*f) (void *);          /* The function to call */
    void *extra_data;           /* Data for the function */
    int priority;               /* priority is used to control the order
                                 * in which the callbacks are invoked */
} Finalize_func_t;
/* When full debugging is enabled, each MPI handle type has a finalize handler
   installed to detect unfreed handles.  */
#define MAX_FINALIZE_FUNC 64
static Finalize_func_t fstack[MAX_FINALIZE_FUNC];
static int fstack_sp = 0;
static int fstack_max_priority = 0;

void MPIR_Add_finalize(int (*f) (void *), void *extra_data, int priority)
{
    /* --BEGIN ERROR HANDLING-- */
    if (fstack_sp >= MAX_FINALIZE_FUNC) {
        /* This is a little tricky.  We may want to check the state of
         * MPIR_Process.mpich_state to decide how to signal the error */
        (void) MPL_internal_error_printf("overflow in finalize stack! "
                                         "Is MAX_FINALIZE_FUNC too small?\n");
        if (OPA_load_int(&MPIR_Process.mpich_state) == MPICH_MPI_STATE__IN_INIT ||
            OPA_load_int(&MPIR_Process.mpich_state) == MPICH_MPI_STATE__POST_INIT) {
            MPID_Abort(NULL, MPI_SUCCESS, 13, NULL);
        } else {
            exit(1);
        }
    }
    /* --END ERROR HANDLING-- */
    fstack[fstack_sp].f = f;
    fstack[fstack_sp].priority = priority;
    fstack[fstack_sp++].extra_data = extra_data;

    if (priority > fstack_max_priority)
        fstack_max_priority = priority;
}

/* Invoke the registered callbacks */
PMPI_LOCAL void MPIR_Call_finalize_callbacks(int min_prio, int max_prio)
{
    int i, j;

    if (max_prio > fstack_max_priority)
        max_prio = fstack_max_priority;
    for (j = max_prio; j >= min_prio; j--) {
        for (i = fstack_sp - 1; i >= 0; i--) {
            if (fstack[i].f && fstack[i].priority == j) {
                fstack[i].f(fstack[i].extra_data);
                fstack[i].f = 0;
            }
        }
    }
}
#else
#ifndef USE_WEAK_SYMBOLS
PMPI_LOCAL void MPIR_Call_finalize_callbacks(int, int);
#endif
#endif

/*@
   MPI_Finalize - Terminates MPI execution environment

   Notes:
   All processes must call this routine before exiting.  The number of
   processes running `after` this routine is called is undefined;
   it is best not to perform much more than a 'return rc' after calling
   'MPI_Finalize'.

Thread and Signal Safety:
The MPI standard requires that 'MPI_Finalize' be called `only` by the same
thread that initialized MPI with either 'MPI_Init' or 'MPI_Init_thread'.

.N Fortran

.N Errors
.N MPI_SUCCESS
@*/
int MPI_Finalize(void)
{
    int mpi_errno = MPI_SUCCESS;
    int rank = MPIR_Process.comm_world->rank;
    MPIR_FUNC_TERSE_FINALIZE_STATE_DECL(MPID_STATE_MPI_FINALIZE);

    double total_acc_comp_time, total_acc_data_trans_time, total_acc_comp_time_with_lock;
    MPIR_Errflag_t errflag = MPIR_ERR_NONE;
    double per_trans_time_min, per_trans_time_max, per_trans_time_avg;
    double per_trans_time;
    double max_acc_comp_time, max_acc_comp_time_with_lock;
    double max_acc_data_trans_time;
    size_t total_data_trans_cnt, total_acc_comp_cnt;
    per_trans_time = MPIDI_PIP_global.acc_data_trans_time / MPIDI_PIP_global.acc_data_trans_cnt;

    MPIR_Reduce(&MPIDI_PIP_global.acc_data_trans_cnt, &total_data_trans_cnt, 1, MPI_LONG_LONG,
                MPI_SUM, 0, MPIR_Process.comm_world, &errflag);
    MPIR_Reduce(&MPIDI_PIP_global.acc_comp_cnt, &total_acc_comp_cnt, 1, MPI_LONG_LONG,
                MPI_SUM, 0, MPIR_Process.comm_world, &errflag);

    MPIR_Reduce(&per_trans_time, &per_trans_time_min, 1, MPI_DOUBLE, MPI_MIN, 0,
                MPIR_Process.comm_world, &errflag);
    MPIR_Reduce(&per_trans_time, &per_trans_time_max, 1, MPI_DOUBLE, MPI_MAX, 0,
                MPIR_Process.comm_world, &errflag);
    MPIR_Reduce(&per_trans_time, &per_trans_time_avg, 1, MPI_DOUBLE, MPI_SUM, 0,
                MPIR_Process.comm_world, &errflag);
    per_trans_time_avg /= MPIR_Process.size;

    MPIR_Reduce(&MPIDI_PIP_global.acc_comp_time, &max_acc_comp_time, 1, MPI_DOUBLE, MPI_MAX, 0,
                MPIR_Process.comm_world, &errflag);
    MPIR_Reduce(&MPIDI_PIP_global.acc_comp_time_with_lock, &max_acc_comp_time_with_lock, 1,
                MPI_DOUBLE, MPI_MAX, 0, MPIR_Process.comm_world, &errflag);
    MPIR_Reduce(&MPIDI_PIP_global.acc_data_trans_time, &max_acc_data_trans_time, 1, MPI_DOUBLE,
                MPI_MAX, 0, MPIR_Process.comm_world, &errflag);


    MPIR_Reduce(&MPIDI_PIP_global.acc_comp_time, &total_acc_comp_time, 1, MPI_DOUBLE, MPI_SUM, 0,
                MPIR_Process.comm_world, &errflag);
    MPIR_Reduce(&MPIDI_PIP_global.acc_comp_time_with_lock, &total_acc_comp_time_with_lock, 1,
                MPI_DOUBLE, MPI_SUM, 0, MPIR_Process.comm_world, &errflag);
    MPIR_Reduce(&MPIDI_PIP_global.acc_data_trans_time, &total_acc_data_trans_time, 1, MPI_DOUBLE,
                MPI_SUM, 0, MPIR_Process.comm_world, &errflag);

    if (MPIR_Process.comm_world->rank == 0) {
        printf
            ("%d avg_comp_wlock %.3lf max_wlock %.3lf avg_acc_comp %.3lf max_acc_comp %.3lf avg_trans %.3lf max_trans %.3lf pt_min %.3lf pt_max %.3lf pt_avg %.3lf trans_cnt %ld acc_cnt %ld\n",
             MPIR_Process.size, total_acc_comp_time_with_lock / MPIR_Process.size,
             max_acc_comp_time_with_lock, total_acc_comp_time / MPIR_Process.size,
             max_acc_comp_time, total_acc_data_trans_time / MPIR_Process.size,
             max_acc_data_trans_time, per_trans_time_min, per_trans_time_max, per_trans_time_avg,
             total_data_trans_cnt, total_acc_comp_cnt);
        fflush(stdout);
    }

    /* Note: Only one thread may ever call MPI_Finalize (MPI_Finalize may
     * be called at most once in any program) */
    MPII_finalize_thread_and_enter_cs();
    MPIR_FUNC_TERSE_FINALIZE_ENTER(MPID_STATE_MPI_FINALIZE);

    /* ... body of routine ... */

    mpi_errno = MPII_finalize_async();
    MPIR_ERR_CHECK(mpi_errno);

    mpi_errno = MPII_finalize_global();
    MPIR_ERR_CHECK(mpi_errno);

    MPII_Timer_finalize();

    /* Call the high-priority callbacks */
    MPIR_Call_finalize_callbacks(MPIR_FINALIZE_CALLBACK_PRIO + 1, MPIR_FINALIZE_CALLBACK_MAX_PRIO);

    MPII_debugger_set_aborting();

    mpi_errno = MPID_Finalize();
    MPIR_ERR_CHECK(mpi_errno);

    /* MPID_Finalize or MPIDI_OFI_mpi_finalize_hook will call MPIR_Allreduce
     * which will still depend on lw_req.
     * FIXME: there should not be MPIR collective calls inside MPID_Finaize.
     */
    /* Free complete request */
    MPIR_Request_free(MPIR_Process.lw_req);

    mpi_errno = MPII_Coll_finalize();
    MPIR_ERR_CHECK(mpi_errno);

    /* Call the low-priority (post Finalize) callbacks */
    MPIR_Call_finalize_callbacks(0, MPIR_FINALIZE_CALLBACK_PRIO - 1);

    MPII_finalize_topo();

    /* Users did not call MPI_T_init_thread(), so we free memories allocated to
     * MPIR_T during MPI_Init here. Otherwise, free them in MPI_T_finalize() */
    if (!MPIR_T_is_initialized())
        MPIR_T_env_finalize();

    /* If performing coverage analysis, make each process sleep for
     * rank * 100 ms, to give time for the coverage tool to write out
     * any files.  It would be better if the coverage tool and runtime
     * was more careful about file updates, though the lack of OS support
     * for atomic file updates makes this harder. */
    MPII_final_coverage_delay(rank);

    /* destroy fine-grained mutex (including dynamic ones) */
    MPIR_Thread_CS_Finalize();

    /* All memory should be freed at this point */
    MPII_finalize_memory_tracing();

    MPII_finalize_thread_and_exit_cs();
    OPA_store_int(&MPIR_Process.mpich_state, MPICH_MPI_STATE__POST_FINALIZED);

    /* ... end of body of routine ... */
  fn_exit:
    MPIR_FUNC_TERSE_FINALIZE_EXIT(MPID_STATE_MPI_FINALIZE);
    return mpi_errno;

  fn_fail:
    /* --BEGIN ERROR HANDLING-- */
#ifdef HAVE_ERROR_CHECKING
    {
        mpi_errno = MPIR_Err_create_code(mpi_errno, MPIR_ERR_RECOVERABLE,
                                         __func__, __LINE__, MPI_ERR_OTHER, "**mpi_finalize", 0);
    }
#endif
    mpi_errno = MPIR_Err_return_comm(0, __func__, mpi_errno);
    MPII_finalize_thread_failed_exit_cs();
    goto fn_exit;
    /* --END ERROR HANDLING-- */
}
