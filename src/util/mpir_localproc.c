/*
 * Copyright (C) by Argonne National Laboratory
 *     See COPYRIGHT in top-level directory
 */

#include "mpiimpl.h"

#if defined(HAVE_LIMITS_H)
#include <limits.h>
#endif
#if defined(HAVE_UNISTD_H)
#include <unistd.h>
#endif
#if defined(HAVE_ERRNO_H)
#include <errno.h>
#endif

/*
=== BEGIN_MPI_T_CVAR_INFO_BLOCK ===

cvars:
    - name        : MPIR_CVAR_COMM_LEADER_NUM
      category    : CH4
      type        : int
      default     : 0x7fffffff
      class       : none
      verbosity   : MPI_T_VERBOSITY_USER_BASIC
      scope       : MPI_T_SCOPE_ALL_EQ
      description : >-
        number of leader processes in a communicator

=== END_MPI_T_CVAR_INFO_BLOCK ===
*/

/* MPIR_Find_local  -- from the list of processes in comm,
 * builds a list of local processes, i.e., processes on this same node.
 *
 * Note that this will not work correctly for spawned or attached
 * processes.
 *
 *  OUT:
 *    local_size_p      - number of processes on this node.
 *    local_rank_p      - rank of this processes among local processes.
 *    local_ranks_p     - (*local_ranks_p)[i]     = the rank in comm
 *                        of the process with local rank i.
 *                        This is of size (*local_size_p).
 *    intranode_table_p - (*intranode_table_p)[i] = the rank in
 *    (optional)          *local_ranks_p of rank i in comm or -1 if not
 *                        applicable.  It is of size comm->remote_size.
 *                        No return if NULL is specified.
 */
int MPIR_Find_local(MPIR_Comm * comm, int *local_size_p, int *local_rank_p,
                    int **local_ranks_p, int **intranode_table_p)
{
    int mpi_errno = MPI_SUCCESS;
    int i, local_size, local_rank;
    int *local_ranks = NULL, *intranode_table = NULL;
    int node_id = -1, my_node_id = -1;

    MPIR_CHKPMEM_DECL(2);

    /* local_ranks will be realloc'ed later to the appropriate size (currently unknown) */
    /* FIXME: realloc doesn't guarantee that the allocated area will be
     * shrunk - so using realloc is not an appropriate strategy. */
    MPIR_CHKPMEM_MALLOC(local_ranks, int *, sizeof(int) * comm->remote_size, mpi_errno,
                        "local_ranks", MPL_MEM_COMM);
    MPIR_CHKPMEM_MALLOC(intranode_table, int *, sizeof(int) * comm->remote_size, mpi_errno,
                        "intranode_table", MPL_MEM_COMM);

    for (i = 0; i < comm->remote_size; ++i)
        intranode_table[i] = -1;

    mpi_errno = MPID_Get_node_id(comm, comm->rank, &my_node_id);
    MPIR_ERR_CHECK(mpi_errno);
    MPIR_Assert(my_node_id >= 0);

    local_size = 0;
    local_rank = -1;

    /* Scan through the list of processes in comm. */
    for (i = 0; i < comm->remote_size; ++i) {
        mpi_errno = MPID_Get_node_id(comm, i, &node_id);
        MPIR_ERR_CHECK(mpi_errno);

        /* The upper level can catch this non-fatal error and should be
         * able to recover gracefully. */
        MPIR_ERR_CHKANDJUMP(node_id < 0, mpi_errno, MPI_ERR_OTHER, "**dynamic_node_ids");

        /* build list of local processes */
        if (node_id == my_node_id) {
            if (i == comm->rank)
                local_rank = local_size;

            intranode_table[i] = local_size;
            local_ranks[local_size] = i;
            ++local_size;
        }
    }

#ifdef ENABLE_DEBUG
    printf("------------------------------------------------------------------------\n");
    printf("[%d]comm = %p\n", comm->rank, comm);
    printf("[%d]comm->size = %d\n", comm->rank, comm->remote_size);
    printf("[%d]comm->rank = %d\n", comm->rank, comm->rank);
    printf("[%d]local_size = %d\n", comm->rank, local_size);
    printf("[%d]local_rank = %d\n", comm->rank, local_rank);
    printf("[%d]local_ranks = %p\n", comm->rank, local_ranks);
    for (i = 0; i < local_size; ++i)
        printf("[%d]  local_ranks[%d] = %d\n", comm->rank, i, local_ranks[i]);
    printf("[%d]intranode_table = %p\n", comm->rank, intranode_table);
    for (i = 0; i < comm->remote_size; ++i)
        printf("[%d]  intranode_table[%d] = %d\n", comm->rank, i, intranode_table[i]);
#endif

    MPIR_CHKPMEM_COMMIT();

    *local_size_p = local_size;
    *local_rank_p = local_rank;

    *local_ranks_p = MPL_realloc(local_ranks, sizeof(int) * local_size, MPL_MEM_COMM);
    MPIR_ERR_CHKANDJUMP(*local_ranks_p == NULL, mpi_errno, MPI_ERR_OTHER, "**nomem2");

    if (intranode_table_p)
        *intranode_table_p = intranode_table;   /* no need to realloc */
    else
        MPL_free(intranode_table);      /* free internally if caller passes NULL */

  fn_exit:
    return mpi_errno;
  fn_fail:
    MPIR_CHKPMEM_REAP();
    goto fn_exit;
}

int MPIR_Find_node_procs_sum_min(MPIR_Comm * comm, int node_num, int *node_id, int *root_rank,
                                 int *node_procs_min, int **node_procs_sum, int **roots_map)
{
    int mpi_errno = MPI_SUCCESS, i;
    int *procs_cnt = (int *) calloc(node_num, sizeof(int));
    int *lnode_procs_sum = NULL;
    int *lroots_map = NULL;
    int lnode_procs_min = MPIR_CVAR_COMM_LEADER_NUM;
    int lnode_id, tmp_node_id;
    int *node_roots_num;
    int roots_num = 0;

    MPIR_CHKPMEM_DECL(2);
    MPIR_CHKPMEM_CALLOC(lnode_procs_sum, int *, sizeof(int) * (node_num + 1), mpi_errno,
                        "lnode_procs_sum", MPL_MEM_COMM);
    MPIR_CHKPMEM_CALLOC(lroots_map, int *, sizeof(int) * comm->local_size, mpi_errno,
                        "intranode_table", MPL_MEM_COMM);

    for (i = 0; i < comm->local_size; ++i) {
        mpi_errno = MPID_Get_node_id(comm, i, &tmp_node_id);
        MPIR_ERR_CHECK(mpi_errno);
        MPIR_ERR_CHKANDJUMP(tmp_node_id < 0, mpi_errno, MPI_ERR_OTHER, "**dynamic_node_ids");
        /* record #processes on each node */
        if (i == comm->rank)
            lnode_id = tmp_node_id;
        procs_cnt[tmp_node_id] += 1;
    }

    lnode_procs_sum[0] = 0;
    for (i = 0; i < node_num; ++i) {
        lnode_procs_min = lnode_procs_min < procs_cnt[i] ? lnode_procs_min : procs_cnt[i];
        if (i != 0) {
            lnode_procs_sum[i] = procs_cnt[i - 1] + lnode_procs_sum[i - 1];
        }
    }
    lnode_procs_sum[node_num] = procs_cnt[node_num - 1] + lnode_procs_sum[node_num - 1];

    *node_procs_sum = lnode_procs_sum;
    *node_procs_min = lnode_procs_min;
    *node_id = lnode_id;
    node_roots_num = (int *) calloc(node_num, sizeof(int));
    for (i = 0; i < comm->local_size; ++i) {
        mpi_errno = MPID_Get_node_id(comm, i, &tmp_node_id);
        MPIR_ERR_CHECK(mpi_errno);
        if (node_roots_num[tmp_node_id] < lnode_procs_min) {
            lroots_map[roots_num] = i;
            if (i == comm->rank)
                *root_rank = roots_num;
            roots_num++;
        }
        node_roots_num[tmp_node_id] += 1;
    }

    *roots_map = lroots_map;

    free(procs_cnt);
    free(node_roots_num);

  fn_exit:
    return mpi_errno;
  fn_fail:
    MPIR_CHKPMEM_REAP();
    goto fn_exit;
}

/* MPIR_Find_external -- from the list of processes in comm,
 * builds a list of external processes, i.e., one process from each node.
 * You can think of this as the root or main process for each node.
 *
 * Note that this will not work correctly for spawned or attached
 * processes.
 *
 *  OUT:
 *    external_size_p   - number of external processes
 *    external_rank_p   - rank of this process among the external
 *                        processes, or -1 if this process is not external
 *    external_ranks_p  - (*external_ranks_p)[i]   = the rank in comm
 *                        of the process with external rank i.
 *                        This is of size (*external_size_p)
 *    internode_table_p - (*internode_table_p)[i]  = the rank in
 *    (optional)          *external_ranks_p of the root of the node
 *                        containing rank i in comm.  It is of size
 *                        comm->remote_size. No return if NULL is specified.
 */
int MPIR_Find_external(MPIR_Comm * comm, int *external_size_p, int *external_rank_p,
                       int **external_ranks_p, int **internode_table_p)
{
    int mpi_errno = MPI_SUCCESS;
    int *nodes;
    int i, external_size, external_rank;
    int *external_ranks, *internode_table;
    int max_node_id, node_id;

    MPIR_CHKLMEM_DECL(1);
    MPIR_CHKPMEM_DECL(2);

    /* Scan through the list of processes in comm and add one
     * process from each node to the list of "external" processes.  We
     * add the first process we find from each node.  nodes[] is an
     * array where we keep track of whether we have already added that
     * node to the list. */

    /* external_ranks will be realloc'ed later to the appropriate size (currently unknown) */
    /* FIXME: realloc doesn't guarantee that the allocated area will be
     * shrunk - so using realloc is not an appropriate strategy. */
    MPIR_CHKPMEM_MALLOC(external_ranks, int *, sizeof(int) * comm->remote_size, mpi_errno,
                        "external_ranks", MPL_MEM_COMM);
    MPIR_CHKPMEM_MALLOC(internode_table, int *, sizeof(int) * comm->remote_size, mpi_errno,
                        "internode_table", MPL_MEM_COMM);

    mpi_errno = MPID_Get_max_node_id(comm, &max_node_id);
    MPIR_ERR_CHECK(mpi_errno);
    MPIR_Assert(max_node_id >= 0);
    MPIR_CHKLMEM_MALLOC(nodes, int *, sizeof(int) * (max_node_id + 1), mpi_errno, "nodes",
                        MPL_MEM_COMM);

    /* nodes maps node_id to rank in external_ranks of leader for that node */
    for (i = 0; i < (max_node_id + 1); ++i)
        nodes[i] = -1;

    external_size = 0;
    external_rank = -1;

    for (i = 0; i < comm->remote_size; ++i) {
        mpi_errno = MPID_Get_node_id(comm, i, &node_id);
        MPIR_ERR_CHECK(mpi_errno);

        /* The upper level can catch this non-fatal error and should be
         * able to recover gracefully. */
        MPIR_ERR_CHKANDJUMP(node_id < 0, mpi_errno, MPI_ERR_OTHER, "**dynamic_node_ids");

        MPIR_Assert(node_id <= max_node_id);

        /* build list of external processes */
        if (nodes[node_id] == -1) {
            if (i == comm->rank)
                external_rank = external_size;
            nodes[node_id] = external_size;
            external_ranks[external_size] = i;
            ++external_size;
        }

        /* build the map from rank in comm to rank in external_ranks */
        internode_table[i] = nodes[node_id];
    }

#ifdef ENABLE_DEBUG
    printf("------------------------------------------------------------------------\n");
    printf("[%d]comm = %p\n", comm->rank, comm);
    printf("[%d]comm->size = %d\n", comm->rank, comm->remote_size);
    printf("[%d]comm->rank = %d\n", comm->rank, comm->rank);
    printf("[%d]external_size = %d\n", comm->rank, external_size);
    printf("[%d]external_rank = %d\n", comm->rank, external_rank);
    printf("[%d]external_ranks = %p\n", comm->rank, external_ranks);
    for (i = 0; i < external_size; ++i)
        printf("[%d]  external_ranks[%d] = %d\n", comm->rank, i, external_ranks[i]);
    printf("[%d]internode_table = %p\n", comm->rank, internode_table);
    for (i = 0; i < comm->remote_size; ++i)
        printf("[%d]  internode_table[%d] = %d\n", comm->rank, i, internode_table[i]);
    printf("[%d]nodes = %p\n", comm->rank, nodes);
    for (i = 0; i < (max_node_id + 1); ++i)
        printf("[%d]  nodes[%d] = %d\n", comm->rank, i, nodes[i]);
#endif

    MPIR_CHKPMEM_COMMIT();

    *external_size_p = external_size;
    *external_rank_p = external_rank;
    *external_ranks_p = MPL_realloc(external_ranks, sizeof(int) * external_size, MPL_MEM_COMM);
    MPIR_ERR_CHKANDJUMP(*external_ranks_p == NULL, mpi_errno, MPI_ERR_OTHER, "**nomem2");

    if (internode_table_p)
        *internode_table_p = internode_table;   /* no need to realloc */
    else
        MPL_free(internode_table);      /* free internally if caller passes NULL */

  fn_exit:
    MPIR_CHKLMEM_FREEALL();
    return mpi_errno;
  fn_fail:
    MPIR_CHKPMEM_REAP();
    goto fn_exit;
}

/* maps rank r in comm_ptr to the rank of the leader for r's node in
   comm_ptr->node_roots_comm and returns this value.

   This function does NOT use mpich error handling.
 */
int MPIR_Get_internode_rank(MPIR_Comm * comm_ptr, int r)
{
    int mpi_errno = MPI_SUCCESS;
    MPIR_Comm_valid_ptr(comm_ptr, mpi_errno, TRUE);
    MPIR_Assert(mpi_errno == MPI_SUCCESS);
    MPIR_Assert(r < comm_ptr->remote_size);
    MPIR_Assert(comm_ptr->comm_kind == MPIR_COMM_KIND__INTRACOMM);
    MPIR_Assert(comm_ptr->internode_table != NULL);

    return comm_ptr->internode_table[r];
}

/* maps rank r in comm_ptr to the rank in comm_ptr->node_comm or -1 if r is not
   a member of comm_ptr->node_comm.

   This function does NOT use mpich error handling.
 */
int MPIR_Get_intranode_rank(MPIR_Comm * comm_ptr, int r)
{
    int mpi_errno = MPI_SUCCESS;
    MPIR_Comm_valid_ptr(comm_ptr, mpi_errno, TRUE);
    MPIR_Assert(mpi_errno == MPI_SUCCESS);
    MPIR_Assert(r < comm_ptr->remote_size);
    MPIR_Assert(comm_ptr->comm_kind == MPIR_COMM_KIND__INTRACOMM);
    MPIR_Assert(comm_ptr->intranode_table != NULL);

    /* FIXME this could/should be a list of ranks on the local node, which
     * should take up much less space on a typical thin(ish)-node system. */
    return comm_ptr->intranode_table[r];
}
