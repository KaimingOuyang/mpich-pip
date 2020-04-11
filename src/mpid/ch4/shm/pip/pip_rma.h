/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 * (C) 2019 by Argonne National Laboratory.
 *     See COPYRIGHT in top-level directory.
 */

#ifndef PIP_RMA_H_INCLUDED
#define PIP_RMA_H_INCLUDED

#include "pip_pre.h"
#include "pip_impl.h"

MPL_STATIC_INLINE_PREFIX int MPIDI_PIP_do_get(void *origin_addr,
                                              int origin_count,
                                              MPI_Datatype origin_datatype,
                                              int target_rank,
                                              MPI_Aint target_disp,
                                              int target_count, MPI_Datatype target_datatype,
                                              MPIR_Win * win)
{
    int mpi_errno = MPI_SUCCESS;
    size_t origin_data_sz = 0, target_data_sz = 0;
    int disp_unit = 0;
    void *base = NULL;
    int dest_type_iscontig, src_type_ifcontig;
    MPIDIG_RMA_OP_CHECK_SYNC(target_rank, win);

    MPIDI_Datatype_check_origin_target_size(origin_datatype, target_datatype,
                                            origin_count, target_count,
                                            origin_data_sz, target_data_sz);
    if (origin_data_sz == 0 || target_data_sz == 0)
        goto fn_exit;

    if (target_rank == win->comm_ptr->rank) {
        base = win->base;
        disp_unit = win->disp_unit;
    } else {
        MPIDIG_win_shared_info_t *shared_table = MPIDIG_WIN(win, shared_table);
        int local_target_rank = win->comm_ptr->intranode_table[target_rank];
        disp_unit = shared_table[local_target_rank].disp_unit;
        base = shared_table[local_target_rank].shm_base_addr;
    }

    MPI_Aint src_data_sz, recv_data_sz;
    uint64_t partner_post = -1;
    char *src_buf = (char *) base + disp_unit * target_disp;
    char *dest_buf = (char *) origin_addr;
    MPIDI_Datatype_check_contig_size(target_datatype, target_count, src_type_ifcontig, src_data_sz);
    MPIDI_Datatype_check_contig_size(origin_datatype, origin_count, dest_type_iscontig,
                                     recv_data_sz);
    recv_data_sz = src_data_sz >= recv_data_sz ? recv_data_sz : src_data_sz;

    MPIR_Datatype *src_dt_ptr;


    struct timespec start, end;
    clock_gettime(CLOCK_MONOTONIC, &start);

    if (src_type_ifcontig && dest_type_iscontig) {
        /* both are contiguous */
        MPIDI_PIP_memcpy_task_enqueue(src_buf, dest_buf, recv_data_sz, partner_post);
    } else if (!src_type_ifcontig && dest_type_iscontig) {
        /* src data is non-contig */
        MPIR_Datatype_get_ptr(target_datatype, src_dt_ptr);
        MPIDI_PIP_pack_task_enqueue((void *) src_buf, target_count, src_dt_ptr,
                                    dest_buf, recv_data_sz, partner_post);
    } else if (src_type_ifcontig && !dest_type_iscontig) {
        /* dest data is non-contig */
        MPIDI_PIP_unpack_task_enqueue((void *) src_buf,
                                      dest_buf, origin_count,
                                      origin_datatype, recv_data_sz, partner_post);
    } else {
        /* both are non-contig */
        MPIR_Datatype_get_ptr(target_datatype, src_dt_ptr);
        MPIDI_PIP_pack_unpack_task_enqueue((void *) src_buf,
                                           target_count, src_dt_ptr, dest_buf,
                                           origin_count, origin_datatype,
                                           recv_data_sz, partner_post);
    }
    clock_gettime(CLOCK_MONOTONIC, &end);
    MPIDI_PIP_global.acc_time +=
        (double) (end.tv_sec - start.tv_sec) + (double) (end.tv_nsec - start.tv_nsec) / 1e9;
    // mpi_errno = MPIR_Localcopy((char *) base + disp_unit * target_disp, target_count,
    //                            target_datatype, origin_addr, origin_count, origin_datatype);

  fn_exit:
    return mpi_errno;
  fn_fail:
    goto fn_exit;
}

MPL_STATIC_INLINE_PREFIX int MPIDI_PIP_mpi_get(void *origin_addr,
                                               int origin_count,
                                               MPI_Datatype
                                               origin_datatype,
                                               int target_rank,
                                               MPI_Aint target_disp,
                                               int target_count,
                                               MPI_Datatype target_datatype, MPIR_Win * win)
{
    int mpi_errno = MPI_SUCCESS;

    /* CH4 schedules operation only based on process locality.
     * Thus the target might not be in shared memory of the window.*/
    if (!MPIDIG_WIN(win, shm_allocated) && target_rank != win->comm_ptr->rank) {
        mpi_errno = MPIDIG_mpi_get(origin_addr, origin_count, origin_datatype,
                                   target_rank, target_disp, target_count, target_datatype, win);
    } else {
        mpi_errno = MPIDI_PIP_do_get(origin_addr, origin_count, origin_datatype, target_rank,
                                     target_disp, target_count, target_datatype, win);
    }

    return mpi_errno;
}

MPL_STATIC_INLINE_PREFIX int MPIDI_PIP_compute_accumulate(void *origin_addr,
                                                          int origin_count,
                                                          MPI_Datatype origin_datatype,
                                                          void *target_addr,
                                                          int target_count,
                                                          MPI_Datatype target_datatype, MPI_Op op)
{
    int mpi_errno = MPI_SUCCESS;
    MPI_Datatype basic_type = MPI_DATATYPE_NULL;
    MPI_Aint predefined_dtp_size = 0, predefined_dtp_count = 0;
    MPI_Aint total_len = 0;
    MPI_Aint origin_dtp_size = 0;
    MPIR_Datatype *origin_dtp_ptr = NULL;
    MPI_Datatype source_dtp, dest_dtp;
    void *packed_buf = NULL;
    int source_count, dest_count;
    MPIR_FUNC_VERBOSE_STATE_DECL(MPID_STATE_MPIDI_POSIX_COMPUTE_ACCUMULATE);
    MPIR_FUNC_VERBOSE_ENTER(MPID_STATE_MPIDI_POSIX_COMPUTE_ACCUMULATE);

    /* Get total length of origin data */
    MPIR_Datatype_get_size_macro(origin_datatype, origin_dtp_size);
    total_len = origin_dtp_size * origin_count;
    if (!MPIR_DATATYPE_IS_PREDEFINED(origin_datatype)) {
        /* Standard (page 425 in 3.1 report) requires predefined datatype or
         * a derived datatype where all basic components are of the same predefined datatype.
         * Thus, basic_type should be correctly set. */
        MPIR_Datatype_get_ptr(origin_datatype, origin_dtp_ptr);
        MPIR_Assert(origin_dtp_ptr != NULL && origin_dtp_ptr->basic_type != MPI_DATATYPE_NULL);

        basic_type = origin_dtp_ptr->basic_type;
        MPIR_Datatype_get_size_macro(basic_type, predefined_dtp_size);
        MPIR_Assert(predefined_dtp_size > 0);

        source_count = total_len / predefined_dtp_size;
        source_dtp = basic_type;

        /* Pack origin data into a contig buffer */
        packed_buf = MPL_malloc(total_len, MPL_MEM_BUFFER);
        MPIR_ERR_CHKANDJUMP(packed_buf == NULL, mpi_errno, MPI_ERR_NO_MEM, "**nomem");

        MPI_Aint actual_pack_bytes;
        mpi_errno = MPIR_Typerep_pack(origin_addr, origin_count, origin_datatype, 0,
                                      packed_buf, total_len, &actual_pack_bytes);
        MPIR_ERR_CHECK(mpi_errno);
        MPIR_Assert(actual_pack_bytes == total_len);

    } else {
        source_count = origin_count;
        source_dtp = origin_datatype;
        packed_buf = origin_addr;
    }

    MPIDI_PIP_acc_iov_t stealing_iov;
    struct iovec *iov;
    if (MPIR_DATATYPE_IS_PREDEFINED(target_datatype)) {
        iov = (struct iovec *) malloc(sizeof(struct iovec));
        iov->iov_base = target_addr;
        iov->iov_len = total_len;

        stealing_iov.cur_iov = 0;
        stealing_iov.cur_addr = (uint64_t) iov->iov_base;
        stealing_iov.cur_len = total_len;
        stealing_iov.iovs = iov;
        stealing_iov.niov = 1;
        dest_count = source_count;
        dest_dtp = target_datatype;
    } else {
        int actual_iov_len;
        MPI_Aint actual_iov_bytes;
        int vec_len;
        MPIR_Datatype *target_dtp_ptr;

        MPIR_Datatype_get_ptr(target_datatype, target_dtp_ptr);
        vec_len = target_dtp_ptr->max_contig_blocks * target_count + 1;
        iov = (struct iovec *) MPL_malloc(vec_len * sizeof(struct iovec), MPL_MEM_RMA);
        if (!iov) {
            mpi_errno =
                MPIR_Err_create_code(MPI_SUCCESS, MPIR_ERR_RECOVERABLE, __func__, __LINE__,
                                     MPI_ERR_OTHER, "**nomem", 0);
            goto fn_exit;
        }

        MPIR_Typerep_to_iov(target_addr, target_count, target_datatype, 0, iov, vec_len, total_len,
                            &actual_iov_len, &actual_iov_bytes);
        vec_len = actual_iov_len;

        stealing_iov.cur_iov = 0;
        stealing_iov.cur_addr = (uint64_t) iov[0].iov_base;
        stealing_iov.cur_len = iov[0].iov_len;
        stealing_iov.iovs = iov;
        stealing_iov.niov = vec_len;
        dest_count = source_count;
        dest_dtp = target_dtp_ptr->basic_type;
    }

    mpi_errno = MPIDI_PIP_compute_acc_op(packed_buf,
                                         source_count,
                                         source_dtp,
                                         target_addr,
                                         dest_count,
                                         dest_dtp,
                                         op, MPIDIG_ACC_SRCBUF_PACKED, total_len, &stealing_iov);
    MPL_free(iov);
    if (!MPIR_DATATYPE_IS_PREDEFINED(origin_datatype))
        MPL_free(packed_buf);
  fn_exit:
    MPIR_FUNC_VERBOSE_EXIT(MPID_STATE_MPIDI_POSIX_COMPUTE_ACCUMULATE);
    return mpi_errno;
  fn_fail:
    goto fn_exit;
}

MPL_STATIC_INLINE_PREFIX int MPIDI_PIP_do_accumulate(const void *origin_addr,
                                                     int origin_count,
                                                     MPI_Datatype origin_datatype,
                                                     int target_rank,
                                                     MPI_Aint target_disp,
                                                     int target_count,
                                                     MPI_Datatype target_datatype, MPI_Op op,
                                                     MPIR_Win * win)
{
    int mpi_errno = MPI_SUCCESS;
    MPIDI_POSIX_win_t *posix_win = &win->dev.shm.posix;
    size_t origin_data_sz = 0, target_data_sz = 0;
    int disp_unit = 0;
    void *base = NULL;
    MPIR_FUNC_VERBOSE_STATE_DECL(MPID_STATE_MPIDI_PIP_DO_ACCUMULATE);
    MPIR_FUNC_VERBOSE_ENTER(MPID_STATE_MPIDI_PIP_DO_ACCUMULATE);

    MPIDIG_RMA_OP_CHECK_SYNC(target_rank, win);

    MPIDI_Datatype_check_size(origin_datatype, origin_count, origin_data_sz);
    MPIDI_Datatype_check_size(target_datatype, target_count, target_data_sz);
    if (origin_data_sz == 0 || target_data_sz == 0)
        goto fn_exit;

    if (target_rank == win->comm_ptr->rank) {
        base = win->base;
        disp_unit = win->disp_unit;
    } else {
        MPIDIG_win_shared_info_t *shared_table = MPIDIG_WIN(win, shared_table);
        int local_target_rank = win->comm_ptr->intranode_table[target_rank];
        disp_unit = shared_table[local_target_rank].disp_unit;
        base = shared_table[local_target_rank].shm_base_addr;
    }

    if (MPIDIG_WIN(win, shm_allocated)) {
        mpi_errno = MPIDI_SHM_PIP_rma_lock(posix_win->shm_mutex_ptr);
        MPIR_ERR_CHECK(mpi_errno);
    }

    mpi_errno = MPIDI_PIP_compute_accumulate((void *) origin_addr, origin_count, origin_datatype,
                                             (char *) base + disp_unit * target_disp,
                                             target_count, target_datatype, op);
    if (MPIDIG_WIN(win, shm_allocated))
        MPIDI_POSIX_RMA_MUTEX_UNLOCK(posix_win->shm_mutex_ptr);

  fn_exit:
    MPIR_FUNC_VERBOSE_EXIT(MPID_STATE_MPIDI_PIP_DO_ACCUMULATE);
    return mpi_errno;
  fn_fail:
    goto fn_exit;
}

MPL_STATIC_INLINE_PREFIX int MPIDI_PIP_mpi_accumulate(const void *origin_addr,
                                                      int origin_count,
                                                      MPI_Datatype origin_datatype,
                                                      int target_rank,
                                                      MPI_Aint target_disp,
                                                      int target_count,
                                                      MPI_Datatype target_datatype, MPI_Op op,
                                                      MPIR_Win * win)
{
    int mpi_errno = MPI_SUCCESS;
    if (!MPIDIG_WIN(win, shm_allocated) && target_rank != win->comm_ptr->rank) {
        mpi_errno = MPIDIG_mpi_accumulate(origin_addr, origin_count, origin_datatype,
                                          target_rank, target_disp, target_count,
                                          target_datatype, op, win);
    } else {
        mpi_errno = MPIDI_PIP_do_accumulate(origin_addr, origin_count, origin_datatype,
                                            target_rank, target_disp, target_count,
                                            target_datatype, op, win);
    }
    return mpi_errno;
}
#endif /* PIP_RECV_H_INCLUDED */
