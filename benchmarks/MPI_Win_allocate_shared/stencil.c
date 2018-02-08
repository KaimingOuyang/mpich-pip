/*
 * Copyright (c) 2012 Torsten Hoefler. All rights reserved.
 *
 * Author(s): Torsten Hoefler <htor@illinois.edu>
 *
 * Modified for PiP evaluation: Atsushi Hori, 2017
 */

#define _GNU_SOURCE
#include <sys/mman.h>
#include <numa.h>
#include <numaif.h>
#include <unistd.h>
#include <string.h>
#include <mpi.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

double *mem;

int r,p;
int npf;
double heat; // total heat in system
#define nsources 3
int locsources[nsources][2]; // sources local to my rank

#define DBG
//#define DBG	do { printf("[%d] %d\n", r, __LINE__ ); } while(0)

int get_page_table_size( void ) {
  int ptsz = 0;
#ifndef FAST
  FILE *fp;
  char *line = NULL;
  char keyword[128];
  size_t n = 0;

  sleep( 5 );
  if( ( fp = fopen( "/proc/meminfo", "r" ) ) != NULL ) {
    while( getline( &line, &n, fp ) > 0 ) {
      if( sscanf( line, "%s %d", keyword, &ptsz ) > 0 &&
	  strcmp( keyword, "PageTables:" ) == 0 ) break;
    }
    free( line );
    fclose( fp );
  }
  //printf( "PageTables: %d [KB]\n", ptsz );
#endif
  return ptsz;
}

int get_page_faults( void ) {
  FILE *fp;
  unsigned long npf = 0;

  if( ( fp = fopen( "/proc/self/stat", "r" ) ) != NULL ) {
    fscanf( fp,
	    "%*d "		/* pid */
	    "%*s "		/* comm */
	    "%*c "		/* state */
	    "%*d "		/* ppid */
	    "%*d "		/* pgrp */
	    "%*d "		/* session */
	    "%*d "		/* tty_ny */
	    "%*d "		/* tpgid */
	    "%*u "		/* flags */
	    "%lu ",		/* minflt */
	    &npf );
  }
  return (int) npf;
}

int main(int argc, char **argv) {
  int ptsz=0;
  int n, energy, niters;

  MPI_Init(&argc, &argv);
  MPI_Comm comm = MPI_COMM_WORLD;
  MPI_Comm_rank(comm, &r);
  MPI_Comm_size(comm, &p);

  if (r==0) {
    // argument checking
    if(argc < 4) {
      if(!r) printf("usage: stencil_mpi <n> <energy> <niters>\n");
      MPI_Finalize();
      exit(1);
    }

    n = atoi(argv[1]); // nxn grid
    energy = atoi(argv[2]); // energy to be injected per iteration
    niters = atoi(argv[3]); // number of iterations

    // distribute arguments
    int args[3] = {n, energy, niters};
    MPI_Bcast(args, 3, MPI_INT, 0, comm);
  }
  else {
    int args[3];
    MPI_Bcast(args, 3, MPI_INT, 0, comm);
    n=args[0]; energy=args[1]; niters=args[2];
  }

  MPI_Comm shmcomm;
  MPI_Comm_split_type(comm, MPI_COMM_TYPE_SHARED, 0, MPI_INFO_NULL, &shmcomm);
  int sp; // rank and size in shmem comm
  MPI_Comm_size(shmcomm, &sp);

  DBG;

  int pdims[2]={0,0};
  MPI_Dims_create(sp, 2, pdims);
  int px = pdims[0];
  int py = pdims[1];

  DBG;

  // determine my coordinates (x,y) -- r=x*a+y in the 2d processor array
  int rx = r % px;
  int ry = r / px;
  // determine my four neighbors
  int north = (ry-1)*px+rx; if(ry-1 < 0)   north = MPI_PROC_NULL;
  int south = (ry+1)*px+rx; if(ry+1 >= py) south = MPI_PROC_NULL;
  int west  = ry*px+rx-1;   if(rx-1 < 0)   west  = MPI_PROC_NULL;
  int east  = ry*px+rx+1;   if(rx+1 >= px) east  = MPI_PROC_NULL;
  // decompose the domain
  int bx = n/px; // block size in x
  int by = n/py; // block size in y
  int offx = rx*bx; // offset in x
  int offy = ry*by; // offset in y

  DBG;

  int size = (bx+2)*(by+2); // process-local grid (including halos (thus +2))
  long szsz = 2*size*sizeof(double);

  double ta;
  double *mem = NULL;
  MPI_Win win;

  DBG;

  MPI_Barrier(shmcomm);
  ta=-MPI_Wtime(); // take time
  MPI_Win_allocate_shared(szsz, 1, MPI_INFO_NULL, shmcomm, &mem, &win);
  memset( (void*) mem, 0, szsz );
  MPI_Barrier(shmcomm);
  ta+=MPI_Wtime();

  DBG;


  double *northptr, *southptr, *eastptr, *westptr;
  double *northptr2, *southptr2, *eastptr2, *westptr2;

  double *anew=mem; // each rank's offset
  double *aold=mem+size; // second half is aold!

  MPI_Aint sz;
  int dsp_unit;
  MPI_Win_shared_query(win, north, &sz, &dsp_unit, &northptr);
  MPI_Win_shared_query(win, south, &sz, &dsp_unit, &southptr);
  MPI_Win_shared_query(win, east, &sz, &dsp_unit, &eastptr);
  MPI_Win_shared_query(win, west, &sz, &dsp_unit, &westptr);

  northptr2 = northptr+size;
  southptr2 = southptr+size;
  eastptr2  = eastptr+size;
  westptr2  = westptr+size;

  if( north == MPI_PROC_NULL ) north = -1;
  if( south == MPI_PROC_NULL ) south = -1;
  if( east  == MPI_PROC_NULL ) east  = -1;
  if( west  == MPI_PROC_NULL ) west  = -1;

  // initialize three heat sources
  int sources[nsources][2] = {{n/2,n/2}, {n/3,n/3}, {n*4/5,n*8/9}};
  int locnsources=0; // number of sources in my area
  DBG;
  for (int i=0; i<nsources; ++i) { // determine which sources are in my patch
    int locx = sources[i][0] - offx;
    int locy = sources[i][1] - offy;
    if(locx >= 0 && locx < bx && locy >= 0 && locy < by) {
      locsources[locnsources][0] = locx+1; // offset by halo zone
      locsources[locnsources][1] = locy+1; // offset by halo zone
      locnsources++;
    }
  }
  DBG;

  MPI_Barrier(shmcomm);
  double tb = MPI_Wtime(); // take time
  MPI_Win_lock_all(0, win);
  for( int iter=0; iter<niters; iter++ ) {
    MPI_Win_sync(win);
    MPI_Barrier(shmcomm);
  }
  double t = MPI_Wtime(); // take time
  tb = t - tb;

  if(!r) ptsz = get_page_table_size();
  npf = get_page_faults();
  DBG;

  t = - MPI_Wtime(); // take time
  for(int iter=0; iter<niters; ++iter) {
    void heat_source(int, int, double*);
    double stencil_body(int, int, int, int, int, int, double*, double*,
			double*, double*, double*, double*,
			double*, double*, double*, double*);
    // refresh heat sources
    heat_source(bx, energy, aold);

    MPI_Win_sync(win);
    MPI_Barrier(shmcomm);
    heat = stencil_body(north, south, west, east,
			bx, by,
			anew, aold,
			northptr, northptr2, southptr, southptr2,
			eastptr, eastptr2, westptr, westptr2);

    //printf( "[%d] heat=%g\n", r, heat );
  }
  DBG;
  MPI_Win_unlock_all(win);
  t+=MPI_Wtime();
  //printf( "[%d] npf %d\n", r, npf );
  npf = get_page_faults() - npf;
  if(!r) ptsz = get_page_table_size() - ptsz;

  // get final heat in the system
  double rheat;
  MPI_Allreduce(&heat, &rheat, 1, MPI_DOUBLE, MPI_SUM, comm);

  int tnpf = 0;
  MPI_Allreduce(&npf, &tnpf, 1, MPI_INT, MPI_SUM, comm);

  if(!r) {
    printf("heat: %g  time, %g, %g, %g, %i, %i, [KB], %i\n", rheat, t, ta, tb, ptsz, szsz*p/1024, tnpf);
  }
  MPI_Win_free(&win);
  MPI_Comm_free(&shmcomm);

  MPI_Finalize();
  return 0;
}
