
MPIBINDIR=../../../install/bin
MPILIBDIR=../../../install/lib

MPICC=$(MPIBINDIR)/mpicc

MPICCFLAGS=-std=c99 -O3

BINS=stencil stencil_fast

OBJ=stencil_body.o

all: $(BINS)

stencil_body.o: stencil_body.c Makefile
	$(MPICC) $(MPICCFLAGS) -c $< -o $@

stencil: stencil.c $(OBJ)
	$(MPICC) $(MPICCFLAGS) $< $(OBJ) -o $@

stencil_fast: stencil.c $(OBJ)
	$(MPICC) $(MPICCFLAGS) -DFAST $< $(OBJ) -o $@

clean:
	rm -f $(BINS) $(OBJ) *~
	rm -f output*.bmp

run: $(BINS)
	$(MPIBINDIR)/mpiexec -np 8 -ppn 8 ./stencil_fast 1024 1 10
	$(MPIBINDIR)/mpiexec -np 8 -ppn 8 ./stencil 1024 1 10
