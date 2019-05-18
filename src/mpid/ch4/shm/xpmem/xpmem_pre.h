#ifndef XPMEM_PRE_INCLUDED
#define XPMEM_PRE_INCLUDED
#include <xpmem.h>
typedef struct ackHeader {
    __s64 local_rank;
    __s64 data_size;
    __s64 page_size;
    __s64 data_offset;
    __s64 exp_offset;
} ackHeader;

extern ackHeader *header_array; 
extern uint64_t src_addr[36];
extern ackHeader prev_header[36];

#endif