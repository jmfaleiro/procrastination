#ifndef UTIL_H
#define UTIL_H

#include <stdio.h>
#include <stdint.h>

static inline void
do_pause()
{
    asm volatile("pause;":::);
}

static inline uint64_t
xchgq(volatile uint64_t *addr, uint64_t new_val)
{
    	uint64_t result;

	// The + in "+m" denotes a read-modify-write operand.
	asm volatile("lock; xchgq %0, %1" :
                     "+m" (*addr), "=a" (result) :
                     "1" (new_val) :
                     "cc");
	return result;
}


static inline uint64_t
fetch_and_increment(volatile uint64_t *variable)
{
    uint64_t counter_value = 1;
    asm volatile ("lock; xaddq %%rax, %1;"
                  : "=a" (counter_value), "+m" (*variable)
                  : "a" (counter_value)
                  : "memory");
    return counter_value + 1;
}

// An indivisible unit of work. 
static inline void
single_work() 
{
    asm volatile("nop;":::"memory");
}

// Use this function to read the timestamp counter. 
// Don't bother with using serializing instructions like cpuid and others,
// found that it works well without them. 
static inline uint64_t
rdtsc()
{
    uint32_t cyclesHigh, cyclesLow;
    asm volatile("rdtsc\n\t"
                 "movl %%edx, %0\n\t"
                 "movl %%eax, %1\n\t"
                 : "=r" (cyclesHigh), "=r" (cyclesLow) ::
                 "%rax", "%rdx");
    return (((uint64_t)cyclesHigh<<32) | cyclesLow);
}

// Check the amount of time (in cycles) required to perform an indivisible
// unit of work. 
// Measured 2 cycles on morz and smorz. 
static inline double
check_pause()
{
    int i;
    uint64_t start_time, end_time;
    start_time = rdtsc();
    for (i = 0; i < 200; ++i) {
        do_pause();
    }
    end_time = rdtsc();
    return ((double)end_time - (double)start_time) / 200.0;
}

// Measure rdtsc overhead. 
static inline double
check_rdtsc()
{
	int i;
	uint64_t start_time, end_time;
	start_time = rdtsc();
	for (i = 0; i < 10000; ++i) {
		end_time = rdtsc();
	}
	
	return (end_time - start_time) / 10000.0;
}

#endif //UTIL_H
