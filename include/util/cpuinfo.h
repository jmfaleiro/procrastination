// Author: Jose M. Faleiro (faleiro.jose.manuel@gmail.com)
// 

#ifndef _CPUINFO_H
#define _CPUINFO_H

#include <pthread.h>
#include <numa.h>

void
init_cpuinfo();

int
get_cpu(int index, int striped);

int
pin_thread(cpu_set_t* cpu);

#endif
