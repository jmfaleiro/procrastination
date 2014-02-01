// Author: Jose M. Faleiro (faleiro.jose.manuel@gmail.com)
//
#include "hash_table.hh"
#include "concurrent_hash_table.hh"
#include "cpuinfo.h"

#include <cassert>
#include <string>
#include <map>
#include <set>
#include <iostream>

#include <pthread.h>
#include <time.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include <stdint.h>

#define 	THOUSAND 	(1 << 10)
#define 	MILLION 	(1 << 20)

using namespace std;

struct ThreadArgs {
  uint64_t *keys;
  bool is_master;
  uint64_t start_index;
  uint64_t end_index;  
  uint64_t *init_flag;		
  uint64_t *start_flag;
  uint32_t cpu_number;
  uint32_t num_threads;
  timespec elapsed_time;
  HashTable<uint64_t, uint64_t> *tbl;
} __attribute__((aligned(CACHE_LINE)));

timespec 
diff_time(timespec start, timespec end) 
{
    timespec temp;
    if ((end.tv_nsec - start.tv_nsec) < 0) {
        temp.tv_sec = end.tv_sec - start.tv_sec - 1;
        temp.tv_nsec = 1000000000+end.tv_nsec-start.tv_nsec;
    }
    else {
        temp.tv_sec = end.tv_sec-start.tv_sec;
        temp.tv_nsec = end.tv_nsec-start.tv_nsec;
    }
    return temp;
}

static inline void
master_wait(uint64_t *init_flag, uint64_t *start_flag, uint32_t num_threads) {
  fetch_and_increment(init_flag);
  while (*init_flag != num_threads)
    ;
  xchgq(start_flag, 1);  
}

static inline void
worker_wait(uint64_t *init_flag, uint64_t *start_flag) {
  fetch_and_increment(init_flag);
  while (*start_flag == 0)
    ;  
}

static inline void
benchmark_function(uint64_t *keys, 
		   uint64_t start, 
		   uint64_t end, 
		   HashTable<uint64_t, uint64_t>* tbl,
		   timespec *time) {
  timespec start_time, end_time;
  clock_gettime(CLOCK_THREAD_CPUTIME_ID, &start_time);  
  for (uint64_t i = start; i < end; ++i) {
    tbl->Put(keys[i], keys[i]);
  }
  clock_gettime(CLOCK_THREAD_CPUTIME_ID, &end_time);
  *time = diff_time(start_time, end_time);
}

void*
thread_function(void *arg) {
  struct ThreadArgs *t_args = (struct ThreadArgs*)arg;
  pin_thread(t_args->cpu_number);
  if (t_args->is_master) {
    master_wait(t_args->init_flag, t_args->start_flag, t_args->num_threads);
  }
  else {
    worker_wait(t_args->init_flag, t_args->start_flag);
  }
  benchmark_function(t_args->keys, 
		     t_args->start_index, 
		     t_args->end_index, 
		     t_args->tbl,
		     &t_args->elapsed_time);
  return NULL;
}

uint64_t*
init_keys(uint32_t num_keys) {
  uint64_t *keys = (uint64_t*)malloc(sizeof(uint64_t)*num_keys);
  memset(keys, 0, sizeof(uint64_t)*num_keys);
  for (int i = 0; i < num_keys; ++i) {
    
    // Generate a 64-bit key
    uint64_t key = (uint64_t)rand();
    key |= ((uint64_t)rand() << 32);
    keys[i] = key;
  }
  return keys;
}

void
multithreaded_test(uint32_t num_keys, 
		   uint32_t table_size, 
		   uint32_t num_threads) {
  assert(!(table_size & (table_size - 1)));
  HashTable<uint64_t, uint64_t> *tbl = 
    new ConcurrentHashTable<uint64_t, uint64_t>(table_size, 10);
  uint64_t *keys = init_keys(num_keys);

  // Create an array of arguments to pass to threads. Make sure there's no cache
  // line bouncing going on.
  struct ThreadArgs *args = 
    (struct ThreadArgs*)malloc(sizeof(struct ThreadArgs)*num_threads);
  assert(!args && (sizeof(*args) % CACHE_LINE) == 0);
  memset(args, 0, sizeof(struct ThreadArgs)*num_threads);

  // Initialize the worker threads.
  pthread_t *workers = (pthread_t*)malloc(sizeof(pthread_t)*(num_threads-1));
  memset(workers, 0, sizeof(pthread_t)*(num_threads - 1));
  
  uint64_t init_flag = 0;
  uint64_t start_flag = 0;

  int delta = num_keys / num_threads;
  int current = 0;
  for (int i = 0; i < num_threads; ++i) {
    args[i].keys = keys;
    args[i].is_master = i == 0? true : false;
    args[i].init_flag = 0;
    args[i].start_flag = 0;
    args[i].start_index = current;
    args[i].end_index = current+delta;
    args[i].cpu_number = i;
    args[i].num_threads = num_threads;
    args[i].tbl = tbl;
    current += delta;
  }
  for (int i = 1; i < num_threads; ++i) {
    pthread_create(&workers[i-1], NULL, thread_function, &args[i]);
  }  
  thread_function(&args[0]);
  for (int i = 1; i < num_threads; ++i) {
    pthread_join(workers[i-1], NULL);
  }
  
  // Output the results.
  for (int i = 0; i < num_threads; ++i) {
    double time_elapsed = args[i].elapsed_time.tv_sec + 
      (double)args[i].elapsed_time.tv_nsec/1000000000.0;
    double throughput = 
      (args[i].end_index - args[i].start_index)/time_elapsed;
    cout << "Thread " << i << " throughput: " << throughput << "\n";
  }
}

void
singlethreaded_test(uint32_t num_keys, uint32_t table_size) {

  assert(!(table_size & (table_size - 1)));
  HashTable<uint64_t, uint64_t> *tbl = 
    new ConcurrentHashTable<uint64_t, uint64_t>(table_size, 10);  
  uint64_t *keys = init_keys(num_keys);

  // Start the clock.. 
  timespec insert_start, insert_end, read_end;
  clock_gettime(CLOCK_REALTIME, &insert_start);

  // Insert a bunch of items into the hash table. 
  for (int i = 0; i < num_keys; ++i) {    
    tbl->Put(keys[i], keys[i]);
  }
  
  // Find out write throughput. 
  clock_gettime(CLOCK_REALTIME, &insert_end);

  uint64_t counter = 0;
  // Make sure that all keys have been properly added. 
  for (int i = 0; i < num_keys; ++i) {
    counter += tbl->Get(keys[i]);
  }

  // Find out read throughput. 
  clock_gettime(CLOCK_REALTIME, &read_end);
  cout << "Counter value: " << counter << "\n";
  insert_start = diff_time(insert_start, insert_end);
  read_end = diff_time(insert_end, read_end);
  
  // Dump the insertion time. 
  cout << "Insertion time: " << insert_start.tv_sec << ".";
  cout << insert_start.tv_nsec << "\n";

  // Dump the read time. 
  cout << "Read time: " << read_end.tv_sec << ".";
  cout << read_end.tv_nsec << "\n";

  double insertion_time = 
    (double)insert_start.tv_sec + ((double)insert_start.tv_nsec / 1000000000.0);
  double read_time = 
    (double)read_end.tv_sec + ((double)read_end.tv_nsec / 1000000000.0);
  
  double write_throughput = (double)(num_keys) / insertion_time;
  double read_throughput = (double)(num_keys) / read_time; 
  cout << "Single threaded write-throughput: " << write_throughput << "\n";
  cout << "Single threaded read-throughput: " << read_throughput << "\n";
}

int
main(int argc, char **argv) {
  srand(time(NULL));
  uint32_t num_keys = 1<<24;
  uint32_t table_size = 1<<20;
  //  single_threaded_test(num_keys, table_size);
  
  uint32_t num_threads = get_num_cpus();  
  multithreaded_test(num_keys, table_size, num_threads);
  return 0;
}
