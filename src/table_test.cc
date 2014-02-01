// Author: Jose M. Faleiro (faleiro.jose.manuel@gmail.com)
//

#include "hash_table.hh"
#include "concurrent_hash_table.hh"

#include <cassert>
#include <string>
#include <map>
#include <set>
#include <iostream>

#include <time.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include <stdint.h>

#define 	THOUSAND 	(1 << 10)
#define 	MILLION 	(1 << 20)

using namespace std;

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


int
main(int argc, char **argv) {
  srand(time(NULL));

  assert(!(MILLION & (MILLION - 1)));
  uint32_t num_keys = 1<<24;
  uint32_t table_size = 1<<20;

  HashTable<uint64_t, uint64_t> *tbl = 
    new ConcurrentHashTable<uint64_t, uint64_t>(table_size, 10);
  
  uint64_t *keys = (uint64_t*)malloc(sizeof(uint64_t)*num_keys);
  memset(keys, 0, sizeof(uint64_t)*num_keys);
  for (int i = 0; i < num_keys; ++i) {
    
    // Generage a 64-bit key
    uint64_t key = (uint64_t)rand();
    key |= ((uint64_t)rand() << 32);
    keys[i] = key;
  }

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
