// Author: Jose M. Faleiro (faleiro.jose.manuel@gmail.com)
//

#include "hash_table.hh"

#include <cassert>
#include <string>
#include <map>
#include <set>
#include <iostream>

#include <string.h>
#include <stdlib.h>
#include <time.h>
#include <stdint.h>

#define 	THOUSAND 	(1 << 10)
#define 	MILLION 	(1 << 20)

using namespace std;

int
main(int argc, char **argv) {
  srand(time(NULL));

  assert(!(MILLION & (MILLION - 1)));
  set<uint64_t> added_keys;
  HashTable<uint64_t, uint64_t> *tbl = 
    new HashTable<uint64_t, uint64_t>(8*MILLION, 10);

  for (int i = 0; i < (1 << 23); ++i) {
    
    // Generate a 64-bit key. 
    uint64_t key = (uint64_t)rand();
    key |= ((uint64_t)rand() << 32);

    // Keep track of the key.
    added_keys.insert(key);
    tbl->Put(key, key);
  }
  
  std::cout << "Finished inserting items.\n";
  int num_done = 0;
  // Make sure that all keys have been properly added. 
  for (set<uint64_t>::iterator it = added_keys.begin(); 
       it != added_keys.end(); 
       ++it) {    
    if (tbl->Get(*it) != *it) {
      std::cout << "Iteration: " << num_done << "\n";
      std::cout << "Got value: " << tbl->Get(*it) << "\n";
      std::cout << "Expected value: " << *it << "\n";
    }
    ++num_done;
    assert(tbl->Get(*it) == *it);
  }
}
