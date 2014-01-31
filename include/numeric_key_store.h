#ifndef 	NUMERIC_KEY_STORE_H
#define 	NUMERIC_KEY_STORE_H

class NumericKeyStore {
  
 public:
  void Put(uint64_t key, uint64_t value) = 0;
  uint64_t Get(uint64_t key) = 0;
  uint64_t Remove(uint64_t key) = 0;
  bool Exists(uint64_t key) = 0;
};

#endif 		// NUMERIC_KEY_STORE_H
