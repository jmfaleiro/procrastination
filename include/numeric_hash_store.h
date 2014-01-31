#ifndef 	NUMERIC_HASH_STORE_H
#define		NUMERIC_HASH_STORE_H

class NumericHashStore : public NumericKeyStore {
 public:
  NumericHashStore(uint64_t array_size, uint64_t num_buckets);
};

#endif		// NUMERIC_HASH_STORE_H
