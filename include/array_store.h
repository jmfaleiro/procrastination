#ifndef 	ARRAY_STORE_H
#define		ARRAY_STORE_H

#include <storage/numeric_key_storage.h>


class ArrayStore : public NumericKeyStore {
  
 private:
  // The array referenced by m_values stores values. 
  uint64_t *m_values;
  bool m_immutable;

 public:
  ArrayStore(uint64_t size);  
  void makeImmutable();
};


#endif		// ARRAY_STORE_H
