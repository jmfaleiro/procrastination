#ifndef 	TABLE_HH_
#define 	TABLE_HH_

template<class K, class V>
class Table {
public:

  virtual void
  Put(K key, V value) = 0;

  virtual V
  Get(K key) = 0;
  
  virtual V*
  GetPtr(K key) = 0;

  virtual V
  Delete(K key) = 0;
};

#endif 		// TABLE_HH_
