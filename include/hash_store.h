#ifndef HASH_STORE_H
#define HASH_STORE_H

#include <tr1/unordered_map>

using namespace std;

template <class K, class V>
class HashStore {
private:
  tr1::unordered_map<K,V*> *m_storage;
  
public:
  // Constructor.
  HashStore() {
    m_storage = new tr1::unordered_map<K, V*>();
  };

  // Interface.   
  void Put(K key, V *value) {
    (*m_storage)[key] = value;
  };

  V* Get(K key) {
    return (*m_storage)[key];
  };

  int Remove(K key) {
    return m_storage->erase(key);
  };
};


#endif // HASH_STORE_H
