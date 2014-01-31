#include <cassert>
#include <cstring>

#include <storage/array_store.h>

ArrayStore::ArrayStore(uint64_t size) {
  m_values = (uint64_t*)malloc(sizeof(uint64_t)*size);
  assert(m_values != NULL);
  memset(m_values, 0, sizeof(uint64_t)*size);
  m_immutable = false;
}

static inline uint64_t
ArrayStore::Get(uint64_t key) {
  return m_values[key];
}

static inline void
ArrayStore::Put(uint64_t key, uint64_t value) {
  if (!m_immutable) {
    m_values[key] = value;
  }
  else {
    assert(false);
  }
}

static inline uint64_t
ArrayStore::Remove(uint64_t key) {
  if (!m_immutable) {
    uint64_t ret = m_values[key];
    m_values[key] = NULL;
  }
  else {
    assert(false);
  }
  return ret;
}
