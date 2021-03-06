#ifndef KEYS_H
#define KEYS_H

class KeyGen {
  
 public:
  virtual uint64_t gen_key(int table, uint32_t* keys) = 0;
  virtual uint64_t get_key_part(int table, uint64_t key) = 0;
};

#endif // KEYS_H
