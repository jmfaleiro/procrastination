#ifndef ACTION_H
#define ACTION_H

#include <cassert>
#include <vector>
#include <stdint.h>
#include "machine.h"
#include "util.h"

class Action;

class CompositeKey {
 public:
  uint64_t m_table;
  uint64_t m_key;

  CompositeKey(uint64_t table, uint64_t key) {
    m_table = table;
    m_key = key;
  }
  
  CompositeKey() {
    m_table = 0;
    m_key = 0;
  }

  bool operator==(const CompositeKey &other) const {
    return other.m_table == this->m_table && other.m_key == this->m_key;
  }

  bool operator!=(const CompositeKey &other) const {
    return !(*this == other);
  }
  
  bool operator<(const CompositeKey &other) const {
      return ((this->m_table < other.m_table) || 
              ((this->m_table == other.m_table) && (this->m_key < other.m_key)));
  }
  
  bool operator>(const CompositeKey &other) const {
      return ((this->m_table > other.m_table) ||
              ((this->m_table == other.m_table) && (this->m_key > other.m_key)));
  }
  
  bool operator<=(const CompositeKey &other) const {
      return !(*this > other);
  }
  
  bool operator>=(const CompositeKey &other) const {
      return !(*this < other);
  }
};

class EagerAction;
struct EagerRecordInfo {
    CompositeKey 					record;
    EagerAction						*dependency;
    bool 							is_write;
    bool 							is_held;
    struct EagerRecordInfo 			*next;
    struct EagerRecordInfo			*prev;
    
    bool operator<(const struct EagerRecordInfo &other) const {
        return (this->record < other.record);
    }
    
    bool operator>(const struct EagerRecordInfo &other) const {
        return (this->record > other.record);
    }
    
    bool operator==(const struct EagerRecordInfo &other) const {
        return (this->record == other.record);
    }
    
    bool operator!=(const struct EagerRecordInfo &other) const {
        return (this->record != other.record);
    }

    bool operator>=(const struct EagerRecordInfo &other) const {
        return !(this->record < other.record);
    }

    bool operator<=(const struct EagerRecordInfo &other) const {
        return !(this->record > other.record);
    }    
};

// The lazy scheduler has an entry of this type for every single record that is
// read or written in the system.
struct DependencyInfo {
    CompositeKey record;
    Action *dependency;
    bool is_write;
    bool is_held;
    int index;

    struct DependencyInfo *next;
    struct DependencyInfo *prev;

    bool operator<(const struct DependencyInfo &other) const {
        return (this->record < other.record);
    }
    
    bool operator>(const struct DependencyInfo &other) const {
        return (this->record > other.record);
    }
    
    bool operator==(const struct DependencyInfo &other) const {
        return (this->record == other.record);
    }
    
    bool operator!=(const struct DependencyInfo &other) const {
        return (this->record != other.record);
    }

    bool operator>=(const struct DependencyInfo &other) const {
        return !(this->record < other.record);
    }

    bool operator<=(const struct DependencyInfo &other) const {
        return !(this->record > other.record);
    }        
};

class EagerAction {
 public:
    volatile uint64_t __attribute__((aligned(CACHE_LINE))) num_dependencies;
    std::vector<struct EagerRecordInfo> writeset;
    std::vector<struct EagerRecordInfo> readset;

    virtual bool IsLinked(EagerAction **ret) { *ret = NULL; return false; };
    virtual void Execute() { };
    virtual void PostExec() { };

    EagerAction 	*next;
    EagerAction 	*prev;
};

struct StateLock {
  uint64_t values[8];
} __attribute__ ((aligned(CACHE_LINE)));

class Action {

 private:
  // Use a 64-bit field to manage the lock + state of the transaction. 
  volatile uint64_t values[8] __attribute__ ((aligned(CACHE_LINE)));

 public:  
  bool is_checkout;
  int state;
  int num_writes;
  bool materialize;
  int is_blind;


  volatile uint64_t start_time;
  volatile uint64_t end_time;
  volatile uint64_t system_start_time;
  volatile uint64_t system_end_time;
  std::vector<struct DependencyInfo> readset;
  std::vector<struct DependencyInfo> writeset;
  std::vector<int> real_writes;
  volatile uint64_t __attribute__((aligned(CACHE_LINE))) sched_start_time;    
  volatile uint64_t __attribute__((aligned(CACHE_LINE))) sched_end_time;    
  
  // Use the following two functions while performing graph traversals, to 
  // ensure mutual exclusion between threads that touch the same transaction. 
  static inline void Lock(Action* action) {

    // Try to set the first element of the values array to 1. 
    while (xchgq(&(action->values[0]), 1) == 1)
      do_pause();
  }
  static inline void Unlock(Action* action) {
    xchgq(&(action->values[0]), 0);
  }

  static inline void ChangeState(Action* action, uint64_t value) {
    xchgq(&(action->values[4]), value);
  }

  static inline uint64_t CheckState(Action* action) {
    return action->values[4];
  }
  
  virtual bool NowPhase() { return true; }
  virtual void LaterPhase() { }
};

#endif // ACTION_H
