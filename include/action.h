#ifndef ACTION_H
#define ACTION_H

#include <cassert>
#include <vector>
#include <stdint.h>
#include "machine.h"
#include "util.h"
#include <pthread.h>

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
    volatile uint64_t				*latch;
    struct EagerRecordInfo 			*next;
    struct EagerRecordInfo			*prev;
    
    EagerRecordInfo() {
        record.m_table = 0;
        record.m_key = 0;
        dependency = NULL;
        is_write = false;
        is_held = false;
        next = NULL;
        prev = NULL;
    }

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
    Action *dependency;
    CompositeKey record;
    bool is_write;
    bool is_held;
    int index;

    bool operator<(const struct DependencyInfo &other) const {
        return (((uint64_t)this->dependency) > ((uint64_t)other.dependency));
    }
    
    bool operator>(const struct DependencyInfo &other) const {
        return (((uint64_t)this->dependency) < ((uint64_t)other.dependency));
    }
    
    bool operator==(const struct DependencyInfo &other) const {
        return (((uint64_t)this->dependency) == ((uint64_t)other.dependency));
    }
    
    bool operator!=(const struct DependencyInfo &other) const {
        return (((uint64_t)this->dependency) != ((uint64_t)other.dependency));
    }

    bool operator>=(const struct DependencyInfo &other) const {
        return (((uint64_t)this->dependency) <= ((uint64_t)other.dependency));
    }

    bool operator<=(const struct DependencyInfo &other) const {
        return (((uint64_t)this->dependency) >= ((uint64_t)other.dependency));
    }        
};

class Action {

 public:  
  bool is_checkout;
  uint32_t materialize;
  int is_blind;

  //  volatile uint64_t start_time;
  //  volatile uint64_t end_time;
  //  volatile uint64_t system_start_time;
  //  volatile uint64_t system_end_time;
  std::vector<struct DependencyInfo> readset;
  std::vector<struct DependencyInfo> writeset;

  //  std::vector<int> real_writes;
  //  volatile uint64_t __attribute__((aligned(CACHE_LINE))) sched_start_time;    
  //  volatile uint64_t __attribute__((aligned(CACHE_LINE))) sched_end_time;    
  //  volatile uint64_t __attribute__((aligned(CACHE_LINE))) lock_word;

  volatile uint64_t __attribute__((aligned(CACHE_LINE))) state;
  uint64_t owner;
  
  virtual bool NowPhase() { return true; }
  virtual void LaterPhase() { }
  virtual bool IsLinked(Action **cont) { *cont = NULL; return false; }
};

/*
static void
ReplicateRecords(Action *action) {
    for (size_t i = 0; i < action->read_deps.size(); ++i) {
        action->readset.push_back(action->read_deps[i].record);        
    }
    for (size_t i = 0; i < action->write_deps.size(); ++i) {
        action->writeset.push_back(action->write_deps[i].record);
    }
}
*/

class EagerAction {
 public:
    volatile uint64_t __attribute__((aligned(CACHE_LINE))) num_dependencies;
    std::vector<struct EagerRecordInfo> writeset;
    std::vector<struct EagerRecordInfo> readset;

    virtual bool IsRoot() { return false; }
    virtual bool IsLinked(EagerAction **ret) { *ret = NULL; return false; };
    virtual void Execute() { };
    virtual void PostExec() { };

    EagerAction 			*next;
    EagerAction 	*prev;
    bool 			finished_execution;
};


#endif // ACTION_H
