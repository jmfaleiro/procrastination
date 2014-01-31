#ifndef LAZY_TXN_H
#define LAZY_TXN_H

#include <string>
#include <vector>

using namespace std;

class ILazyTxn;

// The lazy scheduler has an entry of this type for every single record that is
// read or written in the system.
struct DependencyInfo {
  int record;
  ILazyTxn *dependency;
  bool is_write;
  int index;
};

// Each transaction must implement this interface. For now, keep things simple 
// and let each key correspond to a string. 
class ILazyTxn {
  friend class LazyScheduler;

 private:
  int m_state;
  bool m_materialize;

 protected:
  vector<string> m_read_set;
  vector<string> m_write_set;

 public:
  vector<string> getReadSet() {
    return m_read_set;
  }
  vector<string> getWriteSet() {
    return m_write_set;
  }

  virtual bool NowPhase() = 0;
  virtual void LaterPhase() = 0;
};

#endif // LAZY_TXN_H
