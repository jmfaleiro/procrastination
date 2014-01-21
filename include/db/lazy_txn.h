#ifndef LAZY_TXN_H
#define LAZY_TXN_H

#include <string>
#include <vector>

using namespace std;

// Each transaction must implement this interface. For now, keep things simple 
// and let each key correspond to a string. 
class ILazyTxn {
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
