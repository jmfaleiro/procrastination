#ifndef EAGER_GENERATOR_HH_
#define EAGER_GENERATOR_HH_

#include <action.h>

class EagerGenerator {
public:
    virtual EagerAction* genNext() = 0;
};

#endif // EAGER_GENERATOR_HH_
