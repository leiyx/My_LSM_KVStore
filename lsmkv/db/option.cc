#include "include/option.h"
#include "include/env.h"
#include "include/comparator.h"

namespace lsmkv {
    Option::Option()
        : comparator(DefaultComparator()),
          env(DefaultEnv()) {}
}