#include "common/config.h"

namespace sjtu {
  bool ENABLE_LOGGING=false;  // for virtual table
  std::chrono::duration<long long int> LOG_TIMEOUT =
   std::chrono::seconds(1);
}
