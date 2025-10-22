#pragma once
#include "base_std_hash.h"

namespace Contest {

template<typename Key, typename Value>
using GenericHash = StdHash<Key,Value>;

}