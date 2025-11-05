#pragma once
#include "base_std_hash.h"
// TO BE IMPLEMENTED
#include "robinhood.h"
//#include "cuckoo.h"
//#include "hopscotch.h"

namespace Contest {

// Configure Hasing Algorithm used by changing GenricHash alias.
template<typename Key, typename Value>
using GenericHash = StdHash<Key,Value>;

}