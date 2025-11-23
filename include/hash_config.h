#pragma once

#ifdef USE_ROBINHOOD_HASH
#include "robinhood.h"
template <typename Key, typename Value>
using GenericHash = Contest::RobinhoodHash<Key,Value>;

#elif defined(USE_HOPSCOTCH_HASH)
#include "hopscotch.h"
template <typename Key, typename Value>
using GenericHash = Contest::HopscotchHash<Key,Value>;

#elif defined(USE_CUCKOO_HASH)
#include "cuckoo.h"
template <typename Key, typename Value>
using GenericHash = Contest::CuckooHash<Key,Value>;

#else
#include "base_std_hash.h" // fallback
template <typename Key, typename Value>
using GenericHash = Contest::StdHash<Key,Value>;
#endif
