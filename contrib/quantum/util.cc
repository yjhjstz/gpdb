#include <iostream> 
#include <vector> 
#include <random>
#include <iomanip>

#include <mutex>
#include <unordered_set>
#include "allocator.h"
#include "util.h"



#ifdef __cplusplus
extern "C" {
#endif

//std::list<int, pg_allocator<int> > foo;
typedef std::unordered_set<int64_t, std::hash<int64_t>, std::equal_to<int64_t>, pg_allocator<int64_t> > EPSET;
typedef std::minstd_rand random_t;

/********************************HNSW*************************************/

size_t random_level(int max_links) {
    static random_t g_random;
    // I avoid use of uniform_real_distribution to control how many times random() is called.
    // This makes inserts reproducible across standard libraries.

    // NOTE: This works correctly for standard random engines because their value_type is required to be unsigned.
    auto sample = g_random() - random_t::min();
    auto max_rand = random_t::max() - random_t::min();

    // If max_rand is too large, decrease it so that it can be represented by double.
    if (max_rand > 1048576) {
        sample /= max_rand / 1048576;
        max_rand /= max_rand / 1048576;
    }

    double x = std::min(1.0, std::max(0.0, double(sample) / double(max_rand)));
    return static_cast<size_t>(-std::log(x) / std::log(double(max_links + 1)));
}


ItemPointerSet stlset_create(void) {
    ItemPointerSet ret;
    EPSET* intset = new EPSET;
    //std::unordered_set<uint64_t> *intset = new std::unordered_set<uint64_t>;
    intset->rehash(128);
    //ret.mutex = (void*)new std::mutex;
    ret.set = reinterpret_cast<void*>(intset);

    return ret;
}



bool stlset_add_member(ItemPointerSet *stlset, int64_t p) {
    //std::unordered_set<uint64_t> *intset = reinterpret_cast<std::unordered_set<uint64_t>* >(stlset->set);
    EPSET* intset = reinterpret_cast<EPSET* >(stlset->set);
    //std::mutex *mu = reinterpret_cast<std::mutex*>(stlset->mutex);
    //std::lock_guard<std::mutex> lock(*mu);
    auto result_1 = intset->insert(p);
    if (result_1.second) return true;
    return false;
}

bool stlset_is_member(ItemPointerSet *stlset, int64_t p) {
    //std::unordered_set<uint64_t> *intset = reinterpret_cast<std::unordered_set<uint64_t>* >(stlset->set);
    //std::mutex *mu = reinterpret_cast<std::mutex*>(stlset->mutex);
    //std::lock_guard<std::mutex> lock(*mu);
    EPSET* intset = reinterpret_cast<EPSET* >(stlset->set);
    return intset->find(p) != intset->end();
}

void stlset_release(ItemPointerSet *stlset) {
    //delete (std::mutex*)stlset->mutex;
    delete reinterpret_cast<EPSET* >(stlset->set);
}

#ifdef __cplusplus
}
#endif