#ifndef STORAGE_XDB_DB_VERSION_MERGE_H_
#define STORAGE_XDB_DB_VERSION_MERGE_H_

#include "include/iterator.h"
#include "include/comparator.h"

namespace lsmkv {

class MergedIterator;

Iterator* NewMergedIterator(Iterator** list, size_t num, const Comparator* cmp);

}
#endif // STORAGE_XDB_DB_VERSION_MERGE_H_
