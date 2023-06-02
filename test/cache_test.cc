#include "gtest/gtest.h"
#include "include/cache.h"
namespace lsmkv {
    void NullDeleter(std::string_view, void* value) {
        
    }
    TEST(ExampleTest, InsertAndSearch) {
        
        size_t capacity = 1 << 8;
        Cache* cache = NewLRUCache(capacity);
        std::string val1{"test1"};
        Cache::Handle* e = cache->Insert("test1",&val1, 5, &NullDeleter);
        cache->Release(e);
        e = cache->Lookup("test1");
        std::string* result = reinterpret_cast<std::string*>(cache->Value(e));
        cache->Release(e);
        ASSERT_EQ(*result, val1);
        delete cache;
    }

}