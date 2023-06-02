#include "gtest/gtest.h"
#include "db/sstable/block_reader.h"
#include "db/sstable/block_builder.h"
#include "db/sstable/block_format.h"

namespace lsmkv {
    TEST(ExampleTest, LinearIterater) {
        Option option;
        BlockBuilder builder(&option);
        for (int i = 0; i < 1000; i++) {
            builder.Add(std::to_string(i),"VAL");
        }
        std::string_view s = builder.Finish();
        BlockContents contents;
        contents.data = s;
        contents.heap_allocated_ = false;
        contents.table_cache_ = false;
        BlockReader reader(contents);
        Iterator* iter = reader.NewIterator(DefaultComparator());
        iter->SeekToFirst();
        for (int i = 0; i < 1000; i++) {
            ASSERT_TRUE(iter->Valid());
            ASSERT_EQ(iter->Key(),std::to_string(i));
            ASSERT_EQ(iter->Value(),"VAL");
            iter->Next();
        }
        ASSERT_FALSE(iter->Valid());
        delete iter;
    }

}