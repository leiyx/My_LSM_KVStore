#include "gtest/gtest.h"
#include "crc32c/crc32c.h"
#include "snappy.h"
#include <iostream>
namespace lsmkv {
    TEST(ExampleTest, AddTest) {
        EXPECT_EQ(0, 0);
        EXPECT_EQ(1, 1);
    }
    TEST(ExampleTest, crcTest) {
        const std::uint8_t buffer[] = {'1', '3', '2', '4'};
        std::uint32_t result1,result2,result3;

        // Process a raw buffer.
        result1 = crc32c::Crc32c(buffer, 4);

        // Process a std::string.
        std::string string = "1324";
        string.resize(4);
        result2 = crc32c::Crc32c(string);

        std::string string1 = "1323";
        string1.resize(4);
        result3 = crc32c::Crc32c(string1);

        ASSERT_EQ(result1, result2);
        ASSERT_NE(result1, result3);
    }
    TEST(ExampleTest, snappyTest) {
        std::string buf{"12412415132512351324"};
        std::string compress;
        snappy::Compress(buf.data(), buf.size(), &compress);
        std::string uncompress;
        snappy::Uncompress(compress.data(), compress.size(), &uncompress);
        ASSERT_EQ(buf, uncompress);
    }
}