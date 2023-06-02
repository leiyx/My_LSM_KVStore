#include "util/coding.h"

#include "gtest/gtest.h"

namespace lsmkv {

TEST(CodingTest, Fixed32) {
  std::string str;
  for (uint32_t i = 0; i < 1000; i++) {
    PutFixed32(&str, i);
  }
  const char* data = str.data();
  for (uint32_t i = 0; i < 1000; i++) {
    uint32_t val = DecodeFixed32(data);
    ASSERT_EQ(i, val);
    data += sizeof(uint32_t);
  }
}

TEST(CodingTest, Fixed64) {
  std::string str;
  for (uint64_t i = 0; i < 1000; i++) {
    PutFixed64(&str, i);
  }
  const char* data = str.data();
  for (uint64_t i = 0; i < 1000; i++) {
    uint64_t val = DecodeFixed64(data);
    ASSERT_EQ(i, val);
    data += sizeof(uint64_t);
  }
}

TEST(CodingTest, Varint32) {
  std::string str;
  for (uint32_t i = 0; i < (1 << 16); i++) {
    PutVarint32(&str, i);
  }
  std::string_view s(str);
  for (uint32_t i = 0; i < (1 << 16); i++) {
    uint32_t val;
    bool done = GetVarint32(&s, &val);
    ASSERT_EQ(i, val);
    ASSERT_EQ(done, true);
  }
  for (uint32_t i = 0; i < 100; i++) {
    uint32_t val;
    bool done = GetVarint32(&s, &val);
    ASSERT_EQ(done, false);
  }
}

TEST(CodingTest, Varint64) {
  std::string str;
  for (uint64_t i = 0; i < (1 << 16); i++) {
    PutVarint64(&str, i);
  }
  std::string_view s(str);
  for (uint64_t i = 0; i < (1 << 16); i++) {
    uint64_t val;
    bool done = GetVarint64(&s, &val);
    ASSERT_EQ(i, val);
    ASSERT_EQ(done, true);
  }
  for (uint32_t i = 0; i < 100; i++) {
    uint64_t val;
    bool done = GetVarint64(&s, &val);
    ASSERT_EQ(done, false);
  }
}

TEST(CodingTest, Varint32Overflow) {
  std::string_view s("\x81\x82\x83\x84\x85");
  uint32_t val;
  ASSERT_EQ(DecodeVarint32(s.data(), s.data() + s.size(), &val), nullptr);
}

TEST(CodingTest, LengthPrefixed) {
  std::string str;
  PutLengthPrefixedSlice(&str, std::string_view("huangxuan"));
  PutLengthPrefixedSlice(&str, std::string_view("love"));
  PutLengthPrefixedSlice(&str, std::string_view("xiurui"));
  PutLengthPrefixedSlice(&str, std::string_view("qaq"));

  std::string_view sv(str);
  std::string_view result;
  ASSERT_TRUE(GetLengthPrefixedSlice(&sv, &result));
  ASSERT_EQ("huangxuan", result);
  ASSERT_TRUE(GetLengthPrefixedSlice(&sv, &result));
  ASSERT_EQ("love", result);
  ASSERT_TRUE(GetLengthPrefixedSlice(&sv, &result));
  ASSERT_EQ("xiurui", result);
  ASSERT_TRUE(GetLengthPrefixedSlice(&sv, &result));
  ASSERT_EQ("qaq", result);
  ASSERT_FALSE(GetLengthPrefixedSlice(&sv, &result));
}
}  // namespace lsmkv