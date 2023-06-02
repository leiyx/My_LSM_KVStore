#include <iostream>
#include <random>

#include "crc32c/crc32c.h"
#include "gtest/gtest.h"
#include "include/db.h"

namespace lsmkv {
TEST(ExampleTest, SSTableTest) {
  Option option;
  WriteOption write_option;
  DB* db;
  DestoryDB(option, "/home/lei/MyLSMKV/folder_for_test/sst_write_test");
  Status s =
      DB::Open(option, "/home/lei/MyLSMKV/folder_for_test/sst_write_test", &db);
  ASSERT_TRUE(s.ok());
  for (int i = 0; i < 10000; i++) {
    s = db->Put(write_option, std::to_string(i), std::to_string(i));
    ASSERT_TRUE(s.ok());
  }
  delete db;
  db = nullptr;
  s = DB::Open(option, "/home/lei/MyLSMKV/folder_for_test/sst_write_test", &db);
  ASSERT_TRUE(s.ok());
}
}  // namespace lsmkv