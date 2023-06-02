#include <iostream>

#include "gtest/gtest.h"
#include "include/env.h"
#include "include/iterator.h"
#include "include/sstable_builder.h"
#include "include/sstable_reader.h"
#include "util/file.h"

namespace lsmkv {
TEST(ExampleTest, LinearIterater) {
  Option option;
  option.compress_type = KUnCompress;
  RandomReadFile* read_file;
  WritableFile* write_file;
  Env* env = DefaultEnv();
  std::string filename{"/home/lei/MyLSMKV/folder_for_test/sst_test"};
  Status s = env->NewWritableFile(filename, &write_file);
  ASSERT_TRUE(s.ok());
  SSTableBuilder builder(option, write_file);
  char buf[10];
  for (int i = 0; i < 1000000; i++) {
    std::sprintf(buf, "%06d", i);
    builder.Add(std::string_view(buf, 6), "VAL");
  }
  s = builder.Finish();
  ASSERT_TRUE(s.ok());
  s = write_file->Sync();
  ASSERT_TRUE(s.ok());

  uint64_t file_size;
  s = env->NewRamdomReadFile(filename, &read_file);
  ASSERT_TRUE(s.ok());
  env->FileSize(filename, &file_size);
  std::cout << "builder.FileSize():" << builder.FileSize() << std::endl;
  ASSERT_EQ(file_size, builder.FileSize());
  SSTableReader* reader;
  s = SSTableReader::Open(option, read_file, file_size, &reader);
  ASSERT_TRUE(s.ok());
  ReadOption read_option;
  Iterator* iter = reader->NewIterator(read_option);
  iter->SeekToFirst();
  for (int i = 0; i < 1000000; i++) {
    std::sprintf(buf, "%06d", i);
    std::string_view s = std::string_view(buf, 6);
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->Key(), s);
    ASSERT_EQ(iter->Value(), "VAL");
    iter->Next();
  }
  ASSERT_FALSE(iter->Valid());
  env->RemoveFile(filename);
  delete iter;
  delete reader;
  delete write_file;
}

}  // namespace lsmkv