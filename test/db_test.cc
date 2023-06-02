#include "include/db.h"

#include <iostream>
#include <random>

#include "crc32c/crc32c.h"
#include "gtest/gtest.h"
namespace lsmkv {

TEST(DBTest, Sometest) {
  uint8_t buf[10] = {0x01, 0x02, 0x03};
  uint32_t crc1 = crc32c::Extend(0, buf, 1);
  uint32_t crc2 = crc32c::Extend(0, buf, 3);
  uint32_t crc3 = crc32c::Extend(crc1, buf + 1, 2);
  ASSERT_EQ(crc2, crc3);
}

TEST(DBTest, OpenTest) {
  Option option;
  DB* db;
  DestoryDB(option, "/home/lei/MyLSMKV/folder_for_test/db_test");
  DB::Open(option, "/home/lei/MyLSMKV/folder_for_test/db_test", &db);
  delete db;
  db = nullptr;
  DB::Open(option, "/home/lei/MyLSMKV/folder_for_test/db_test", &db);
  delete db;
}

TEST(DBTest, RecoverTest) {
  Option option;
  WriteOption write_option;
  ReadOption read_option;
  DB* db;
  DestoryDB(option, "/home/lei/MyLSMKV/folder_for_test/db_test");
  DB::Open(option, "/home/lei/MyLSMKV/folder_for_test/db_test", &db);
  db->Put(write_option, "a", "1");
  db->Put(write_option, "a", "2");
  db->Put(write_option, "a", "3");
  db->Put(write_option, "a", "4");
  db->Put(write_option, "a", "5");
  db->Put(write_option, "a", "6");
  std::string result;
  db->Get(read_option, "a", &result);
  ASSERT_EQ(result, "6");
  delete db;
  db = nullptr;
  DB::Open(option, "/home/lei/MyLSMKV/folder_for_test/db_test", &db);
  std::string result1;
  db->Get(read_option, "a", &result1);
  ASSERT_EQ(result1, "6");
  delete db;
}

TEST(DBTest, SimpleTest) {
  Option option;
  WriteOption write_option;
  ReadOption read_option;
  DB* db;
  DestoryDB(option, "/home/lei/MyLSMKV/folder_for_test/db_test");
  DB::Open(option, "/home/lei/MyLSMKV/folder_for_test/db_test", &db);
  db->Put(write_option, "a", "1");
  db->Put(write_option, "a", "2");
  db->Put(write_option, "a", "3");
  db->Put(write_option, "a", "4");
  db->Put(write_option, "a", "5");
  db->Put(write_option, "a", "6");
  std::string result;
  db->Get(read_option, "a", &result);
  ASSERT_EQ(result, "6");
  delete db;
}
TEST(DBTest, MutiKeyTest) {
  Option option;
  WriteOption write_option;
  ReadOption read_option;
  DB* db;
  DestoryDB(option, "/home/lei/MyLSMKV/folder_for_test/db_test");
  DB::Open(option, "/home/lei/MyLSMKV/folder_for_test/db_test", &db);
  std::mt19937 rng(std::random_device{}());
  const size_t data_size = 1000;
  std::vector<std::string> data(data_size);
  for (int iter = 0; iter < 10; iter++) {
    for (int i = 0; i < data_size; i++) {
      std::string val = std::to_string(rng() % data_size);
      db->Put(write_option, std::to_string(i), val);
      data[i] = val;
    }
  }
  std::string result;
  for (int i = 0; i < data_size; i++) {
    db->Get(read_option, std::to_string(i), &result);
    ASSERT_EQ(result, data[i]);
  }
  delete db;
}

TEST(DBTest, DeletionTest) {
  Option option;
  WriteOption write_option;
  ReadOption read_option;
  DB* db;
  DestoryDB(option, "/home/lei/MyLSMKV/folder_for_test/db_test");
  DB::Open(option, "/home/lei/MyLSMKV/folder_for_test/db_test", &db);
  std::mt19937 rng(std::random_device{}());
  std::vector<std::string> data(1000);
  for (int iter = 0; iter < 1; iter++) {
    for (int i = 0; i < 1000; i++) {
      std::string val = std::to_string(rng() % 1000);
      db->Put(write_option, std::to_string(i), val);
      data[i] = val;
    }
  }
  for (int i = 0; i < 100; i++) {
    size_t k = (rng() % 1000);
    db->Delete(write_option, std::to_string(i));
    data[i] = "";
  }
  std::string result;
  for (int i = 0; i < 1000; i++) {
    db->Get(read_option, std::to_string(i), &result);
    ASSERT_EQ(result, data[i]);
  }
  delete db;
}
const int KthreadNum = 30;
struct TestState {
  TestState(DB* db) : db(db), rng(std::random_device{}()), done(0) {}
  DB* db;
  std::atomic<uint32_t> kv[KthreadNum];
  std::mt19937 rng;
  std::atomic<uint32_t> done;
};
struct ThreadState {
  int id;
  TestState* state;
};
void TestThread(void* arg) {
  ThreadState* state = reinterpret_cast<ThreadState*>(arg);
  int id = state->id;
  TestState* ts = state->state;
  WriteOption write_option;
  ReadOption read_option;
  Status s;
  char val[2000];
  for (int i = 0; i < 100000; i++) {
    int key = ts->rng() % 10;
    ts->kv[id].store(i, std::memory_order_release);
    if ((ts->rng() % 2) == 0) {
      std::snprintf(val, sizeof(val), "%d.%d.%-100d", key, id, i);
      s = ts->db->Put(write_option, std::to_string(key), val);
      // std::cout<<"put key = "<<key<<" val = " << val<<std::endl;
      ASSERT_TRUE(s.ok());
    } else {
      std::string result;
      s = ts->db->Get(read_option, std::to_string(key), &result);
      // std::cout<<"get key = "<<key<<" val = " << result<<std::endl;
      if (!s.IsNotFound()) {
        int v_key, v_id, v_i;
        ASSERT_EQ(3, sscanf(result.data(), "%d.%d.%d", &v_key, &v_id, &v_i))
            << result;
        ASSERT_EQ(v_key, key);
        ASSERT_GE(v_id, 0);
        ASSERT_LE(v_i, ts->kv[v_id].load(std::memory_order_acquire));
      }
    }
  }
  ts->done.fetch_add(1, std::memory_order_release);
}
void TestThread1(void* arg) {
  // std::cout<<"testthread "<< std::this_thread::get_id() <<"start"
  // <<std::endl;
  ThreadState* state = reinterpret_cast<ThreadState*>(arg);
  TestState* ts = state->state;
  WriteOption write_option;
  Status s;
  for (int i = 0; i < 100000; i++) {
    s = ts->db->Put(write_option, "1", "0");
  }
  ts->done.fetch_add(1, std::memory_order_release);
}
TEST(DBTest, ConcurrencyTest) {
  Option option;
  WriteOption write_option;
  DB* db;
  DestoryDB(option, "/home/lei/MyLSMKV/folder_for_test/db_test");
  DB::Open(option, "/home/lei/MyLSMKV/folder_for_test/db_test", &db);
  TestState test_state(db);
  ThreadState thread[KthreadNum];
  // std::cout<<"ConcurrencyTest start"<<std::endl;
  for (int i = 0; i < KthreadNum; i++) {
    thread[i].state = &test_state;
    thread[i].id = i;
    std::thread t(TestThread, &thread[i]);
    t.detach();
  }
  while (test_state.done.load(std::memory_order_acquire) < KthreadNum) {
    std::this_thread::sleep_for(std::chrono::microseconds(100));
  }
  // std::cout<<"ConcurrencyTest done"<<std::endl;
  delete db;
}

}  // namespace lsmkv