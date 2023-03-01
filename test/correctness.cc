#include <cstdint>
#include <iostream>
#include <string>

#include "test.h"

class CorrectnessTest : public Test {
 private:
  const uint64_t SIMPLE_TEST_MAX = 512;
  const uint64_t LARGE_TEST_MAX = 1024 * 20;

  void regular_test(uint64_t max) {
    uint64_t i;

    // Test a single key
    /*EXPECT(not_found, store.Get(1));
    store.Put(1, "SE");
    EXPECT("SE", store.Get(1));
    EXPECT(true, store.Del(1));
    EXPECT(not_found, store.Get(1));
    EXPECT(false, store.Del(1));

    phase();*/

    // Test multiple key-value pairs
    for (i = 0; i < max; ++i) {
      store.Put(i, std::string(i + 1, 's'));
      EXPECT(std::string(i + 1, 's'), store.Get(i));
    }
    phase();

    std::vector<std::future<std::string>> tasks_;
    for (i = 5; i <= 200; i += 3) {
      tasks_.emplace_back(store.GetTask(i * 50));
      tasks_.emplace_back(store.GetTask((i + 1) * 50));
      tasks_.emplace_back(store.GetTask((i + 2) * 50));
      store.PutTask(i + 5 * i, std::string(i + 5 * i + 1, 's'));
    }

    for (i = 5; i <= 100; ++i) {
      EXPECT(std::string((i * 50) + 1, 's'), tasks_[i - 5].get());
    }
    phase();

    // Test after all insertions
    /*for (i = 0; i < max; ++i)
        EXPECT(std::string(i+1, 's'), store.Get(i));
    phase();*/

    for (i = 1; i < max; i += 4) {
      EXPECT(std::string(i, 's'), store.Get(i - 1));
    }
    phase();

    /*for (i = 0; i < max; i+=7)
        EXPECT(std::string(i+2, 's'), store.Get(i));
    phase();*/

    // Test deletions
    for (i = 0; i < max; i += 2) store.Del(i);

    for (i = 0; i < max; ++i)
      EXPECT((i & 1) ? std::string(i + 1, 's') : not_found, store.Get(i));
    phase();

    report();
  }

 public:
  CorrectnessTest(const std::string &dir, bool v = true) : Test(dir, v) {}

  void start_test(void *args = NULL) override {
    std::cout << "KVStore Correctness Test" << std::endl;

    // std::cout << "[Simple Test]" << std::endl;
    // regular_test(SIMPLE_TEST_MAX);

    std::cout << "[Large Test]" << std::endl;
    regular_test(LARGE_TEST_MAX);
  }
};

int main(int argc, char *argv[]) {
  bool verbose = true;

  //	//std::cout << "Usage: " << argv[0] << " [-v]" << std::endl;
  //	std::cout << "  -v: print extra info for failed tests [currently ";
  //	std::cout << (verbose ? "ON" : "OFF")<< "]" << std::endl;
  //	std::cout << std::endl;
  //	std::cout.flush();

  CorrectnessTest test("./data", verbose);

  test.start_test();

  return 0;
}
