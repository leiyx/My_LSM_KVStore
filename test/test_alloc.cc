
#include <iostream>

#include "alloc.h"

using namespace std;

void Test() {
  FreeListAlloc freelist_alloc;
  int ncount = 4;
  int *a = (int *)freelist_alloc.Allocate(sizeof(int) * ncount);
  for (int i = 0; i < ncount; ++i) {
    a[i] = i;
  }

  std::cout << "a[](" << a << "): ";
  for (int i = 0; i < ncount; ++i) {
    std::cout << a[i] << " ";
  }

  freelist_alloc.Deallocate(a, sizeof(int) * ncount);

  std::cout << std::endl;
  std::cout << "a[](" << a << "): ";
  for (int i = 0; i < ncount; ++i) {
    std::cout << a[i] << " ";
  }
}
int main() {
  Test();
  return 0;
}
