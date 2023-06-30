#include <iostream>
#include "internal-driver/Driver.h"
#include "internal-driver/Configuration.h"
#include "tbb/scalable_allocator.h"

int main(int argc, char** argv) {
//  auto ret = scalable_allocation_mode(TBBMALLOC_USE_HUGE_PAGES, 1);
//  cout << "Huge page usage return value: " << (ret == TBBMALLOC_OK) << endl;
  Config config { };
  config.initialize(argc, argv);

  Driver driver(config);
  driver.run();

  cout << "Done" << endl;
  return 0;
}
