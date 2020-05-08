#include <gtest/gtest.h>
#include <random>

extern "C" size_t random_level(int max_links);



TEST(Util, random_level0) {
  std::map<int, int> cc;
  for (int i = 1; i < 10000000; i++) {
    int r = random_level(16);
    cc[r]++;
  }
  for(auto iter=cc.begin();iter!=cc.end();iter++) {
    printf("%d->%d\n", iter->first, iter->second);
  }
}

int main(int argc, char* argv[])
{
    testing::InitGoogleTest(&argc,argv);
    (void)RUN_ALL_TESTS();
    return 0;
}