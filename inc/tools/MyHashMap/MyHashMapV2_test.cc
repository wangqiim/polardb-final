#include <iostream>
#include <assert.h>
#include "MyHashMapV2.h"
using namespace std;

int main() {
    MyHashMapV2<int, int> mp;
    mp.reserve(4);

    assert(mp.begin() == mp.end());
    {
        auto res = mp.insert({1, 2});
        assert(res.second == true);
    }
    {
        auto res = mp.insert({1, 2});
        assert(res.second == false);
    }
    mp.insert({2, 3});
    assert(mp.begin() != mp.end());
    assert(mp.size() == 2);

    assert(mp.find(1) != mp.end());
    assert(mp.find(1).First() == 1 && mp.find(1).Second() == 2);

    assert(mp.find(2) != mp.end());
    assert(mp.find(2).First() == 2 && mp.find(2).Second() == 3);

    assert(mp.find(3) == mp.end());

    // 插入到同一个桶内！
    for (int i = 3; i < 100; i += 4) {
        mp.insert({i, 3});
    }
    for (int i = 3; i < 100; i += 4) {
        assert(mp.find(i) != mp.end());
        // cout << "mp.find(i).First() = " << mp.find(i).First() << ", mp.find(i).Second() = " <<  mp.find(i).Second() << endl;
        assert(mp.find(i).First() == i && mp.find(i).Second() == 3);
    }

    // 看看size和迭代器遍历，最终数量是否一直
    uint64_t size = 0;
    for (auto iter = mp.begin(); iter != mp.end(); ++iter) {
        size++;
    }
    assert(mp.size() == size);

    return 0;
}
