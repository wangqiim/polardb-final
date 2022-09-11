#include <iostream>
#include <assert.h>
#include "MyHashMap.h"
using namespace std;

int main() {
    MyHashMap<int, int> mp;
    mp.reserve(4);

    assert(mp.begin() == mp.end());
    mp.insert({1, 2});
    mp.insert({2, 3});
    assert(mp.begin() != mp.end());
    assert(mp.size() == 2);

    assert(mp.find(1) != mp.end());
    assert(mp.find(1)->first == 1 && mp.find(1)->second == 2);

    assert(mp.find(2) != mp.end());
    assert(mp.find(2)->first == 2 && mp.find(2)->second == 3);

    assert(mp.find(3) == mp.end());

    // 插入到同一个桶内！
    for (int i = 3; i < 100; i += 4) {
        mp.insert({i, 3});
    }
    for (int i = 3; i < 100; i += 4) {
        assert(mp.find(i) != mp.end());
        // cout << "mp.find(i)->first = " << mp.find(i)->first << ", mp.find(i)->second = " <<  mp.find(i)->second << endl;
        assert(mp.find(i)->first == i && mp.find(i)->second == 3);
    }

    // 看看size和迭代器遍历，最终数量是否一直
    uint64_t size = 0;
    for (auto iter = mp.begin(); iter != mp.end(); ++iter) {
        size++;
    }
    assert(mp.size() == size);

    return 0;
}
