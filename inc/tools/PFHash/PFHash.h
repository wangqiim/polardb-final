#ifndef PFHASH_H
#define PFHASH_H

#include<iostream>
#include<cstdio>
#include<cstdlib>
#include<cstring>
#include<cmath>
#include<algorithm>
#include <utility>
#include<vector>
#include<set>
#include<ctime>
#define MAXN 50000000
#define N (MAXN + 5)
// #define SHORT 32768
#define P 998244353
#define P2 1000000007
#define EMPTY 0
#define FILLED (!EMPTY)
#define ll long long
using namespace std;

// int n,n4;
// uint64_t a[N];
// pair<uint64_t,uint64_t>b[N];
// set<uint64_t> S;
static bool is_prime(int x) {
    int sq = sqrt(x);
    for(int i = 2; i <= sq; i++)
        if (!(x % i)) {
            return false;
        }
    return true;
}

static int find_prime(int x) {
    while (!is_prime(x))
        x++;
    return x;
}

template <typename KeyT, typename ValueT>
class PFHash
{
private:

    int lim;
    uint32_t ha1,hb1;
    ll sum;

public:
    typedef std::pair<KeyT,ValueT> value_type;

    struct Bucket
    {
        int sz, sz2;
        uint32_t ha2, hb2;
        vector<value_type> x;
        vector<value_type> v;

        int size() {
            return sz;
        }

        void clean() {
            sz=0;
            sz2=0;
            x.clear();
            v.clear();
        }

        void add(value_type y) {
            sz++;
            x.push_back(y);
        }

        void ranhab() {
            ha2 = rand() % P2;
            hb2 = rand() % P2;
        }

        int cal(const uint64_t *y, const int len) {
            return ((*y) % P2 * ha2 + hb2) % P2 % sz2;
        }


        void build() {
            sz2 = find_prime(sz * sz);
            // printf("sz2 %d\n",sz2);
            v.clear();
            // if(bf && sz2>=4)
            //     puts("fuck");
            value_type tmp = make_pair(KeyT(0), ValueT(0));
            // v = vector<value_type>(sz2, tmp);
            for (int i = 0; i < sz2; i++) {
                v.push_back(tmp);
            }
            bool flag = 1;
            while(flag) {
                ranhab();
                for (int i = 0; i < sz2; i++)
                    // f.reset(i);
                    v[i] = tmp;
                flag = 0;
                for(value_type i: x) {
                    // if( sz2>=4)
                    //     printf("%d %d %u %p\n", ha2, hb2, i.first, i.second);
                    int p = cal((const uint64_t *)&i.first, sizeof(i.first));
                    // printf("%u %d %u\n", i.first, p, i.second);
                    if(v[p].first == tmp.first) {
                        // f[p] = FILLED;
                        v[p] = i;
                    }
                    else {
                        flag = 1;
                        break;
                    }
                }
            }
            x.clear();
        }

        value_type find(const uint64_t *y, const int len) {
            int p = cal(y, len);
            return v[p];
        }
    }*H;

    PFHash(int lim_ = MAXN + 1) :lim(lim_) {
    }

    void setlim(int lim_) {
        lim = find_prime(lim_);
    }

    void randomhab() {
        ha1 = rand() % P;
        hb1 = rand() % P;
    }

    int calpos(const uint64_t *x, const int len) {
        // ll res = 0;
        // for (int i = 0; i < len; i++) {
        //     res = (res * ha1 + x[i] + hb1) % P;
        // }
        // return (int)(res) % lim;
        return ((*x) % P * ha1 + hb1) % P % lim;
    }

    value_type find(const uint64_t *x, const int len) {
        int y = calpos(x, len);
        // printf("%u %d sz=%d sz2=%lld\n", *((uint32_t*)x), y, H[y].sz, H[y].sz2);
        return H[y].find(x, len);
    }

    void bulk_load(int n, value_type *a) {
        H = (Bucket*)malloc(sizeof(Bucket) * lim);
        srand(time(0));
        // printf("lim %d\n",lim);
        // phase 1:
        // puts("aaa");
        int n4 = n << 2;
        while(1) {
            randomhab();
            sum = 0;
            for(int i = 0; i < lim; i++) H[i].clean();
            for(int i = 1; i <= n; i++) {
                int p = calpos((const uint64_t *)&a[i].first, sizeof(a[i].first));
                H[p].add(a[i]);
            }
            for (int i = 0; i < lim; i++) {
                // if (H[i].sz > 10){
                //     printf("%d\n", H[i].sz);
                // }
                sum += (ll)H[i].sz * H[i].sz;
            }
            // cout<<"sum="<<sum<<endl;
            if(sum <= n4) break;
        }

        // phase 2:
        // puts("bbb");
        // printf("H= %p\n", H);
        for(int i = 0; i < lim; i++) {
            // if (i % 1000000 == 0) {
            //     printf("loading records %d\n", i);
            // }
            // printf("H= %p\n", H+i);
            if(H[i].size() > 0) {
                // if(H[i].sz >= 2)
                //     printf("i=%d:\n", i);
                // bool flag=i>98990;
                H[i].build();
            }
        }
    }
};

// int main(){
// 	int tim=clock();
// 	srand(time(0));
//     n=MAXN;
//     // puts("fuck");
//     for(int i=1;i<=n;i++){
//         a[i]=rand()%P;
//         while(S.count(a[i])){
//             a[i]=rand()%P;
//         }
//         b[i]=make_pair(a[i], a[i]);
//         S.insert(a[i]);
//         // printf("%u\n",a[i]);
//     }

//     n4=n<<2;
//     PFHash<uint64_t,uint64_t> pfh;
//     pfh.bulk_load(n,b);
//     int tim2=clock();
//     cerr<<tim2-tim<<endl;
//     int T=n;
//     while(T--){
//         uint64_t y=a[rand()%n+1];
//         // printf("%u\n", y);
//         uint64_t ans=pfh.find((const uint64_t *)&y, sizeof(y)).second;
//         if(y != ans){
//             printf("%ld %ld\n", y, ans);
//         }
//     }
// 	cerr<<clock()-tim2<<endl;
// 	return 0;
// }

#endif