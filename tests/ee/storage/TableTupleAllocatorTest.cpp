/* This file is part of VoltDB.
 * Copyright (C) 2008-2020 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#include "harness.h"
#include "common/debuglog.h"
#include "storage/TableTupleAllocator.hpp"
#include <algorithm>
#include <array>
#include <cstdio>
#include <cstdarg>
#include <cstring>
#include <random>
#include <thread>
#include <chrono>

using TableTupleAllocatorTest = Test;
// These tests are geared towards debug build, relying on some
// constants defined differently in the src file.
#ifndef NDEBUG

using namespace voltdb::storage;
using namespace std;

static random_device rd;
/**
 * For usage, see commented test in HelloWorld
 */
template<size_t len> class StringGen {
    unsigned char m_queryBuf[len + 1];
    size_t m_state = 0;
    static void reset(unsigned char* dst) {
        memset(dst, 1, len);
    }
protected:
    virtual unsigned char* query(size_t state) {
        return of(state, m_queryBuf);
    }
public:
    inline explicit StringGen() {
        reset(m_queryBuf);
    }
    inline void reset() {
        m_state = 0;
        reset(m_queryBuf);
    }
    inline unsigned char* get() {
        return query(m_state++);
    }
    inline size_t last_state() const noexcept {
        return m_state;
    }
    inline unsigned char* fill(unsigned char* dst) {
        memcpy(dst, get(), len);
        return dst;
    }
    inline void* fill(void* dst) {
        return fill(reinterpret_cast<unsigned char*>(dst));
    }
    static unsigned char* of(size_t state, unsigned char* dst) {       // ENC
        reset(dst);
        for(size_t s = state, pos = 0; s && pos <= len; ++pos) {
            size_t s1 = s, acc = 1;
            while(s1 > 255) {
                s1 /= 255;
                acc *= 255;
            }
            dst[pos] = s % 255 + 1;
            s /= 255;
        }
        return dst;
    }
    inline static size_t of(unsigned char const* dst) {      // DEC
        size_t r = 0, i = 0, j = 0;
        // rfind 1st position > 1
        for (j = len - 1; j > 0 && dst[j] == 1; --j) {}
        for (i = 0; i <= j; ++i) {
            r = r * 255 + dst[j - i] - 1;
        }
        return r;
    }
    inline static bool same(void const* dst, size_t state) {
        static unsigned char buf[len+1];
        return ! memcmp(dst, of(state, buf), len);
    }
    static string hex(unsigned char const* src) {
        static char n[7];
        const int perLine = 16;
        string r;
        r.reserve(len * 5.2);
        for(size_t pos = 0; pos < len; ++pos) {
            snprintf(n, sizeof n, "0x%x ", src[pos]);
            r.append(n);
            if (pos % perLine == perLine - 1) {
                r.append("\n");
            }
        }
        return r.append("\n");
    }
    inline static string hex(void const* src) {
        return hex(reinterpret_cast<unsigned char const*>(src));
    }
    inline static string hex(void* src) {
        return hex(reinterpret_cast<unsigned char const*>(src));
    }
    inline static string hex(size_t state) {
        static unsigned char buf[len];
        return hex(of(state, buf));
    }
};

TEST_F(TableTupleAllocatorTest, RollingNumberComparison) {
#define RollingNumberComparisons(type)                                            \
    ASSERT_TRUE(less_rolling(numeric_limits<type>::max(),                         \
                static_cast<type>(numeric_limits<type>::max() + 1)));             \
    ASSERT_FALSE(less_rolling(static_cast<type>(numeric_limits<type>::max() + 1), \
                numeric_limits<type>::max()))
    RollingNumberComparisons(unsigned char);
    RollingNumberComparisons(unsigned short);
    RollingNumberComparisons(unsigned);
    RollingNumberComparisons(unsigned long);
    RollingNumberComparisons(unsigned long long);
    RollingNumberComparisons(size_t);
#undef RollingNumberComparisons
}

constexpr size_t TupleSize = 16;       // bytes per allocation
constexpr size_t AllocsPerChunk = 512 / TupleSize;     // 512 comes from ChunkHolder::chunkSize()
constexpr size_t NumTuples = 16 * AllocsPerChunk;     // # allocations: fits in 256 chunks

template<typename Alloc> void* remove_single(Alloc& alloc, void const* p) {
    alloc.remove_reserve(1);
    alloc.remove_add(const_cast<void*>(p));
    void* r = nullptr;
    assert(1 ==
           alloc.template remove_force<truth>([&r](vector<pair<void*, void*>> const& entries) noexcept {
               if (! entries.empty()) {
                   assert(entries.size() == 1);
                   r = memcpy(entries[0].first, entries[0].second, TupleSize);
               }
           }).first);
    return r;
}

template<typename Alloc> void remove_multiple(Alloc& alloc, size_t n, ...) {
    alloc.remove_reserve(n);
    va_list args;
    va_start(args, n);
    for (size_t i = 0; i < n; ++i) {
        alloc.remove_add(const_cast<void*>(va_arg(args, void const*)));
    }
    assert(n ==
           alloc.template remove_force<truth>([](vector<pair<void*, void*>> const& entries) noexcept {
               for_each(entries.begin(), entries.end(),
                        [](pair<void*, void*> const& entry) {memcpy(entry.first, entry.second, TupleSize);});
           }).first);
}

template<typename Chunks>
void testNonCompactingChunks(size_t outOfOrder) {
    using Gen = StringGen<TupleSize>;
    Gen gen;
    Chunks alloc(TupleSize);
    array<void*, NumTuples> addresses;
    size_t i;

    assert(alloc.empty());
    for(i = 0; i < NumTuples; ++i) {
        addresses[i] = gen.fill(alloc.allocate());
    }

    // Test non-sequential free() calls
    for(size_t i = 0; i < outOfOrder; ++i) {
        for(size_t j = 0; j < NumTuples; ++j) {
            if (j % outOfOrder == i) {
                assert(Gen::same(addresses[j], j));
                alloc.free(addresses[j]);
                // non-compacting chunks don't compact upon free()'s
                // Myth: so long as I don't release the last of
                // every #outOfOrder-th allocation, the chunk
                // itself holds.
                assert(i + 1 == outOfOrder || Gen::same(addresses[j], j));
            }
        }
    }
    assert(alloc.empty());                 // everything gone
}

template<typename Chunks>
void testIteratorOfNonCompactingChunks() {
    using const_iterator = typename IterableTableTupleChunks<Chunks, truth>::const_iterator;
    using iterator = typename IterableTableTupleChunks<Chunks, truth>::iterator;
    using Gen = StringGen<TupleSize>;
    using addresses_type = array<void*, NumTuples>;
    Gen gen;
    Chunks alloc(TupleSize);
    size_t i;
    addresses_type addresses;

    for(i = 0; i < NumTuples; ++i) {
        addresses[i] = gen.fill(alloc.allocate());
    }
    assert(alloc.size() == NumTuples);
    class Checker {
        size_t m_index = 0;                // Note that NonCompactingChunks uses singly-linked list
        map<void const*, size_t> m_remains;
    public:
        Checker(addresses_type const& addr) {
            for(size_t i = 0; i < tuple_size<addresses_type>::value; ++i) {
                m_remains.emplace(addr[i], i);
            }
        }
        void operator() (void const* p) {
            assert(m_remains.count(p));
            assert(Gen::same(p, m_remains[p]));
            m_remains.erase(p);
        }
        void operator() (void* p) {
            assert(m_remains.count(p));
            assert(Gen::same(p, m_remains[p]));
            m_remains.erase(p);
        }
        bool complete() const noexcept {
            return m_remains.empty();
        }
    };

    Checker c2{addresses};
    for_each<iterator>(alloc, c2);
    assert(c2.complete());

    // free in FIFO order: using functor
    class Remover {
        size_t m_i = 0;
        Chunks& m_chunks;
    public:
        Remover(Chunks& c): m_chunks(c) {}
        void operator()(void* p) {
            m_chunks.free(p);
            ++m_i;
        }
        size_t invocations() const {
            return m_i;
        }
    } remover{alloc};
    for_each<iterator>(alloc, remover);
    assert(remover.invocations() == NumTuples);
    assert(alloc.empty());

    // free in FIFO order: using lambda
    // First, re-allocate everything
    for(i = 0; i < NumTuples; ++i) {
        gen.fill(alloc.allocate());
    }
    assert(! alloc.empty());
    assert(alloc.size() == NumTuples);
    i = 0;
    for_each<iterator>(alloc, [&alloc, &i](void* p) { alloc.free(p); ++i; });
    assert(i == NumTuples);
    assert(alloc.empty());
    // Iterating on empty chunks is a no-op
    for_each<iterator>(alloc, [&alloc, &i](void* p) { alloc.free(p); ++i; });
    assert(i == NumTuples);
    // expected compiler error: cannot mix const_iterator with non-const
    // lambda function. Uncomment next 2 lines to see
    /*for_each<typename IterableTableTupleChunks<Chunks, truth>::const_iterator>(
            alloc, [&alloc, &i](void* p) { alloc.free(p); ++i; });*/
}

template<typename Alloc, typename Compactible = typename Alloc::Compact> struct TrackedDeleter {
    Alloc& m_alloc;
    bool& m_freed;
    TrackedDeleter(Alloc& alloc, bool& freed) : m_alloc(alloc), m_freed(freed) {}
    void operator()(void* p) {
        m_freed |= m_alloc.free(p) != nullptr;
    }
};
// non-compacting chunks' free() is void. Not getting tested, see
// comments at caller.
template<typename Alloc> struct TrackedDeleter<Alloc, integral_constant<bool, false>> {
    Alloc& m_alloc;
    bool& m_freed;
    TrackedDeleter(Alloc& alloc, bool& freed) : m_alloc(alloc), m_freed(freed) {}
    void operator()(void* p) {
        m_freed = true;
        m_alloc.free(p);
    }
};

template<typename Tag>
class MaskedStringGen : public StringGen<TupleSize> {
    using super = StringGen<TupleSize>;
    size_t const m_skipped;
    unsigned char* mask(unsigned char* p, size_t state) const {
        if (state % m_skipped) {
            Tag::set(p);
        } else {
            Tag::reset(p);
        }
        return p;
    }
public:
    explicit MaskedStringGen(size_t skipped): m_skipped(skipped) {}
    unsigned char* query(size_t state) override {
        return mask(super::query(state), state);
    }
    inline bool same(void const* dst, size_t state) {
        static unsigned char buf[TupleSize+1];
        return ! memcmp(dst, mask(super::of(state, buf), state), TupleSize);
    }
};

template<typename Chunks, size_t NthBit>
void testCustomizedIterator(size_t skipped) {      // iterator that skips on every #skipped# elems
    using Tag = NthBitChecker<NthBit>;
    using const_iterator = typename IterableTableTupleChunks<Chunks, Tag>::const_iterator;
    using iterator = typename IterableTableTupleChunks<Chunks, Tag>::iterator;
    MaskedStringGen<Tag> gen(skipped);

    Chunks alloc(TupleSize);
    array<void*, NumTuples> addresses;
    size_t i;
    for(i = 0; i < NumTuples; ++i) {
        addresses[i] = gen.fill(alloc.allocate());
        assert(gen.same(addresses[i], i));
    }
    i = 0;
    auto const& alloc_cref = alloc;
    fold<const_iterator>(alloc_cref, [&i, &addresses, skipped](void const* p) {
        if (i % skipped == 0) {
            ++i;
        }
        if (Chunks::Compact::value) {
            assert(p == addresses[i]);
        }
        ++i;
    });
    assert(i == NumTuples);
}

// expression template used to apply variadic NthBitChecker
template<typename Tuple, size_t N> struct Apply {
    using Tag = typename tuple_element<N, Tuple>::type;
    Apply<Tuple, N-1> const m_next{};
    inline unsigned char* operator()(unsigned char* p) const {
        Tag::reset(p);
        return m_next(p);
    }
};
template<typename Tuple> struct Apply<Tuple, 0> {
    using Tag = typename tuple_element<0, Tuple>::type;
    inline unsigned char* operator()(unsigned char* p) const {
        Tag::reset(p);
        return p;
    }
};

/**
 * Test of HookedCompactingChunks using its RW iterator that
 * effects (GC) as snapshot process continues.
 */
template<typename Chunk, gc_policy pol>
void testHookedCompactingChunks() {
    using Hook = TxnPreHook<NonCompactingChunks<Chunk>, HistoryRetainTrait<pol>>;
    using Alloc = HookedCompactingChunks<Hook>;
    using Gen = StringGen<TupleSize>;
    using addresses_type = array<void const*, NumTuples>;
    Gen gen;
    Alloc alloc(TupleSize);
    auto const& alloc_cref = alloc;
    addresses_type addresses;
    assert(alloc.empty());
    size_t i;
    for(i = 0; i < NumTuples; ++i) {
        addresses[i] = alloc.allocate();
        memcpy(const_cast<void*>(addresses[i]), gen.get(), TupleSize);
    }
    auto iterp = alloc.template freeze<truth>();               // check later that snapshot iterator persists over > 10,000 txns
    using const_iterator = typename IterableTableTupleChunks<Alloc, truth>::const_iterator;
    using snapshot_iterator = typename IterableTableTupleChunks<Alloc, truth>::hooked_iterator;
    auto const verify_snapshot_const = [&alloc_cref]() {
        using const_snapshot_iterator = typename IterableTableTupleChunks<Alloc, truth>::const_hooked_iterator;
        size_t i = 0;
        fold<const_snapshot_iterator>(alloc_cref, [&i](void const* p) {
            assert(Gen::same(p, i++));
        });
        assert(i == NumTuples);
    };
    i = 0;
    fold<const_iterator>(alloc_cref, [&i](void const* p) { assert(Gen::same(p, i++)); });
    assert(i == NumTuples);
    // Operations during snapshot. The indexes used in each step
    // are absolute.
    // 1. Update: record 200-1200 <= 2200-3200
    // 2. Delete: record 100 - 900
    // 3. Batch Delete: record 910 - 999
    // 4. Insert: 500 records
    // 5. Update: 2000 - 2200 <= 0 - 200
    // 6. Delete: 3099 - 3599
    // 7. Randomized 5,000 operations

    // Step 1: update
    for (i = 200; i < 1200; ++i) {
        alloc.template update<truth>(const_cast<void*>(addresses[i]));
        memcpy(const_cast<void*>(addresses[i]), addresses[i + 2000], TupleSize);
    }
    verify_snapshot_const();

    // Step 2: deletion
    for (i = 100; i < 900; ++i) {
        remove_single(alloc, addresses[i]);
    }
    verify_snapshot_const();

    // Step 3: batch deletion
    alloc.remove_reserve(90);
    for (i = 909; i < 999; ++i) {
        alloc.remove_add(const_cast<void*>(addresses[i]));
    }
    alloc.template remove_force<truth>([](vector<pair<void*, void*>> const& entries){
        for_each(entries.begin(), entries.end(),
                 [](pair<void*, void*> const& entry) {memcpy(entry.first, entry.second, TupleSize);});
    });
    verify_snapshot_const();

    // Step 4: insertion
    for (i = 0; i < 500; ++i) {
        memcpy(alloc.allocate(), gen.get(), TupleSize);
    }
    verify_snapshot_const();

    // Step 5: update
    for (i = 2000; i < 2200; ++i) {
        alloc.template update<truth>(const_cast<void*>(addresses[i]));
        memcpy(const_cast<void*>(addresses[i]), gen.get(), TupleSize);
    }
    verify_snapshot_const();

    // Step 6: deletion
    for (i = 3099; i < 3599; ++i) {
        remove_single(alloc, addresses[i]);
    }
    verify_snapshot_const();

    // Step 7: randomized operations
    vector<void const*> latest;
    latest.reserve(NumTuples);
    fold<const_iterator>(alloc_cref, [&latest](void const* p) { latest.emplace_back(p); });
    // FIXME: a known bug when you replace rd() next line to seed 63558933, change upper bound on i to 4935,
    // and set a break point on case 2 when i == 4934. The
    // verifier errornouly reads out extra artificial data beyond
    // 8192 values.
    mt19937 rgen(rd());
    uniform_int_distribution<size_t> range(0, latest.size() - 1), changeTypes(0, 3), whole(0, NumTuples - 1);
    void const* p1 = nullptr;
    for (i = 0; i < 8000;) {
        size_t ii;
        vector<void*> tb_removed;
        switch(changeTypes(rgen)) {
            case 0:                                            // insertion
                p1 = memcpy(alloc.allocate(), gen.get(), TupleSize);
                break;
            case 1:                                            // deletion
                ii = range(rgen);
                if (latest[ii] == nullptr) {
                    continue;
                } else {
                    try {
                        remove_single(alloc, addresses[ii]);
                        latest[ii] = p1;
                        p1 = nullptr;
                    } catch (range_error const&) {             // if we tried to delete a non-existent address, pick another option
                        alloc.remove_reset();
                        continue;
                    }
                    break;
                }
            case 2:                                            // update
                ii = range(rgen);
                if (latest[ii] == nullptr) {
                    continue;
                } else {
                    alloc.template update<truth>(const_cast<void*>(latest[ii]));
                    memcpy(const_cast<void*>(latest[ii]), gen.get(), TupleSize);
                    latest[ii] = p1;
                    p1 = nullptr;
                }
                break;
            case 3:                                            // batch remove, using separate APIs
                tb_removed.clear();
                fold<typename IterableTableTupleChunks<Alloc, truth>::const_iterator>(
                        static_cast<Alloc const&>(alloc), [&tb_removed, &rgen, &whole](void const* p) {
                            if (static_cast<double>(whole(rgen)) / NumTuples < 0.01) {     // 1% chance getting picked for batch deletion
                                tb_removed.emplace_back(const_cast<void*>(p));
                            }
                        });
                alloc.remove_reserve(tb_removed.size());
                for_each(tb_removed.cbegin(), tb_removed.cend(),
                         [&alloc](void* p) { alloc.remove_add(p); });
                assert(alloc.template remove_force<truth>([](vector<pair<void*, void*>> const& entries){
                    for_each(entries.begin(), entries.end(),
                             [](pair<void*, void*> const& entry) {memcpy(entry.first, entry.second, TupleSize);});
                }).first == tb_removed.size());
            default:;
        }
        ++i;
    }
    verify_snapshot_const();
    // simulates actual snapshot process: memory clean up as we go
    i = 0;
    while (! iterp->drained()) {                               // explicit use of snapshot RW iterator
        void const* p = **iterp;
        assert(Gen::same(p, i++));
        alloc.release(p);                                      // snapshot of the tuple finished
        ++*iterp;
    }
    assert(i == NumTuples);
    alloc.template thaw<truth>();
}

template<typename Chunk, gc_policy pol>
void testHookedCompactingChunksBatchRemove_single1() {
    using HookAlloc = NonCompactingChunks<Chunk>;
    using Hook = TxnPreHook<HookAlloc, HistoryRetainTrait<pol>>;
    using Alloc = HookedCompactingChunks<Hook>;
    using Gen = StringGen<TupleSize>;
    using addresses_type = array<void const*, AllocsPerChunk>;
    Gen gen;
    Alloc alloc(TupleSize);
    auto const& alloc_cref = alloc;
    addresses_type addresses;
    assert(alloc.empty());
    size_t i;
    for(i = 0; i < AllocsPerChunk; ++i) {
        addresses[i] = alloc.allocate();
        memcpy(const_cast<void*>(addresses[i]), gen.get(), TupleSize);
    }
    alloc.template freeze<truth>();
    auto const verify_snapshot_const = [&alloc_cref]() {
        using const_snapshot_iterator = typename IterableTableTupleChunks<Alloc, truth>::const_hooked_iterator;
        size_t i = 0;
        fold<const_snapshot_iterator>(alloc_cref, [&i](void const* p) {
            assert(p == nullptr || Gen::same(p, i++));
        });
        assert(i == AllocsPerChunk);
    };
    alloc.remove_reserve(10);
    for (i = AllocsPerChunk - 10; i < AllocsPerChunk; ++i) {       // batch remove last 10 entries
        alloc.remove_add(const_cast<void*>(addresses[i]));
    }
    alloc.template remove_force<truth>([](vector<pair<void*, void*>> const& entries){
        for_each(entries.begin(), entries.end(),
                 [](pair<void*, void*> const& entry) {memcpy(entry.first, entry.second, TupleSize);});
    });
    verify_snapshot_const();
    alloc.template thaw<truth>();
}

template<typename Chunk, gc_policy pol>
void testHookedCompactingChunksBatchRemove_single2() {
    using HookAlloc = NonCompactingChunks<Chunk>;
    using Hook = TxnPreHook<HookAlloc, HistoryRetainTrait<pol>>;
    using Alloc = HookedCompactingChunks<Hook>;
    using Gen = StringGen<TupleSize>;
    using addresses_type = array<void const*, AllocsPerChunk>;
    Gen gen;
    Alloc alloc(TupleSize);
    auto const& alloc_cref = alloc;
    addresses_type addresses;
    assert(alloc.empty());
    size_t i;
    for(i = 0; i < AllocsPerChunk; ++i) {
        addresses[i] = alloc.allocate();
        memcpy(const_cast<void*>(addresses[i]), gen.get(), TupleSize);
    }
    alloc.template freeze<truth>();
    // verifies both const snapshot iterator, and destructuring iterator
    auto const verify_snapshot_const = [&alloc, &alloc_cref]() {
        using const_snapshot_iterator = typename IterableTableTupleChunks<Alloc, truth>::const_hooked_iterator;
        using snapshot_iterator = typename IterableTableTupleChunks<Alloc, truth>::hooked_iterator;
        size_t i = 0;
        fold<const_snapshot_iterator>(alloc_cref, [&i](void const* p) {
            assert(p == nullptr || Gen::same(p, i++));
        });
        assert(i == AllocsPerChunk);
        i = 0;
        for_each<snapshot_iterator>(alloc, [&i](void const* p) {
            assert(p == nullptr || Gen::same(p, i++));
        });
        assert(i == AllocsPerChunk);
    };
    alloc.remove_reserve(10);
    for (i = 0; i < 10; ++i) {       // batch remove last 10 entries
        alloc.remove_add(const_cast<void*>(addresses[i]));
    }
    for (i = 0; i < 10; ++i) {       // inserts another 10 different entries
        memcpy(alloc.allocate(), gen.get(), TupleSize);
    }
    alloc.template remove_force<truth>([](vector<pair<void*, void*>> const& entries){
        for_each(entries.begin(), entries.end(),
                 [](pair<void*, void*> const& entry) {memcpy(entry.first, entry.second, TupleSize);});
    });
    verify_snapshot_const();
    alloc.template thaw<truth>();
}

template<typename Chunk, gc_policy pol>
void testHookedCompactingChunksBatchRemove_single3() {         // correctness on txn view: single elem remove
    using HookAlloc = NonCompactingChunks<Chunk>;
    using Hook = TxnPreHook<HookAlloc, HistoryRetainTrait<pol>>;
    using Alloc = HookedCompactingChunks<Hook>;
    using Gen = StringGen<TupleSize>;
    using addresses_type = array<void const*, 10>;
    Gen gen;
    Alloc alloc(TupleSize);
    auto const& alloc_cref = alloc;
    addresses_type addresses;
    assert(alloc.empty());
    size_t i;
    for(i = 0; i < 10; ++i) {
        addresses[i] = alloc.allocate();
        memcpy(const_cast<void*>(addresses[i]), gen.get(), TupleSize);
    }
    alloc.template freeze<truth>();
    alloc.remove_reserve(1);
    alloc.remove_add(const_cast<void*>(addresses[4]));      // 9 => 4
    alloc.template remove_force<truth>([](vector<pair<void*, void*>> const& entries){
        for_each(entries.begin(), entries.end(),
                 [](pair<void*, void*> const& entry) {memcpy(entry.first, entry.second, TupleSize);});
    });
    i = 0;
    fold<typename IterableTableTupleChunks<Alloc, truth>::const_iterator>(
            alloc_cref,
            [&i](void const* p) {
                assert(Gen::same(p, i == 4 ? 9 : i));
                ++i;
            });
    assert(i == 9);
    alloc.template thaw<truth>();
}

template<typename Chunk, gc_policy pol>
void testHookedCompactingChunksBatchRemove_single4() {         // correctness on txn view: single elem table, remove the only row
    using HookAlloc = NonCompactingChunks<Chunk>;
    using Hook = TxnPreHook<HookAlloc, HistoryRetainTrait<pol>>;
    using Alloc = HookedCompactingChunks<Hook>;
    Alloc alloc(TupleSize);
    void* p = alloc.allocate();
    alloc.remove_reserve(1);
    alloc.remove_add(p);
    alloc.template remove_force<truth>([](vector<pair<void*, void*>> const& entries){
        for_each(entries.begin(), entries.end(),
                 [](pair<void*, void*> const& entry) {memcpy(entry.first, entry.second, TupleSize);});
    });
    assert(alloc.empty());
}

template<typename Chunk, gc_policy pol>
void testHookedCompactingChunksBatchRemove_multi1() {
    using HookAlloc = NonCompactingChunks<Chunk>;
    using Hook = TxnPreHook<HookAlloc, HistoryRetainTrait<pol>>;
    using Alloc = HookedCompactingChunks<Hook>;
    using Gen = StringGen<TupleSize>;
    using addresses_type = array<void const*, AllocsPerChunk * 3>;
    Gen gen;
    Alloc alloc(TupleSize);
    addresses_type addresses;
    assert(alloc.empty());
    size_t i;
    for(i = 0; i < AllocsPerChunk * 3; ++i) {
        addresses[i] = alloc.allocate();
        memcpy(const_cast<void*>(addresses[i]), gen.get(), TupleSize);
    }
    // verifies both const snapshot iterator, and destructuring iterator
    auto const verify_snapshot_const = [&alloc]() {
        auto const& alloc_cref = alloc;
        using const_snapshot_iterator = typename IterableTableTupleChunks<Alloc, truth>::const_hooked_iterator;
        using snapshot_iterator = typename IterableTableTupleChunks<Alloc, truth>::hooked_iterator;
        size_t i = 0;
        fold<const_snapshot_iterator>(alloc_cref, [&i](void const* p) {
            assert(p == nullptr || Gen::same(p, i++));
        });
        assert(i == AllocsPerChunk * 3);
        i = 0;
        for_each<snapshot_iterator>(alloc, [&i](void const* p) {
            assert(p == nullptr || Gen::same(p, i++));
        });
        assert(i == AllocsPerChunk * 3);
    };
    alloc.template freeze<truth>();

    auto iter = addresses.begin();
    alloc.remove_reserve(60);
    for (i = 0; i < 3; ++i) {
        for_each(iter, next(iter, 10), [&alloc](void const* p) {
            alloc.remove_add(const_cast<void*>(p));
        });
        advance(iter, AllocsPerChunk - 10);
        for_each(iter, next(iter, 10), [&alloc](void const* p) {
            alloc.remove_add(const_cast<void*>(p));
        });
        advance(iter, 10);
    }
    alloc.template remove_force<truth>([](vector<pair<void*, void*>> const& entries){
        for_each(entries.begin(), entries.end(),
                 [](pair<void*, void*> const& entry) {memcpy(entry.first, entry.second, TupleSize);});
    });
    verify_snapshot_const();
    alloc.template thaw<truth>();
}

template<typename Chunk, gc_policy pol>
void testHookedCompactingChunksBatchRemove_multi2() {
    using HookAlloc = NonCompactingChunks<Chunk>;
    using Hook = TxnPreHook<HookAlloc, HistoryRetainTrait<pol>>;
    using Alloc = HookedCompactingChunks<Hook>;
    using Gen = StringGen<TupleSize>;
    using addresses_type = array<void const*, NumTuples>;
    Gen gen;
    Alloc alloc(TupleSize);
    addresses_type addresses;
    assert(alloc.empty());
    size_t i;
    for(i = 0; i < NumTuples; ++i) {
        addresses[i] = alloc.allocate();
        memcpy(const_cast<void*>(addresses[i]), gen.get(), TupleSize);
    }
    // verifies both const snapshot iterator, and destructuring iterator
    auto const verify_snapshot_const = [&alloc]() {
        auto const& alloc_cref = alloc;
        using const_snapshot_iterator = typename IterableTableTupleChunks<Alloc, truth>::const_hooked_iterator;
        using snapshot_iterator = typename IterableTableTupleChunks<Alloc, truth>::hooked_iterator;
        size_t i = 0;
        fold<const_snapshot_iterator>(alloc_cref, [&i](void const* p) {
            assert(p == nullptr || Gen::same(p, i++));
        });
        assert(i == NumTuples);
    };
    alloc.template freeze<truth>();

    alloc.remove_reserve(NumTuples / 2);
    for(auto iter = addresses.cbegin();                             // remove every other
        iter != addresses.cend();) {
        alloc.remove_add(const_cast<void*>(*iter));
        if (++iter == addresses.cend()) {
            break;
        } else {
            ++iter;
        }
    }
    alloc.template remove_force<truth>([](vector<pair<void*, void*>> const& entries){
        for_each(entries.begin(), entries.end(),
                 [](pair<void*, void*> const& entry) {memcpy(entry.first, entry.second, TupleSize);});
    });
    verify_snapshot_const();
    alloc.template thaw<truth>();
}

template<typename Chunk, gc_policy pol> struct TestHookedCompactingChunks2 {
    inline void operator()() const {
        testHookedCompactingChunks<Chunk, pol>();
        // batch removal tests assume head-compacting direction
        testHookedCompactingChunksBatchRemove_single1<Chunk, pol>();
        testHookedCompactingChunksBatchRemove_single2<Chunk, pol>();
        testHookedCompactingChunksBatchRemove_single3<Chunk, pol>();
        testHookedCompactingChunksBatchRemove_single4<Chunk, pol>();
        testHookedCompactingChunksBatchRemove_multi1<Chunk, pol>();
        testHookedCompactingChunksBatchRemove_multi2<Chunk, pol>();
    }
};
template<typename Chunk> struct TestHookedCompactingChunks1 {
    static TestHookedCompactingChunks2<Chunk, gc_policy::never> const s1;
    static TestHookedCompactingChunks2<Chunk, gc_policy::always> const s2;
    static TestHookedCompactingChunks2<Chunk, gc_policy::batched> const s3;
    inline void operator()() const {
        s1();
        s2();
        s3();
    }
};
template<typename Chunk>TestHookedCompactingChunks2<Chunk, gc_policy::never>
const TestHookedCompactingChunks1<Chunk>::s1{};
template<typename Chunk>TestHookedCompactingChunks2<Chunk, gc_policy::always>
const TestHookedCompactingChunks1<Chunk>::s2{};
template<typename Chunk>TestHookedCompactingChunks2<Chunk, gc_policy::batched>
const TestHookedCompactingChunks1<Chunk>::s3{};

struct TestHookedCompactingChunks {
    static TestHookedCompactingChunks1<EagerNonCompactingChunk> const s1;
    static TestHookedCompactingChunks1<LazyNonCompactingChunk> const s2;
    inline void operator()() const {
        s1();
        s2();
    }
};
TestHookedCompactingChunks1<EagerNonCompactingChunk> const TestHookedCompactingChunks::s1{};
TestHookedCompactingChunks1<LazyNonCompactingChunk> const TestHookedCompactingChunks::s2{};

/**
 * Simulates how MP execution works: interleaved snapshot
 * advancement with txn in progress.
 */
template<typename Chunk, gc_policy pol>
void testInterleavedCompactingChunks() {
    using Alloc = HookedCompactingChunks<TxnPreHook<NonCompactingChunks<Chunk>, HistoryRetainTrait<pol>>>;
    using Gen = StringGen<TupleSize>;
    using addresses_type = array<void const*, NumTuples>;
    Gen gen;
    Alloc alloc(TupleSize);
    addresses_type addresses;
    assert(alloc.empty());
    size_t i;
    for(i = 0; i < NumTuples; ++i) {
        addresses[i] = alloc.allocate();
        memcpy(const_cast<void*>(addresses[i]), gen.get(), TupleSize);
    }
    alloc.template freeze<truth>();
    using const_iterator = typename IterableTableTupleChunks<Alloc, truth>::const_iterator;
    using snapshot_iterator = typename IterableTableTupleChunks<Alloc, truth>::hooked_iterator;
    using explicit_iterator_type = pair<size_t, snapshot_iterator>;
    explicit_iterator_type explicit_iter(0, snapshot_iterator::begin(alloc));
    auto verify = [] (explicit_iterator_type& iter) {
        assert(*iter.second == nullptr || Gen::same(*iter.second, iter.first));
    };
    auto advance_verify = [&verify](explicit_iterator_type& iter) {     // returns false on draining
        if (iter.second.drained()) {
            return false;
        }
        verify(iter);
        ++iter.first;
        ++iter.second;
        if (! iter.second.drained()) {
            verify(iter);
            return true;
        } else {
            return false;
        }
    };
    auto advances_verify = [&advance_verify] (explicit_iterator_type& iter, size_t advances) {
        for (size_t i = 0; i < advances; ++i) {
            if (! advance_verify(iter)) {
                return false;
            }
        }
        return true;
    };

    mt19937 rgen(rd());
    uniform_int_distribution<size_t> range(0, NumTuples - 1), changeTypes(0, 2),
            advanceTimes(0, 4);
    void const* p1 = nullptr;
    for (i = 0; i < 8000;) {
        size_t i1, i2;
        switch(changeTypes(rgen)) {
            case 0:                                            // insertion
                memcpy(const_cast<void*&>(p1) = alloc.allocate(), gen.get(), TupleSize);
                break;
            case 1:                                            // deletion
                i1 = range(rgen);
                if (addresses[i1] == nullptr) {
                    continue;
                } else {
                    try {
                        remove_single(alloc, addresses[i1]);
                        addresses[i1] = p1;
                        p1 = nullptr;
                    } catch (range_error const&) {                 // if we tried to delete a non-existent address, pick another option
                        alloc.remove_reset();
                        continue;
                    }
                    break;
                }
            case 2:                                            // update
            default:;
                i1 = range(rgen); i2 = range(rgen);
                if (i1 == i2 || addresses[i1] == nullptr || addresses[i2] == nullptr) {
                    continue;
                } else {
                    alloc.template update<truth>(const_cast<void*>(addresses[i2]));
                    memcpy(const_cast<void*>(addresses[i2]), addresses[i1], TupleSize);
                    addresses[i2] = p1;
                    p1 = nullptr;
                }
        }
        ++i;
        if (! advances_verify(explicit_iter, advanceTimes(rgen))) {
            break;                                             // verified all snapshot iterators
        }
    }
    alloc.template thaw<truth>();
}

template<typename Chunk, gc_policy pol> struct TestInterleavedCompactingChunks2 {
    inline void operator()() const {
        for (size_t i = 0; i < 16; ++i) {                      // repeat randomized test
            testInterleavedCompactingChunks<Chunk, pol>();
        }
    }
};
template<typename Chunk> struct TestInterleavedCompactingChunks1 {
    static TestInterleavedCompactingChunks2<Chunk, gc_policy::never> const s1;
    static TestInterleavedCompactingChunks2<Chunk, gc_policy::always> const s2;
    static TestInterleavedCompactingChunks2<Chunk, gc_policy::batched> const s3;
    inline void operator()() const {
        s1();
        s2();
        s3();
    }
};
template<typename Chunk>TestInterleavedCompactingChunks2<Chunk, gc_policy::never>
const TestInterleavedCompactingChunks1<Chunk>::s1{};
template<typename Chunk>TestInterleavedCompactingChunks2<Chunk, gc_policy::always>
const TestInterleavedCompactingChunks1<Chunk>::s2{};
template<typename Chunk>TestInterleavedCompactingChunks2<Chunk, gc_policy::batched>
const TestInterleavedCompactingChunks1<Chunk>::s3{};

struct TestInterleavedCompactingChunks {
    static TestInterleavedCompactingChunks1<EagerNonCompactingChunk> const s1;
    static TestInterleavedCompactingChunks1<LazyNonCompactingChunk> const s2;
    inline void operator()() const {
        s1();
        s2();
    }
};
TestInterleavedCompactingChunks1<EagerNonCompactingChunk> const TestInterleavedCompactingChunks::s1{};
TestInterleavedCompactingChunks1<LazyNonCompactingChunk> const TestInterleavedCompactingChunks::s2{};

template<typename Chunk, gc_policy pol, CompactingChunks::remove_direction dir>
void testRemovesFromEnds(size_t batch) {
    using Alloc = HookedCompactingChunks<TxnPreHook<NonCompactingChunks<Chunk>, HistoryRetainTrait<pol>>>;
    using Gen = StringGen<TupleSize>;
    using addresses_type = array<void const*, NumTuples>;
    Alloc alloc(TupleSize);
    addresses_type addresses;
    Gen gen;
    assert(alloc.empty());
    size_t i;
    for(i = 0; i < NumTuples; ++i) {
        memcpy(const_cast<void*>(addresses[i] = alloc.allocate()), gen.get(), TupleSize);
    }
    assert(alloc.size() == NumTuples);
    // remove tests
    if (dir == CompactingChunks::remove_direction::from_head) {      // remove from head
        for (i = 0; i < batch; ++i) {
            alloc.remove(dir, addresses[i]);
        }
        alloc.remove(dir, nullptr);     // completion
        assert(alloc.size() == NumTuples - batch);
        fold<typename IterableTableTupleChunks<Alloc, truth>::const_iterator>(
                static_cast<Alloc const&>(alloc),
                [&i](void const* p) { assert(Gen::same(p, i++)); });
        assert(i == NumTuples);
        alloc.template freeze<truth>();
        try {                                                  // not allowed when frozen
            alloc.remove(dir, nullptr);
            assert(false);                                     // should have failed
        } catch (logic_error const& e) {
            assert(! strcmp(e.what(),
                            "HookedCompactingChunks::remove(dir, ptr): Cannot remove from head when frozen"));
            alloc.template thaw<truth>();
        }
    } else {                                                   // remove from tail
        for (i = NumTuples - 1; i >= NumTuples - batch && i < NumTuples; --i) {
            alloc.remove(dir, addresses[i]);
        }
        assert(alloc.size() == NumTuples - batch);
        i = 0;
        until<typename IterableTableTupleChunks<Alloc, truth>::const_iterator>(
                static_cast<Alloc const&>(alloc),
                [batch, &i](void const* p) {
                    assert(Gen::same(p, i));
                    return ++i >= NumTuples - batch;
                });
        assert(i == NumTuples - batch);
        // remove everything, add something back
        if (! alloc.empty()) {
            for (--i; i > 0; --i) {
                alloc.remove(dir, addresses[i]);
            }
            alloc.remove(dir, addresses[0]);
            assert(alloc.empty());
        }
        for (i = 0; i < NumTuples; ++i) {
            memcpy(alloc.allocate(), gen.get(), TupleSize);
        }
        fold<typename IterableTableTupleChunks<Alloc, truth>::const_iterator>(
                static_cast<Alloc const&>(alloc),
                [&i](void const* p) {
                    assert(Gen::same(p, i++));
                });
        assert(i == NumTuples * 2);
    }
}

template<typename Chunk, gc_policy pol, CompactingChunks::remove_direction dir>
struct TestRemovesFromEnds3 {
    inline void operator()() const {
        testRemovesFromEnds<Chunk, pol, dir>(0);
        testRemovesFromEnds<Chunk, pol, dir>(NumTuples);
        testRemovesFromEnds<Chunk, pol, dir>(AllocsPerChunk - 1);
        testRemovesFromEnds<Chunk, pol, dir>(AllocsPerChunk);
        testRemovesFromEnds<Chunk, pol, dir>(AllocsPerChunk + 1);
        testRemovesFromEnds<Chunk, pol, dir>(AllocsPerChunk * 15 + 2);
    }
};

template<typename Chunk, gc_policy pol> struct TestRemovesFromEnds2 {
    static TestRemovesFromEnds3<Chunk, pol, CompactingChunks::remove_direction::from_head> const s1;
    static TestRemovesFromEnds3<Chunk, pol, CompactingChunks::remove_direction::from_tail> const s2;
    inline void operator()() const {
        s1();
        s2();
    }
};
template<typename Chunk, gc_policy pol>
TestRemovesFromEnds3<Chunk, pol, CompactingChunks::remove_direction::from_head> const TestRemovesFromEnds2<Chunk, pol>::s1{};
template<typename Chunk, gc_policy pol>
TestRemovesFromEnds3<Chunk, pol, CompactingChunks::remove_direction::from_tail> const TestRemovesFromEnds2<Chunk, pol>::s2{};

template<typename Chunk> struct TestRemovesFromEnds1 {
    static TestRemovesFromEnds2<Chunk, gc_policy::never> const s1;
    static TestRemovesFromEnds2<Chunk, gc_policy::always> const s2;
    static TestRemovesFromEnds2<Chunk, gc_policy::batched> const s3;
    inline void operator()() const {
        s1();
        s2();
        s3();
    }
};
template<typename Chunk> TestRemovesFromEnds2<Chunk, gc_policy::never> const TestRemovesFromEnds1<Chunk>::s1{};
template<typename Chunk> TestRemovesFromEnds2<Chunk, gc_policy::always> const TestRemovesFromEnds1<Chunk>::s2{};
template<typename Chunk> TestRemovesFromEnds2<Chunk, gc_policy::batched> const TestRemovesFromEnds1<Chunk>::s3{};

struct TestRemovesFromEnds {
    inline void operator()() const {
        TestRemovesFromEnds1<EagerNonCompactingChunk>()();
        TestRemovesFromEnds1<LazyNonCompactingChunk>()();
    }
};


string address(void const* p) {
    ostringstream oss;
    oss<<p;
    return oss.str();
}


class finalize_verifier {
    using Gen = StringGen<TupleSize>;
    size_t const m_total;
    unordered_set<size_t> m_states{};
public:
    finalize_verifier(size_t n) : m_total(n) {
        assert(m_total > 0);
        m_states.reserve(m_total);
    }
    void reset(size_t n) {
        assert(n > 0);
        const_cast<size_t&>(m_total) = n;
        m_states.clear();
        m_states.reserve(n);
    }
    void operator()(void const* p) {
        assert(m_states.emplace(Gen::of(reinterpret_cast<unsigned char const*>(p))).second);
    }
    unordered_set<size_t> const& seen() const {
        return m_states;
    }
    bool ok(size_t start) const {
        if (m_states.size() != m_total) {
            return false;
        } else {
            auto const bounds = minmax_element(m_states.cbegin(), m_states.cend());
            return *bounds.first == start &&
                   *bounds.first + m_total - 1 == *bounds.second;
        }
    }
};

void* tuple_memcpy(void* dst, void const* src) {
    return memcpy(dst, src, TupleSize);
}


TEST_F(TableTupleAllocatorTest, TestFinalizer_FrozenRemovals) {
    unordered_set<void*> rowTracker(NumTuples*2);
    // test batch removal when frozen, then thaw.
    auto const cleaner = [this, &rowTracker] (void const* p) {
        ASSERT_EQ(rowTracker.erase(const_cast<void*>(p)), 1);
    };
    auto const copier = [this, &rowTracker] (void* fresh, void const* dest) {
        memcpy(fresh, dest, TupleSize);
        bool inserted = rowTracker.insert(fresh).second;
        if (!inserted) {
            FAIL("copier insert failed");
        }
        return fresh;
    };

    for (int remove1Forwards = 0; remove1Forwards < 2; remove1Forwards++) {
        cout << "\n";
        cout.flush();
        for (int removeRowCountPass1 = 257; removeRowCountPass1 < NumTuples; removeRowCountPass1++) {
            //           if (removeRowCountPass1 % 10 == 0) {
            cout << removeRowCountPass1 << ",";
            cout.flush();
            //           }
            for (int testloop = 0; testloop < NumTuples; testloop++) {
                {
                    using Alloc = HookedCompactingChunks<TxnPreHook<NonCompactingChunks<EagerNonCompactingChunk>, HistoryRetainTrait<gc_policy::always>>>;
                    using Gen = StringGen<TupleSize>;
                    Gen gen;
                    finalize_verifier verifier{NumTuples};
                    Alloc alloc(TupleSize, {cleaner, copier});
                    auto chunksize = alloc.chunkSize();
                    auto rowcount = chunksize / alloc.tupleSize();
                    rowcount += rowcount + 1;
                    array<void const *, NumTuples> addresses;
                    size_t i;
                    void *lostRowAddr = nullptr;
                    if (!rowTracker.empty()) {
                        cout << "Test " << (remove1Forwards == 0 ? "Forward" : "Reverse") << " Delete "
                             << removeRowCountPass1 <<
                             " after snapshoting " << testloop << " rows has rows from prior loop";
                        cout.flush();
                        exit(-1);
                    }
                    for (i = 0; i < NumTuples; ++i) {
                        if (i == 287) {
                            cout << "address of lost row: ";
                        }
                        void *allocatedAddr = gen.fill(alloc.allocate());
                        if (i == 287) {
                            lostRowAddr = allocatedAddr;
                            cout << allocatedAddr << "\n";
                        }
                        addresses[i] = allocatedAddr;
                        rowTracker.insert(allocatedAddr).second;
                    }
                    auto iter = alloc.template freeze<truth>();
                    for (int preStream = 0; preStream < testloop; preStream++) {
                        ASSERT_FALSE(iter->drained());
                        verifier(**iter);
                        ++*iter;
                    }
                    alloc.remove_reserve(removeRowCountPass1);
                    if (remove1Forwards == 0) {
                        for (i = NumTuples - 1; i >= NumTuples - removeRowCountPass1; i--) {
                            alloc.remove_add(const_cast<void *>(addresses[i]));
                        }
                    } else {
                        for (i = 0; i < removeRowCountPass1; i++) {
                            alloc.remove_add(const_cast<void *>(addresses[i]));
                        }
                    }
                    pair<size_t, size_t> rslt = alloc.template remove_force<truth>(
                            [this, &rowTracker, &lostRowAddr](vector<pair<void *, void *>> const &entries) noexcept {
                                for_each(entries.begin(), entries.end(),
                                         [&rowTracker, &lostRowAddr](pair<void *, void *> const &entry) {
                                             if (entry.first == lostRowAddr) {
                                                 cout << "moving new row into the target addr\n";
                                                 cout.flush();
                                             }
                                             if (entry.second == lostRowAddr) {
                                                 cout << "stepping on row with the target addr\n";
                                                 cout.flush();
                                             }
                                             auto erased = rowTracker.erase(entry.second) == 1;
                                             auto inserted = rowTracker.insert(entry.first).second;
                                             if (!erased || !inserted) {
                                                 cout << "remove_force failed";
//                                     exit(-1);
                                             }
                                             memcpy(entry.first, entry.second, TupleSize);
                                         });
                            });

                    ASSERT_EQ(removeRowCountPass1, rslt.first);
                    for (i = testloop; i < NumTuples; i++) {
                        ASSERT_FALSE(iter->drained());
                        verifier(**iter);
                        ++*iter;
                    }
                    ASSERT_TRUE(iter->drained());

                    alloc.template thaw<truth>();
                    // At thaw time, those copies in the batch should be removed
                    // (and finalized before being deallocated), since snapshot iterator needs them
                    ASSERT_EQ(NumTuples, verifier.seen().size());
                    for (i = 0; i < NumTuples; i++) {
                        ASSERT_NE(verifier.seen().cend(), verifier.seen().find(i));
                    }
//                    ASSERT_EQ(rowTracker.size(), NumTuples - removeRowCountPass1);
                }
                while (!rowTracker.empty()) {
                    void* lostRow = *(rowTracker.cbegin());
                    cout << "Lost Row with address (" << lostRow << "): " << StringGen<TupleSize>::hex(lostRow);
                    cout.flush();
                    rowTracker.erase(rowTracker.begin());
//                    exit(-1);
                }
            }
        }
    }
}


#endif

int main() {
    return TestSuite::globalInstance()->runAll();
}
