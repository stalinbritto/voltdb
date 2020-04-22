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

void testBatchDeleteScenarios(bool removeFromTail, bool snapshot,
                              int startRemoveRow, int endRemoveRow,
                              int startSnapshotRow, int endSnapshotRow) {
    unordered_set<void*> rowTracker(NumTuples*2);
    // test batch removal when frozen, then thaw.
    auto const cleaner = [&rowTracker] (void const* p) {
        int rowsFound = rowTracker.erase(const_cast<void*>(p));
        if (rowsFound != 1) {
            cout << "Tried to clean a row that was not found in the tracker\n";
            throw std::exception();
        }
    };
    auto const copier = [&rowTracker] (void* fresh, void const* dest) {
        memcpy(fresh, dest, TupleSize);
        bool inserted = rowTracker.insert(fresh).second;
        if (!inserted) {
            cout << "Deep copy stepped on another row before it was cleaned\n";
            throw std::exception();
        }
        return fresh;
    };

    if (!snapshot) {
        startSnapshotRow = endSnapshotRow = 0;
    }
    cout << "\n";
    cout.flush();
    for (int removeRowCountPass1 = startRemoveRow; removeRowCountPass1 <= endRemoveRow; removeRowCountPass1++) {
        if (removeRowCountPass1 % 10 == 0) {
            cout << removeRowCountPass1 << ",";
            cout.flush();
        }
        for (int testloop = startSnapshotRow; testloop <= endSnapshotRow; testloop++) {
            {
                using Alloc = HookedCompactingChunks<TxnPreHook<NonCompactingChunks<EagerNonCompactingChunk>, HistoryRetainTrait<gc_policy::always>>>;
                using SnapshotIterator = IterableTableTupleChunks<Alloc, truth>::hooked_iterator;
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
                    cout << "Test " << (removeFromTail ? "Reverse" : "Forward") << " Delete "
                         << removeRowCountPass1 <<
                         " after snapshoting " << testloop << " rows has rows from prior loop";
                    cout.flush();
                    throw std::exception();
                }
                for (i = 0; i < NumTuples; ++i) {
                    if (i == NumTuples+1) {
                        cout << "address of lost row: ";
                    }
                    void *allocatedAddr = gen.fill(alloc.allocate());
                    if (i == NumTuples+1) {
                        lostRowAddr = allocatedAddr;
                        cout << allocatedAddr << "\n";
                    }
                    addresses[i] = allocatedAddr;
                    rowTracker.insert(allocatedAddr).second;
                }
                std::shared_ptr<SnapshotIterator> iter;
                if (snapshot) {
                    iter = alloc.template freeze<truth>();
                    for (int preStream = 0; preStream < testloop; preStream++) {
                        if (iter->drained()) {
                            cout << "Premature snapshot drain state before batch delete\n";
                            throw std::exception();
                        }
                        verifier(**iter);
                        ++*iter;
                    }
                }
                alloc.remove_reserve(removeRowCountPass1);
                if (removeFromTail) {
                    for (i = NumTuples - 1; i >= NumTuples - removeRowCountPass1; i--) {
                        alloc.remove_add(const_cast<void *>(addresses[i]));
                    }
                } else {
                    for (i = 0; i < removeRowCountPass1; i++) {
                        alloc.remove_add(const_cast<void *>(addresses[i]));
                    }
                }
                pair<size_t, size_t> rslt = alloc.template remove_force<truth>(
                    [&rowTracker, &lostRowAddr](vector<pair<void *, void *>> const &entries) noexcept {
                        for_each(entries.begin(), entries.end(),
                                [&rowTracker, &lostRowAddr](pair<void *, void *> const &entry) {
//                            if (entry.first == lostRowAddr) {
//                                 cout << "moving new row into the target addr\n";
//                                 cout.flush();
//                            }
//                            if (entry.second == lostRowAddr) {
//                                cout << "stepping on row with the target addr\n";
//                                cout.flush();
//                            }
                            auto erased = rowTracker.erase(entry.second) == 1;
                            auto inserted = rowTracker.insert(entry.first).second;
                            if (!erased || !inserted) {
                                cout << "remove_force failed";
                                throw std::exception();
                            }
                            memcpy(entry.first, entry.second, TupleSize);
                        });
                });

                if (removeRowCountPass1 != rslt.first) {
                    cout << "Unexpected remove batch count from remove_force\n";
                    throw std::exception();
                }
                if (snapshot) {
                    for (i = testloop; i < NumTuples; i++) {
                        if (iter->drained()) {
                            cout << "Premature snapshot drain state after batch delete\n";
                            throw std::exception();
                        }
                        verifier(**iter);
                        ++*iter;
                    }
                    if (!iter->drained()) {
                        cout << "Snapshot should have been drained\n";
                        throw std::exception();
                    }

                    alloc.template thaw<truth>();

                    // At thaw time, those copies in the batch should be removed
                    // (and finalized before being deallocated), since snapshot iterator needs them
                    for (i = 0; i < NumTuples; i++) {
                        if (verifier.seen().find(i) == verifier.seen().cend()) {
                            cout << "Row " << i << " was not found in the snapshot\n";
                            throw std::exception();
                        }
                    }
                }

                if (rowTracker.size() != NumTuples - removeRowCountPass1) {
                    cout << "The number of tuples being tracked after thaw (" << rowTracker.size() <<
                         ") is not equal the the number expected (" << (NumTuples - removeRowCountPass1) << ")\n";
                    cout.flush();
//                    throw std::exception();
                }
            }
            // Verify that after the allocator is out of scope all rows are deallocated.
            if (!rowTracker.empty()) {
                do {
                    void *lostRow = *(rowTracker.cbegin());
                    cout << "Lost Row with address (" << lostRow << "): " << StringGen<TupleSize>::hex(lostRow);
                    cout.flush();
                    rowTracker.erase(rowTracker.begin());
                } while (!rowTracker.empty());
                throw std::exception();
            }
        }
    }
}


TEST_F(TableTupleAllocatorTest, TestNonOverlappingTailBatchRemove) {
    try {
        testBatchDeleteScenarios(true, true, 256, 256, 0, 511);
    }
    catch (std::exception e) {
        FAIL("Unexpected Termination");
    }
}

TEST_F(TableTupleAllocatorTest, TestOverlappedBatchRemove) {
    try {
        testBatchDeleteScenarios(true, true, 257, 257, 0, 0);
    }
    catch (std::exception e) {
        FAIL("Unexpected Termination");
    }
}

TEST_F(TableTupleAllocatorTest, TestSingleChunkBatchRemove) {
    try {
        testBatchDeleteScenarios(false, true, 48, 48, 0, 511);
    }
    catch (std::exception e) {
        FAIL("Unexpected Termination");
    }
}

TEST_F(TableTupleAllocatorTest, TestMultiChunkBatchRemove) {
    try {
        testBatchDeleteScenarios(false, true, 49, 49, 0, 0);
    }
    catch (std::exception e) {
        FAIL("Unexpected Termination");
    }
}

//TEST_F(TableTupleAllocatorTest, TestFinalizer_FrozenRemovals) {
//    unordered_set<void*> rowTracker(NumTuples*2);
//    // test batch removal when frozen, then thaw.
//    auto const cleaner = [this, &rowTracker] (void const* p) {
//        ASSERT_EQ(rowTracker.erase(const_cast<void*>(p)), 1);
//    };
//    auto const copier = [this, &rowTracker] (void* fresh, void const* dest) {
//        memcpy(fresh, dest, TupleSize);
//        bool inserted = rowTracker.insert(fresh).second;
//        if (!inserted) {
//            FAIL("copier insert failed");
//        }
//        return fresh;
//    };
//
//    for (int remove1Forwards = 0; remove1Forwards < 2; remove1Forwards++) {
//        cout << "\n";
//        cout.flush();
//        for (int removeRowCountPass1 = 0; removeRowCountPass1 < NumTuples; removeRowCountPass1++) {
//            if (removeRowCountPass1 % 10 == 0) {
//                cout << removeRowCountPass1 << ",";
//                cout.flush();
//            }
//            for (int testloop = 0; testloop < NumTuples; testloop++) {
//                {
//                    using Alloc = HookedCompactingChunks<TxnPreHook<NonCompactingChunks<EagerNonCompactingChunk>, HistoryRetainTrait<gc_policy::always>>>;
//                    using Gen = StringGen<TupleSize>;
//                    Gen gen;
//                    finalize_verifier verifier{NumTuples};
//                    Alloc alloc(TupleSize, {cleaner, copier});
//                    auto chunksize = alloc.chunkSize();
//                    auto rowcount = chunksize / alloc.tupleSize();
//                    rowcount += rowcount + 1;
//                    array<void const *, NumTuples> addresses;
//                    size_t i;
//                    void *lostRowAddr = nullptr;
//                    if (!rowTracker.empty()) {
//                        cout << "Test " << (remove1Forwards == 0 ? "Forward" : "Reverse") << " Delete "
//                             << removeRowCountPass1 <<
//                             " after snapshoting " << testloop << " rows has rows from prior loop";
//                        cout.flush();
//                        exit(-1);
//                    }
//                    for (i = 0; i < NumTuples; ++i) {
//                        if (i == NumTuples+1) {
//                            cout << "address of lost row: ";
//                        }
//                        void *allocatedAddr = gen.fill(alloc.allocate());
//                        if (i == NumTuples+1) {
//                            lostRowAddr = allocatedAddr;
//                            cout << allocatedAddr << "\n";
//                        }
//                        addresses[i] = allocatedAddr;
//                        rowTracker.insert(allocatedAddr).second;
//                    }
//                    auto iter = alloc.template freeze<truth>();
//                    for (int preStream = 0; preStream < testloop; preStream++) {
//                        ASSERT_FALSE(iter->drained());
//                        verifier(**iter);
//                        ++*iter;
//                    }
//                    alloc.remove_reserve(removeRowCountPass1);
//                    if (remove1Forwards == 0) {
//                        for (i = NumTuples - 1; i >= NumTuples - removeRowCountPass1; i--) {
//                            alloc.remove_add(const_cast<void *>(addresses[i]));
//                        }
//                    } else {
//                        for (i = 0; i < removeRowCountPass1; i++) {
//                            alloc.remove_add(const_cast<void *>(addresses[i]));
//                        }
//                    }
//                    pair<size_t, size_t> rslt = alloc.template remove_force<truth>(
//                            [this, &rowTracker, &lostRowAddr](vector<pair<void *, void *>> const &entries) noexcept {
//                                for_each(entries.begin(), entries.end(),
//                                         [&rowTracker, &lostRowAddr](pair<void *, void *> const &entry) {
//                                             if (entry.first == lostRowAddr) {
//                                                 cout << "moving new row into the target addr\n";
//                                                 cout.flush();
//                                             }
//                                             if (entry.second == lostRowAddr) {
//                                                 cout << "stepping on row with the target addr\n";
//                                                 cout.flush();
//                                             }
//                                             auto erased = rowTracker.erase(entry.second) == 1;
//                                             auto inserted = rowTracker.insert(entry.first).second;
//                                             if (!erased || !inserted) {
//                                                 cout << "remove_force failed";
////                                     exit(-1);
//                                             }
//                                             memcpy(entry.first, entry.second, TupleSize);
//                                         });
//                            });
//
//                    ASSERT_EQ(removeRowCountPass1, rslt.first);
//                    for (i = testloop; i < NumTuples; i++) {
//                        ASSERT_FALSE(iter->drained());
//                        verifier(**iter);
//                        ++*iter;
//                    }
//                    ASSERT_TRUE(iter->drained());
//
//                    alloc.template thaw<truth>();
//                    // At thaw time, those copies in the batch should be removed
//                    // (and finalized before being deallocated), since snapshot iterator needs them
//                    ASSERT_EQ(NumTuples, verifier.seen().size());
//                    for (i = 0; i < NumTuples; i++) {
//                        ASSERT_NE(verifier.seen().cend(), verifier.seen().find(i));
//                    }
//
//                    if (rowTracker.size() != NumTuples - removeRowCountPass1) {
//                        cout << "The number of tuples being tracked after thaw (" << rowTracker.size() <<
//                                ") is not equal the the number expected (" << (NumTuples - removeRowCountPass1) << ")\n";
//                        cout.flush();
////                        exit(-1);
//                    }
//                }
//                // Verify that after the allocator is out of scope all rows are deallocated.
//                while (!rowTracker.empty()) {
//                    void* lostRow = *(rowTracker.cbegin());
//                    cout << "Lost Row with address (" << lostRow << "): " << StringGen<TupleSize>::hex(lostRow);
//                    cout.flush();
//                    rowTracker.erase(rowTracker.begin());
//                    exit(-1);
//                }
//            }
//        }
//    }
//}


#endif

int main() {
    return TestSuite::globalInstance()->runAll();
}
