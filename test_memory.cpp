#include <gtest/gtest.h>
#include <sys/mman.h>
#include <cstdint>
#include <cstring>
#include <cstdlib>

static inline size_t round_up_64(size_t sz) {
    return (sz + 63) & ~size_t(63);
}

void* allocate_pages(size_t size) {
    void* ptr = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0);
    if (ptr == MAP_FAILED) {
        ptr = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    }
    return ptr;
}

void free_pages(void* ptr, size_t size) {
    if (ptr && ptr != MAP_FAILED) {
        munmap(ptr, size);
    }
}

struct RibSoA {
    uint32_t* prefix_id = nullptr;
    uint32_t* next_hop = nullptr;
    size_t size = 0;
    size_t capacity = 0;

    void allocate(size_t cap) {
        capacity = cap;
        prefix_id = (uint32_t*)allocate_pages(capacity * sizeof(uint32_t));
        next_hop = (uint32_t*)allocate_pages(capacity * sizeof(uint32_t));
        size = 0;
    }

    void grow() {
        size_t new_cap = capacity == 0 ? 16 : capacity * 2;
        uint32_t* new_pfx = (uint32_t*)allocate_pages(new_cap * sizeof(uint32_t));
        uint32_t* new_nh = (uint32_t*)allocate_pages(new_cap * sizeof(uint32_t));

        if (size > 0) {
            memcpy(new_pfx, prefix_id, size * sizeof(uint32_t));
            memcpy(new_nh, next_hop, size * sizeof(uint32_t));
        }

        free_pages(prefix_id, capacity * sizeof(uint32_t));
        free_pages(next_hop, capacity * sizeof(uint32_t));

        prefix_id = new_pfx;
        next_hop = new_nh;
        capacity = new_cap;
    }

    void push(uint32_t pid, uint32_t nh) {
        if (size >= capacity) grow();
        prefix_id[size] = pid;
        next_hop[size] = nh;
        size++;
    }

    void free_all() {
        if (capacity > 0) {
            free_pages(prefix_id, capacity * sizeof(uint32_t));
            free_pages(next_hop, capacity * sizeof(uint32_t));
            capacity = 0;
            size = 0;
        }
    }
};

TEST(MemorySuite, HugePageFallbackAndFree) {
    void* ptr = allocate_pages(1024);
    EXPECT_NE(ptr, MAP_FAILED) << "Memory allocation failed.";
    free_pages(ptr, 1024);
}

TEST(MemorySuite, AlignedAllocStandardSize) {
    size_t elements = 10;
    size_t size_bytes = round_up_64(elements * sizeof(uint64_t));
    EXPECT_EQ(size_bytes % 64, 0u);
    void* ptr = aligned_alloc(64, size_bytes);
    EXPECT_NE(ptr, nullptr);
    EXPECT_EQ(reinterpret_cast<uintptr_t>(ptr) % 64, 0u);
    free(ptr);
}

TEST(MemorySuite, SoADynamicGrowthAndCapacity) {
    RibSoA test_rib;
    test_rib.allocate(2);

    for (uint32_t i = 0; i < 1000; ++i) {
        test_rib.push(100000 + i, i);
    }

    EXPECT_EQ(test_rib.size, 1000u);
    EXPECT_GE(test_rib.capacity, 1000u);
    EXPECT_EQ(test_rib.prefix_id[999], 100999u);
    EXPECT_EQ(test_rib.next_hop[999], 999u);

    test_rib.free_all();
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
