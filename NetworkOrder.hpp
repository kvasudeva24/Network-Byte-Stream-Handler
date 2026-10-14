#include <cstdint>
#include <cstddef>

inline constexpr size_t USERNAME_LEN = 16;
inline constexpr size_t SYMBOL_LEN = 8;

#pragma pack(push, 1)
typedef struct NetworkOrder {
    uint64_t       order_id;
    char           username[USERNAME_LEN];
    char           symbol[SYMBOL_LEN];
    uint8_t        side;
    uint64_t       price;
    std::uint32_t  quantity;
    std::uint64_t  timestamp;
    std::uint64_t  checksum;
    std::uint8_t   padding[3];
} NetworkOrder;
#pragma pack(pop)