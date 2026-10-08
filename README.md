# Network Byte Stream Handler

📖 Background reading: [What is a socket?](https://www.geeksforgeeks.org/computer-networks/socket-in-computer-network/) · [What is endianness? Why does it matter?](https://www.geeksforgeeks.org/dsa/little-and-big-endian-mystery/)

A handler for a stream of orders arriving over a socket. A sender writes orders in network byte order, each with a checksum. The receiver reads the stream, verifies each checksum, converts the fields back to host byte order, and passes valid orders on.

---

## Background

When a person wants to submit a trade, they send data to an exchange, and that happens over a **socket**. On the exchange's side, the data sits in a fixed-size kernel buffer (around 128–256 KB) until the receiver calls `read()` and asks for a certain number of bytes.

How many bytes to ask for matters:

- **Reading one byte at a time** costs a system call per byte. Each call switches into kernel mode, copies the data, and switches back, which adds enormous overhead.
- **Draining slowly hurts the sender.** Assume both sides use TCP; UDP or another protocol is an implementation detail. TCP has flow control: the receiver advertises how much space it has left. A slow reader lets the buffer fill up, so the advertised window shrinks and the sender is throttled.

---

## Wire format: `NetworkOrder`

Each order is a fixed-size, **packed** record. All multi-byte integers are big-endian (network byte order) on the wire.

```cpp
#include <cstddef>
#include <cstdint>

inline constexpr std::size_t USERNAME_LEN = 16;
inline constexpr std::size_t SYMBOL_LEN   = 8;

#pragma pack(push, 1)          // no compiler padding: both hosts agree on layout
struct NetworkOrder {
    std::uint64_t order_id;
    char          username[USERNAME_LEN];   // fixed-length, NUL-padded
    char          symbol[SYMBOL_LEN];
    std::uint8_t  side;                     // 0 = buy, 1 = sell
    std::int64_t  price;                    // integer ticks, see below
    std::uint32_t quantity;
    std::uint64_t timestamp;
    std::uint64_t checksum;
    std::uint8_t  padding[3];               // always zero; rounds the order up to 64 bytes
};
#pragma pack(pop)

static_assert(sizeof(NetworkOrder) == 64, "NetworkOrder must be 64 bytes");
```

- **The struct is packed.** Two hosts compile the same struct, so we need to make sure its layout stays identical on both sides. Packing means no padding is inserted between fields, and every byte belongs to a field.
- **There are 3 bytes of explicit padding after the checksum.** They bring the order from 61 to 64 bytes. If a field of an order is split between two cache lines, reading it can incur a longer second load. Padding to 64 bytes means orders stored back to back don't drift across those boundaries. The padding bytes are always zero and are not part of the checksum.
- **Strings are fixed `char` arrays** copied inline. They have no byte order, so they are never swapped.
- **Prices are always integers** (ticks, e.g. `$101.25` → `10125`). Floating-point math is a poor fit for exchanges for three reasons. It takes more cycles. It leaves imprecise remainders, since values like `0.1` can't be represented exactly. And there is no standard way to send a float over the wire in network byte order.

---

## Byte order

- **Sender:** convert every multi-byte numeric field host → network before sending.
- **Receiver:** convert network → host after verifying the checksum.

| Width  | Host → Network | Network → Host |
|--------|----------------|----------------|
| 32-bit | `htonl`        | `ntohl`        |
| 64-bit | `htobe64`      | `be64toh`      |

`side` (one byte) and the `char` arrays need no conversion.

---

## Checksum

The sender adds up every byte before the checksum field and takes the result mod `UINT64_MAX`. The receiver recomputes this sum and drops the order if the two don't match.

- **Byte order doesn't affect the result.** Converting between host and network order only rearranges bytes within each field, and addition doesn't care about order. You get the same sum before or after conversion.
- **The sum is bounded.** There are 53 bytes, each at most 255, so the sum is at most 13,515. It can never come close to overflowing a `uint64_t`, which means the mod never actually changes the value.
> **Note:** We use a checksum to check whether the network introduced bit errors, but this checksum is weak. Real exchanges use stronger error detection; this is a simplification.

---

## Reading from the socket

**We recommend a read size of one page (4096 bytes).** A page-sized buffer maps onto a single memory page. Larger buffers span more pages, which can cause page faults and stalls on the hot path.

TCP is a byte stream, so a single `read()` can return part of an order or several orders at once.

> **HINT:** Use a circular (ring) buffer for reads. Part of an order can be cut off between `read()` calls. A ring buffer lets you keep the partial bytes and append the next read after them, so the order is complete once the rest arrives. If the ring buffer's size is a multiple of 64 bytes, no order ever wraps around the end of the buffer.

---

## Tests

Every valid order your handler receives should be added to a vector (`std::vector<NetworkOrder>`). The test suite checks the orders in that vector.

The tests also measure how fast your handler drains the socket buffer. Reading too slowly will fail.

The test suite checks that the handler:

- drains the buffer fast enough
- converts all numeric fields correctly, including 64-bit fields
- rejects orders with corrupted bytes
- handles orders split across reads and multiple orders per read