/**
 * Test suite for parse_data().
 *
 * Each test builds a byte stream of NetworkOrders in network byte order,
 * sends it over a loopback TCP socket, and checks the vector parse_data()
 * fills against the expected orders (in host byte order).
 *
 * parse_data() runs in a forked child process so that crashes, uncaught
 * exceptions, early exit() calls and hangs are all caught and reported.
 *
 * Tests run from easiest to hardest and stop at the first failure.
 *
 * Build: clang++ -std=c++17 -O2 -pthread main.cpp parse_data.cpp -o run_tests
 */
#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <exception>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

#include "NetworkOrder.hpp"

void parse_data(std::vector<NetworkOrder>& orders, int socket);

static_assert(sizeof(NetworkOrder) == 64, "NetworkOrder must be 64 bytes");
static_assert(offsetof(NetworkOrder, checksum) == 53, "checksum must start at byte 53");

namespace {

using Clock = std::chrono::steady_clock;

constexpr std::size_t ORDER_SIZE      = sizeof(NetworkOrder);
constexpr std::size_t CHECKSUM_OFFSET = offsetof(NetworkOrder, checksum);
using WireOrder = std::array<std::uint8_t, ORDER_SIZE>;

// ---------------------------------------------------------------------------
// Building orders and encoding them to the wire format
// ---------------------------------------------------------------------------

void put_be(std::uint8_t* p, std::uint64_t v, int width) {
    for (int i = width - 1; i >= 0; --i) {
        p[i] = static_cast<std::uint8_t>(v & 0xFF);
        v >>= 8;
    }
}

std::uint64_t wire_checksum(const std::uint8_t* bytes) {
    std::uint64_t sum = 0;
    for (std::size_t i = 0; i < CHECKSUM_OFFSET; ++i) sum += bytes[i];
    return sum % UINT64_MAX;
}

// Encodes a host-order order into big-endian wire bytes with a valid checksum.
WireOrder encode(const NetworkOrder& o) {
    WireOrder b{};
    put_be(b.data() + offsetof(NetworkOrder, order_id), o.order_id, 8);
    std::memcpy(b.data() + offsetof(NetworkOrder, username), o.username, USERNAME_LEN);
    std::memcpy(b.data() + offsetof(NetworkOrder, symbol), o.symbol, SYMBOL_LEN);
    b[offsetof(NetworkOrder, side)] = o.side;
    put_be(b.data() + offsetof(NetworkOrder, price), o.price, 8);
    put_be(b.data() + offsetof(NetworkOrder, quantity), o.quantity, 4);
    put_be(b.data() + offsetof(NetworkOrder, timestamp), o.timestamp, 8);
    put_be(b.data() + CHECKSUM_OFFSET, wire_checksum(b.data()), 8);
    return b;
}

NetworkOrder make_order(std::uint64_t id, const std::string& user, const std::string& sym,
                        std::uint8_t side, std::uint64_t price, std::uint32_t qty,
                        std::uint64_t ts) {
    NetworkOrder o;
    std::memset(&o, 0, sizeof o);
    o.order_id = id;
    std::memcpy(o.username, user.data(), std::min(user.size(), USERNAME_LEN));
    std::memcpy(o.symbol, sym.data(), std::min(sym.size(), SYMBOL_LEN));
    o.side      = side;
    o.price     = price;
    o.quantity  = qty;
    o.timestamp = ts;
    o.checksum  = wire_checksum(encode(o).data());
    return o;
}

NetworkOrder random_order(std::mt19937_64& rng, std::uint64_t id) {
    static const char* users[] = {"alice", "bob", "carol_the_trader", "dave", "eve42", "mallory"};
    static const char* syms[]  = {"AAPL", "MSFT", "GOOGL", "BRK.B", "TSLA", "NVDA", "ABCDEFGH"};
    return make_order(id, users[rng() % 6], syms[rng() % 7], static_cast<std::uint8_t>(rng() & 1),
                      rng(), static_cast<std::uint32_t>(rng()), rng());
}

struct Stream {
    std::vector<std::uint8_t> wire;
    std::vector<NetworkOrder> expected;

    void add(const NetworkOrder& o) {
        WireOrder b = encode(o);
        wire.insert(wire.end(), b.begin(), b.end());
        expected.push_back(o);
    }
    // Sends the order with one byte flipped; it must be rejected.
    void add_corrupted(const NetworkOrder& o, std::size_t byte, std::uint8_t mask = 0xFF) {
        WireOrder b = encode(o);
        b[byte] ^= mask;
        wire.insert(wire.end(), b.begin(), b.end());
    }
};

// ---------------------------------------------------------------------------
// Socket plumbing
// ---------------------------------------------------------------------------

bool write_all(int fd, const void* data, std::size_t len) {
    const auto* p = static_cast<const std::uint8_t*>(data);
    while (len > 0) {
        ssize_t n = ::write(fd, p, len);
        if (n < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        p += n;
        len -= static_cast<std::size_t>(n);
    }
    return true;
}

bool make_tcp_pair(int& sender, int& receiver, std::string& err) {
    int lis = ::socket(AF_INET, SOCK_STREAM, 0);
    if (lis < 0) { err = std::strerror(errno); return false; }

    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port        = 0;
    socklen_t alen       = sizeof addr;
    if (::bind(lis, reinterpret_cast<sockaddr*>(&addr), sizeof addr) < 0 ||
        ::listen(lis, 1) < 0 ||
        ::getsockname(lis, reinterpret_cast<sockaddr*>(&addr), &alen) < 0) {
        err = std::strerror(errno);
        ::close(lis);
        return false;
    }

    sender = ::socket(AF_INET, SOCK_STREAM, 0);
    if (sender < 0 || ::connect(sender, reinterpret_cast<sockaddr*>(&addr), sizeof addr) < 0) {
        err = std::strerror(errno);
        ::close(lis);
        return false;
    }
    receiver = ::accept(lis, nullptr, nullptr);
    ::close(lis);
    if (receiver < 0) { err = std::strerror(errno); ::close(sender); return false; }

    // Send each write as its own segment so split-read tests really split.
    int one = 1;
    ::setsockopt(sender, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
#ifdef SO_NOSIGPIPE
    ::setsockopt(sender, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof one);
#endif
    return true;
}

// ---------------------------------------------------------------------------
// Running the user's parse_data() in an isolated child process
// ---------------------------------------------------------------------------

struct Chunk {
    std::size_t len;
    int delay_us;  // sleep before sending this chunk
};

struct Scenario {
    std::vector<std::uint8_t> wire;
    std::vector<Chunk> chunks;  // empty = send everything at once
    int timeout_ms = 5000;
};

struct RunResult {
    std::string error;  // empty if parse_data() returned normally
    std::vector<NetworkOrder> orders;
    double send_seconds = 0;
};

void send_scenario(int fd, const Scenario& sc, double& seconds) {
    auto start      = Clock::now();
    std::size_t off = 0;
    if (sc.chunks.empty()) {
        write_all(fd, sc.wire.data(), sc.wire.size());
    } else {
        for (const Chunk& c : sc.chunks) {
            if (c.delay_us > 0) std::this_thread::sleep_for(std::chrono::microseconds(c.delay_us));
            std::size_t len = std::min(c.len, sc.wire.size() - off);
            if (!write_all(fd, sc.wire.data() + off, len)) break;
            off += len;
        }
    }
    seconds = std::chrono::duration<double>(Clock::now() - start).count();
    ::shutdown(fd, SHUT_WR);
    ::close(fd);
}

[[noreturn]] void child_main(int receiver, int result_fd) {
    std::vector<NetworkOrder> orders;
    std::uint8_t status = 0;
    std::string msg;
    try {
        parse_data(orders, receiver);
    } catch (const std::exception& e) {
        status = 1;
        msg    = std::string("parse_data() threw an exception: ") + e.what();
    } catch (...) {
        status = 1;
        msg    = "parse_data() threw a non-std::exception value";
    }
    std::fflush(stdout);
    std::fflush(stderr);

    write_all(result_fd, &status, 1);
    if (status == 0) {
        std::uint64_t n = orders.size();
        write_all(result_fd, &n, sizeof n);
        write_all(result_fd, orders.data(), n * ORDER_SIZE);
    } else {
        std::uint64_t n = msg.size();
        write_all(result_fd, &n, sizeof n);
        write_all(result_fd, msg.data(), n);
    }
    ::close(result_fd);
    ::_exit(0);
}

RunResult run_parse(const Scenario& sc) {
    RunResult r;
    int sender = -1, receiver = -1;
    std::string err;
    if (!make_tcp_pair(sender, receiver, err)) {
        r.error = "test harness could not create a socket: " + err;
        return r;
    }
    int pipefd[2];
    if (::pipe(pipefd) < 0) {
        r.error = std::string("test harness could not create a pipe: ") + std::strerror(errno);
        ::close(sender);
        ::close(receiver);
        return r;
    }

    std::fflush(stdout);
    std::fflush(stderr);
    pid_t pid = ::fork();
    if (pid < 0) {
        r.error = std::string("test harness could not fork: ") + std::strerror(errno);
        ::close(sender); ::close(receiver); ::close(pipefd[0]); ::close(pipefd[1]);
        return r;
    }
    if (pid == 0) {
        ::close(sender);
        ::close(pipefd[0]);
        child_main(receiver, pipefd[1]);
    }

    ::close(receiver);
    ::close(pipefd[1]);
    std::thread tx(send_scenario, sender, std::cref(sc), std::ref(r.send_seconds));

    // Collect the child's results until EOF or timeout.
    std::vector<std::uint8_t> out;
    bool timed_out = false;
    auto deadline  = Clock::now() + std::chrono::milliseconds(sc.timeout_ms);
    std::uint8_t buf[65536];
    for (;;) {
        auto left = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - Clock::now()).count();
        if (left <= 0) { timed_out = true; break; }
        pollfd p{pipefd[0], POLLIN, 0};
        int rc = ::poll(&p, 1, static_cast<int>(left));
        if (rc < 0 && errno == EINTR) continue;
        if (rc == 0) { timed_out = true; break; }
        ssize_t n = ::read(pipefd[0], buf, sizeof buf);
        if (n > 0) out.insert(out.end(), buf, buf + n);
        else if (n == 0 || errno != EINTR) break;
    }
    if (timed_out) ::kill(pid, SIGKILL);
    int wstatus = 0;
    ::waitpid(pid, &wstatus, 0);
    ::close(pipefd[0]);
    tx.join();  // child is gone, so any blocked write fails and returns

    char msg[256];
    if (timed_out) {
        std::snprintf(msg, sizeof msg,
                      "timed out after %.1fs: parse_data() never returned (it should return "
                      "once read() returns 0, i.e. the sender closed the socket)",
                      sc.timeout_ms / 1000.0);
        r.error = msg;
        return r;
    }
    if (WIFSIGNALED(wstatus)) {
        int sig = WTERMSIG(wstatus);
        std::snprintf(msg, sizeof msg, "parse_data() crashed with signal %d (%s)", sig, strsignal(sig));
        r.error = msg;
        return r;
    }
    if (WIFEXITED(wstatus) && out.empty()) {
        std::snprintf(msg, sizeof msg, "the process exited with code %d before parse_data() returned",
                      WEXITSTATUS(wstatus));
        r.error = msg;
        return r;
    }

    std::uint64_t n = 0;
    if (out.size() < 1 + sizeof n) { r.error = "test harness received a truncated result"; return r; }
    std::memcpy(&n, out.data() + 1, sizeof n);
    const std::uint8_t* body = out.data() + 1 + sizeof n;
    std::size_t body_len     = out.size() - 1 - sizeof n;
    if (out[0] != 0) {
        r.error.assign(reinterpret_cast<const char*>(body), std::min<std::size_t>(n, body_len));
        return r;
    }
    if (body_len != n * ORDER_SIZE) { r.error = "test harness received a truncated result"; return r; }
    r.orders.resize(n);
    std::memcpy(r.orders.data(), body, body_len);
    return r;
}

// ---------------------------------------------------------------------------
// Comparing results
// ---------------------------------------------------------------------------

std::string fmt_u64(std::uint64_t v) {
    char b[64];
    std::snprintf(b, sizeof b, "%llu (0x%016llx)", (unsigned long long)v, (unsigned long long)v);
    return b;
}

std::string fmt_chars(const char* p, std::size_t len) {
    std::string s = "\"";
    for (std::size_t i = 0; i < len; ++i) {
        unsigned char c = static_cast<unsigned char>(p[i]);
        if (c >= 32 && c < 127) s += static_cast<char>(c);
        else { char b[8]; std::snprintf(b, sizeof b, "\\x%02x", c); s += b; }
    }
    return s + "\"";
}

std::string diff_u64(const char* name, std::uint64_t exp, std::uint64_t got, int width) {
    if (exp == got) return "";
    std::string s = std::string(name) + ": expected " + fmt_u64(exp) + ", got " + fmt_u64(got);
    if (width == 8 && got == __builtin_bswap64(exp))
        s += " -- bytes are reversed, the field was not converted from network byte order";
    else if (width == 4 && got == __builtin_bswap32(static_cast<std::uint32_t>(exp)))
        s += " -- bytes are reversed, the field was not converted from network byte order";
    else if (width == 8 && got == (exp >> 32))
        s += " -- only the high 32 bits survived; was a 32-bit conversion (ntohl) used on a 64-bit field?";
    return s;
}

std::string diff_order(const NetworkOrder& e, const NetworkOrder& g) {
    std::string d;
    if (!(d = diff_u64("order_id", e.order_id, g.order_id, 8)).empty()) return d;
    if (std::memcmp(e.username, g.username, USERNAME_LEN) != 0)
        return "username: expected " + fmt_chars(e.username, USERNAME_LEN) + ", got " +
               fmt_chars(g.username, USERNAME_LEN);
    if (std::memcmp(e.symbol, g.symbol, SYMBOL_LEN) != 0)
        return "symbol: expected " + fmt_chars(e.symbol, SYMBOL_LEN) + ", got " +
               fmt_chars(g.symbol, SYMBOL_LEN);
    if (!(d = diff_u64("side", e.side, g.side, 1)).empty()) return d;
    if (!(d = diff_u64("price", e.price, g.price, 8)).empty()) return d;
    if (!(d = diff_u64("quantity", e.quantity, g.quantity, 4)).empty()) return d;
    if (!(d = diff_u64("timestamp", e.timestamp, g.timestamp, 8)).empty()) return d;
    if (!(d = diff_u64("checksum", e.checksum, g.checksum, 8)).empty()) return d;
    return "";
}

std::string compare(const std::vector<NetworkOrder>& exp, const std::vector<NetworkOrder>& got) {
    std::size_t common = std::min(exp.size(), got.size());
    for (std::size_t i = 0; i < common; ++i) {
        std::string d = diff_order(exp[i], got[i]);
        if (!d.empty()) {
            std::string s = "orders[" + std::to_string(i) + "]." + d;
            if (exp.size() != got.size())
                s += " (also: expected " + std::to_string(exp.size()) + " orders, got " +
                     std::to_string(got.size()) + ")";
            return s;
        }
    }
    if (exp.size() != got.size()) {
        std::string s = "expected " + std::to_string(exp.size()) + " orders in the vector, got " +
                        std::to_string(got.size());
        if (got.size() > exp.size()) s += " (extra orders were added: corrupted or incomplete orders must be dropped)";
        else s += " (orders are missing)";
        return s;
    }
    return "";
}

std::string run_and_check(const Scenario& sc, const std::vector<NetworkOrder>& expected) {
    RunResult r = run_parse(sc);
    if (!r.error.empty()) return r.error;
    return compare(expected, r.orders);
}

Scenario scenario(const Stream& s) {
    Scenario sc;
    sc.wire = s.wire;
    return sc;
}

// ---------------------------------------------------------------------------
// Test cases, easiest to hardest. Each returns "" on pass or a failure reason.
// ---------------------------------------------------------------------------

std::string test_empty_stream() {
    Stream s;
    return run_and_check(scenario(s), s.expected);
}

std::string test_single_order() {
    Stream s;
    s.add(make_order(1, "alice", "AAPL", 0, 10125, 100, 1700000000));
    return run_and_check(scenario(s), s.expected);
}

std::string test_strings_and_side() {
    Stream s;
    s.add(make_order(1, "alice", "AAPL", 0, 100, 1, 1));                // NUL-padded strings
    s.add(make_order(2, "bob_the_trader_x", "ABCDEFGH", 1, 200, 2, 2)); // full length, no NUL
    s.add(make_order(3, "", "", 1, 300, 3, 3));                         // all-NUL strings
    return run_and_check(scenario(s), s.expected);
}

std::string test_quantity_conversion() {
    Stream s;
    s.add(make_order(1, "carol", "MSFT", 0, 1, 0x01020304u, 1));
    s.add(make_order(2, "carol", "MSFT", 1, 1, 0xFFFFFFFFu, 1));
    s.add(make_order(3, "carol", "MSFT", 0, 1, 0x80000000u, 1));
    s.add(make_order(4, "carol", "MSFT", 1, 1, 0x000000FFu, 1));
    return run_and_check(scenario(s), s.expected);
}

std::string test_64bit_conversion() {
    Stream s;
    s.add(make_order(0x0102030405060708ull, "dave", "TSLA", 0, 0xFFFFFFFF00000001ull, 7,
                     1760000000123456789ull));
    s.add(make_order(UINT64_MAX, "dave", "TSLA", 1, 0x8000000000000000ull, 7, 0x00000001FFFFFFFFull));
    s.add(make_order(0x00000000000000FFull, "dave", "TSLA", 0, 0x0123456789ABCDEFull, 7, UINT64_MAX - 1));
    return run_and_check(scenario(s), s.expected);
}

std::string test_multiple_orders_one_read() {
    std::mt19937_64 rng(6);
    Stream s;
    for (std::uint64_t i = 0; i < 32; ++i) s.add(random_order(rng, i));
    return run_and_check(scenario(s), s.expected);
}

std::string test_rejects_corrupted_order() {
    Stream s;
    NetworkOrder o = make_order(1, "eve", "NVDA", 1, 50000, 10, 123456);
    s.add_corrupted(o, offsetof(NetworkOrder, price) + 7, 0x01);  // single bit flip in price
    return run_and_check(scenario(s), s.expected);
}

std::string test_mixed_valid_and_corrupted() {
    std::mt19937_64 rng(8);
    Stream s;
    for (std::uint64_t i = 0; i < 40; ++i) {
        NetworkOrder o = random_order(rng, i);
        switch (i % 4) {
            case 2: s.add_corrupted(o, rng() % CHECKSUM_OFFSET); break;           // payload byte
            case 3: s.add_corrupted(o, CHECKSUM_OFFSET + rng() % 8); break;       // checksum byte
            default: s.add(o);
        }
    }
    return run_and_check(scenario(s), s.expected);
}

std::string test_order_split_across_reads() {
    std::mt19937_64 rng(9);
    Stream s;
    for (std::uint64_t i = 0; i < 3; ++i) s.add(random_order(rng, i));
    Scenario sc = scenario(s);
    // Splits land inside order 1, across orders 1/2 and inside order 3's checksum.
    sc.chunks = {{30, 0}, {50, 30000}, {100, 30000}, {12, 30000}};
    return run_and_check(sc, s.expected);
}

std::string test_byte_by_byte() {
    std::mt19937_64 rng(10);
    Stream s;
    for (std::uint64_t i = 0; i < 3; ++i) s.add(random_order(rng, i));
    Scenario sc = scenario(s);
    for (std::size_t i = 0; i < sc.wire.size(); ++i) sc.chunks.push_back({1, 300});
    sc.timeout_ms = 10000;
    return run_and_check(sc, s.expected);
}

std::string test_truncated_final_order() {
    std::mt19937_64 rng(11);
    Stream s;
    s.add(random_order(rng, 1));
    s.add(random_order(rng, 2));
    WireOrder partial = encode(random_order(rng, 3));
    s.wire.insert(s.wire.end(), partial.begin(), partial.begin() + 40);  // sender closes mid-order
    Scenario sc = scenario(s);
    sc.chunks = {{64, 0}, {64 + 20, 20000}, {20, 20000}};
    return run_and_check(sc, s.expected);
}

std::string test_random_chunks_stress() {
    std::mt19937_64 rng(12);
    Stream s;
    for (std::uint64_t i = 0; i < 20000; ++i) {
        NetworkOrder o = random_order(rng, i);
        if (rng() % 10 == 0) s.add_corrupted(o, rng() % (CHECKSUM_OFFSET + 8), 1u << (rng() % 8));
        else s.add(o);
    }
    Scenario sc = scenario(s);
    std::size_t sent = 0;
    while (sent < sc.wire.size()) {
        std::size_t len = (rng() % 8 == 0) ? 4000 + rng() % 9000 : 1 + rng() % 700;  // some > 4096
        int delay       = (rng() % 40 == 0) ? 1000 : 0;
        sc.chunks.push_back({len, delay});
        sent += len;
    }
    sc.timeout_ms = 20000;
    return run_and_check(sc, s.expected);
}

// Time for a reference reader (4096-byte reads, no processing) to drain `wire`.
double baseline_drain_seconds(const std::vector<std::uint8_t>& wire) {
    int sender = -1, receiver = -1;
    std::string err;
    if (!make_tcp_pair(sender, receiver, err)) return 0;
    std::thread rx([receiver] {
        char buf[4096];
        while (::read(receiver, buf, sizeof buf) > 0) {}
        ::close(receiver);
    });
    Scenario sc;
    sc.wire        = wire;
    double seconds = 0;
    send_scenario(sender, sc, seconds);
    rx.join();
    return seconds;
}

std::string test_drain_speed() {
    std::mt19937_64 rng(13);
    Stream s;
    for (std::uint64_t i = 0; i < 200000; ++i) s.add(random_order(rng, i));  // 12.8 MB
    Scenario sc   = scenario(s);
    sc.timeout_ms = 30000;

    double baseline = baseline_drain_seconds(sc.wire);
    double limit    = std::max(1.0, 10.0 * baseline);

    RunResult r = run_parse(sc);
    if (!r.error.empty()) return r.error;
    std::string d = compare(s.expected, r.orders);
    if (!d.empty()) return d;

    char msg[256];
    if (r.send_seconds > limit) {
        std::snprintf(msg, sizeof msg,
                      "draining 12.8 MB took %.3fs, limit is %.3fs (reference reader: %.3fs) -- "
                      "read in larger chunks (e.g. 4096 bytes) instead of a few bytes at a time",
                      r.send_seconds, limit, baseline);
        return msg;
    }
    std::printf("      drained 12.8 MB in %.3fs (limit %.3fs, reference %.3fs)\n", r.send_seconds,
                limit, baseline);
    return "";
}

struct TestCase {
    const char* name;
    std::string (*fn)();
};

const TestCase TESTS[] = {
    {"Empty stream: returns on EOF with no orders", test_empty_stream},
    {"Single valid order", test_single_order},
    {"Strings and side are copied unchanged", test_strings_and_side},
    {"32-bit quantity is converted to host order", test_quantity_conversion},
    {"64-bit fields are converted to host order", test_64bit_conversion},
    {"Multiple orders in a single read", test_multiple_orders_one_read},
    {"Rejects an order with a corrupted byte", test_rejects_corrupted_order},
    {"Keeps valid orders, drops corrupted ones", test_mixed_valid_and_corrupted},
    {"Orders split across reads", test_order_split_across_reads},
    {"Stream arrives one byte at a time", test_byte_by_byte},
    {"Incomplete final order is discarded", test_truncated_final_order},
    {"Stress: 20k orders, random chunk sizes, corruption", test_random_chunks_stress},
    {"Drains the socket buffer fast enough", test_drain_speed},
};

}  // namespace

int main() {
    ::signal(SIGPIPE, SIG_IGN);

    const bool color  = ::isatty(STDOUT_FILENO);
    const char* green = color ? "\033[32m" : "";
    const char* red   = color ? "\033[31m" : "";
    const char* reset = color ? "\033[0m" : "";

    const int total = static_cast<int>(sizeof TESTS / sizeof TESTS[0]);
    std::printf("Running %d parse_data() tests\n\n", total);

    int passed = 0;
    for (int i = 0; i < total; ++i) {
        std::printf("[%2d/%d] %s\n", i + 1, total, TESTS[i].name);
        std::fflush(stdout);

        std::string failure;
        try {
            failure = TESTS[i].fn();
        } catch (const std::exception& e) {
            failure = std::string("test harness error: ") + e.what();
        }

        if (!failure.empty()) {
            std::printf("      %sFAIL%s: %s\n\n", red, reset, failure.c_str());
            std::printf("%sPassed %d/%d, failed test %d: %s%s\n", red, passed, total, i + 1,
                        TESTS[i].name, reset);
            return 1;
        }
        std::printf("      %sPASS%s\n", green, reset);
        ++passed;
    }

    std::printf("\n%sPassed %d/%d, all tests passed%s\n", green, passed, total, reset);
    return 0;
}
