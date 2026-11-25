#include "rtbus/detail/message_type.hpp"

#include <gtest/gtest.h>

#include <cstdint>

namespace rtbus::detail {
namespace {

struct Position {
  double x;
  double y;
};

// Same size and alignment as Position: only the name tells them apart.
struct Velocity {
  double dx;
  double dy;
};

// Reference values published with the FNV algorithm.
TEST(MessageTypeTest, Fnv1aMatchesReferenceValues) {
  EXPECT_EQ(fnv1a("", 0), 0xcbf29ce484222325ULL);
  EXPECT_EQ(fnv1a("a", 1), 0xaf63dc4c8601ec8cULL);
  EXPECT_EQ(fnv1a("foobar", 6), 0x85944171f73967e8ULL);
}

TEST(MessageTypeTest, SameTypeHasSameFingerprint) {
  EXPECT_EQ(message_type<Position>().fingerprint, message_type<Position>().fingerprint);
}

TEST(MessageTypeTest, TypesOfSameShapeHaveDifferentFingerprints) {
  EXPECT_NE(message_type<Position>().fingerprint, message_type<Velocity>().fingerprint);
}

TEST(MessageTypeTest, RecordsMessageSize) {
  EXPECT_EQ(message_type<Position>().size, sizeof(Position));
  EXPECT_EQ(message_type<std::uint8_t>().size, 1u);
}

}  // namespace
}  // namespace rtbus::detail
