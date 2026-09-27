#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <type_traits>
#include <typeinfo>

#include "rtbus/detail/chunk_pool.hpp"

namespace rtbus::detail {

// What the two sides of a topic must agree on before they share memory.
struct MessageType {
  std::uint64_t fingerprint;
  std::size_t size;
};

// 64-bit FNV-1a: a few lines, and the same result in every process and every run, which
// std::hash does not promise.
inline std::uint64_t fnv1a(const void* data, std::size_t size,
                           std::uint64_t hash = 14695981039346656037ULL) {
  const auto* bytes = static_cast<const unsigned char*>(data);
  for (std::size_t i = 0; i < size; ++i) {
    hash ^= bytes[i];
    hash *= 1099511628211ULL;
  }
  return hash;
}

// The mangled type name (identical for GCC and Clang on Linux, which share one ABI) tells
// types apart; size and alignment catch a type that kept its name but changed shape between
// builds. A layout change that keeps both is not caught: the stage 5 code generator closes
// that gap by hashing the fields themselves.
template <typename T>
MessageType message_type() {
  static_assert(std::is_trivially_copyable_v<T>,
                "rtbus messages must be trivially copyable: plain structs, with no "
                "std::string, std::vector or other types that own memory");
  static_assert(std::is_default_constructible_v<T>,
                "rtbus messages must be default constructible (a loan constructs one in place)");
  static_assert(alignof(T) <= ChunkPool::kAlignment,
                "rtbus messages must not need more than 64-byte alignment");

  const char* name = typeid(T).name();
  const std::uint64_t shape[] = {sizeof(T), alignof(T)};
  const std::uint64_t name_hash = fnv1a(name, std::strlen(name));
  return {fnv1a(shape, sizeof(shape), name_hash), sizeof(T)};
}

}  // namespace rtbus::detail
