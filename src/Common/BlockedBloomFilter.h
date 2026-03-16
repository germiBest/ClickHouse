#pragma once

/// Blocked bloom filter for runtime JOIN filters.
///
/// Three variant implementations are available for benchmarking:
///   BlockedBloomFilter_8hash.h  — 8-hash salt-multiply, 32-byte blocks, AVX2 SIMD
///   BlockedBloomFilter_4hash.h  — 4-hash salt-multiply, 16-byte blocks, SSE/NEON SIMD
///   BlockedBloomFilter_arrow.h  — Arrow-style mask table, 8-byte blocks, scalar
///
/// To switch variants, change the include below and recompile.
/// Only one variant's .cpp should be compiled (the others are not in CMakeLists).

#include <Common/BlockedBloomFilter_4hash.h>
