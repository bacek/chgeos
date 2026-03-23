#pragma once

#include <cstdint>
#include <streambuf>
#include <vector>

namespace ch {

using Vector = std::vector<uint8_t>;

class VectorWriteBuf : public std::streambuf {
private:
  Vector &_bytes;

protected:
  // Single-byte fallback (e.g. flush of odd trailing byte).
  int_type overflow(int_type c) override {
    if (c != traits_type::eof())
      _bytes.push_back(static_cast<uint8_t>(c));
    return c;
  }

  // Bulk binary write — called by ostream::write() / WKBWriter.
  // Appends raw bytes directly without any character transformation,
  // giving explicit binary + append semantics for the backing vector.
  std::streamsize xsputn(const char* s, std::streamsize n) override {
    _bytes.insert(_bytes.end(),
                  reinterpret_cast<const uint8_t*>(s),
                  reinterpret_cast<const uint8_t*>(s) + n);
    return n;
  }

public:
  VectorWriteBuf(Vector &buf) : _bytes(buf) {}
};


} // namespace ch
