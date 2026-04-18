#ifndef CHGEOS_MEM_H
#define CHGEOS_MEM_H

#include "clickhouse.hpp"
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <streambuf>

namespace ch {

// ABI: ClickHouse reads {ptr, size} at offsets 0 and 4 (WASM32).
// data_ and size_ are the first two fields — layout is fixed, privacy is irrelevant to ABI.
class raw_buffer {
  uint8_t  *data_;
  uint32_t  size_;
  uint32_t  cap_;

  void grow_to(uint32_t new_cap) {
    void *p = ::realloc(data_, new_cap);
    if (!p) ch::panic("alloc failed");
    data_ = static_cast<uint8_t *>(p);
    cap_  = new_cap;
  }

public:
  explicit raw_buffer(uint32_t size)
      : data_(size ? static_cast<uint8_t *>(malloc(size)) : nullptr),
        size_(size), cap_(size) {
    if (size && !data_) ch::panic("alloc failed");
  }

  ~raw_buffer() { ::free(data_); }

  raw_buffer(const raw_buffer&)            = delete;
  raw_buffer& operator=(const raw_buffer&) = delete;

  raw_buffer(raw_buffer&& o) noexcept
      : data_(o.data_), size_(o.size_), cap_(o.cap_) {
    o.data_ = nullptr; o.size_ = 0; o.cap_ = 0;
  }
  raw_buffer& operator=(raw_buffer&& o) noexcept {
    if (this != &o) {
      ::free(data_);
      data_ = o.data_; size_ = o.size_; cap_ = o.cap_;
      o.data_ = nullptr; o.size_ = 0; o.cap_ = 0;
    }
    return *this;
  }

  uint8_t       *data()     noexcept { return data_; }
  const uint8_t *data()     const noexcept { return data_; }
  uint32_t       size()     const noexcept { return size_; }
  uint32_t       capacity() const noexcept { return cap_; }
  bool           empty()    const noexcept { return size_ == 0; }

  uint8_t       *begin() noexcept { return data_; }
  uint8_t       *end()   noexcept { return data_ + size_; }
  const uint8_t *begin() const noexcept { return data_; }
  const uint8_t *end()   const noexcept { return data_ + size_; }

  uint8_t& operator[](uint32_t i)       noexcept { return data_[i]; }
  uint8_t  operator[](uint32_t i) const noexcept { return data_[i]; }

  void clear() noexcept { size_ = 0; }

  void reserve(uint32_t new_cap) {
    if (new_cap > cap_) grow_to(new_cap);
  }

  void resize(uint32_t new_size, uint8_t fill = 0) {
    if (new_size > cap_) {
      uint32_t c = cap_ ? cap_ : 1;
      while (c < new_size) c *= 2;
      grow_to(c);
    }
    if (new_size > size_) memset(data_ + size_, fill, new_size - size_);
    size_ = new_size;
  }

  void push_back(uint8_t v) {
    if (size_ == cap_) grow_to(cap_ ? cap_ * 2 : 1);
    data_[size_++] = v;
  }

  void insert(uint32_t pos, const uint8_t *src, uint32_t len) {
    if (len > UINT32_MAX - size_) ch::panic("raw_buffer overflow");
    uint32_t new_size = size_ + len;
    if (new_size > cap_) {
      uint32_t c = cap_ ? cap_ : 1;
      while (c < new_size) c *= 2;
      grow_to(c);
    }
    std::memmove(data_ + pos + len, data_ + pos, size_ - pos);
    std::memcpy(data_ + pos, src, len);
    size_ = new_size;
  }

  void append(const uint8_t *src, uint32_t len) {
    if (len > UINT32_MAX - size_) ch::panic("raw_buffer overflow");
    uint32_t new_size = size_ + len;
    if (new_size > cap_) {
      uint32_t c = cap_ ? cap_ : 1;
      while (c < new_size) c *= 2;
      grow_to(c);
    }
    memcpy(data_ + size_, src, len);
    size_ = new_size;
  }
};

// Satisfies std::output_iterator<uint8_t> and provides the write(ptr, len)
// bulk method detected by msgpack23 Packer's if-constexpr optimisation.
class raw_buffer_back_inserter {
  raw_buffer *buf_;

public:
  using value_type      = uint8_t;
  using difference_type = std::ptrdiff_t;

  explicit raw_buffer_back_inserter(raw_buffer *buf) : buf_(buf) { buf_->clear(); }

  raw_buffer_back_inserter &operator*()     { return *this; }
  raw_buffer_back_inserter &operator++()    { return *this; }
  raw_buffer_back_inserter  operator++(int) { return *this; }
  raw_buffer_back_inserter &operator=(uint8_t v) { buf_->push_back(v); return *this; }

  void write(const uint8_t *src, uint32_t len) { buf_->append(src, len); }
};

// std::streambuf backed by raw_buffer — bridges GEOS WKBWriter (which needs an
// std::ostream) to raw_buffer without the extra allocation that a std::vector-
// backed streambuf would introduce when copying into raw_buffer later.
class raw_write_buf : public std::streambuf {
  raw_buffer &buf_;

protected:
  int_type overflow(int_type c) override {
    if (c != traits_type::eof())
      buf_.push_back(static_cast<uint8_t>(c));
    return c;
  }
  std::streamsize xsputn(const char *s, std::streamsize n) override {
    buf_.append(reinterpret_cast<const uint8_t *>(s), static_cast<uint32_t>(n));
    return n;
  }

public:
  explicit raw_write_buf(raw_buffer &buf) : buf_(buf) {}
};


}

extern "C" {

__attribute__((export_name("clickhouse_create_buffer")))
ch::raw_buffer *
clickhouse_create_buffer(uint32_t size);

__attribute__((export_name("clickhouse_destroy_buffer")))
void
clickhouse_destroy_buffer(uint8_t *buf);

} // extern "C"


#endif
