#pragma once

#include <sys/uio.h>

#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "absl/container/inlined_vector.h"

namespace kv {

// Appends RESP2 replies to a string.
class RespWriter {
 public:
  explicit RespWriter(std::string* out) : out_(out) {}

  void SimpleString(std::string_view s);
  void Ok() { out_->append("+OK\r\n"); }
  // `msg` is the full error text such as "ERR syntax error", without '-' or CRLF.
  // CR and LF inside it are replaced with spaces, as Redis does.
  void Error(std::string_view msg);
  void Integer(int64_t v);
  void Bulk(std::string_view s);
  void NullBulk() { out_->append("$-1\r\n"); }
  void ArrayHeader(size_t n);
  void NullArray() { out_->append("*-1\r\n"); }
  void Raw(std::string_view s) { out_->append(s); }

  std::string* buffer() { return out_; }

 private:
  void Header(char prefix, int64_t n);

  std::string* out_;
};

// The output side of one connection.
//
// Design after Dragonfly (DC-09): SinkReplyBuilder in src/facade/reply_builder.h. Replies
// collect in 8 KB chunks and leave in a single writev over an iovec array with 16 entries
// stored inline. Large replies that already live in their own string (values copied back
// from another shard) are moved in and referenced by their own iovec instead of being copied.
//
// The connection keeps two of these: one being filled and one owned by the socket while a
// send is in flight, so nothing a pending writev points at is ever modified.
class ReplyBuffer {
 public:
  static constexpr size_t kChunkSize = 8192;      // DC-09: kMaxBufferSize
  static constexpr size_t kMoveThreshold = 4096;  // Replies this big are referenced, not copied.

  ReplyBuffer() = default;
  ReplyBuffer(const ReplyBuffer&) = delete;
  ReplyBuffer& operator=(const ReplyBuffer&) = delete;

  // The chunk new replies should be written into. The pointer is valid until the next call
  // that modifies this buffer.
  std::string* Tail();

  void Append(std::string_view s) { Tail()->append(s); }
  void Append(std::string&& s);

  bool Empty() const { return Bytes() == 0; }
  // O(1): chunks before the last are sealed and counted once.
  size_t Bytes() const { return sealed_ + (used_ > 0 ? chunks_[used_ - 1].data.size() : 0); }

  // An iovec per non-empty chunk. Valid until the buffer is modified.
  std::span<const iovec> Iovecs();

  // Drops the contents but keeps one small chunk's memory for reuse.
  void Clear();

 private:
  struct Chunk {
    std::string data;
    bool large = false;
  };

  Chunk& OpenChunk();

  std::vector<Chunk> chunks_;
  size_t used_ = 0;    // chunks_[0, used_) are live; the rest are spare.
  size_t sealed_ = 0;  // Bytes in chunks_[0, used_ - 1).
  absl::InlinedVector<iovec, 16> iov_;
};

}  // namespace kv
