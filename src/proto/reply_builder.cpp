#include "proto/reply_builder.h"

#include <charconv>

namespace kv {

void RespWriter::SimpleString(std::string_view s) {
  out_->push_back('+');
  out_->append(s);
  out_->append("\r\n");
}

void RespWriter::Error(std::string_view msg) {
  out_->push_back('-');
  size_t start = out_->size();
  out_->append(msg);
  for (size_t i = start; i < out_->size(); ++i) {
    char& c = (*out_)[i];
    if (c == '\r' || c == '\n') c = ' ';
  }
  out_->append("\r\n");
}

void RespWriter::Header(char prefix, int64_t n) {
  char buf[24];
  buf[0] = prefix;
  auto [end, ec] = std::to_chars(buf + 1, buf + sizeof(buf) - 2, n);
  end[0] = '\r';
  end[1] = '\n';
  out_->append(buf, static_cast<size_t>(end + 2 - buf));
}

void RespWriter::Integer(int64_t v) { Header(':', v); }

void RespWriter::Bulk(std::string_view s) {
  Header('$', static_cast<int64_t>(s.size()));
  out_->append(s);
  out_->append("\r\n");
}

void RespWriter::ArrayHeader(size_t n) { Header('*', static_cast<int64_t>(n)); }

ReplyBuffer::Chunk& ReplyBuffer::OpenChunk() {
  if (used_ > 0) sealed_ += chunks_[used_ - 1].data.size();
  if (used_ == chunks_.size()) chunks_.emplace_back();
  return chunks_[used_++];
}

std::string* ReplyBuffer::Tail() {
  if (used_ > 0) {
    Chunk& last = chunks_[used_ - 1];
    if (!last.large && last.data.size() < kChunkSize) return &last.data;
  }
  Chunk& c = OpenChunk();
  c.large = false;
  c.data.clear();
  if (c.data.capacity() < kChunkSize) c.data.reserve(kChunkSize);
  return &c.data;
}

void ReplyBuffer::Append(std::string&& s) {
  if (s.size() < kMoveThreshold) {
    Tail()->append(s);
    return;
  }
  Chunk& c = OpenChunk();
  c.large = true;
  c.data = std::move(s);
}

std::span<const iovec> ReplyBuffer::Iovecs() {
  iov_.clear();
  for (size_t i = 0; i < used_; ++i) {
    std::string& d = chunks_[i].data;
    if (!d.empty()) iov_.push_back(iovec{d.data(), d.size()});
  }
  return {iov_.data(), iov_.size()};
}

void ReplyBuffer::Clear() {
  // Keep the first small chunk's allocation; release large ones and extra chunks.
  size_t keep = 0;
  for (size_t i = 0; i < chunks_.size(); ++i) {
    if (keep == 0 && !chunks_[i].large && chunks_[i].data.capacity() <= 4 * kChunkSize) {
      if (i != 0) std::swap(chunks_[0], chunks_[i]);
      chunks_[0].data.clear();
      keep = 1;
      break;
    }
  }
  chunks_.resize(keep);
  used_ = 0;
  sealed_ = 0;
  iov_.clear();
}

}  // namespace kv
