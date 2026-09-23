#pragma once

namespace rtbus {

// Owns a POSIX file descriptor and closes it when destroyed. Move-only:
// copying would mean two owners closing the same descriptor.
class FileDescriptor {
 public:
  FileDescriptor() noexcept = default;
  explicit FileDescriptor(int fd) noexcept : fd_(fd) {}
  ~FileDescriptor();

  FileDescriptor(const FileDescriptor&) = delete;
  FileDescriptor& operator=(const FileDescriptor&) = delete;
  FileDescriptor(FileDescriptor&& other) noexcept;
  FileDescriptor& operator=(FileDescriptor&& other) noexcept;

  [[nodiscard]] int get() const noexcept { return fd_; }
  [[nodiscard]] bool valid() const noexcept { return fd_ >= 0; }

  // Closes the current descriptor (if any) and takes ownership of `fd`.
  void reset(int fd = -1) noexcept;

 private:
  int fd_ = -1;
};

}  // namespace rtbus
