#ifndef METAFLOW_DATA_UTIL
#define METAFLOW_DATA_UTIL

#include <memory>
#include <sstream>
#include <string>
#include <type_traits>
#include <vector>

#include "arrow/result.h"
#include "arrow/status.h"

namespace ops {

/*! \brief Join a list of strings with delimeter
 */
std::string join_str(const std::vector<std::string> &parts,
                     const std::string &delimeter);

std::vector<std::string>
split_str(const std::string& str, const std::string& delimiter);

template <typename T, typename... Args>
std::unique_ptr<T> make_unique(Args... args) {
  return std::unique_ptr<T>(new T(std::forward<Args>(args)...));
}

inline void check_status(const arrow::Status &status) {
  if (!status.ok()) {
    throw std::runtime_error(status.ToString());
  }
}

template <typename T> const T &unwrap(const arrow::Result<T> &result) {
  check_status(result.status());
  return result.ValueOrDie();
}

template <typename T> T &&unwrap(arrow::Result<T> &&result) {
  check_status(result.status());
  return std::move(result.ValueOrDie());
}

bool is_null(const uint8_t *bitmap, int64_t pos);

} // namespace ops
#endif
