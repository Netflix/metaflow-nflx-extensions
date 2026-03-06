#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "util.h"

namespace ops {
std::string join_str(const std::vector<std::string> &parts,
                     const std::string &delimeter) {
  std::ostringstream joined;
  for (size_t i = 0; i < parts.size(); ++i) {
    joined << parts[i];
    if (i != parts.size() - 1) {
      joined << delimeter;
    }
  }
  return joined.str();
}

std::vector<std::string>
split_str(const std::string& str, const std::string& delimiter) {
  std::vector<std::string> result;
  size_t start = 0;
  size_t end = str.find(delimiter);

  while (end != std::string::npos) {
    result.push_back(str.substr(start, end - start));
    start = end + delimiter.length();
    end = str.find(delimiter, start);
  }

  result.push_back(str.substr(start));
  return result;
}

bool is_null(const uint8_t *bitmap, int64_t pos) {
  return (bitmap[pos / 8] & (1 << (pos % 8))) == 0;
}
} // namespace ops
