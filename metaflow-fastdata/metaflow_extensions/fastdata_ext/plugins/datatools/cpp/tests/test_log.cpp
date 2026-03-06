#include "clog.h"

int main() {
  DEBUG("debug topic", "An debug message");
  INFO("info topic", "An info message");
  WARNING("warning topic", "An warning message");
  ERROR("error topic", "An error message");
}
