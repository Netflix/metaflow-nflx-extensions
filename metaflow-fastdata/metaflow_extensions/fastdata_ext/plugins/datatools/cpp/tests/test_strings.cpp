#include <stdlib.h>
#include "util.h"
#include "clog.h"

static char *rand_string(char *str, size_t size)
{
    const char charset[] = "abcdefghijklmnopqrstuvwxyz";
    if (size) {
        --size;
        for (size_t n = 0; n < size; n++) {
            int key = rand() % (int) (sizeof charset - 1);
            str[n] = charset[key];
        }
        str[size] = '\0';
    }
    return str;
}

int main() {

  char* string = (char*) malloc(1);
  int64_t size = 0;
  int64_t max_size = 1;

  for (int i = 0; i < 100; ++i) {
    char str[256];
    rand_string(str, rand() % 256);
    DEBUG("test_strings", "%d concat '%s'", i, str);
    string = append_string(str, -1, string, &size, &max_size);
  }

  string = append_string("", -1, string, &size, &max_size);
}
