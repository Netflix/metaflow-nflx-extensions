#ifndef METAFLOW_DATA_LOG_H
#define METAFLOW_DATA_LOG_H

#include <chrono>
#include <sstream>
#include <string>
#include <unistd.h>

static FILE *display_stream = stdout;

/* \brief A simple RAII logging class

*/
class Log {
public:
  Log() : os_() {}
  Log(const Log &) = delete;
  Log(Log &&) = delete;
  Log &operator=(const Log &) = delete;
  Log &operator=(Log &&) = delete;

  ~Log() {
    os_ << "\033[0m";
    os_ << std::endl;
    fprintf(display_stream, "%s", os_.str().c_str());
    fflush(display_stream);
  }

  std::ostringstream &get() {
    os_ << "\033[1;36m";
    os_ << "LOG: ";
    return os_;
  }

private:
  std::ostringstream os_;
};

/* \brief A time elappsed log

*/
class TimeElapsedLog {
public:
  TimeElapsedLog(const std::string &title)
      : os_(), start_(std::chrono::high_resolution_clock::now()) {
    os_ << "\033[1;36m";
    os_ << "Time elapsed (" << title << "): ";
  }
  TimeElapsedLog(const TimeElapsedLog &) = delete;
  TimeElapsedLog(TimeElapsedLog &&) = delete;
  TimeElapsedLog &operator=(const TimeElapsedLog &) = delete;
  TimeElapsedLog &operator=(TimeElapsedLog &&) = delete;

  ~TimeElapsedLog() {
    auto nw = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration<float, std::micro>(nw - start_);
    os_ << duration.count() / 1000.0F << "(ms)";
    os_ << "\033[0m" << std::endl;
    fprintf(display_stream, "%s", os_.str().c_str());
    fflush(display_stream);
  }

private:
  std::ostringstream os_;
  std::chrono::time_point<std::chrono::high_resolution_clock> start_;
};

#ifdef LOGGING
#define USE_LOGGING 1
#define TIMEIT(MSG) TimeElapsedLog time_elappsed_log(MSG);
#else
#define USE_LOGGING 0
#define TIMEIT(MSG)                                                            \
  do {                                                                         \
  } while (0);
#endif

#define LOG                                                                    \
  if (USE_LOGGING == 0)                                                        \
    ;                                                                          \
  else                                                                         \
  Log().get()

#endif /* METAFLOW_DATA_LOG_H */
