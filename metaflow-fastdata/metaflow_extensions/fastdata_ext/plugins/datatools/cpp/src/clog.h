#ifndef METAFLOW_DATA_LOG
#define METAFLOW_DATA_LOG

#include <stdio.h>

#ifdef __cplusplus
extern "C" {
#endif

#ifndef LOG_LEVEL
  #define LOG_LEVEL 0
#endif

// Constants for text effects
#define NORMAL		0
#define BRIGHT 		1

// Constants for text color
#define BLACK 		0
#define RED		    1
#define GREEN		2
#define YELLOW		3
#define BLUE		4
#define MAGENTA		5
#define CYAN		6
#define	WHITE		7

// Constnats for log levels
#define LOG_LEVEL_ERROR 1
#define LOG_LEVEL_WARNING 2
#define LOG_LEVEL_INFO 3
#define LOG_LEVEL_DEBUG 4

/*
  Change the text color and effect
*/
static inline void textcolor(int attr, int fg)
{
  char command[8];
  sprintf(command, "%c[%d;%dm", 0x1B, attr, fg + 30);
  printf("%s", command);
}

/*
  Reset color to terminal default
*/
static inline void resetcolor()
{
  char command[5];
  sprintf(command, "%c[0m", 0x1B);
  printf("%s", command);
}

/*
  Macro for compile away logging given a level. Use one of the macros below
  instead of this function directly.
*/
#define LOG(level, level_string, color, topic, ...)                  \
  if (LOG_LEVEL < level)                                             \
    ;                                                                \
  else {                                                             \
    textcolor(BRIGHT, color);                                        \
    printf("%-8s %-16s ", level_string, topic);                      \
    textcolor(NORMAL, color);                                        \
    printf(__VA_ARGS__);                                             \
    resetcolor();                                                    \
    printf("\n");}

/*
  Macros for logging different types of events to stdout. Defining the Macro
  LOG_LEVEL will control which events are displayed.

  Examples:
  ERROR("NetworkError", "A networking error occured");
  INFO("DownloadStatus", "Download %3.2f complete", 0.0)

*/
#define ERROR(topic, ...) \
  LOG(LOG_LEVEL_ERROR, "ERROR", RED, topic, __VA_ARGS__)

#define WARNING(topic, ...) \
  LOG(LOG_LEVEL_WARNING, "WARNING", YELLOW, topic, __VA_ARGS__)

#define INFO(topic, ...) \
  LOG(LOG_LEVEL_INFO, "INFO", CYAN, topic, __VA_ARGS__)

#define DEBUG(topic, ...)                                            \
  if (LOG_LEVEL < LOG_LEVEL_DEBUG)                                   \
    ;                                                                \
  else {                                                             \
    textcolor(BRIGHT, BLUE);                                         \
    printf("%-8s %-16s ", "DEBUG", topic);                           \
    textcolor(NORMAL, BLUE);                                         \
    printf(__VA_ARGS__);                                             \
    printf(" (%s::%d)", __FILE__, __LINE__);                         \
    resetcolor();                                                    \
    printf("\n");}

#ifdef __cplusplus
}
#endif

#endif // METAFLOW_DATA_LOG
