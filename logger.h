#pragma once

/***************************************************************************
 *   Copyright (C) 2008 by H-Store Project                                 *
 *   Brown University                                                      *
 *   Massachusetts Institute of Technology                                 *
 *   Yale University                                                       *
 *                                                                         *
 *   This software may be modified and distributed under the terms         *
 *   of the MIT license.  See the LICENSE file for details.                *
 *                                                                         *
 ***************************************************************************/

/**
 * @file logger.h
 * @brief Logging macros that can be optimized out
 * @author Hideaki, modified by Anuj, modified by Mason
 */

#include <ctime>
#include <string>

// Log levels: higher means more verbose
#define LOG_LEVEL_OFF 0
#define LOG_LEVEL_ERROR 1  // Only fatal conditions
#define LOG_LEVEL_WARN 2  // Conditions from which it's possible to recover
#define LOG_LEVEL_INFO 3  // Reasonable to log (e.g., proxy statistics)

// Unique logging conditions for debugging (0 or 1)
#define LOG_DEBUG_RAFT_SMALL 0
#define LOG_DEBUG_RAFT 0
#define LOG_DEBUG_FAILOVER 0
#define LOG_DEBUG_DT 0
#define LOG_DEBUG_TIMERS 0
#define LOG_DEBUG_THREAD 0
#define LOG_DEBUG_RECOVERY 0
#define LOG_DEBUG_BITMAPS 0
#define LOG_DEBUG_SEQ 0
#define LOG_DEBUG_COMPACTION 0
#define LOG_DEBUG_GC 0
#define LOG_DEBUG_FIFO 0
#define LOG_DEBUG_SERVER 0 // CATSKeeper Server
#define LOG_DEBUG_CLIENT 0 // CATSKeeper Server

// #define LOG_LEVEL_REORDER 4  // Too frequent to log (e.g., reordered pkts)
// #define LOG_LEVEL_TRACE 5  // Extremely frequent (e.g., all datapath pkts)
// #define LOG_LEVEL_CC 6     // Even congestion control decisions!

#define LOG_DEFAULT_STREAM stdout

// Log messages with "reorder" or higher verbosity get written to
// logger_trace_file_or_default_stream.
// This can be stdout for basic debugging, or
// eRPC's trace file for more involved debugging.

#define logger_trace_file_or_default_stream LOG_DEFAULT_STREAM

// If ERPC_LOG_LEVEL is not defined, default to the highest level so that
// YouCompleteMe does not report compilation errors
//#ifndef LOG_LEVEL
#define CATS_LOG_LEVEL LOG_LEVEL_INFO
//#endif

static void output_log_header(int level);

#if CATS_LOG_LEVEL >= LOG_LEVEL_ERROR
#define LOG_ERROR(...)                                             \
  flockfile(LOG_DEFAULT_STREAM); \
  output_log_header(LOG_DEFAULT_STREAM, LOG_LEVEL_ERROR); \
  fprintf(LOG_DEFAULT_STREAM, __VA_ARGS__);                    \
  fflush(LOG_DEFAULT_STREAM); \
  funlockfile(LOG_DEFAULT_STREAM)
#else
#define LOG_ERROR(...) ((void)0)
#endif

#if CATS_LOG_LEVEL >= LOG_LEVEL_WARN
#define LOG_WARN(...)                                             \
  flockfile(LOG_DEFAULT_STREAM); \
  output_log_header(LOG_DEFAULT_STREAM, LOG_LEVEL_WARN); \
  fprintf(LOG_DEFAULT_STREAM, __VA_ARGS__);                   \
  fflush(LOG_DEFAULT_STREAM); \
  funlockfile(LOG_DEFAULT_STREAM)
#else
#define LOG_WARN(...) ((void)0)
#endif

///*
#if CATS_LOG_LEVEL >= LOG_LEVEL_INFO
#define LOG_INFO(...)                                             \
  flockfile(LOG_DEFAULT_STREAM); \
  output_log_header(LOG_DEFAULT_STREAM, LOG_LEVEL_INFO); \
  fprintf(LOG_DEFAULT_STREAM, __VA_ARGS__);                   \
  fflush(LOG_DEFAULT_STREAM); \
  funlockfile(LOG_DEFAULT_STREAM)
#else
#define LOG_INFO(...) ((void)0)
#endif
//*/

/*
//output_log_header(LOG_DEFAULT_STREAM, LOG_LEVEL_INFO);
#if CATS_LOG_LEVEL >= LOG_LEVEL_INFO
#define LOG_INFO(...)                                             \
  fprintf(LOG_DEFAULT_STREAM, __VA_ARGS__);                   \
  fflush(LOG_DEFAULT_STREAM);
#else
#define LOG_INFO(...) ((void)0)
#endif
*/

/***
 * Unique logging definitions
***/
// template for adding new unigue logger condition
/*
#if LOG_DEBUG_XXX
#define LOG_XXX) \
  fprintf(logger_trace_file_or_default_stream, \
    "%s %s: ", get_formatted_time().c_str(), "XXX"); \
  output_log_header(logger_trace_file_or_default_stream, LOG_DEBUG_XXX); \
  fprintf(logger_trace_file_or_default_stream, __VA_ARGS__); \
fflush(logger_trace_file_or_default_stream)
#else
#define LOG_XXX(...) ((void)0)
#endif
*/

#if LOG_DEBUG_RAFT_SMALL
#define LOG_RAFT_SMALL(...) \
  fprintf(logger_trace_file_or_default_stream, \
  "%s %s: ", get_formatted_time().c_str(), "RAFT_SMALL"); \
  fprintf(logger_trace_file_or_default_stream, __VA_ARGS__); \
  fflush(logger_trace_file_or_default_stream)
#else
#define LOG_RAFT_SMALL(...) ((void)0)
#endif

#if LOG_DEBUG_RAFT
#define LOG_RAFT(...) \
  fprintf(logger_trace_file_or_default_stream, \
    "%s %s: ", get_formatted_time().c_str(), "RAFT"); \
  fprintf(logger_trace_file_or_default_stream, __VA_ARGS__); \
  fflush(logger_trace_file_or_default_stream)
#else
#define LOG_RAFT(...) ((void)0)
#endif

#if LOG_DEBUG_FAILOVER
#define LOG_FAILOVER(...) \
  fprintf(logger_trace_file_or_default_stream, \
    "%s %s: ", get_formatted_time().c_str(), "FAILOVER"); \
  fprintf(logger_trace_file_or_default_stream, __VA_ARGS__); \
  fflush(logger_trace_file_or_default_stream)
#else
#define LOG_FAILOVER(...) ((void)0)
#endif

#if LOG_DEBUG_DT
#define LOG_DT(...) \
  fprintf(logger_trace_file_or_default_stream, \
    "%s %s: ", get_formatted_time().c_str(), "DT"); \
  fprintf(logger_trace_file_or_default_stream, __VA_ARGS__); \
  fflush(logger_trace_file_or_default_stream)
#else
#define LOG_DT(...) ((void)0)
#endif

#if LOG_DEBUG_TIMERS
#define LOG_TIMERS(...) \
  fprintf(logger_trace_file_or_default_stream, \
    "%s %s: ", get_formatted_time().c_str(), "TIMERS"); \
  fprintf(logger_trace_file_or_default_stream, __VA_ARGS__); \
  fflush(logger_trace_file_or_default_stream)
#else
#define LOG_TIMERS(...) ((void)0)
#endif

#if LOG_DEBUG_THREAD
#define LOG_THREAD(...) \
  fprintf(logger_trace_file_or_default_stream, \
    "%s %s: ", get_formatted_time().c_str(), "THREAD"); \
  fprintf(logger_trace_file_or_default_stream, __VA_ARGS__); \
  fflush(logger_trace_file_or_default_stream)
#else
#define LOG_THREAD(...) ((void)0)
#endif

#if LOG_DEBUG_RECOVERY
#define LOG_RECOVERY(...) \
  fprintf(logger_trace_file_or_default_stream, \
    "%s %s: ", get_formatted_time().c_str(), "RECOVERY"); \
  fprintf(logger_trace_file_or_default_stream, __VA_ARGS__); \
  fflush(logger_trace_file_or_default_stream)
#else
#define LOG_RECOVERY(...) ((void)0)
#endif

#if LOG_DEBUG_BITMAPS
#define LOG_BITMAP(...) \
  fprintf(logger_trace_file_or_default_stream, \
    "%s %s: ", get_formatted_time().c_str(), "BITMAPS"); \
  fprintf(logger_trace_file_or_default_stream, __VA_ARGS__); \
  fflush(logger_trace_file_or_default_stream)
#else
#define LOG_BITMAP(...) ((void)0)
#endif

#if LOG_DEBUG_SEQ
#define LOG_SEQ(...) \
  fprintf(logger_trace_file_or_default_stream, \
    "%s %s: ", get_formatted_time().c_str(), "SEQ"); \
  fprintf(logger_trace_file_or_default_stream, __VA_ARGS__); \
  fflush(logger_trace_file_or_default_stream)
#else
#define LOG_SEQ(...) ((void)0)
#endif

#if LOG_DEBUG_COMPACTION
#define LOG_COMPACTION(...) \
  fprintf(logger_trace_file_or_default_stream, \
    "%s %s: ", get_formatted_time().c_str(), "COMPACTION"); \
  fprintf(logger_trace_file_or_default_stream, __VA_ARGS__); \
  fflush(logger_trace_file_or_default_stream)
#else
#define LOG_COMPACTION(...) ((void)0)
#endif

#if LOG_DEBUG_GC
#define LOG_GC(...) \
  fprintf(logger_trace_file_or_default_stream, \
    "%s %s: ", get_formatted_time().c_str(), "GC"); \
  fprintf(logger_trace_file_or_default_stream, __VA_ARGS__); \
  fflush(logger_trace_file_or_default_stream)
#else
#define LOG_GC(...) ((void)0)
#endif

#if LOG_DEBUG_FIFO
#define LOG_FIFO(...) \
  fprintf(logger_trace_file_or_default_stream, \
    "%s %s: ", get_formatted_time().c_str(), "FIFO"); \
  fprintf(logger_trace_file_or_default_stream, __VA_ARGS__); \
  fflush(logger_trace_file_or_default_stream)
#else
#define LOG_FIFO(...) ((void)0)
#endif

#if LOG_DEBUG_SERVER
#define LOG_SERVER(...) \
  fprintf(logger_trace_file_or_default_stream, \
    "%s %s: ", get_formatted_time().c_str(), "SERVER"); \
  fprintf(logger_trace_file_or_default_stream, __VA_ARGS__); \
  fflush(logger_trace_file_or_default_stream)
#else
#define LOG_SERVER(...) ((void)0)
#endif

#if LOG_DEBUG_CLIENT
#define LOG_CLIENT(...) \
  fprintf(logger_trace_file_or_default_stream, \
    "%s %s: ", get_formatted_time().c_str(), "CLIENT"); \
  fprintf(logger_trace_file_or_default_stream, __VA_ARGS__); \
  fflush(logger_trace_file_or_default_stream)
#else
#define LOG_CLIENT(...) ((void)0)
#endif

/// Return decent-precision time formatted as seconds:microseconds
static std::string get_formatted_time() {
  struct timespec t;
  clock_gettime(CLOCK_REALTIME, &t);
  char buf[20];
  uint32_t seconds = t.tv_sec % 100;  // Rollover every 100 seconds
  uint32_t usec = t.tv_nsec / 1000;

  sprintf(buf, "%u:%06u", seconds, usec);
  return std::string(buf);
}

// Output log message header
static void output_log_header(FILE *stream, int level) {
  std::string formatted_time = get_formatted_time();

  const char *type;
  switch (level) {
    case LOG_LEVEL_ERROR: type = "CATS_ERROR";
      break;
    case LOG_LEVEL_WARN: type = "CATS_WARNG";
      break;
    case LOG_LEVEL_INFO: type = "CATS_INFO";
      break;
    default: type = "CATS_UNKWN";
  }

  fprintf(stream, "%s %s: ", formatted_time.c_str(), type);
}