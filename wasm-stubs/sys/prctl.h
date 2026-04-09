/* Stub sys/prctl.h for Emscripten — Poco Foundation uses this only for
   thread-name setting which is a no-op in single-threaded WASM. */
#pragma once
#define PR_SET_NAME 15
#define PR_GET_NAME 16
static inline int prctl(int, ...) { return 0; }
