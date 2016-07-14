# - Find LIBEVENT (lz4.h, liblz4.a, liblz4.so, and liblz4.so.1)
# LIBEVENT_ROOT hints the location
#
# This module defines
# LIBEVENT_INCLUDE_DIR, directory containing headers
# LIBEVENT_LIBS, directory containing lz4 libraries
# LIBEVENT_STATIC_LIB, path to liblz4.a
# lz4 - static library

set(LIBEVENT_SEARCH_LIB_PATH
  ${LIBEVENT_ROOT}/lib
  /usr/lib/x86_64-linux-gnu/
)

set(LIBEVENT_SEARCH_INCLUDE_DIR
  ${LIBEVENT_ROOT}/include
)

find_path(LIBEVENT_INCLUDE_DIR ev.h
  PATHS ${LIBEVENT_SEARCH_INCLUDE_DIR}
  DOC "Path to LIBEVENT headers"
  )

find_library(LIBEVENT_LIBS NAMES ev
  PATHS ${LIBEVENT_SEARCH_LIB_PATH}
        NO_DEFAULT_PATH
  DOC "Path to LIBEVENT library"
)

find_library(LIBEVENT_DYNAMIC_LIB NAMES libev.so
  PATHS ${LIBEVENT_SEARCH_LIB_PATH}
        NO_DEFAULT_PATH
  DOC "Path to LIBEVENT dynamic library"
)

find_library(LIBEVENT_STATIC_LIB NAMES libev.a
  PATHS ${LIBEVENT_SEARCH_LIB_PATH}
        NO_DEFAULT_PATH
  DOC "Path to LIBEVENT static library"
)

if (NOT LIBEVENT_LIBS OR NOT LIBEVENT_STATIC_LIB)
  message(FATAL_ERROR "Libevent includes and libraries NOT found. "
    "Looked for headers in ${LIBEVENT_SEARCH_INCLUDE_DIR}, "
    "and for libs in ${LIBEVENT_SEARCH_LIB_PATH}")
  set(LIBEVENT_FOUND FALSE)
else()
  set(LIBEVENT_FOUND TRUE)
  add_library(libevent STATIC IMPORTED)
  add_library(libevent_so SHARED IMPORTED)
  set_target_properties(libevent PROPERTIES IMPORTED_LOCATION "${LIBEVENT_STATIC_LIB}")
  set_target_properties(libevent_so PROPERTIES IMPORTED_LOCATION "${LIBEVENT_DYNAMIC_LIB}")
endif ()

mark_as_advanced(
  LIBEVENT_INCLUDE_DIR
  LIBEVENT_LIBS
  LIBEVENT_STATIC_LIB
  LIBEVENT_DYNAMIC_LIB
  libevent
)
