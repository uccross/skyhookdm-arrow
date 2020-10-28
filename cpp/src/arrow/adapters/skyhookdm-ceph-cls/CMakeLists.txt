find_package(librados REQUIRED)
include_directories(${LIBRADOS_INCLUDE_DIR})

add_subdirectory(src)