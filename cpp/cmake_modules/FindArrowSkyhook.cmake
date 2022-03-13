# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# - Find Arrow Skyhook (arrow/dataset/api.h, libarrow_dataset.a, libarrow_dataset.so)
#
# This module requires Arrow from which it uses
#  arrow_find_package()
#
# This module defines
#  ARROW_SKYHOOK_FOUND, whether Arrow Skyhook has been found
#  ARROW_SKYHOOK_IMPORT_LIB,
#    path to libarrow_dataset's import library (Windows only)
#  ARROW_SKYHOOK_INCLUDE_DIR, directory containing headers
#  ARROW_SKYHOOK_LIB_DIR, directory containing Arrow Skyhook libraries
#  ARROW_SKYHOOK_SHARED_LIB, path to libarrow_dataset's shared library
#  ARROW_SKYHOOK_STATIC_LIB, path to libarrow_dataset.a

if(DEFINED ARROW_SKYHOOK_FOUND)
  return()
endif()

set(find_package_arguments)
if(${CMAKE_FIND_PACKAGE_NAME}_FIND_VERSION)
  list(APPEND find_package_arguments "${${CMAKE_FIND_PACKAGE_NAME}_FIND_VERSION}")
endif()
if(${CMAKE_FIND_PACKAGE_NAME}_FIND_REQUIRED)
  list(APPEND find_package_arguments REQUIRED)
endif()
if(${CMAKE_FIND_PACKAGE_NAME}_FIND_QUIETLY)
  list(APPEND find_package_arguments QUIET)
endif()
find_package(Arrow ${find_package_arguments})

if(ARROW_FOUND)
  arrow_find_package(ARROW_SKYHOOK
                     "${ARROW_HOME}"
                     arrow_skyhook_client
                     skyhook/client/file_skyhook.h
                     ArrowSkyhook
                     arrow-skyhook)
  if(NOT ARROW_SKYHOOK_VERSION)
    set(ARROW_SKYHOOK_VERSION "${ARROW_VERSION}")
  endif()
endif()

if("${ARROW_SKYHOOK_VERSION}" VERSION_EQUAL "${ARROW_VERSION}")
  set(ARROW_SKYHOOK_VERSION_MATCH TRUE)
else()
  set(ARROW_SKYHOOK_VERSION_MATCH FALSE)
endif()

mark_as_advanced(ARROW_SKYHOOK_IMPORT_LIB
                 ARROW_SKYHOOK_INCLUDE_DIR
                 ARROW_SKYHOOK_LIBS
                 ARROW_SKYHOOK_LIB_DIR
                 ARROW_SKYHOOK_SHARED_IMP_LIB
                 ARROW_SKYHOOK_SHARED_LIB
                 ARROW_SKYHOOK_STATIC_LIB
                 ARROW_SKYHOOK_VERSION
                 ARROW_SKYHOOK_VERSION_MATCH)

find_package_handle_standard_args(
  ArrowSkyhook
  REQUIRED_VARS ARROW_SKYHOOK_INCLUDE_DIR ARROW_SKYHOOK_LIB_DIR
                ARROW_SKYHOOK_VERSION_MATCH
  VERSION_VAR ARROW_SKYHOOK_VERSION)
set(ARROW_SKYHOOK_FOUND ${ArrowSkyhook_FOUND})

if(ArrowSkyhook_FOUND AND NOT ArrowSkyhook_FIND_QUIETLY)
  message(STATUS "Found the Arrow Skyhook by ${ARROW_SKYHOOK_FIND_APPROACH}")
  message(STATUS "Found the Arrow Skyhook shared library: ${ARROW_SKYHOOK_SHARED_LIB}")
  message(STATUS "Found the Arrow Skyhook import library: ${ARROW_SKYHOOK_IMPORT_LIB}")
  message(STATUS "Found the Arrow Skyhook static library: ${ARROW_SKYHOOK_STATIC_LIB}")
endif()
