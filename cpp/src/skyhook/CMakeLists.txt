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
# specific language governing permissions and limitationsn
# under the License.

#
# arrow_skyhook
#
# define project properties
project(arrow_skyhook)
cmake_minimum_required(VERSION 3.11)
set(ARROW_BUILD_STATIC OFF)

# define the targets to build
add_custom_target(arrow_skyhook_client)
add_custom_target(cls_skyhook)

# define the dependencies
find_package(librados REQUIRED)
include_directories(${LIBRADOS_INCLUDE_DIR})
set(ARROW_DATASET_LINK_STATIC arrow_dataset_static)
set(ARROW_DATASET_LINK_SHARED arrow_dataset_static)
set(ARROW_DATASET_LINK_STATIC ${ARROW_DATASET_LINK_STATIC} ${LIBRADOS_LIBRARIES})
set(ARROW_DATASET_LINK_SHARED ${ARROW_DATASET_LINK_SHARED} ${LIBRADOS_LIBRARIES})

# define the client and cls sources
set(ARROW_SKYHOOK_CLIENT_SOURCES client/file_skyhook.cc protocol/rados_internal.cc)
set(ARROW_SKYHOOK_CLS_SOURCES cls/cls_skyhook.cc protocol/rados_internal.cc)

# define the client library
add_arrow_lib(arrow_skyhook_client
              BUILD_SHARED
              ON
              SOURCES
              ${ARROW_SKYHOOK_CLIENT_SOURCES}
              OUTPUTS
              ARROW_SKYHOOK_CLIENT_LIBRARIES
              SHARED_LINK_LIBS
              ${ARROW_DATASET_LINK_SHARED}
              STATIC_LINK_LIBS
              ${ARROW_DATASET_LINK_STATIC}
              DEPENDENCIES
              arrow_dataset_static)

#  define the cls library
add_arrow_lib(cls_skyhook
              BUILD_SHARED
              ON
              SOURCES
              ${ARROW_SKYHOOK_CLS_SOURCES}
              OUTPUTS
              ARROW_SKYHOOK_CLS_LIBRARIES
              SHARED_LINK_LIBS
              ${ARROW_DATASET_LINK_SHARED}
              STATIC_LINK_LIBS
              ${ARROW_DATASET_LINK_STATIC}
              DEPENDENCIES
              arrow_dataset_static)

add_dependencies(arrow_skyhook_client ${ARROW_SKYHOOK_CLIENT_LIBRARIES})
add_dependencies(cls_skyhook ${ARROW_SKYHOOK_CLS_LIBRARIES})

if(ARROW_TEST_LINKAGE STREQUAL "static")
  set(ARROW_DATASET_TEST_LINK_LIBS arrow_dataset_static ${ARROW_TEST_STATIC_LINK_LIBS})
else()
  set(ARROW_DATASET_TEST_LINK_LIBS arrow_dataset_shared ${ARROW_TEST_SHARED_LINK_LIBS})
endif()

add_arrow_test(cls_skyhook_test
               SOURCES
               cls/cls_skyhook_test.cc
               EXTRA_LINK_LIBS
               ${ARROW_DATASET_TEST_LINK_LIBS}
               PREFIX
               "arrow-cls")
