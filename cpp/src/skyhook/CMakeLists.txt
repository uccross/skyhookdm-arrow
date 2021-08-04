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
project(arrow_skyhook)
cmake_minimum_required(VERSION 3.11)

# define the targets to build
add_custom_target(arrow_skyhook_client)
add_custom_target(arrow_skyhook_server)

set(ARROW_BUILD_STATIC OFF)

# define the dependencies
find_package(librados REQUIRED)
include_directories(${LIBRADOS_INCLUDE_DIR})
set(ARROW_DATASET_LINK_STATIC arrow_dataset_static)
set(ARROW_DATASET_LINK_SHARED arrow_dataset_static)
set(ARROW_DATASET_LINK_STATIC ${ARROW_DATASET_LINK_STATIC} ${LIBRADOS_LIBRARIES})
set(ARROW_DATASET_LINK_SHARED ${ARROW_DATASET_LINK_SHARED} ${LIBRADOS_LIBRARIES})

# define the client and server sources
set(ARROW_SKYHOOK_CLIENT_SOURCES client/file_skyhook.cc protocol/rados_internal.cc)
set(ARROW_SKYHOOK_SERVER_SOURCES storage/cls_skyhook.cc protocol/rados_internal.cc)

# define the client library
add_arrow_lib(arrow_skyhook_client
              BUILD_SHARED
              ON
              SOURCES
              ${ARROW_SKYHOOK_SOURCES}
              OUTPUTS
              ARROW_SKYHOOK_CLIENT_LIBRARIES
              SHARED_LINK_LIBS
              ${ARROW_DATASET_LINK_SHARED}
              STATIC_LINK_LIBS
              ${ARROW_DATASET_LINK_STATIC}
              DEPENDENCIES
              arrow_dataset_static)

#  define the server library
add_arrow_lib(arrow_skyhook_server
              BUILD_SHARED
              ON
              SOURCES
              ${ARROW_SKYHOOK_SERVER_SOURCES}
              OUTPUTS
              ARROW_SKYHOOK_SERVER_LIBRARIES
              SHARED_LINK_LIBS
              ${ARROW_DATASET_LINK_SHARED}
              STATIC_LINK_LIBS
              ${ARROW_DATASET_LINK_STATIC}
              DEPENDENCIES
              arrow_dataset_static)

add_dependencies(arrow_skyhook_client ${ARROW_SKYHOOK_CLIENT_LIBRARIES})
add_dependencies(arrow_skyhook_server ${ARROW_SKYHOOK_SERVER_LIBRARIES})
