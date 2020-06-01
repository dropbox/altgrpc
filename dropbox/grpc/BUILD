# Copyright (c) 2020 Dropbox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

cc_library(
    name = "server",
    srcs = [
        "server.cc",
        "server_call.cc",
    ],
    hdrs = [
        "server.h",
        "server_call.h",
    ],
    visibility = [":__subpackages__"],
    deps = [
        ":future",
        ":internal",
        ":time",
        "@com_github_grpc_grpc//:grpc",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:optional",
    ],
)

cc_library(
    name = "servicer",
    srcs = ["servicer.cc"],
    hdrs = ["servicer.h"],
    visibility = [
        ":__subpackages__",
        "//cpp/dropbox:__subpackages__",
    ],
    deps = [
        "@com_github_grpc_grpc//:grpc",
        "@com_github_grpc_grpc//:grpc++",
    ],
)

cc_library(
    name = "completion_queue_watcher",
    srcs = ["completion_queue_watcher.cc"],
    hdrs = ["completion_queue_watcher.h"],
    visibility = [
        ":__subpackages__",
        "//cpp/dropbox:__subpackages__",
    ],
    deps = [
        ":internal",
        ":refcount",
        "@com_github_grpc_grpc//:grpc++",
    ],
)

cc_library(
    name = "time",
    hdrs = ["time.h"],
    visibility = [
        ":__subpackages__",
        "//cpp/dropbox:__subpackages__",
    ],
    deps = [
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/time",
    ],
)

cc_library(
    name = "future",
    srcs = ["future.cc"],
    hdrs = ["future.h"],
    visibility = [
        ":__subpackages__",
        "//cpp/dropbox/courier:__subpackages__",
    ],
    deps = [
        ":completion_queue_watcher",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/container:inlined_vector",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/time",
    ],
)

cc_library(
    name = "internal",
    hdrs = ["internal.h"],
    visibility = [":__subpackages__"],
    deps = [
        ":refcount",
        "@com_github_grpc_grpc//:grpc++",
    ],
)

cc_library(
    name = "refcount",
    hdrs = ["refcount.h"],
    visibility = [
        ":__subpackages__",
        "//cpp/dropbox/courier:__subpackages__",
        "//rust/dropbox/grpc:__subpackages__",
    ],
    deps = [
        "@com_google_absl//absl/base:core_headers",
    ],
)

cc_library(
    name = "metadata",
    srcs = ["metadata.cc"],
    hdrs = ["metadata.h"],
    visibility = [":__subpackages__"],
    deps = [
        "//dropbox/types:expected",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
    ],
)
