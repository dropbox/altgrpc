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

load("//bazel:cython_library.bzl", "pyx_library")

pyx_library(
    name = "python",
    srcs = glob([
        "*.pxd",
        "*.pxi",
        "*.pyx",
	"plugins/**/*.pxi",
        "interop.cc",
        "auth_metadata_plugin.cc",
        "async_invoker.cc",
        "async_invoker_python2.cc",
    ]),
    deps = [
        ":auth_metadata_plugin",
        ":client",
        ":common",
        ":dispatch",
        ":dummy_server",
        ":interop",
        ":server",
        "//dropbox/types:expected",
        "//dropbox/grpc:metadata",
        "//dropbox/grpc:refcount",
        "//dropbox/grpc:servicer",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/time",
    ],
    visibility = ["//visibility:public"],
)

cc_library(
    name = "auth_metadata_plugin",
    hdrs = ["auth_metadata_plugin.h"],
    deps = [":interop"],
)

cc_library(
    name = "client",
    srcs = [
        "active_channel.cc",
        "channel.cc",
        "channel_connectivity_watcher.cc",
        "channel_connectivity_watcher.h",
        "client_rpc.cc",
        "rendezvous.cc",
    ],
    hdrs = [
        "active_channel.h",
        "async_invoker.h",
        "channel.h",
        "client.h",
        "rendezvous.h",
    ],
    deps = [
        ":common",
        ":dispatch",
        ":interop",
        ":semaphore",
        "//dropbox/grpc:completion_queue_watcher",
        "//dropbox/grpc:internal",
        "//dropbox/grpc:time",
        "@com_google_absl//absl/container:flat_hash_set",
    ],
)

cc_library(
    name = "server",
    srcs = [
        "server.cc",
        "server_rpc.cc",
        "servicer_context.cc",
    ],
    hdrs = [
        "server.h",
    ],
    deps = [
        ":common",
        ":dispatch",
        ":interop",
        "//dropbox/grpc:server",
        "@com_github_grpc_grpc//:grpc++",
    ],
)

cc_library(
    name = "dispatch",
    srcs = ["dispatch.cc"],
    hdrs = ["dispatch.h"],
    deps = [
        ":interop",
        "//dropbox/grpc:future",
        "//dropbox/grpc:refcount",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:optional",
    ],
)

cc_library(
    name = "common",
    srcs = ["common.cc"],
    hdrs = ["common.h"],
    deps = [
        ":interop",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/time",
    ],
)

cc_library(
    name = "interop",
    hdrs = ["interop.h"],
    deps = [
        "//dropbox/types:expected",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/time",
        "@com_google_absl//absl/types:optional",
    ],
)

cc_library(
    name = "dummy_server",
    srcs = ["dummy_server.cc"],
    hdrs = ["dummy_server.h"],
    deps = [
        "@com_github_grpc_grpc//:grpc",
        "@com_github_grpc_grpc//:grpc++",
    ],
)

cc_library(
    name = "semaphore",
    hdrs = ["semaphore.h"],
    deps = [
        ":interop",
        "@com_github_grpc_grpc//:grpc++",
    ],
)
