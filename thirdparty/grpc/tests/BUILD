load("@com_github_grpc_grpc//bazel:python_rules.bzl", "py2and3_test")

GRPCIO_TESTS_UNIT = [
    "abort_test.py",
    "auth_context_test.py",
    "channel_args_test.py",
    "channel_close_test.py",
    "channel_connectivity_test.py",
    "channel_ready_future_test.py",
    #"compression_test.py",
    "credentials_test.py",
    "empty_message_test.py",
    "error_message_encoding_test.py",
    "interceptor_test.py",
    "invalid_metadata_test.py",
    "metadata_flags_test.py",
    "metadata_code_details_test.py",
    "metadata_test.py",
    "rpc_test.py",
]

py_library(
    name = "socket_util",
    srcs = ["socket_util.py"],
)

py_library(
    name = "tcp_proxy",
    srcs = ["tcp_proxy.py"],
)

py_library(
    name = "resources",
    srcs = ["resources.py"],
    data = [
        "//thirdparty/grpc/tests/credentials",
    ],
    testonly = 1,
)

py_library(
    name = "test_common",
    testonly = True,
    srcs = [
        "logging_pool.py",
        "test_common.py",
        "test_constants.py",
        "test_control.py",
        "thread_pool.py",
    ],
)

[
    py2and3_test(
        name = test_file_name[:-3],
        size = "small",
        srcs = [test_file_name],
        data = [
            "//thirdparty/grpc/tests/credentials",
        ],
        main = test_file_name,
	imports =["../../../dropbox/grpc/python"],
        deps = [
            ":resources",
            ":socket_util",
            ":tcp_proxy",
            ":test_common",
            "//dropbox/grpc/python",
            "@six//:six",
        ] + select({
            "//conditions:default": ["@futures//:futures", "@enum34//:enum34"],
            "@com_github_grpc_grpc//:python3": [],
        }),
    )
    for test_file_name in GRPCIO_TESTS_UNIT
]

