cc_library(
    name = "expected",
    srcs = [
        "expected.cc",
    ],
    hdrs = [
        "expected.h",
    ],
    visibility = [
        "//visibility:public",
    ],
    deps = [
        "@com_google_absl//absl/types:variant",
    ],
)

cc_test(
    name = "expected_test",
    srcs = [
        "expected_test.cc",
    ],
    deps = [
        ":expected",
        "@com_google_googletest//:gtest_main",
    ],
)
