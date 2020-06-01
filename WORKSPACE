workspace(name = "com_github_dropbox_altgrpc")

load("//bazel:workspace.bzl", "altgrpc_deps")

altgrpc_deps()

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

http_archive(
    name = "com_github_grpc_grpc",
    urls = [
        "https://github.com/grpc/grpc/archive/v1.28.1.tar.gz",
    ],
    strip_prefix = "grpc-1.28.1",
)

load("@com_github_grpc_grpc//bazel:grpc_deps.bzl", "grpc_deps")

grpc_deps()

http_archive(
    name = "build_bazel_apple_support",
    url = "https://github.com/bazelbuild/apple_support/releases/download/0.6.0/apple_support.0.6.0.tar.gz",
    sha256 = "7356dbd44dea71570a929d1d4731e870622151a5f27164d966dda97305f33471",
)

http_archive(
    name = "build_bazel_rules_swift",
    url = "https://github.com/bazelbuild/rules_swift/releases/download/0.11.1/rules_swift.0.11.1.tar.gz",
    sha256 = "96a86afcbdab215f8363e65a10cf023b752e90b23abf02272c4fc668fcb70311",
)

load("@com_github_grpc_grpc//bazel:grpc_python_deps.bzl", "grpc_python_deps")

grpc_python_deps()

