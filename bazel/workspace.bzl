def _bazel_version_repository_impl(repository_ctx):
    s = "bazel_version = \"" + native.bazel_version + "\""
    repository_ctx.file("bazel_version.bzl", s)
    repository_ctx.file("BUILD", "")

def altgrpc_deps():
    bazel_version_repository = repository_rule(
        implementation = _bazel_version_repository_impl,
        local = True,
    )

    bazel_version_repository(
        name = "upb_bazel_version",
    )

