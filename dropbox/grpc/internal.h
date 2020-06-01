#ifndef DROPBOX_GRPC_INTERNAL_H_
#define DROPBOX_GRPC_INTERNAL_H_

#include <grpcpp/channel.h>
#include <grpcpp/impl/codegen/async_stream.h>
#include <grpcpp/impl/codegen/byte_buffer.h>
#include <grpcpp/impl/codegen/client_context.h>
#include <grpcpp/impl/codegen/client_unary_call.h>
#include <grpcpp/impl/codegen/completion_queue.h>
#include <grpcpp/impl/codegen/completion_queue_tag.h>
#include <grpcpp/impl/codegen/rpc_method.h>
#include <grpcpp/impl/codegen/sync_stream.h>

#include <memory>
#include <string>

namespace dropbox {
namespace grpc {

using RpcMethod = ::grpc::internal::RpcMethod;

inline ::grpc::Status BlockingUnaryCall(const std::shared_ptr<::grpc::Channel>& channel, const RpcMethod& method,
                                        ::grpc::ClientContext* context, const ::grpc::ByteBuffer& request,
                                        ::grpc::ByteBuffer* result) {
  return ::grpc::internal::BlockingUnaryCall<::grpc::ByteBuffer, ::grpc::ByteBuffer>(channel.get(), method, context,
                                                                                     request, result);
}

inline ::grpc::ClientAsyncWriter<::grpc::ByteBuffer>* CreateClientAsyncWriter(
    const std::shared_ptr<::grpc::Channel>& channel, ::grpc::CompletionQueue* cq, const RpcMethod& method,
    ::grpc::ClientContext* context, ::grpc::ByteBuffer* response) {
  return ::grpc::internal::ClientAsyncWriterFactory<::grpc::ByteBuffer>::Create(channel.get(), cq, method, context,
                                                                                response, false, nullptr);
}

inline ::grpc::ClientAsyncReader<::grpc::ByteBuffer>* CreateClientAsyncReader(
    const std::shared_ptr<::grpc::Channel>& channel, ::grpc::CompletionQueue* cq, const RpcMethod& method,
    ::grpc::ClientContext* context, const ::grpc::ByteBuffer& request) {
  return ::grpc::internal::ClientAsyncReaderFactory<::grpc::ByteBuffer>::Create(channel.get(), cq, method, context,
                                                                                request, false, nullptr);
}

inline ::grpc::ClientAsyncReaderWriter<::grpc::ByteBuffer, ::grpc::ByteBuffer>* CreateClientAsyncReaderWriter(
    const std::shared_ptr<::grpc::Channel>& channel, ::grpc::CompletionQueue* cq, const RpcMethod& method,
    ::grpc::ClientContext* context) {
  return ::grpc::internal::ClientAsyncReaderWriterFactory<::grpc::ByteBuffer, ::grpc::ByteBuffer>::Create(
      channel.get(), cq, method, context, false, nullptr);
}

inline ::grpc::ClientWriter<::grpc::ByteBuffer>* CreateClientWriter(const std::shared_ptr<::grpc::Channel>& channel,
                                                                    const RpcMethod& method,
                                                                    ::grpc::ClientContext* context,
                                                                    ::grpc::ByteBuffer* response) {
  return ::grpc::internal::ClientWriterFactory<::grpc::ByteBuffer>::Create(channel.get(), method, context, response);
}

inline ::grpc::ClientReader<::grpc::ByteBuffer>* CreateClientReader(const std::shared_ptr<::grpc::Channel>& channel,
                                                                    const RpcMethod& method,
                                                                    ::grpc::ClientContext* context,
                                                                    const ::grpc::ByteBuffer& request) {
  return ::grpc::internal::ClientReaderFactory<::grpc::ByteBuffer>::Create(channel.get(), method, context, request);
}

inline void SetCurrentThreadName(const std::string& name) noexcept {
  gpr_log(GPR_DEBUG, "SetCurrentThreadName: '%s'", name.c_str());
#ifdef __linux__
  int err = pthread_setname_np(pthread_self(), name.c_str());
  if (err != 0) {
    gpr_log(GPR_INFO, "SetCurrentThreadName: failed to set thread name: %s", strerror(err));
  }
#elif __APPLE__
  int err = pthread_setname_np(name.c_str());
  if (err != 0) {
    gpr_log(GPR_INFO, "SetCurrentThreadName: failed to set thread name: %s", strerror(err));
  }
#else
  gpr_log(GPR_INFO, "SetCurrentThreadName: unsupported platform");
#endif
}

}  // namespace grpc
}  // namespace dropbox

#endif  // DROPBOX_GRPC_INTERNAL_H_
