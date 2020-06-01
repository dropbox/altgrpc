// Copyright (c) 2020 Dropbox, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef DROPBOX_GRPC_PYTHON_SERVER_H_
#define DROPBOX_GRPC_PYTHON_SERVER_H_

#include <grpcpp/impl/codegen/grpc_library.h>

#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/synchronization/mutex.h"
#include "absl/time/time.h"
#include "absl/types/optional.h"
#include "dropbox/grpc/future.h"
#include "dropbox/grpc/python/common.h"
#include "dropbox/grpc/python/dispatch.h"
#include "dropbox/grpc/python/interop.h"
#include "dropbox/grpc/server.h"
#include "dropbox/types/expected.h"
#include "grpcpp/grpcpp.h"

namespace dropbox {
namespace grpc {
namespace python {

class Server;

struct RequestHandler {
  RequestHandler() {}
  RequestHandler(PyObject* thread_pool_submit, ObjectHandle service_pipeline,
                 absl::optional<size_t> concurrency_limit = absl::nullopt);

  void AddMethod(std::string full_method_path) { full_method_paths.emplace_back(std::move(full_method_path)); }

  std::shared_ptr<Dispatcher> dispatcher;
  grpc::RequestHandlerSharedPtr handler;
  std::vector<std::string> full_method_paths;
};

struct RequestHandlers {
  RequestHandlers() {}
  explicit RequestHandlers(RequestHandler default_handler) {
    handlers.default_handler = std::move(default_handler.handler);
    dispatchers.emplace_back(std::move(default_handler.dispatcher));
  }

  void AddHandler(RequestHandler handler) {
    for (std::string& method : handler.full_method_paths) {
      handlers.handler_by_method.emplace(std::move(method), handler.handler);
    }
    dispatchers.emplace_back(std::move(handler.dispatcher));
  }

  grpc::RequestHandlers handlers;
  std::vector<std::shared_ptr<Dispatcher>> dispatchers;
};

class Server final : ::grpc::GrpcLibraryCodegen {
 public:
  bool BuildAndStart(::grpc::ServerBuilder* builder, RequestHandlers handlers, size_t thread_count) noexcept;
  ~Server();

  void Loop() noexcept;
  PyObject* Stop(absl::optional<double> grace = absl::nullopt) noexcept;

 private:
  void StopInternal(double grace) noexcept;

  std::unique_ptr<grpc::Server> server_;
  std::vector<std::shared_ptr<Dispatcher>> dispatchers_;
  AutoReleasedObject stop_event_;

  absl::Mutex mutex_;
  bool stopping_ = false;
  std::thread shutdown_thread_;
};

class ServicerContext final : ::grpc::GrpcLibraryCodegen {
 public:
  ServicerContext() noexcept {}
  ~ServicerContext();

  void Start(RefCountedPtr<ServerCall> call, std::shared_ptr<Dispatcher> dispatcher,
             AutoReleasedObject servicer_context, ObjectHandle service_pipeline) noexcept;
  void SetSerDes(MessageSerializer response_serializer, MessageDeserializer request_deserializer) {
    response_serializer_ = std::move(response_serializer);
    request_deserializer_ = std::move(request_deserializer);
  }

  bool IsActive() noexcept;
  types::Float TimeRemaining() noexcept;  // Optional[double]
  void Cancel() noexcept;
  bool AddCallback(std::function<void()> callback) noexcept;

  types::Str Method() noexcept;                // str
  types::Tuple InvocationMetadata() noexcept;  // Tuple[(str metadata_key, bytes/str metadata_value)]
  types::Str Peer() noexcept;                  // str
  types::Tuple PeerIdentities() noexcept;      // Tuple[bytes]
  types::Str PeerIdentityKey() noexcept;       // str
  types::Dict AuthContext() noexcept;          // Dict[str, Tuple[bytes]]

  void SetCompression(grpc_compression_algorithm compression) noexcept;
  void DisableNextMessageCompression() noexcept;

  void AddInitialMetadata(std::string key, std::string value) noexcept;
  bool SendInitialMetadataBlocking() noexcept;
  void AddTrailingMetadata(std::string key, std::string value) noexcept;

  PyObject* Read();
  void Write(PyObject* message);

  void SetCode(grpc_status_code status_code) noexcept;
  void SetDetails(std::string details) noexcept;

  void Abort(::grpc::Status abortion_status) noexcept;
  void End(const ::grpc::ByteBuffer& last_message) noexcept;
  void End() noexcept;

  void AbortUnrecognizedMethod() noexcept;
  void AbortHandlerLookupFailed() noexcept;

  Dispatcher* dispatcher() const noexcept { return dispatcher_.get(); }
  ServerCall* call() const noexcept { return call_.get(); }

 private:
  void EnsureInitialMetadata() noexcept;

  RefCountedPtr<ServerCall> call_;
  std::shared_ptr<Dispatcher> dispatcher_;

  std::once_flag initial_metadata_, peer_loaded_, peer_identity_key_loaded_, peer_identities_loaded_,
      auth_context_loaded_;
  AutoReleasedObject method_, invocation_metadata_, peer_, peer_identity_key_, peer_identities_, auth_context_;
  MessageDeserializer request_deserializer_;
  MessageSerializer response_serializer_;

  absl::Mutex status_mutex_;
  absl::optional<::grpc::StatusCode> status_code_;
  std::string details_;
};

namespace rpc {

class MethodHandler : public std::enable_shared_from_this<MethodHandler> {
 public:
  class Private;
  MethodHandler(const Private&, bool request_streaming, bool response_streaming, bool iterators,
                MessageDeserializer request_deserializer, MessageSerializer response_serializer, ObjectHandle handler)
      : request_streaming_(request_streaming),
        response_streaming_(response_streaming),
        iterators_(iterators),
        request_deserializer_(std::move(request_deserializer)),
        response_serializer_(std::move(response_serializer)),
        handler_(std::move(handler)) {}

  static std::shared_ptr<MethodHandler> Create(bool request_streaming, bool response_streaming, bool iterators,
                                               ObjectHandle handler, MessageDeserializer request_desrializer,
                                               MessageSerializer response_serializer);

  void Handle(ObjectHandle servicer_context, ServicerContext* call) const;

 private:
  void HandleUnaryRequest(ObjectHandle servicer_context, ServicerContext* call) const;
  void HandleStreamingRequest(ObjectHandle servicer_context, ServicerContext* call) const;
  void HandleIteratorLessCall(ObjectHandle servicer_context, ServicerContext* call) const;
  void ContinueUnaryResponse(ObjectHandle servicer_context, ServicerContext* call, AutoReleasedObject request) const;
  void ContinueStreamingResponse(ObjectHandle servicer_context, ServicerContext* call,
                                 AutoReleasedObject request_iterator) const;
  void Continue(ObjectHandle servicer_context, ServicerContext* call, AutoReleasedObject request_or_iterator) const;
  void StreamResponses(ObjectHandle servicer_context, ServicerContext* call,
                       AutoReleasedObject response_iterator) const;
  void StreamResponsesAsync(ObjectHandle servicer_context, ServicerContext* call,
                            AutoReleasedObject response_iterator) const;

  const bool request_streaming_, response_streaming_, iterators_;
  MessageDeserializer request_deserializer_;
  MessageSerializer response_serializer_;
  const ObjectHandle handler_;
};

}  // namespace rpc
}  // namespace python
}  // namespace grpc
}  // namespace dropbox

#endif  // DROPBOX_GRPC_PYTHON_SERVER_H_
