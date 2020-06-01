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

#ifndef DROPBOX_GRPC_SERVER_H_
#define DROPBOX_GRPC_SERVER_H_

#include <grpcpp/generic/async_generic_service.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/impl/codegen/completion_queue_tag.h>
#include <grpcpp/impl/codegen/grpc_library.h>

#include <atomic>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "absl/base/call_once.h"
#include "absl/container/flat_hash_map.h"
#include "absl/synchronization/mutex.h"
#include "absl/types/optional.h"
#include "dropbox/grpc/future.h"
#include "dropbox/grpc/internal.h"
#include "dropbox/grpc/server_call.h"

namespace dropbox {
namespace grpc {

class RequestHandler {
 public:
  RequestHandler(std::function<void(RefCountedPtr<ServerCall>)> handler, absl::optional<size_t> concurrency_limit);

  // Increment active calls. Returns true iff concurrency limit is not exceeded yet.
  // This call must always be matched by DecrementActiveCalls one.
  bool IncrementActiveCalls() {
    if (concurrency_limit_.has_value()) {
      return ++active_calls_ <= concurrency_limit_;
    }
    // Always allow requests is limit is not specified.
    return true;
  }

  // Decrement number of active calls.
  void DecrementActiveCalls() {
    if (concurrency_limit_.has_value()) {
      --active_calls_;
    }
  }

  // Handle the call.
  void Handle(RefCountedPtr<ServerCall> call) { handler_(call); }

 private:
  const std::function<void(RefCountedPtr<ServerCall>)> handler_;
  const absl::optional<size_t> concurrency_limit_;
  std::atomic<size_t> active_calls_;
};

using RequestHandlerSharedPtr = std::shared_ptr<RequestHandler>;

struct RequestHandlers {
  absl::flat_hash_map<std::string, RequestHandlerSharedPtr> handler_by_method;
  RequestHandlerSharedPtr default_handler;

  RequestHandlerSharedPtr Get(const std::string& method) const {
    auto it = handler_by_method.find(method);
    if (it != handler_by_method.end()) {
      return it->second;
    }
    return default_handler;
  }
};

class Server : private ::grpc::GrpcLibraryCodegen {
 public:
  class Private;

  Server(Private, std::unique_ptr<::grpc::Server> server, std::unique_ptr<::grpc::AsyncGenericService> service,
         std::vector<std::unique_ptr<::grpc::ServerCompletionQueue>> cqs, RequestHandlers handlers);
  ~Server();
  void AcceptCalls();
  void Stop(double grace);

  RefCountedPtr<Task> stopped() const noexcept { return stopped_task_; }

 private:
  struct Worker : CompletionQueueTag {
    Worker(Server* server, std::unique_ptr<::grpc::ServerCompletionQueue> cq);
    static void Start(Worker* const self);
    void Proceed(bool status) noexcept override { server->OnRequest(this, status); }

    Server* const server;
    const std::unique_ptr<::grpc::ServerCompletionQueue> cq;
    std::thread thread;
    RefCountedPtr<ServerCall> call;
  };

  void AcceptCall(Worker* const worker);
  void OnRequest(Worker* const worker, bool ok);

  const std::unique_ptr<::grpc::Server> server_;
  const std::unique_ptr<::grpc::AsyncGenericService> service_;
  const RequestHandlers handlers_;
  std::vector<std::unique_ptr<Worker>> workers_;

  absl::Mutex mutex_;
  absl::once_flag shutdown_once_;
  size_t all_calls_ = 0;
  RefCountedPtr<Task> stopped_task_ = CreateTask();
};

void AddPointerArgument(::grpc::ServerBuilder* builder, const std::string& name, void* value);
std::unique_ptr<Server> BuildAndStartServer(::grpc::ServerBuilder* builder, RequestHandlers handlers,
                                            size_t thread_count = 0) noexcept;

}  // namespace grpc
}  // namespace dropbox

#endif  // DROPBOX_GRPC_SERVER_H_
