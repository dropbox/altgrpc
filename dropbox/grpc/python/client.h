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

#ifndef DROPBOX_GRPC_PYTHON_CLIENT_H_
#define DROPBOX_GRPC_PYTHON_CLIENT_H_

#include <grpcpp/client_context.h>

#include "dropbox/grpc/completion_queue_watcher.h"
#include "dropbox/grpc/internal.h"
#include "dropbox/grpc/python/active_channel.h"
#include "dropbox/grpc/python/common.h"

namespace dropbox {
namespace grpc {
namespace python {

class Rendezvous;
class RpcInvoker : private ::grpc::GrpcLibraryCodegen {
 public:
  RpcInvoker() noexcept {}
  ~RpcInvoker();

  void Initialize(std::shared_ptr<ActiveChannel> channel, std::string rpc_name, RpcMethod::RpcType rpc_type,
                  std::shared_ptr<const MessageSerializer> request_serializer,
                  std::shared_ptr<const MessageDeserializer> response_deserializer) noexcept;
  void InvokeUnaryUnary(PyObject* py_rendezvous, Rendezvous* rendezvous, PyObject* request) noexcept;
  void InvokeUnaryStream(PyObject* py_rendezvous, Rendezvous* rendezvous, PyObject* request) noexcept;
  void InvokeStreamUnary(PyObject* py_rendezvous, Rendezvous* rendezvous, PyObject* request_iterator) noexcept;
  void InvokeStreamStream(PyObject* py_rendezvous, Rendezvous* rendezvous, PyObject* request_iterator) noexcept;

 private:
  std::shared_ptr<ActiveChannel> channel_;
  absl::optional<const RpcMethod> method_;
  std::string method_name_;
  std::shared_ptr<const MessageSerializer> request_serializer_;
  std::shared_ptr<const MessageDeserializer> response_deserializer_;
};

std::shared_ptr<CompletionQueueWatcher> ClientCompletionQueueWatcher();

}  // namespace python
}  // namespace grpc
}  // namespace dropbox

#endif  // DROPBOX_GRPC_PYTHON_CLIENT_H_
