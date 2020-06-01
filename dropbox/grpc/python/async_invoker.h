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

#ifndef DROPBOX_GRPC_PYTHON_ASYNC_INVOKER_H_
#define DROPBOX_GRPC_PYTHON_ASYNC_INVOKER_H_

#include <memory>
#include <string>

#include "dropbox/grpc/internal.h"
#include "dropbox/grpc/python/active_channel.h"
#include "dropbox/grpc/python/common.h"
#include "dropbox/grpc/python/interop.h"

namespace dropbox {
namespace grpc {
namespace python {

PyObject* AsyncUnaryUnaryInvoker(std::shared_ptr<ActiveChannel> channel, std::string rpc_name,
                                 RpcMethod::RpcType rpc_type,
                                 std::shared_ptr<const MessageSerializer> request_serializer,
                                 std::shared_ptr<const MessageDeserializer> response_deserializer) noexcept;

PyObject* AsyncUnaryStreamInvoker(std::shared_ptr<ActiveChannel> channel, std::string rpc_name,
                                  RpcMethod::RpcType rpc_type,
                                  std::shared_ptr<const MessageSerializer> request_serializer,
                                  std::shared_ptr<const MessageDeserializer> response_deserializer) noexcept;

PyObject* AsyncStreamUnaryInvoker(std::shared_ptr<ActiveChannel> channel, std::string rpc_name,
                                  RpcMethod::RpcType rpc_type,
                                  std::shared_ptr<const MessageSerializer> request_serializer,
                                  std::shared_ptr<const MessageDeserializer> response_deserializer) noexcept;

PyObject* AsyncStreamStreamInvoker(std::shared_ptr<ActiveChannel> channel, std::string rpc_name,
                                   RpcMethod::RpcType rpc_type,
                                   std::shared_ptr<const MessageSerializer> request_serializer,
                                   std::shared_ptr<const MessageDeserializer> response_deserializer) noexcept;

}  // namespace python
}  // namespace grpc
}  // namespace dropbox

#endif  // DROPBOX_GRPC_PYTHON_ASYNC_INVOKER_H_
