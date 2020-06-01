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

// The following boilerplate is required to be the first thing included
extern "C" {
#define PY_SSIZE_T_CLEAN
#include "Python.h"
}
#if PY_MAJOR_VERSION <= 2

#include "dropbox/grpc/python/active_channel.h"
#include "dropbox/grpc/python/async_invoker.h"

namespace dropbox {
namespace grpc {
namespace python {

PyObject* AsyncUnaryUnaryInvoker(std::shared_ptr<ActiveChannel> channel, std::string rpc_name,
                                 RpcMethod::RpcType rpc_type,
                                 std::shared_ptr<const MessageSerializer> request_serializer,
                                 std::shared_ptr<const MessageDeserializer> response_deserializer) noexcept {
  PyErr_SetString(PyExc_NotImplementedError, "asyncio is not supported under Python 2");
  return nullptr;
}

PyObject* AsyncUnaryStreamInvoker(std::shared_ptr<ActiveChannel> channel, std::string rpc_name,
                                  RpcMethod::RpcType rpc_type,
                                  std::shared_ptr<const MessageSerializer> request_serializer,
                                  std::shared_ptr<const MessageDeserializer> response_deserializer) noexcept {
  PyErr_SetString(PyExc_NotImplementedError, "asyncio is not supported under Python 2");
  return nullptr;
}

PyObject* AsyncStreamUnaryInvoker(std::shared_ptr<ActiveChannel> channel, std::string rpc_name,
                                  RpcMethod::RpcType rpc_type,
                                  std::shared_ptr<const MessageSerializer> request_serializer,
                                  std::shared_ptr<const MessageDeserializer> response_deserializer) noexcept {
  PyErr_SetString(PyExc_NotImplementedError, "asyncio is not supported under Python 2");
  return nullptr;
}

PyObject* AsyncStreamStreamInvoker(std::shared_ptr<ActiveChannel> channel, std::string rpc_name,
                                   RpcMethod::RpcType rpc_type,
                                   std::shared_ptr<const MessageSerializer> request_serializer,
                                   std::shared_ptr<const MessageDeserializer> response_deserializer) noexcept {
  PyErr_SetString(PyExc_NotImplementedError, "asyncio is not supported under Python 2");
  return nullptr;
}

}  // namespace python
}  // namespace grpc
}  // namespace dropbox

#endif
