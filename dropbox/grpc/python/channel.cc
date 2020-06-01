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

#include "dropbox/grpc/python/channel.h"

#include <grpcpp/create_channel.h>

#include "absl/base/attributes.h"
#include "dropbox/grpc/python/async_invoker.h"
#include "dropbox/grpc/python/client.h"
#include "dropbox/grpc/time.h"

namespace dropbox {
namespace grpc {
namespace python {

namespace {

extern "C" {
PyObject* Cython_NewUnaryUnaryMultiCallable(RpcInvoker** invoker);
PyObject* Cython_NewUnaryStreamMultiCallable(RpcInvoker** invoker);
PyObject* Cython_NewStreamUnaryMultiCallable(RpcInvoker** invoker);
PyObject* Cython_NewStreamStreamMultiCallable(RpcInvoker** invoker);
}  // extern "C"

}  // namespace

void Channel::Initialize(const std::string& target, ::grpc::ChannelArguments* channel_args,
                         std::shared_ptr<::grpc::ChannelCredentials> credentials) noexcept {
  channel_ = ActiveChannel::Create(target, std::move(credentials), std::move(*channel_args));
  connectivity_watcher_ = MakeRefCounted<ChannelConnectivityWatcher>(channel_->channel());
}

PyObject* Channel::UnaryUnary(PyObject* py_method_name, PyObject* request_serializer,
                              PyObject* response_deserializer) noexcept {
  AutoReleasedObject encoded_method_name{_utf8_bytes(py_method_name)};
  if (ABSL_PREDICT_FALSE(!encoded_method_name)) {
    return nullptr;
  }
  auto method_name = types::Bytes{encoded_method_name.get()}.ToString();
  RpcInvoker* invoker;
  auto callable = AutoReleasedObject{Cython_NewUnaryUnaryMultiCallable(&invoker)};
  invoker->Initialize(channel_, std::move(method_name), RpcMethod::RpcType::NORMAL_RPC,
                      std::make_shared<const MessageSerializer>(GenericSerializer(request_serializer)),
                      std::make_shared<const MessageDeserializer>(GenericDeserializer(response_deserializer)));
  return callable.release();
}

PyObject* Channel::UnaryStream(PyObject* py_method_name, PyObject* request_serializer,
                               PyObject* response_deserializer) noexcept {
  AutoReleasedObject encoded_method_name{_utf8_bytes(py_method_name)};
  if (ABSL_PREDICT_FALSE(!encoded_method_name)) {
    return nullptr;
  }
  auto method_name = types::Bytes{encoded_method_name.get()}.ToString();
  RpcInvoker* invoker;
  auto callable = AutoReleasedObject{Cython_NewUnaryStreamMultiCallable(&invoker)};
  invoker->Initialize(channel_, std::move(method_name), RpcMethod::RpcType::SERVER_STREAMING,
                      std::make_shared<const MessageSerializer>(GenericSerializer(request_serializer)),
                      std::make_shared<const MessageDeserializer>(GenericDeserializer(response_deserializer)));
  return callable.release();
}

PyObject* Channel::StreamUnary(PyObject* py_method_name, PyObject* request_serializer,
                               PyObject* response_deserializer) noexcept {
  AutoReleasedObject encoded_method_name{_utf8_bytes(py_method_name)};
  if (ABSL_PREDICT_FALSE(!encoded_method_name)) {
    return nullptr;
  }
  auto method_name = types::Bytes{encoded_method_name.get()}.ToString();
  RpcInvoker* invoker;
  auto callable = AutoReleasedObject{Cython_NewStreamUnaryMultiCallable(&invoker)};
  invoker->Initialize(channel_, std::move(method_name), RpcMethod::RpcType::CLIENT_STREAMING,
                      std::make_shared<const MessageSerializer>(GenericSerializer(request_serializer)),
                      std::make_shared<const MessageDeserializer>(GenericDeserializer(response_deserializer)));
  return callable.release();
}

PyObject* Channel::StreamStream(PyObject* py_method_name, PyObject* request_serializer,
                                PyObject* response_deserializer) noexcept {
  AutoReleasedObject encoded_method_name{_utf8_bytes(py_method_name)};
  if (ABSL_PREDICT_FALSE(!encoded_method_name)) {
    return nullptr;
  }
  auto method_name = types::Bytes{encoded_method_name.get()}.ToString();
  RpcInvoker* invoker;
  auto callable = AutoReleasedObject{Cython_NewStreamStreamMultiCallable(&invoker)};
  invoker->Initialize(channel_, std::move(method_name), RpcMethod::RpcType::BIDI_STREAMING,
                      std::make_shared<const MessageSerializer>(GenericSerializer(request_serializer)),
                      std::make_shared<const MessageDeserializer>(GenericDeserializer(response_deserializer)));
  return callable.release();
}

PyObject* Channel::AsyncUnaryUnary(PyObject* py_method_name, PyObject* request_serializer,
                                   PyObject* response_deserializer) noexcept {
  AutoReleasedObject encoded_method_name{_utf8_bytes(py_method_name)};
  if (ABSL_PREDICT_FALSE(!encoded_method_name)) {
    return nullptr;
  }
  auto method_name = types::Bytes{encoded_method_name.get()}.ToString();
  return AsyncUnaryUnaryInvoker(
      channel_, std::move(method_name), RpcMethod::RpcType::NORMAL_RPC,
      std::make_shared<const MessageSerializer>(GenericSerializer(request_serializer)),
      std::make_shared<const MessageDeserializer>(GenericDeserializer(response_deserializer)));
}

PyObject* Channel::AsyncUnaryStream(PyObject* py_method_name, PyObject* request_serializer,
                                    PyObject* response_deserializer) noexcept {
  AutoReleasedObject encoded_method_name{_utf8_bytes(py_method_name)};
  if (ABSL_PREDICT_FALSE(!encoded_method_name)) {
    return nullptr;
  }
  auto method_name = types::Bytes{encoded_method_name.get()}.ToString();
  return AsyncUnaryStreamInvoker(
      channel_, std::move(method_name), RpcMethod::RpcType::SERVER_STREAMING,
      std::make_shared<const MessageSerializer>(GenericSerializer(request_serializer)),
      std::make_shared<const MessageDeserializer>(GenericDeserializer(response_deserializer)));
}

PyObject* Channel::AsyncStreamUnary(PyObject* py_method_name, PyObject* request_serializer,
                                    PyObject* response_deserializer) noexcept {
  AutoReleasedObject encoded_method_name{_utf8_bytes(py_method_name)};
  if (ABSL_PREDICT_FALSE(!encoded_method_name)) {
    return nullptr;
  }
  auto method_name = types::Bytes{encoded_method_name.get()}.ToString();
  return AsyncStreamUnaryInvoker(
      channel_, std::move(method_name), RpcMethod::RpcType::CLIENT_STREAMING,
      std::make_shared<const MessageSerializer>(GenericSerializer(request_serializer)),
      std::make_shared<const MessageDeserializer>(GenericDeserializer(response_deserializer)));
}

PyObject* Channel::AsyncStreamStream(PyObject* py_method_name, PyObject* request_serializer,
                                     PyObject* response_deserializer) noexcept {
  AutoReleasedObject encoded_method_name{_utf8_bytes(py_method_name)};
  if (ABSL_PREDICT_FALSE(!encoded_method_name)) {
    return nullptr;
  }
  auto method_name = types::Bytes{encoded_method_name.get()}.ToString();
  return AsyncStreamStreamInvoker(
      channel_, std::move(method_name), RpcMethod::RpcType::BIDI_STREAMING,
      std::make_shared<const MessageSerializer>(GenericSerializer(request_serializer)),
      std::make_shared<const MessageDeserializer>(GenericDeserializer(response_deserializer)));
}

RpcInvoker::~RpcInvoker() {
  auto nogil = gil::Release();
  channel_.reset();
  request_serializer_ = nullptr;
  response_deserializer_ = nullptr;
}

void RpcInvoker::Initialize(std::shared_ptr<ActiveChannel> channel, std::string rpc_name, RpcMethod::RpcType rpc_type,
                            std::shared_ptr<const MessageSerializer> request_serializer,
                            std::shared_ptr<const MessageDeserializer> response_deserializer) noexcept {
  channel_ = std::move(channel);
  method_name_ = std::move(rpc_name);
  method_.emplace(method_name_.c_str(), rpc_type);
  request_serializer_ = std::move(request_serializer);
  response_deserializer_ = std::move(response_deserializer);
}

std::shared_ptr<CompletionQueueWatcher> ClientCompletionQueueWatcher() {
  static std::shared_ptr<CompletionQueueWatcher> watchers[] = {
      std::make_shared<CompletionQueueWatcher>("pychan-cq1"),
      std::make_shared<CompletionQueueWatcher>("pychan-cq2"),
      std::make_shared<CompletionQueueWatcher>("pychan-cq3"),
  };
  thread_local size_t index = absl::GetCurrentTimeNanos();
  return watchers[index++ % (sizeof(watchers) / sizeof(*watchers))];
}

}  // namespace python
}  // namespace grpc
}  // namespace dropbox
