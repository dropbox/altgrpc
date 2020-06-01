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

#ifndef DROPBOX_GRPC_PYTHON_CHANNEL_H_
#define DROPBOX_GRPC_PYTHON_CHANNEL_H_

#include <grpcpp/security/credentials.h>
#include <grpcpp/support/channel_arguments.h>

#include <memory>
#include <string>

#include "dropbox/grpc/python/active_channel.h"
#include "dropbox/grpc/python/channel_connectivity_watcher.h"
#include "dropbox/grpc/python/interop.h"

namespace dropbox {
namespace grpc {
namespace python {

class Channel {
 public:
  Channel() noexcept {}
  ~Channel() {
    auto nogil = gil::Release();
    if (connectivity_watcher_.get()) {
      connectivity_watcher_->Shutdown();
      connectivity_watcher_.reset();
    }
    channel_.reset();
  }

  void Initialize(const std::string& target, ::grpc::ChannelArguments* channel_args,
                  std::shared_ptr<::grpc::ChannelCredentials> credentials) noexcept;
  PyObject* UnaryUnary(PyObject* method_name, PyObject* request_serializer, PyObject* response_deserializer) noexcept;
  PyObject* UnaryStream(PyObject* method_name, PyObject* request_serializer, PyObject* response_deserializer) noexcept;
  PyObject* StreamUnary(PyObject* method_name, PyObject* request_serializer, PyObject* response_deserializer) noexcept;
  PyObject* StreamStream(PyObject* method_name, PyObject* request_serializer, PyObject* response_deserializer) noexcept;
  PyObject* AsyncUnaryUnary(PyObject* method_name, PyObject* request_serializer,
                            PyObject* response_deserializer) noexcept;
  PyObject* AsyncUnaryStream(PyObject* method_name, PyObject* request_serializer,
                             PyObject* response_deserializer) noexcept;
  PyObject* AsyncStreamUnary(PyObject* method_name, PyObject* request_serializer,
                             PyObject* response_deserializer) noexcept;
  PyObject* AsyncStreamStream(PyObject* method_name, PyObject* request_serializer,
                              PyObject* response_deserializer) noexcept;
  PyObject* WaitForConnected(PyObject* timeout) noexcept { return channel_->WaitForConnected(timeout); }
  PyObject* GetState(bool try_to_connect) noexcept { return channel_->GetState(try_to_connect); }
  void Subscribe(PyObject* callback, bool try_to_connect) noexcept {
    connectivity_watcher_->Subscribe(callback, try_to_connect);
  }
  void Unsubscribe(PyObject* callback) noexcept { connectivity_watcher_->Unsubscribe(callback); }
  void Close() noexcept {
    auto nogil = gil::Release();
    connectivity_watcher_->Shutdown();
    channel_->Close();
  }

 private:
  std::shared_ptr<ActiveChannel> channel_;
  RefCountedPtr<ChannelConnectivityWatcher> connectivity_watcher_;
};

}  // namespace python
}  // namespace grpc
}  // namespace dropbox

#endif  // DROPBOX_GRPC_PYTHON_CHANNEL_H_
