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

#ifndef DROPBOX_GRPC_PYTHON_CHANNEL_CONNECTIVITY_WATCHER_H_
#define DROPBOX_GRPC_PYTHON_CHANNEL_CONNECTIVITY_WATCHER_H_

#include <grpcpp/channel.h>

#include <memory>
#include <string>
#include <vector>

#include "absl/synchronization/mutex.h"
#include "dropbox/grpc/completion_queue_watcher.h"
#include "dropbox/grpc/internal.h"
#include "dropbox/grpc/python/dispatch.h"
#include "dropbox/grpc/python/interop.h"
#include "dropbox/grpc/refcount.h"

namespace dropbox {
namespace grpc {
namespace python {

class ChannelConnectivityWatcher : public RefCounted<ChannelConnectivityWatcher>, CompletionQueueTag {
 public:
  explicit ChannelConnectivityWatcher(const std::shared_ptr<::grpc::Channel>& channel) noexcept : channel_(channel) {}
  void Subscribe(PyObject* callback, bool try_to_connect) noexcept;
  void Unsubscribe(PyObject* callback) noexcept;
  void Shutdown() noexcept;

 private:
  void Proceed(bool ok) noexcept override;

  const std::weak_ptr<::grpc::Channel> channel_;
  absl::Mutex mutex_;
  bool initial_try_to_connect_ = false;
  bool shutting_down_ = false;
  grpc_connectivity_state last_observed_;
  std::vector<ObjectHandle> callbacks_;
  std::shared_ptr<CompletionQueueWatcher> watcher_;
  std::shared_ptr<Dispatcher> dispatcher_;
};

}  // namespace python
}  // namespace grpc
}  // namespace dropbox

#endif  // DROPBOX_GRPC_PYTHON_CHANNEL_CONNECTIVITY_WATCHER_H_
