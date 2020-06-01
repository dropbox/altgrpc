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

#ifndef DROPBOX_GRPC_PYTHON_ACTIVE_CHANNEL_H_
#define DROPBOX_GRPC_PYTHON_ACTIVE_CHANNEL_H_

#include <grpc/grpc.h>
#include <grpcpp/channel.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/support/channel_arguments.h>

#include <memory>
#include <string>

#include "absl/container/flat_hash_set.h"
#include "absl/synchronization/mutex.h"
#include "absl/types/optional.h"
#include "dropbox/grpc/python/interop.h"

namespace dropbox {
namespace grpc {
namespace python {

class ActiveChannel {
 public:
  class Private;
  ActiveChannel(const Private&, const std::string& target, std::shared_ptr<::grpc::ChannelCredentials> credentials,
                ::grpc::ChannelArguments channel_args);
  static std::shared_ptr<ActiveChannel> Create(const std::string& target,
                                               std::shared_ptr<::grpc::ChannelCredentials> credentials,
                                               ::grpc::ChannelArguments channel_args);

  void Register(grpc_call* call) {
    if (call) {
      grpc_call_ref(call);
      absl::ReleasableMutexLock lock{&mutex_};
      if (closing_) {
        lock.Release();
        grpc_call_cancel_with_status(call, GRPC_STATUS_CANCELLED, "Channel closed", nullptr);
        grpc_call_unref(call);
      } else {
        active_calls_.insert(call);
      }
    }
  }

  void Release(grpc_call* call) {
    if (closing_) {
      return;
    }
    absl::ReleasableMutexLock lock{&mutex_};
    if (closing_) {
      return;
    }
    if (active_calls_.erase(call)) {
      lock.Release();
      grpc_call_unref(call);
    }
  }

  PyObject* GetState(bool try_to_connect) noexcept;
  PyObject* WaitForConnected(PyObject* timeout) noexcept;
  void Close() noexcept;

  constexpr const std::shared_ptr<::grpc::Channel>& channel() const noexcept { return channel_; }

 private:
  std::shared_ptr<::grpc::Channel> channel_;
  absl::optional<ExitBarrier> exit_barrier_;
  absl::Mutex mutex_;
  bool closing_ = false;
  // TODO(mehrdad): think of ways to optimize this under contention
  absl::flat_hash_set<grpc_call*> active_calls_;
};

}  // namespace python
}  // namespace grpc
}  // namespace dropbox

#endif  // DROPBOX_GRPC_PYTHON_ACTIVE_CHANNEL_H_
