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

#include "dropbox/grpc/python/channel_connectivity_watcher.h"

#include <utility>
#include <vector>

#include "dropbox/grpc/python/client.h"
#include "dropbox/grpc/python/common.h"
#include "dropbox/grpc/time.h"

namespace dropbox {
namespace grpc {
namespace python {

void ChannelConnectivityWatcher::Subscribe(PyObject* py_callback, bool try_to_connect) noexcept {
  auto callback = AddRef(py_callback);
  bool start_loop = false;
  auto grpc_channel = channel_.lock();
  if (!grpc_channel) {
    Shutdown();
    return;
  }
  {
    absl::MutexLock lock{&mutex_};
    if (shutting_down_) {
      return;
    }
    if (try_to_connect) {
      initial_try_to_connect_ = true;
    }
    callbacks_.emplace_back(std::move(callback));
    if (!watcher_) {
      start_loop = true;
      watcher_ = ClientCompletionQueueWatcher();
      dispatcher_ = ClientQueue();
      last_observed_ = grpc_channel->GetState(try_to_connect);
    } else if (try_to_connect) {
      // Ensure we try to connect, even if the previous invocation has been false.
      grpc_channel->GetState(true);
    }
  }
  // There's a potential for race here; the callback may be called
  // once here and once from the callback notification procedure,
  // but the risk should be acceptable.
  AutoReleasedObject py_state{ConnectivityFromState(last_observed_)};
  CallPython(py_callback, py_state.get());
  if (start_loop) {
    auto nogil = gil::Release();
    absl::MutexLock lock{&mutex_};
    grpc_channel->NotifyOnStateChange(last_observed_, absl::InfiniteFuture(), watcher_->CompletionQueue(),
                                      Ref().release());
  }
}
void ChannelConnectivityWatcher::Unsubscribe(PyObject* callback) noexcept {
  absl::MutexLock lock{&mutex_};
  if (shutting_down_) {
    return;
  }
  for (auto it = callbacks_.begin(); it != callbacks_.end(); ++it) {
    if (it->get() == callback) {
      callbacks_.erase(it);
      return;
    }
  }
}

void ChannelConnectivityWatcher::Shutdown() noexcept {
  absl::MutexLock lock{&mutex_};
  shutting_down_ = true;
  decltype(callbacks_){}.swap(callbacks_);
}

void ChannelConnectivityWatcher::Proceed(bool ok) noexcept {
  if (!ok) {
    Unref();
    return;
  }
  std::vector<ObjectHandle> callbacks_snapshot;
  {
    absl::ReleasableMutexLock lock{&mutex_};
    if (shutting_down_) {
      lock.Release();
      Unref();
      return;
    }
    auto grpc_channel = channel_.lock();
    if (!grpc_channel) {
      shutting_down_ = true;
      decltype(callbacks_)().swap(callbacks_);
      return;
    }
    auto current_state = grpc_channel->GetState(initial_try_to_connect_);
    if (current_state != last_observed_) {
      last_observed_ = current_state;
      callbacks_snapshot = callbacks_;
    }
  }
  if (!callbacks_snapshot.empty()) {
    dispatcher_->RunInPool([callbacks_ = std::move(callbacks_snapshot), state = last_observed_] {
      AutoReleasedObject py_state{ConnectivityFromState(state)};
      for (auto& callback : callbacks_) {
        CallPython(callback.get(), py_state.get());
      }
    });
  }
  {
    absl::MutexLock lock{&mutex_};
    auto grpc_channel = channel_.lock();
    if (!shutting_down_ && !callbacks_.empty() && grpc_channel) {
      grpc_channel->NotifyOnStateChange(last_observed_, absl::InfiniteFuture(), watcher_->CompletionQueue(), this);
      return;
    }
    watcher_.reset();
    dispatcher_.reset();
  }
  Unref();
}

}  // namespace python
}  // namespace grpc
}  // namespace dropbox
