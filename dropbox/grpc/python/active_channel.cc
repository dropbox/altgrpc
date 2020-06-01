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

#include "dropbox/grpc/python/active_channel.h"

#include <grpcpp/create_channel.h>

#include <utility>

#include "dropbox/grpc/python/common.h"
#include "dropbox/grpc/time.h"

namespace dropbox {
namespace grpc {
namespace python {

class ActiveChannel::Private {};

ActiveChannel::ActiveChannel(const Private&, const std::string& target,
                             std::shared_ptr<::grpc::ChannelCredentials> credentials,
                             ::grpc::ChannelArguments channel_args)
    : channel_(::grpc::CreateCustomChannel(target, std::move(credentials), std::move(channel_args))) {}

std::shared_ptr<ActiveChannel> ActiveChannel::Create(const std::string& target,
                                                     std::shared_ptr<::grpc::ChannelCredentials> credentials,
                                                     ::grpc::ChannelArguments channel_args) {
  auto channel = std::make_shared<ActiveChannel>(Private{}, target, std::move(credentials), std::move(channel_args));
  channel->exit_barrier_.emplace();
  if (!channel->exit_barrier_) {
    channel->closing_ = true;
    channel->exit_barrier_.reset();
    channel->channel_.reset();
  } else {
    channel->exit_barrier_->RegisterOnExitCallback([channel] { channel->Close(); });
  }
  return channel;
}

PyObject* ActiveChannel::GetState(bool try_to_connect) noexcept {
  ExitBarrier barrier;
  auto grpc_channel = channel_;
  if (!barrier || !grpc_channel) {
    return ConnectivityFromState(GRPC_CHANNEL_SHUTDOWN);
  }
  grpc_connectivity_state return_value;
  {
    auto nogil = gil::Release();
    return_value = grpc_channel->GetState(try_to_connect);
  }
  return ConnectivityFromState(return_value);
}

PyObject* ActiveChannel::WaitForConnected(PyObject* timeout) noexcept {
  absl::Time deadline = absl::InfiniteFuture();
  if (timeout != kNone) {
    auto converted_timeout = types::Float{timeout}.ToDouble();
    if (converted_timeout.IsValid()) {
      deadline = absl::Now() + absl::Seconds(converted_timeout.Value());
    } else {
      std::move(converted_timeout).Error().Restore();
      return nullptr;
    }
  }
  PyObject* return_value;
  {
    auto grpc_channel = channel_;
    if (!grpc_channel) {
      Cython_RaiseChannelClosed();
      return nullptr;
    }
    ExitBarrier barrier;
    auto nogil = gil::Release();
    return_value = grpc_channel->WaitForConnected(deadline) ? kTrue : kFalse;
  }
  return AddRef(return_value).release();
}

void ActiveChannel::Close() noexcept {
  absl::flat_hash_set<grpc_call*> calls;
  {
    absl::MutexLock lock{&mutex_};
    if (closing_) {
      return;
    }
    closing_ = true;
    calls = std::move(active_calls_);
  }
  exit_barrier_.reset();
  for (auto call : calls) {
    grpc_call_cancel_with_status(call, GRPC_STATUS_CANCELLED, "Channel closed", nullptr);
    grpc_call_unref(call);
  }
  channel_.reset();
}

}  // namespace python
}  // namespace grpc
}  // namespace dropbox
