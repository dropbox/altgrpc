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

#include <utility>

#include "absl/container/flat_hash_map.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "dropbox/grpc/server.h"
#include "grpcpp/generic/async_generic_service.h"
#include "grpcpp/grpcpp.h"
#include "grpcpp/impl/codegen/status_code_enum.h"

namespace dropbox {
namespace grpc {

ServerCall::ServerCall() noexcept : stream_(&context_), done_task_(CreateFuture<ServerCall*>()) {
  context_.AsyncNotifyWhenDone(this);
}

void ServerCall::StartCall() {
  // AsyncNotifyWhenDone is an anomaly in gRPC API:
  // https://github.com/grpc/grpc/issues/10136
  //
  // It needs to be called before the call is associated to an incoming
  // request by the server, but if the call never starts, you will never
  // see the tag.  Therefore we need to ensure we clean the task up
  // in the destructor if the call never starts.
  Ref().release();
  absl::MutexLock lock{&mutex_};
  GPR_DEBUG_ASSERT(state_ == kNotStarted);
  state_ = kActive;
}

RefCountedPtr<Task> ServerCall::SendInitialMetadata() {
  auto send_initial_metadata_task = CreateTask();
  stream_.SendInitialMetadata(send_initial_metadata_task->RefTag());
  return send_initial_metadata_task;
}

RefCountedPtr<Future<::grpc::ByteBuffer>> ServerCall::Read() {
  absl::MutexLock lock{&mutex_};
  if (state_ != kActive) {
    gpr_log(GPR_DEBUG, "Read called on handler in non-active state (%d)", state_.load());
    auto task = CreateFuture<::grpc::ByteBuffer>();
    task->Finalize(false);
    return task;
  }
  mutex_.Await(absl::Condition{&can_read_});
  auto read_future = CreateFuture<::grpc::ByteBuffer>();
  const bool ABSL_ATTRIBUTE_UNUSED is_callback_added = read_future->AddCallback([self = Ref()](auto task) {
    absl::MutexLock lock{&self->mutex_};
    self->can_read_ = true;
  });
  GPR_DEBUG_ASSERT(is_callback_added);
  can_read_ = false;
  stream_.Read(&read_future->result(), read_future->RefTag());
  return read_future;
}

RefCountedPtr<Task> ServerCall::Write(const ::grpc::ByteBuffer& buffer) {
  absl::MutexLock lock{&mutex_};
  if (state_ != kActive) {
    gpr_log(GPR_DEBUG, "Write called on handler in non-active state (%d)", state_.load());
    auto task = CreateTask();
    task->Finalize(false);
    return task;
  }
  mutex_.Await(absl::Condition{&can_write_});
  auto task = CreateTask();
  const bool ABSL_ATTRIBUTE_UNUSED is_callback_added = task->AddCallback([self = Ref()](auto task) {
    absl::MutexLock lock{&self->mutex_};
    self->can_write_ = true;
  });
  GPR_DEBUG_ASSERT(is_callback_added);
  can_write_ = false;
  stream_.Write(buffer, task->RefTag());
  return task;
}

void ServerCall::OnFinish(bool ok) {
  {
    absl::MutexLock lock{&mutex_};
    if (state_ == kDone) {
      return;
    }
    state_ = kDone;
    can_write_ = true;
    can_read_ = true;
    done_task_->result() = this;
  }
  done_task_->Finalize(true);
}

RefCountedPtr<Task> ServerCall::WriteAndFinish(const ::grpc::ByteBuffer& buffer, ::grpc::Status status) {
  absl::MutexLock lock{&mutex_};
  if (state_ != kActive) {
    gpr_log(GPR_DEBUG, "WriteAndFinish called on handler in non-active state (%d)", state_.load());
    auto task = CreateTask();
    task->Finalize(false);
    return task;
  }
  state_ = kFinishing;
  mutex_.Await(absl::Condition{&can_write_});
  auto task = CreateTask();
  const bool ABSL_ATTRIBUTE_UNUSED is_callback_added =
      task->AddCallback([self = Ref()](auto task) { self->OnFinish(task->ok()); });
  GPR_DEBUG_ASSERT(is_callback_added);
  can_write_ = false;
  can_read_ = false;
  stream_.WriteAndFinish(buffer, ::grpc::WriteOptions{}, status, task->RefTag());
  return task;
}

RefCountedPtr<Task> ServerCall::Finish(::grpc::Status status) {
  absl::MutexLock lock{&mutex_};
  if (state_ != kActive) {
    gpr_log(GPR_DEBUG, "Finish called on handler in non-active state (%d)", state_.load());
    auto task = CreateTask();
    task->Finalize(false);
    return task;
  }
  state_ = kFinishing;
  mutex_.Await(absl::Condition{&can_write_});
  auto task = CreateTask();
  const bool ABSL_ATTRIBUTE_UNUSED is_callback_added =
      task->AddCallback([self = Ref()](auto task) { self->OnFinish(task->ok()); });
  GPR_DEBUG_ASSERT(is_callback_added);
  can_write_ = false;
  can_read_ = false;
  stream_.Finish(status, task->RefTag());
  return task;
}

}  // namespace grpc
}  // namespace dropbox
