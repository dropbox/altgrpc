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

#include "dropbox/grpc/python/rendezvous.h"

#include <chrono>

#include "absl/strings/str_cat.h"
#include "dropbox/grpc/python/channel.h"
#include "dropbox/grpc/python/dispatch.h"

namespace dropbox {
namespace grpc {
namespace python {

Rendezvous::Rendezvous() noexcept { context_.emplace(); }

::grpc::ClientContext* Rendezvous::context() noexcept { return &context_.value(); }

Rendezvous::~Rendezvous() {
  auto gil = gil::Release();
  context_.reset();
}

bool Rendezvous::IsActive() noexcept { return !done_; }

bool Rendezvous::IsDone() noexcept { return done_; }

bool Rendezvous::IsCancelled() noexcept { return done_ && status_.error_code() == ::grpc::StatusCode::CANCELLED; }

bool Rendezvous::IsRunning() noexcept { return !done_; }

types::Float Rendezvous::TimeRemaining() noexcept {
  absl::optional<double> time_remaining;
  const auto deadline = context()->deadline();
  if (deadline != std::chrono::system_clock::time_point::max()) {
    const auto now = std::chrono::system_clock::now();
    time_remaining = deadline <= now ? 0 : std::chrono::duration<double>(deadline - now).count();
  }
  return time_remaining ? types::Float::FromDouble(time_remaining.value()) : types::Float{AddRef(kNone).release()};
}

bool Rendezvous::Cancel() noexcept {
  if (!done_) {
    context()->TryCancel();
  }
  return false;
}

types::Metadata Rendezvous::InitialMetadata() noexcept {
  if (!initial_metadata_ready_) {
    ExitBarrier exit_barrier;
    if (ABSL_PREDICT_TRUE(exit_barrier)) {
      auto gil_acquirer = gil::Release();
      if (!initial_metadata_awaiter_.Decrement(gil_acquirer)) {
        return nullptr;
      }
    }
  }
  if (initial_metadata_valid_) {
    return types::Metadata::FromMultimap(context()->GetServerInitialMetadata());
  }
  return types::Metadata::FromMultimap({});
}

types::Metadata Rendezvous::TrailingMetadata() noexcept {
  if (ABSL_PREDICT_FALSE(!PrepareResult())) {
    return nullptr;
  }
  return types::Metadata::FromMultimap(context()->GetServerTrailingMetadata());
}

absl::optional<::grpc::StatusCode> Rendezvous::StatusCode() noexcept {
  if (ABSL_PREDICT_FALSE(!PrepareResult())) {
    return absl::nullopt;
  }
  return status_.error_code();
}

PyObject* Rendezvous::Code() noexcept {
  auto status_code = StatusCode();
  if (ABSL_PREDICT_TRUE(status_code)) {
    return _lookup_status_from_code(*status_code);
  }
  return nullptr;
}

types::Unicode Rendezvous::Details() noexcept {
  if (ABSL_PREDICT_FALSE(!PrepareResult())) {
    return nullptr;
  }
  return decltype(Details())::FromUtf8Bytes(status_.error_message());
}

types::Str Rendezvous::DebugErrorString() noexcept {
  return decltype(DebugErrorString())::FromUtf8Bytes(context()->debug_error_string());
}

types::Str Rendezvous::StringRepresentation() noexcept {
  std::string result;
  if (!done_) {
    result = "<Rendezvous of in-flight RPC>";
  } else {
    result = absl::StrCat("<Rendezvous of RPC that terminated with:\n\tstatus = \"", status_.error_code(),
                          "\"\n\tdetails = \"", status_.error_message(), "\">");
  }
  return decltype(StringRepresentation())::FromUtf8Bytes(result);
}

bool Rendezvous::AddCallback(PyObject* callback) noexcept {
  if (callback == kNone) {
    return true;
  }
  absl::MutexLock lock{&mutex_};
  if (done_) {
    return false;
  }
  callbacks_.push_back(AddRef(callback).release());
  return true;
}

void Rendezvous::SetBlockingReader(std::function<PyObject*(PyObject*)> reader) noexcept {
  start_called_ = true;
  blocking_reader_ = std::move(reader);
}

PyObject* Rendezvous::Next(PyObject* self) noexcept {
  if (start_called_) {
    return blocking_reader_(self);
  }
  Cython_Raise(self);
  return nullptr;
}

PyObject* Rendezvous::Exception(PyObject* self, PyObject* timeout) noexcept {
  if (ABSL_PREDICT_FALSE(!PrepareResult(timeout))) {
    return nullptr;
  }
  if (status_.ok()) {
    return AddRef(kNone).release();
  }
  if (status_.error_code() == ::grpc::StatusCode::CANCELLED) {
    Cython_RaiseFutureCancelledError();
    return nullptr;
  }
  return AddRef(self).release();
}

PyObject* Rendezvous::Traceback(PyObject* self, PyObject* timeout) noexcept {
  if (ABSL_PREDICT_FALSE(!PrepareResult(timeout))) {
    return nullptr;
  }
  if (status_.ok()) {
    return AddRef(kNone).release();
  }
  if (status_.error_code() == ::grpc::StatusCode::CANCELLED) {
    Cython_RaiseFutureCancelledError();
    return nullptr;
  }
  return Cython_CaptureTraceback(self);
}

void Rendezvous::UnblockInitialMetadata() noexcept {
  absl::MutexLock lock{&mutex_};
  if (initial_metadata_ready_) {
    return;
  }
  initial_metadata_valid_ = true;
  initial_metadata_ready_ = true;
  initial_metadata_awaiter_.Increment();
}

void Rendezvous::Finalize(::grpc::Status status, AutoReleasedObject result) noexcept {
  {
    absl::MutexLock lock{&mutex_};
    if (done_) {
      return;
    }
    if (!initial_metadata_ready_) {
      initial_metadata_valid_ = false;
      initial_metadata_ready_ = true;
      initial_metadata_awaiter_.Increment();
    }
    status_ = std::move(status);
    if (result) {
      result_ = std::move(result);
    }
    done_ = true;
  }
  if (!callbacks_.empty()) {
    Dispatcher::RegisterCallbackInPool(CallbackQueue(), [callbacks = std::move(callbacks_)]() noexcept {
      for (auto& callback : callbacks) {
        AutoReleasedObject decref{callback};
        CallPython(callback);
      }
    });
  }
  done_semaphore_.Increment();
  if (channel_) {
    channel_->Release(context()->c_call());
    channel_.reset();
  }
}

}  // namespace python
}  // namespace grpc
}  // namespace dropbox
