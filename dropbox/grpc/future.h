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

#ifndef DROPBOX_GRPC_FUTURE_H_
#define DROPBOX_GRPC_FUTURE_H_

#include <grpc/impl/codegen/log.h>

#include <functional>
#include <memory>

#include "absl/base/attributes.h"
#include "absl/container/inlined_vector.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/time.h"
#include "dropbox/grpc/completion_queue_watcher.h"
#include "dropbox/grpc/refcount.h"

namespace dropbox {
namespace grpc {

class Task : public RefCountedCompletionQueueTag<Task> {
 public:
  virtual ~Task() = default;
  // Enlists a callback to be run at the completion of the task.
  // If Task is already completed, false is returned and nothing
  // gets registered.
  ABSL_MUST_USE_RESULT bool AddCallback(std::function<void(Task*)>);

  // Enlists a callback to be run at the completion of the task.
  // If the task is completed, callback will be run immediately
  // on the current thread.
  void CallBackWhenDone(std::function<void(Task*)> callback) {
    if (!AddCallback(callback)) {
      callback(this);
    }
  }

  void Await();
  bool Await(const absl::Time deadline);
  bool Await(const absl::Duration timeout);
  constexpr bool ok() const noexcept { return ok_; }
  constexpr bool done() const noexcept { return done_; }

  void Finalize(bool ok) noexcept;

 private:
  void ProceedReffed(bool ok) noexcept { Finalize(ok); }

  absl::Mutex mu_;
  bool done_ = false;
  bool ok_ = false;
  absl::InlinedVector<std::function<void(Task*)>, 5> callbacks_;
};

template <typename T>
class Future : public Task {
 public:
  ABSL_MUST_USE_RESULT bool AddCallback(std::function<void(Future<T>*)> callback) {
    return this->Task::AddCallback([callback](auto task) { callback(static_cast<Future<T>*>(task)); });
  }
  void CallBackWhenDone(std::function<void(Future<T>*)> callback) {
    if (!AddCallback(callback)) {
      callback(this);
    }
  }
  constexpr const T& result() const noexcept { return result_; }
  constexpr T& result() noexcept { return result_; }

 private:
  T result_;
};

// TODO(mehrdad): Lift Task into BaseTask and make Task == Future<Void>
inline RefCountedPtr<Task> CreateTask() { return MakeRefCounted<Task>(); }

template <typename T>
RefCountedPtr<Future<T>> CreateFuture() {
  return MakeRefCounted<Future<T>>();
}

}  // namespace grpc
}  // namespace dropbox

#endif  // DROPBOX_GRPC_FUTURE_H_
