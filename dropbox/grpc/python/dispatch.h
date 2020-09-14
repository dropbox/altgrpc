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

#ifndef DROPBOX_GRPC_PYTHON_DISPATCH_H_
#define DROPBOX_GRPC_PYTHON_DISPATCH_H_

#include <functional>
#include <memory>
#include <queue>
#include <string>
#include <thread>
#include <utility>

#include "absl/container/inlined_vector.h"
#include "absl/synchronization/mutex.h"
#include "dropbox/grpc/future.h"
#include "dropbox/grpc/python/interop.h"
#include "dropbox/grpc/refcount.h"

namespace dropbox {
namespace grpc {
namespace python {

class ThreadPool : public RefCounted<ThreadPool> {
 public:
  using Callback = std::function<void()>;
  ThreadPool(std::string name, size_t max_thread_count);
  void Shutdown();
  bool Submit(Callback&& callback);

 private:
  const std::string name_;
  const size_t max_thread_count_;

  absl::Mutex mutex;
  absl::CondVar cv;
  bool shutting_down = false;
  size_t active = 0;
  size_t idle = 0;
  std::queue<Callback> queue;

  void StartNewThread();
  static void Loop(RefCountedPtr<ThreadPool> self, size_t index);
};

class Dispatcher {
 private:
  struct State {
    explicit State(AutoReleasedObject submit) noexcept;
    void Enqueue(std::function<void()>&& fn) noexcept;
    void Shutdown() noexcept;
    static void Loop(std::shared_ptr<State> self);
    void RunInPool(std::function<void()>&& func) noexcept;

    ObjectHandle thread_pool_submit;
    RefCountedPtr<ThreadPool> native_pool;
    absl::Mutex mutex;
    absl::CondVar next_cv;
    absl::InlinedVector<std::function<void()>, 128> dispatch_queues[2], *queue = &dispatch_queues[0],
                                                                        *call_queue = &dispatch_queues[1];
    bool shutdown = false;
  };

 public:
  using DispatchCallback = std::function<void()>;

  explicit Dispatcher(std::shared_ptr<State> state) noexcept;
  ~Dispatcher();
  void StartLoop() noexcept;
  void RegisterCallback(ObjectHandle callback) noexcept;
  void RegisterCallback(std::function<void()> callback) noexcept;
  void RegisterCallback(Task* task, ObjectHandle callback) noexcept;
  void RegisterCallback(Task* task, std::function<void()> callback) noexcept;
  static void RegisterCallbackInPool(const std::shared_ptr<Dispatcher>& self, std::function<void()>&& func) noexcept;
  void RunInPool(std::function<void()>&& func) noexcept;
  void Shutdown() noexcept;

  template <typename T>
  std::function<void(T)> GetRequestHandler(std::function<void(T)> callback) noexcept {
    return [this, callback = std::move(callback)](T call) {
      state_->Enqueue([call = std::move(call), callback]() { callback(call); });
    };
  }

  static std::shared_ptr<Dispatcher> New(PyObject* thread_pool_submit) noexcept;

 private:
  std::shared_ptr<State> state_;
};

std::shared_ptr<Dispatcher> ClientQueue() noexcept;
std::shared_ptr<Dispatcher> CallbackQueue() noexcept;

}  // namespace python
}  // namespace grpc
}  // namespace dropbox

#endif  // DROPBOX_GRPC_PYTHON_DISPATCH_H_
