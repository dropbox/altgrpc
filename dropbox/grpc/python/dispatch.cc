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

#include "dropbox/grpc/python/dispatch.h"

#include <deque>
#include <thread>
#include <utility>

#include "absl/strings/str_cat.h"
#include "absl/types/optional.h"
#include "dropbox/grpc/internal.h"

namespace dropbox {
namespace grpc {
namespace python {

namespace {

extern "C" void Cython_SubmitToThreadPool(PyObject* submit_method, std::function<void()> callback) noexcept;

constexpr size_t kClientThreadPoolSize = 4;

std::shared_ptr<Dispatcher> NewClientQueue() noexcept {
  auto dispatcher = Dispatcher::New(nullptr);
  dispatcher->StartLoop();
  return dispatcher;
}

}  // namespace

Dispatcher::Dispatcher(std::shared_ptr<State> state) noexcept : state_(std::move(state)) {}
Dispatcher::~Dispatcher() { Shutdown(); }
void Dispatcher::Shutdown() noexcept { state_->Shutdown(); }

void Dispatcher::RegisterCallbackInPool(const std::shared_ptr<Dispatcher>& self,
                                        std::function<void()>&& func) noexcept {
  if (self->state_->thread_pool_submit.get()) {
    self->RegisterCallback([self, func = std::move(func)]() mutable noexcept { self->RunInPool(std::move(func)); });
  } else {
    self->state_->native_pool->Submit(std::move(func));
  }
}
void Dispatcher::RunInPool(std::function<void()>&& func) noexcept {
  if (state_->thread_pool_submit.get()) {
    state_->RunInPool(std::move(func));
  } else {
    state_->native_pool->Submit(std::move(func));
  }
}

std::shared_ptr<Dispatcher> ClientQueue() noexcept {
  static std::shared_ptr<Dispatcher> dispatcher = NewClientQueue();
  return dispatcher;
}

std::shared_ptr<Dispatcher> CallbackQueue() noexcept {
  static std::shared_ptr<Dispatcher> dispatcher = NewClientQueue();
  return dispatcher;
}

std::shared_ptr<Dispatcher> Dispatcher::New(PyObject* thread_pool_submit) noexcept {
  return std::make_shared<Dispatcher>(std::make_shared<State>(AddRef(thread_pool_submit)));
}

Dispatcher::State::State(AutoReleasedObject submit) noexcept : thread_pool_submit(submit) {}

void Dispatcher::State::RunInPool(std::function<void()>&& func) noexcept {
  if (func) {
    if (thread_pool_submit) {
      Cython_SubmitToThreadPool(thread_pool_submit.get(), std::move(func));
    } else {
      native_pool->Submit(std::move(func));
    }
  }
}

void Dispatcher::StartLoop() noexcept {
  if (!state_->thread_pool_submit) {
    state_->native_pool = MakeRefCounted<ThreadPool>("pychan-disp", kClientThreadPoolSize);
  }
  std::thread{Dispatcher::State::Loop, state_}.detach();
}

void Dispatcher::State::Loop(std::shared_ptr<State> self) {
  PythonExecCtx py_exec_ctx;
  py_exec_ctx.RegisterOnExitCallback([self] { self->Shutdown(); });
  if (!py_exec_ctx.Acquired()) {
    return;
  }
  for (;;) {
    {
      auto nogil = gil::Release();
      absl::MutexLock lock{&self->mutex};
      while (!self->shutdown && self->queue->empty()) {
        self->next_cv.Wait(&self->mutex);
      }
      if (self->shutdown && self->queue->empty()) {
        break;
      }
      std::swap(self->call_queue, self->queue);
    }
    for (auto& callback : *self->call_queue) {
      callback();
      callback = nullptr;
    }
    self->call_queue->clear();
  }
}

void Dispatcher::RegisterCallback(ObjectHandle callback) noexcept {
  if (callback) {
    state_->Enqueue([callback = std::move(callback)]() noexcept {
      CallPython(callback.get());  // Log Python exception?
    });
  }
}

void Dispatcher::RegisterCallback(std::function<void()> callback) noexcept {
  if (callback) {
    state_->Enqueue(std::move(callback));
  }
}

void Dispatcher::RegisterCallback(Task* task, ObjectHandle py_callback) noexcept {
  if (task && py_callback && !task->AddCallback([state = state_, py_callback](auto task) mutable noexcept {
        state->Enqueue([py_callback = std::move(py_callback)]() noexcept {
          CallPython(py_callback.get());  // Log Python exception?
        });
      })) {
    CallPython(py_callback.get());  // Log Python exception?
  }
}

void Dispatcher::RegisterCallback(Task* task, std::function<void()> callback) noexcept {
  if (task && callback && !task->AddCallback([state = state_, callback](auto task) mutable noexcept {
        state->Enqueue(std::move(callback));
      })) {
    try {
      callback();
    } catch (...) {
      // Log exception?
    }
  }
}

void Dispatcher::State::Shutdown() noexcept {
  {
    absl::MutexLock lock{&mutex};
    shutdown = true;
  }
  next_cv.Signal();
  if (native_pool.get()) {
    native_pool->Shutdown();
  }
}

void Dispatcher::State::Enqueue(std::function<void()>&& fn) noexcept {
  {
    absl::MutexLock lock{&mutex};
    queue->emplace_back(std::move(fn));
  }
  next_cv.Signal();
}

void ThreadPool::Shutdown() {
  absl::MutexLock lock{&mutex};
  if (shutting_down) {
    return;
  }
  shutting_down = true;
  cv.SignalAll();
}

ThreadPool::ThreadPool(std::string name, size_t max_thread_count)
    : name_(std::move(name)), max_thread_count_(max_thread_count) {}

void ThreadPool::StartNewThread() { std::thread{&Loop, Ref(), active++}.detach(); }

void ThreadPool::Loop(RefCountedPtr<ThreadPool> self, size_t index) {
  SetCurrentThreadName(absl::StrCat(self->name_, index));
  PythonExecCtx py_exec_ctx;
  if (!py_exec_ctx.Acquired()) {
    absl::MutexLock lock{&self->mutex};
    self->active--;
    self->cv.SignalAll();
    return;
  }
  py_exec_ctx.RegisterOnExitCallback([self] { self->Shutdown(); });
  gpr_log(GPR_DEBUG, "Starting threadpool '%s' -- thread %ld", self->name_.c_str(), index);
  for (;;) {
    std::function<void()> callback;
    {
      auto nogil = gil::Release();
      bool shutdown = false;
      bool signal = false;
      {
        absl::MutexLock lock{&self->mutex};
        while (self->queue.empty() && !self->shutting_down) {
          self->idle++;
          self->cv.Wait(&self->mutex);
          self->idle--;
        }
        if (!self->queue.empty()) {
          callback = std::move(self->queue.front());
          self->queue.pop();
          signal = self->queue.size();
        } else {
          if ((shutdown = self->shutting_down)) {
            self->active--;
          }
        }
      }
      if (shutdown) {
        self->cv.SignalAll();
        gpr_log(GPR_DEBUG, "Exit threadpool '%s' -- thread %ld", self->name_.c_str(), index);
        return;
      }
      if (signal) {
        self->cv.Signal();
      }
    }
    callback();
  }
}

bool ThreadPool::Submit(Callback&& callback) {
  bool new_thread;
  {
    absl::MutexLock lock{&mutex};
    queue.emplace(std::move(callback));
    if ((new_thread = idle == 0 && active < max_thread_count_ && !shutting_down)) {
      StartNewThread();
    }
  }
  if (!new_thread) {
    cv.Signal();
  }
  return true;
}

}  // namespace python
}  // namespace grpc
}  // namespace dropbox
