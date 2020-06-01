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

#ifndef DROPBOX_GRPC_PYTHON_SEMAPHORE_H_
#define DROPBOX_GRPC_PYTHON_SEMAPHORE_H_

#if __APPLE__
#include "absl/synchronization/mutex.h"
#include "absl/time/clock.h"
#else
#include <semaphore.h>
#endif

#include <memory>

#include "absl/time/time.h"
#include "dropbox/grpc/python/interop.h"

namespace dropbox {
namespace grpc {
namespace python {

#if __APPLE__
class Semaphore {
 public:
  explicit Semaphore(unsigned int initial_value = 0) noexcept : value_(initial_value) {}

  struct IncrementSemaphoreOnDelete {
    void operator()(Semaphore* semaphore) const noexcept { semaphore->Increment(); }
  };
  using AutoIncrement = std::unique_ptr<Semaphore, IncrementSemaphoreOnDelete>;

  AutoIncrement Decrement(decltype(gil::Release())& gil_handle) noexcept {
    constexpr auto kCheckForInterruptsQuantum = absl::Milliseconds(500);
    GPR_DEBUG_ASSERT(gil_handle != nullptr);
    mutex_.Lock();
    while (!value_) {
      if (cond_.WaitWithTimeout(&mutex_, kCheckForInterruptsQuantum)) {
        mutex_.Unlock();
        if (interop::CheckForSignals(gil_handle)) {
          return nullptr;
        }
        mutex_.Lock();
      }
    }
    value_--;
    mutex_.Unlock();
    return AutoIncrement{this};
  }

  AutoIncrement DecrementWithDeadline(decltype(gil::Release())& gil_handle, absl::Time deadline,
                                      bool* timed_out) noexcept {
    constexpr auto kCheckForInterruptsQuantum = absl::Milliseconds(500);
    GPR_DEBUG_ASSERT(gil_handle != nullptr);
    mutex_.Lock();
    while (!value_) {
      const auto interrupt_deadline = absl::Now() + kCheckForInterruptsQuantum;
      const bool short_timeout = interrupt_deadline > deadline;
      const auto& effective_deadline = short_timeout ? deadline : interrupt_deadline;
      if (cond_.WaitWithDeadline(&mutex_, effective_deadline)) {
        mutex_.Unlock();
        if (short_timeout) {
          *timed_out = true;
          return nullptr;
        }
        if (interop::CheckForSignals(gil_handle)) {
          *timed_out = false;
          return nullptr;
        }
        mutex_.Lock();
      }
    }
    value_--;
    mutex_.Unlock();
    return AutoIncrement{this};
  }

  void Increment() noexcept {
    {
      absl::MutexLock lock{&mutex_};
      value_++;
    }
    cond_.Signal();
  }

 private:
  absl::Mutex mutex_;
  absl::CondVar cond_;
  unsigned int value_;
};
#else   // #if __APPLE__
class Semaphore {
 public:
  explicit Semaphore(unsigned int initial_value = 0) noexcept { sem_init(&sem_, 0, initial_value); }

  struct IncrementSemaphoreOnDelete {
    void operator()(Semaphore* semaphore) const noexcept { semaphore->Increment(); }
  };
  using AutoIncrement = std::unique_ptr<Semaphore, IncrementSemaphoreOnDelete>;

  AutoIncrement Decrement(decltype(gil::Release())& gil_handle) noexcept {
    GPR_DEBUG_ASSERT(gil_handle != nullptr);
    while (sem_wait(&sem_) == -1) {
      GPR_DEBUG_ASSERT(errno == EINTR);
      if (interop::CheckForSignals(gil_handle)) {
        return nullptr;
      }
    }
    return AutoIncrement{this};
  }

  AutoIncrement DecrementWithDeadline(decltype(gil::Release())& gil_handle, absl::Time deadline,
                                      bool* timed_out) noexcept {
    GPR_DEBUG_ASSERT(gil_handle != nullptr);
    // NOTE: there's a potential race if we get a signal and system clock changes.
    // Unfortunately, sem_timedwait does not support monotonic clock and
    // pthread_cond_timedwait does not have a definite behavior with respect
    // to signals.
    auto raw_deadline = absl::ToTimespec(deadline);
    while (sem_timedwait(&sem_, &raw_deadline) == -1) {
      switch (errno) {
        case EINTR:
          if (interop::CheckForSignals(gil_handle)) {
            *timed_out = false;
            return nullptr;
          }
          continue;
        case ETIMEDOUT:
          *timed_out = true;
          return nullptr;
        default:
          gpr_log(GPR_ERROR, "sem_timedwait: returned unknown error[errno=%d] -- abort", errno);
          abort();
      }
    }
    return AutoIncrement{this};
  }

  void Increment() noexcept { sem_post(&sem_); }

 private:
  sem_t sem_;
};
#endif  // #if __APPLE__

}  // namespace python
}  // namespace grpc
}  // namespace dropbox

#endif  // DROPBOX_GRPC_PYTHON_SEMAPHORE_H_
