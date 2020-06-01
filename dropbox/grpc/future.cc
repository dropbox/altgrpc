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

#include "dropbox/grpc/future.h"

#include <memory>
#include <utility>

#include "absl/time/clock.h"

namespace dropbox {
namespace grpc {

void Task::Finalize(bool ok) noexcept {
  {
    absl::MutexLock lock(&mu_);
    GPR_DEBUG_ASSERT(!done_);
    done_ = true;
    ok_ = ok;
  }
  for (auto& registered_callback : callbacks_) {
    auto callback = std::move(registered_callback);
    try {
      callback(this);
    } catch (std::exception& ex) {
      gpr_log(GPR_ERROR, "Task callback invocation failed: %s", ex.what());
    } catch (...) {
      gpr_log(GPR_ERROR, "Task callback invocation failed: UNKNOWN EXCEPTION TYPE");
    }
  }
  callbacks_.clear();
}

void Task::Await() {
  absl::ReaderMutexLock lock(&mu_);
  mu_.Await(absl::Condition(&done_));
}

bool Task::Await(absl::Time deadline) {
  absl::ReaderMutexLock lock(&mu_);
  return mu_.AwaitWithDeadline(absl::Condition(&done_), deadline);
}

bool Task::Await(absl::Duration timeout) {
  absl::ReaderMutexLock lock(&mu_);
  return mu_.AwaitWithTimeout(absl::Condition(&done_), timeout);
}

bool Task::AddCallback(std::function<void(Task*)> callback) {
  absl::MutexLock lock(&mu_);
  if (done_) {
    return false;
  }
  callbacks_.emplace_back(std::move(callback));
  return true;
}

}  // namespace grpc
}  // namespace dropbox
