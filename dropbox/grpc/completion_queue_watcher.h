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

#ifndef DROPBOX_GRPC_COMPLETION_QUEUE_WATCHER_H_
#define DROPBOX_GRPC_COMPLETION_QUEUE_WATCHER_H_

#include <grpcpp/completion_queue.h>

#include <memory>
#include <string>

#include "dropbox/grpc/refcount.h"

namespace dropbox {
namespace grpc {

class CompletionQueueTag {
 public:
  virtual ~CompletionQueueTag() {}
  virtual void Proceed(bool status) noexcept = 0;
};

template <typename T>
class RefCountedCompletionQueueTag : public CompletionQueueTag, public RefCounted<T> {
 public:
  virtual void ProceedReffed(bool status) noexcept = 0;

  void* RefTag() {
    RefCounted<T>::Ref().release();
    return static_cast<CompletionQueueTag*>(this);
  }

 private:
  void Proceed(bool status) noexcept final {
    ProceedReffed(status);
    RefCounted<T>::Unref();
  }
};

class CompletionQueueWatcher {
 public:
  explicit CompletionQueueWatcher(std::string thread_name);
  CompletionQueueWatcher(CompletionQueueWatcher const&) = delete;
  void operator=(CompletionQueueWatcher const&) = delete;
  ~CompletionQueueWatcher();
  static std::shared_ptr<CompletionQueueWatcher> Default();

  // Returns an unowned pointer to the grpc::CompletionQueue
  // object watched by this CompletionQueueWatcher.
  // The caller needs to ensure the owning CompletionQueueWatcher
  // object is alive when using the returned CompletionQueue
  // and needs to ensure it is kept alive till it receives callbacks
  // for all the tags it has put in.
  ::grpc::CompletionQueue* CompletionQueue() const noexcept { return completion_queue_.get(); }
  ::grpc::CompletionQueue* CompletionQueue() noexcept { return completion_queue_.get(); }

 private:
  std::shared_ptr<::grpc::CompletionQueue> completion_queue_;
};

}  // namespace grpc
}  // namespace dropbox

#endif  // DROPBOX_GRPC_COMPLETION_QUEUE_WATCHER_H_
