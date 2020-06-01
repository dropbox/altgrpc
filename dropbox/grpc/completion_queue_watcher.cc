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

#include "dropbox/grpc/completion_queue_watcher.h"

#include <cstring>
#include <thread>
#include <utility>

#include "dropbox/grpc/internal.h"

namespace dropbox {
namespace grpc {

namespace {

void Loop(std::shared_ptr<::grpc::CompletionQueue> completion_queue, std::string thread_name) {
  gpr_log(GPR_DEBUG, "Start watch loop '%s' cq=%p", thread_name.c_str(), completion_queue->cq());
  SetCurrentThreadName(thread_name);
  void* tag;
  bool ok;
  while (completion_queue->Next(&tag, &ok)) {
    if (tag) {
      try {
        static_cast<CompletionQueueTag*>(tag)->Proceed(ok);
      } catch (std::exception& ex) {
        gpr_log(GPR_ERROR, "Tag invocation failed: %s", ex.what());
      }
    }
  }
  gpr_log(GPR_DEBUG, "Finish watch loop '%s' cq=%p", thread_name.c_str(), completion_queue->cq());
}

}  // namespace

CompletionQueueWatcher::CompletionQueueWatcher(std::string thread_name)
    : completion_queue_(std::make_shared<::grpc::CompletionQueue>()) {
  std::thread{&Loop, completion_queue_, std::move(thread_name)}.detach();
}

CompletionQueueWatcher::~CompletionQueueWatcher() {
  gpr_log(GPR_DEBUG, "Shutting down watcher cq=%p", completion_queue_->cq());
  completion_queue_->Shutdown();
}

std::shared_ptr<CompletionQueueWatcher> CompletionQueueWatcher::Default() {
  static auto default_watcher = std::make_shared<CompletionQueueWatcher>("default-cq");
  return default_watcher;
}

}  // namespace grpc
}  // namespace dropbox
