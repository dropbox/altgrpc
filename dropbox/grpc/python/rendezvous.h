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

#ifndef DROPBOX_GRPC_PYTHON_RENDEZVOUS_H_
#define DROPBOX_GRPC_PYTHON_RENDEZVOUS_H_

#include <grpcpp/channel.h>
#include <grpcpp/support/status.h>

#include <atomic>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "absl/container/inlined_vector.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/time.h"
#include "absl/types/optional.h"
#include "dropbox/grpc/completion_queue_watcher.h"
#include "dropbox/grpc/internal.h"
#include "dropbox/grpc/python/active_channel.h"
#include "dropbox/grpc/python/channel.h"
#include "dropbox/grpc/python/common.h"
#include "dropbox/grpc/python/dispatch.h"
#include "dropbox/grpc/python/interop.h"
#include "dropbox/grpc/python/semaphore.h"

namespace dropbox {
namespace grpc {
namespace python {

class Rendezvous {
 public:
  Rendezvous() noexcept;
  ~Rendezvous();

  bool IsActive() noexcept;
  bool IsRunning() noexcept;
  bool IsDone() noexcept;
  bool IsCancelled() noexcept;
  bool Cancel() noexcept;
  types::Float TimeRemaining() noexcept;
  types::Metadata InitialMetadata() noexcept;
  types::Metadata TrailingMetadata() noexcept;
  absl::optional<::grpc::StatusCode> StatusCode() noexcept;
  PyObject* Code() noexcept;
  types::Unicode Details() noexcept;
  types::Str DebugErrorString() noexcept;
  bool AddCallback(PyObject* callback) noexcept;
  types::Str StringRepresentation() noexcept;

  void UnblockInitialMetadata() noexcept;
  void Finalize(::grpc::Status status, AutoReleasedObject result = nullptr) noexcept;

  // Unary response:
  PyObject* Result(PyObject* self, PyObject* timeout) noexcept;
  PyObject* Exception(PyObject* self, PyObject* timeout) noexcept;
  PyObject* Traceback(PyObject* self, PyObject* timeout) noexcept;

  // Streaming response:
  PyObject* Next(PyObject* self) noexcept;
  void SetBlockingReader(std::function<PyObject*(PyObject*)> reader) noexcept;

  ::grpc::ClientContext* context() noexcept;
  void SetChannel(std::shared_ptr<ActiveChannel> channel) noexcept { channel_ = std::move(channel); }

 private:
  bool PrepareResult(PyObject* timeout = nullptr) noexcept;

  absl::optional<::grpc::ClientContext> context_;
  std::shared_ptr<ActiveChannel> channel_;
  std::atomic<bool> done_{false};
  Semaphore done_semaphore_;

  std::atomic<bool> initial_metadata_ready_{false};
  bool initial_metadata_valid_ = false;
  Semaphore initial_metadata_awaiter_;

  absl::Mutex mutex_;
  absl::InlinedVector<PyObject*, 3> callbacks_;  // Released upon execution
  ::grpc::Status status_;

  // Unary response:
  AutoReleasedObject result_;

  // Streaming response:
  bool start_called_ = false;
  std::function<PyObject*(PyObject*)> blocking_reader_;
};

inline void SetClientContextMetadata(::grpc::ClientContext* context, PyObject* metadata) noexcept {
  if (metadata && metadata != kNone) {
    types::Metadata{metadata}.ApplyToClientContext(context);
  }
}
inline void SetClientContextTimeout(::grpc::ClientContext* context, PyObject* timeout) noexcept {
  time::SetClientContextTimeout(context, timeout);
}

}  // namespace python
}  // namespace grpc
}  // namespace dropbox

#endif  // DROPBOX_GRPC_PYTHON_RENDEZVOUS_H_
