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

#ifndef DROPBOX_GRPC_SERVER_CALL_H_
#define DROPBOX_GRPC_SERVER_CALL_H_

#include <grpcpp/generic/async_generic_service.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/impl/codegen/completion_queue_tag.h>
#include <grpcpp/impl/codegen/grpc_library.h>

#include <atomic>
#include <memory>

#include "absl/synchronization/mutex.h"
#include "absl/types/optional.h"
#include "dropbox/grpc/future.h"
#include "dropbox/grpc/internal.h"

namespace dropbox {
namespace grpc {

class ServerCall : public RefCountedCompletionQueueTag<ServerCall>, ::grpc::GrpcLibraryCodegen {
 public:
  ServerCall() noexcept;
  ~ServerCall() {}

  void StartCall();
  RefCountedPtr<Task> SendInitialMetadata();
  RefCountedPtr<Future<::grpc::ByteBuffer>> Read();
  RefCountedPtr<Task> Write(const ::grpc::ByteBuffer& buffer);
  RefCountedPtr<Task> WriteAndFinish(const ::grpc::ByteBuffer& buffer, ::grpc::Status status);
  RefCountedPtr<Task> Finish(::grpc::Status status);

  constexpr const ::grpc::GenericServerContext& context() const noexcept { return context_; }
  constexpr ::grpc::GenericServerContext& context() noexcept { return context_; }
  constexpr ::grpc::GenericServerAsyncReaderWriter& stream() noexcept { return stream_; }
  constexpr RefCountedPtr<Future<ServerCall*>>& done() noexcept { return done_task_; }

 private:
  enum State { kNotStarted, kActive, kFinishing, kDone };
  void ProceedReffed(bool) noexcept override {}
  void OnFinish(bool ok);

  ::grpc::GenericServerContext context_;
  ::grpc::GenericServerAsyncReaderWriter stream_;

  absl::Mutex mutex_;
  RefCountedPtr<Future<ServerCall*>> done_task_;
  std::atomic<State> state_{kNotStarted};
  bool can_read_ = true;
  bool can_write_ = true;
};

}  // namespace grpc
}  // namespace dropbox

#endif  // DROPBOX_GRPC_SERVER_CALL_H_
