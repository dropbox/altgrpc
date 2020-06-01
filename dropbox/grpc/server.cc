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

#include "dropbox/grpc/server.h"

#include <grpcpp/generic/async_generic_service.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/impl/codegen/completion_queue_tag.h>

#include <algorithm>
#include <functional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "absl/base/attributes.h"
#include "absl/memory/memory.h"
#include "dropbox/grpc/future.h"
#include "dropbox/grpc/time.h"

namespace dropbox {
namespace grpc {

RequestHandler::RequestHandler(std::function<void(RefCountedPtr<ServerCall>)> handler,
                               absl::optional<size_t> concurrency_limit)
    : handler_(std::move(handler)), concurrency_limit_(concurrency_limit), active_calls_(0) {}

class Server::Private {};

Server::Server(Private, std::unique_ptr<::grpc::Server> server, std::unique_ptr<::grpc::AsyncGenericService> service,
               std::vector<std::unique_ptr<::grpc::ServerCompletionQueue>> cqs, RequestHandlers handlers)
    : server_(std::move(server)), service_(std::move(service)), handlers_(std::move(handlers)) {
  workers_.reserve(cqs.size());
  for (size_t i = 0; i < cqs.size(); ++i) {
    workers_.emplace_back(absl::make_unique<Worker>(this, std::move(cqs[i])));
  }
}

Server::Worker::Worker(Server* server, std::unique_ptr<::grpc::ServerCompletionQueue> cq)
    : server(server), cq(std::move(cq)) {
  thread = std::thread{Server::Worker::Start, this};
}

void Server::Worker::Start(Worker* const self) {
  void* tag;
  bool ok;
  while (self->cq->Next(&tag, &ok)) {
    if (tag) {
      static_cast<grpc::CompletionQueueTag*>(tag)->Proceed(ok);
    }
  }
}

void Server::Stop(double grace) {
  absl::call_once(shutdown_once_, [this, grace] {
    server_->Shutdown(absl::Now() + absl::Seconds(grace));

    absl::MutexLock lock{&mutex_};
    auto ready_to_shut_down = [this]() { return all_calls_ == 0; };
    mutex_.Await(absl::Condition(&ready_to_shut_down));
    for (auto& worker : workers_) {
      worker->cq->Shutdown();
    }
    for (auto& worker : workers_) {
      auto& thread = worker->thread;
      if (thread.joinable()) {
        thread.join();
      }
    }
    stopped_task_->Finalize(true);
  });
}

void Server::AcceptCall(Worker* const worker) {
  absl::MutexLock lock{&mutex_};
  worker->call = MakeRefCounted<ServerCall>();
  auto& call = *worker->call;
  const auto cq = worker->cq.get();
  service_->RequestCall(&call.context(), &call.stream(), cq, cq, worker);
  all_calls_++;
}

void Server::OnRequest(Worker* const worker, bool ok) {
  absl::ReleasableMutexLock lock{&mutex_};
  if (ok) {
    RequestHandlerSharedPtr handler = handlers_.Get(worker->call->context().method());
    const bool is_allowed_by_concurrency = handler->IncrementActiveCalls();
    auto call = std::move(worker->call);
    call->done()->CallBackWhenDone([this, handler](auto) {
      absl::MutexLock lock{&mutex_};
      handler->DecrementActiveCalls();
      --all_calls_;
    });
    call->StartCall();
    if (!is_allowed_by_concurrency) {
      lock.Release();
      call->context().AddTrailingMetadata("x-dropbox-dropped", "");
      call->Finish({::grpc::StatusCode::RESOURCE_EXHAUSTED, "Concurrency limit exceeded"})
          ->CallBackWhenDone([this, worker](auto) { AcceptCall(worker); });
    } else {
      lock.Release();
      AcceptCall(worker);
      handler->Handle(std::move(call));
    }
  } else {
    --all_calls_;
  }
}

void Server::AcceptCalls() {
  for (const auto& worker : workers_) {
    AcceptCall(worker.get());
  }
}

Server::~Server() { Stop(0); }

void AddPointerArgument(::grpc::ServerBuilder* builder, const std::string& name, void* value) {
  class PtrOption final : public ::grpc::ServerBuilderOption {
   public:
    PtrOption(const ::grpc::string& name, void* value) : name_(name), value_(value) {}

    void UpdateArguments(::grpc::ChannelArguments* args) override { args->SetPointer(name_, value_); }
    void UpdatePlugins(std::vector<std::unique_ptr<::grpc::ServerBuilderPlugin>>* plugins) override {}

   private:
    const ::grpc::string name_;
    void* const value_;
  };
  builder->SetOption(absl::make_unique<PtrOption>(name, value));
}

std::unique_ptr<Server> BuildAndStartServer(::grpc::ServerBuilder* builder, RequestHandlers handlers,
                                            size_t thread_count) noexcept {
  if (thread_count == 0) {
    thread_count = std::min(std::thread::hardware_concurrency(), 6u);
  }

  auto service = std::make_unique<::grpc::AsyncGenericService>();
  builder->RegisterAsyncGenericService(service.get());
  std::vector<std::unique_ptr<::grpc::ServerCompletionQueue>> cqs;
  cqs.reserve(thread_count);
  for (size_t i = 0; i < thread_count; ++i) {
    cqs.emplace_back(builder->AddCompletionQueue());
  }

  auto server = builder->BuildAndStart();
  if (!server) {
    return nullptr;
  }

  return std::make_unique<Server>(Server::Private{}, std::move(server), std::move(service), std::move(cqs),
                                  std::move(handlers));
}

}  // namespace grpc
}  // namespace dropbox
