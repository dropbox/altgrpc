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

#include "dropbox/grpc/python/server.h"

#include <limits>
#include <utility>

namespace dropbox {
namespace grpc {
namespace python {

extern "C" PyObject* Cython_NewThreadingEvent();
extern "C" void Cython_SetThreadingEvent(PyObject* event);
extern "C" PyObject* Cython_NewServicerContext(ServicerContext**);

RequestHandler::RequestHandler(PyObject* thread_pool_submit, ObjectHandle service_pipeline,
                               absl::optional<size_t> concurrency_limit) {
  dispatcher = Dispatcher::New(thread_pool_submit);
  handler = std::make_shared<grpc::RequestHandler>(
      dispatcher->GetRequestHandler<RefCountedPtr<ServerCall>>(
          [dispatcher = dispatcher, service_pipeline = std::move(service_pipeline)](auto call) mutable {
            ServicerContext* py_call;
            AutoReleasedObject servicer_context{Cython_NewServicerContext(&py_call)};
            py_call->Start(std::move(call), std::move(dispatcher), std::move(servicer_context),
                           std::move(service_pipeline));
          }),
      concurrency_limit);
}

Server::~Server() {
  StopInternal(0);
  auto nogil = gil::Release();
  if (shutdown_thread_.joinable()) {
    shutdown_thread_.join();
  }
  server_.reset();
}

void Server::Loop() noexcept {
  stop_event_ = AutoReleasedObject{Cython_NewThreadingEvent()};
  for (const auto& dispatcher : dispatchers_) {
    dispatcher->RegisterCallback(server_->stopped().get(),
                                 [dispatcher_copy = dispatcher, stop_event_ref = AddRef(stop_event_).release()] {
                                   Cython_SetThreadingEvent(stop_event_ref);
                                   AutoReleasedObject{stop_event_ref}.reset();
                                   dispatcher_copy->Shutdown();
                                 });
  }
  auto nogil = gil::Release();
  for (const auto& dispatcher : dispatchers_) {
    dispatcher->StartLoop();
  }
  server_->AcceptCalls();
}

void Server::StopInternal(double grace) noexcept {
  if (!server_ || !stop_event_) {
    return;
  }
  {
    auto nogil = gil::Release();
    server_->Stop(grace);
  }
}

PyObject* Server::Stop(absl::optional<double> grace) noexcept {
  if (!stop_event_) {
    return AddRef(kNone).release();
  }
  {
    absl::MutexLock lock{&mutex_};
    // TODO(mehrdad): allow tightening of grace period
    if (stopping_) {
      return AddRef(stop_event_).release();
    }
    stopping_ = true;
  }

  if (grace) {
    shutdown_thread_ = std::thread([this, grace = grace.value()] {
      PythonExecCtx py_exec_ctx;
      py_exec_ctx.RegisterOnExitCallback([this] { StopInternal(0); });
      StopInternal(grace);
    });
  } else {
    StopInternal(0);
  }
  return AddRef(stop_event_).release();
}

bool Server::BuildAndStart(::grpc::ServerBuilder* builder, RequestHandlers handlers, size_t thread_count) noexcept {
  auto grpc_server = grpc::BuildAndStartServer(builder, std::move(handlers.handlers), thread_count);
  if (grpc_server) {
    server_ = std::move(grpc_server);
    dispatchers_ = std::move(handlers.dispatchers);
    return true;
  }
  return false;
}

}  // namespace python
}  // namespace grpc
}  // namespace dropbox
