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

#include "dropbox/grpc/python/dummy_server.h"

namespace dropbox {
namespace grpc {
namespace python {

DummyServer::DummyServer()
    : server_(grpc_server_create(nullptr, nullptr)), cq_(grpc_completion_queue_create_for_next(nullptr)) {
  grpc_server_register_completion_queue(server_, cq_, nullptr);
}

DummyServer::~DummyServer() {
  grpc_server_shutdown_and_notify(server_, cq_, nullptr);
  grpc_completion_queue_next(cq_, gpr_inf_future(GPR_CLOCK_REALTIME), nullptr);
  grpc_completion_queue_shutdown(cq_);
  grpc_server_destroy(server_);
  grpc_completion_queue_destroy(cq_);
}

int DummyServer::Bind(const char* addr) { return grpc_server_add_insecure_http2_port(server_, addr); }

}  // namespace python
}  // namespace grpc
}  // namespace dropbox
