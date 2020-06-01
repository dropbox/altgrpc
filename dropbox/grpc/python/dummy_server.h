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

#ifndef DROPBOX_GRPC_PYTHON_DUMMY_SERVER_H_
#define DROPBOX_GRPC_PYTHON_DUMMY_SERVER_H_

#include <grpc/grpc.h>
#include <grpcpp/impl/codegen/grpc_library.h>

namespace dropbox {
namespace grpc {
namespace python {

// DummyServer creates a gRPC server and binds to ports
// but never actually accepts calls.  This is a hack to
// help mimic the original gRPC Python API which binds
// immediately as opposed to C++ ServerBuilder API
// that binds on start atomically.  Specifically,
// this helps bridge the cases where you bind to port
// 0 and expect to know which port you actually got.
class DummyServer : private ::grpc::GrpcLibraryCodegen {
 public:
  DummyServer();
  ~DummyServer();
  int Bind(const char* addr);

 private:
  grpc_server* server_;
  grpc_completion_queue* cq_;
};

}  // namespace python
}  // namespace grpc
}  // namespace dropbox

#endif  // DROPBOX_GRPC_PYTHON_DUMMY_SERVER_H_
