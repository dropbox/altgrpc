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

#ifndef DROPBOX_GRPC_SERVICER_H_
#define DROPBOX_GRPC_SERVICER_H_

#include <grpcpp/grpcpp.h>

#include <memory>

namespace dropbox {
namespace grpc {

class Servicer {
 public:
  virtual ~Servicer();

  // Register all necessary servicers in the builder.
  // This method will be called before ServerBuilder::BuildAndStart.
  virtual void Register(::grpc::ServerBuilder *builder) = 0;

  // Start call on the completion queues.
  // This method will be called right after ServerBuilder::BuildAndStart.
  virtual void Start() = 0;

  // Shutdown all corresponding completion queues.
  // This method will be called right after end of Server::Shutdown call.
  virtual void Shutdown() = 0;
};

}  // namespace grpc
}  // namespace dropbox

#endif  // DROPBOX_GRPC_SERVICER_H_
