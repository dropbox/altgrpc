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

#include "dropbox/grpc/python/common.h"

#include <utility>

namespace dropbox {
namespace grpc {
namespace python {
namespace {

Expected<::grpc::ByteBuffer, Void> SerializeToByteBuffer(PyObject* object) {
  auto buffer = types::Bytes{object}.ToByteBuffer();
  if (!buffer.Valid()) {
    return MakeUnexpected<Void>({});
  }
  return buffer;
}

Expected<AutoReleasedObject, Void> DeserializeFromByteBuffer(const ::grpc::ByteBuffer& buffer) {
  auto bytes = types::Bytes::FromByteBuffer(buffer);
  if (!bytes.IsValid()) {
    return MakeUnexpected<Void>({});
  }
  return AutoReleasedObject{std::move(bytes).Value()};
}

}  // namespace

MessageDeserializer GenericDeserializer(ObjectHandle deserializer) noexcept {
  if (!deserializer) {
    return &DeserializeFromByteBuffer;
  }
  return [deserializer = std::move(deserializer)](
             const ::grpc::ByteBuffer& buffer) noexcept -> Expected<AutoReleasedObject, Void> {
    auto bytes = DeserializeFromByteBuffer(buffer);
    if (!bytes.IsValid()) {
      return bytes.GetUnexpected();
    }
    auto expected_value = CallPython(deserializer.get(), bytes.Value().get());
    if (expected_value.IsValid()) {
      return std::move(expected_value).Value();
    }
    return MakeUnexpected<Void>({});
  };
}

MessageSerializer GenericSerializer(ObjectHandle serializer) noexcept {
  if (!serializer) {
    return &SerializeToByteBuffer;
  }
  return [serializer = std::move(serializer)](PyObject* object) noexcept -> Expected<::grpc::ByteBuffer, Void> {
    auto bytes = CallPython(serializer.get(), object);
    if (!bytes.IsValid()) {
      return MakeUnexpected<Void>({});
    }
    return SerializeToByteBuffer(bytes.Value().get());
  };
}

static PyObject* kConnectivities[5];

void InitializeConnectivityObjects(PyObject* idle, PyObject* connecting, PyObject* ready, PyObject* transient_failure,
                                   PyObject* shutdown) noexcept {
  kConnectivities[GRPC_CHANNEL_IDLE] = AddRef(idle).release();
  kConnectivities[GRPC_CHANNEL_CONNECTING] = AddRef(connecting).release();
  kConnectivities[GRPC_CHANNEL_READY] = AddRef(ready).release();
  kConnectivities[GRPC_CHANNEL_TRANSIENT_FAILURE] = AddRef(transient_failure).release();
  kConnectivities[GRPC_CHANNEL_SHUTDOWN] = AddRef(shutdown).release();
}

PyObject* ConnectivityFromState(grpc_connectivity_state state) noexcept {
  if (ABSL_PREDICT_FALSE(state < 0 || state > sizeof(kConnectivities) / sizeof(*kConnectivities))) {
    return AddRef(kNone).release();
  }
  return AddRef(kConnectivities[state]).release();
}

}  // namespace python
}  // namespace grpc
}  // namespace dropbox
