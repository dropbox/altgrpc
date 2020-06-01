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

#ifndef DROPBOX_GRPC_PYTHON_COMMON_H_
#define DROPBOX_GRPC_PYTHON_COMMON_H_

#include <grpc/impl/codegen/connectivity_state.h>
#include <grpcpp/support/byte_buffer.h>

#include <functional>

#include "dropbox/grpc/python/interop.h"
#include "dropbox/types/expected.h"

namespace dropbox {
namespace grpc {
namespace python {

using MessageDeserializer = std::function<Expected<AutoReleasedObject, Void>(const ::grpc::ByteBuffer&)>;
using MessageSerializer = std::function<Expected<::grpc::ByteBuffer, Void>(PyObject*)>;

MessageDeserializer GenericDeserializer(ObjectHandle deserializer) noexcept;
MessageSerializer GenericSerializer(ObjectHandle serializer) noexcept;

void InitializeConnectivityObjects(PyObject* idle, PyObject* connecting, PyObject* ready, PyObject* transient_failure,
                                   PyObject* shutdown) noexcept;
PyObject* ConnectivityFromState(grpc_connectivity_state state) noexcept;

extern "C" {
extern PyTypeObject RpcErrorType, AbortionExceptionType;
void Cython_Raise(PyObject* exception);
void Cython_RaiseStopIteration();
void Cython_RaiseRpcError(void);
void Cython_RaiseSystemExit();
void Cython_RaiseFutureTimeoutError();
void Cython_RaiseFutureCancelledError();
void Cython_RaiseChannelClosed();
PyObject* Cython_CaptureTraceback(PyObject* exception);
// TODO(mehrdad): move the following to C++ and expose to Python
PyObject* _lookup_status_from_code(::grpc::StatusCode code);
PyObject* _utf8_bytes(PyObject* str_or_bytes);
}  // extern "C"

}  // namespace python
}  // namespace grpc
}  // namespace dropbox

#endif  // DROPBOX_GRPC_PYTHON_COMMON_H_
