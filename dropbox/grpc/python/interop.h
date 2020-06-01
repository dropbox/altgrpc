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

#ifndef DROPBOX_GRPC_PYTHON_INTEROP_H_
#define DROPBOX_GRPC_PYTHON_INTEROP_H_

#include <grpcpp/client_context.h>
#include <grpcpp/support/byte_buffer.h>
#include <grpcpp/support/string_ref.h>

#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "dropbox/types/expected.h"

#ifndef Py_PYTHON_H
// Forward declare Python symbols if we don't have to depend on Python.h
// so we can separately compile most of the C++ code with Bazel
typedef struct _object PyObject;
typedef struct _typeobject PyTypeObject;
#endif

namespace dropbox {
namespace grpc {
namespace python {

namespace gil {

namespace detail {
struct ReacquireGlobalInterpreterLockOnDelete {
  void operator()(void* p) const noexcept;
};
}  // namespace detail

std::unique_ptr<void, detail::ReacquireGlobalInterpreterLockOnDelete> Release() noexcept;

}  // namespace gil

namespace interop {

bool CheckForSignals(decltype(gil::Release())& gil_handle) noexcept;

void Initialize() noexcept;
void Exit() noexcept;

}  // namespace interop

class ExitBarrier final {
 public:
  ExitBarrier() noexcept;
  void RegisterOnExitCallback(std::function<void()> tear_down_callback);
  ~ExitBarrier();
  constexpr operator bool() const noexcept { return acquired_; }

 private:
  bool acquired_;
  absl::optional<size_t> callback_index_;
};

class PythonExecCtx final {
 public:
  PythonExecCtx() noexcept;
  ~PythonExecCtx();
  bool Acquired() noexcept;
  void RegisterOnExitCallback(std::function<void()> tear_down_callback);

 private:
  struct PythonExecCtxImpl;
  std::unique_ptr<PythonExecCtxImpl> impl_;
};

namespace detail {
struct PyObject_Deleter {
  void operator()(PyObject* p) const noexcept;
};

}  // namespace detail

// AutoReleasedObject decrements the reference count of a Python
// object when it goes out of scope.  Instances of this class are
// movable but not copyable.  Unlike ObjectHandle, users should
// ensure destruction happens when GIL is held.
// AutoReleasedObject steals a reference count when constructed.
using AutoReleasedObject = std::unique_ptr<PyObject, detail::PyObject_Deleter>;
AutoReleasedObject AddRef(const AutoReleasedObject& object) noexcept;
AutoReleasedObject AddRef(PyObject* object) noexcept;
PyObject* ValueOrNone(const AutoReleasedObject& object) noexcept;

// ObjectHandle ensures deallocation of a PyObject handed over to C++
// code by deallocating it when the last use is deallocated.
// There is a separate reference count kept in the C++ layer via a
// shared_ptr so that we do not need to hold the GIL when we copy
// the object around.
class ObjectHandle final {
 public:
  ObjectHandle() {}
  ObjectHandle(PyObject* object);  // NOLINT(runtime/explicit): support passing
                                   // of objects from Cython without cast
  explicit ObjectHandle(const AutoReleasedObject& object) : ObjectHandle(object.get()) {}
  ObjectHandle(AutoReleasedObject&& object);  // NOLINT(runtime/explicit): moving from
                                              // AutoReleasedObject without cast
  PyObject* get() const noexcept { return object_.get(); }
  operator bool() const noexcept { return object_.get(); }
  void reset() { object_.reset(); }
  PyObject* SafeReturn() const noexcept;

 private:
  std::shared_ptr<PyObject> object_;
};

struct ExceptionDetails {
  ExceptionDetails() noexcept {}
  ExceptionDetails(PyObject* ptype, PyObject* pvalue, PyObject* ptraceback) noexcept
      : type(ptype), value(pvalue), traceback(ptraceback) {}
  void Restore() noexcept;
  bool IsOfType(PyTypeObject* type_object) const noexcept;
  AutoReleasedObject type, value, traceback;
};

Expected<AutoReleasedObject, ExceptionDetails> CallPython(PyObject* receiver) noexcept;
Expected<AutoReleasedObject, ExceptionDetails> CallPython(PyObject* receiver, PyObject*) noexcept;
Expected<AutoReleasedObject, ExceptionDetails> CallPython(PyObject* receiver, PyObject*, PyObject*) noexcept;
Expected<AutoReleasedObject, ExceptionDetails> CallPython(PyObject* receiver, PyObject*, PyObject*, PyObject*) noexcept;
Expected<AutoReleasedObject, ExceptionDetails> CallPython(PyObject* receiver, PyObject*, PyObject*, PyObject*,
                                                          PyObject*) noexcept;
Expected<AutoReleasedObject, ExceptionDetails> CallPython(PyObject* receiver, PyObject*, PyObject*, PyObject*,
                                                          PyObject*, PyObject*) noexcept;
Expected<AutoReleasedObject, ExceptionDetails> CallPython(PyObject* receiver, PyObject*, PyObject*, PyObject*,
                                                          PyObject*, PyObject*, PyObject*) noexcept;

namespace iterator {
Expected<AutoReleasedObject, ExceptionDetails> GetIterator(PyObject* iterable) noexcept;

enum IterationStatus {
  kBreak,
  kContinue,
  kEndOfSequence,
};

Expected<IterationStatus, ExceptionDetails> Next(PyObject* iterator,
                                                 const std::function<bool(AutoReleasedObject)>& fn) noexcept;
Expected<IterationStatus, ExceptionDetails> ForEach(PyObject* iterable,
                                                    const std::function<bool(AutoReleasedObject)>& fn) noexcept;

class Interface {
 public:
  virtual ~Interface() = default;
  virtual PyObject* Next() = 0;
  virtual void Dispose() = 0;
};

AutoReleasedObject Create(Interface* iterator);

}  // namespace iterator

namespace types {

class Metadata {
 public:
  Metadata(std::nullptr_t) noexcept : object_(nullptr) {}  // NOLINT(runtime/explicit)
  explicit Metadata(PyObject* obj) : object_(obj) {}
  static Metadata FromMultimap(const std::multimap<::grpc::string_ref, ::grpc::string_ref>&);
  bool ApplyToClientContext(::grpc::ClientContext* context) noexcept;
  bool Apply(const std::function<void(const std::pair<std::string, std::string>&)>& fn) noexcept;
  constexpr PyObject* get() const noexcept { return object_; }
  constexpr operator PyObject*() const noexcept { return get(); }

 private:
  PyObject* object_;
};

std::pair<std::string, std::string> MetadatumFromKeyValue(PyObject* key, PyObject* value);

class Bytes {
 public:
  explicit Bytes(PyObject* obj) : object_(obj) {}
  static Bytes FromUtf8Bytes(absl::string_view);
  static Expected<Bytes, std::string> FromByteBuffer(const ::grpc::ByteBuffer& buffer) noexcept;
  ::grpc::ByteBuffer ToByteBuffer();
  std::string ToString() const;
  Expected<absl::string_view, Void> ToStringView() const noexcept;
  constexpr PyObject* get() const noexcept { return object_; }
  constexpr operator PyObject*() const noexcept { return get(); }

 private:
  PyObject* object_;
};

class Unicode {
 public:
  Unicode(std::nullptr_t) noexcept : object_(nullptr) {}  // NOLINT(runtime/explicit)
  explicit Unicode(PyObject* obj) : object_(obj) {}
  static Unicode FromUtf8Bytes(absl::string_view);
  std::string ToString() const;
  constexpr PyObject* get() const noexcept { return object_; }
  constexpr operator PyObject*() const noexcept { return get(); }

 private:
  PyObject* object_;
};

class Str {
 public:
  Str(std::nullptr_t) noexcept : object_(nullptr) {}  // NOLINT(runtime/explicit)
  explicit Str(PyObject* obj) : object_(obj) {}
  static Str FromUtf8Bytes(absl::string_view);
  static absl::optional<std::string> GetUtf8Bytes(PyObject* bytes_or_unicode) noexcept;
  std::string ToString() const;
  constexpr PyObject* get() const noexcept { return object_; }
  constexpr operator PyObject*() const noexcept { return get(); }

 private:
  PyObject* object_;
};

class Float {
 public:
  explicit Float(PyObject* obj) : object_(obj) {}
  static Float FromDouble(double value) noexcept;
  constexpr PyObject* get() const noexcept { return object_; }
  constexpr operator PyObject*() const noexcept { return get(); }
  Expected<double, ExceptionDetails> ToDouble() noexcept;

 private:
  PyObject* object_;
};

class Tuple {
 public:
  explicit Tuple(PyObject* obj) : object_(obj) {}
  static Tuple New(size_t size) noexcept;
  constexpr PyObject* get() const noexcept { return object_; }
  constexpr operator PyObject*() const noexcept { return get(); }
  // To be used only for new tuples; steals a reference to object.
  void FillItem(size_t index, PyObject* object) noexcept;

 private:
  PyObject* object_;
};

class Dict {
 public:
  explicit Dict(PyObject* obj) : object_(obj) {}
  static Dict New() noexcept;
  constexpr PyObject* get() const noexcept { return object_; }
  constexpr operator PyObject*() const noexcept { return get(); }
  void Set(PyObject* key, PyObject* value) noexcept;

 private:
  PyObject* object_;
};

}  // namespace types

namespace time {

void SetClientContextTimeout(::grpc::ClientContext* context, PyObject* timeout) noexcept;

}  // namespace time

extern PyObject* const kNone;
extern PyObject* const kTrue;
extern PyObject* const kFalse;

}  // namespace python
}  // namespace grpc
}  // namespace dropbox

#endif  // DROPBOX_GRPC_PYTHON_INTEROP_H_
