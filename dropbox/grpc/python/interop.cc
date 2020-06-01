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

// The following boilerplate is required to be the first thing included
extern "C" {
#define PY_SSIZE_T_CLEAN
#include "Python.h"
}
#if PY_MAJOR_VERSION < 3
#include "structseq.h"  // NOLINT(build/include): not included from Python.h in 2.7
#else
#define Py_TPFLAGS_HAVE_ITER 0
#endif
#include <algorithm>
#include <thread>

#include "absl/container/inlined_vector.h"
#include "absl/strings/match.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "dropbox/grpc/metadata.h"
#include "dropbox/grpc/python/interop.h"
#include "dropbox/grpc/refcount.h"
#include "dropbox/grpc/time.h"

namespace dropbox {
namespace grpc {
namespace python {
namespace {

class Deallocator : public RefCounted<Deallocator> {
 public:
  void operator()(PyObject* ptr) {
    if (!ptr) {
      return;
    }
    {
      absl::MutexLock lock{&mutex_};
      current_->push_back(ptr);
      if (state_ == kNotStarted) {
        state_ = kDeallocating;
        std::thread{[self = Ref()] { self->Loop(); }}.detach();
      }
    }
    cv_.Signal();
  }

  void Shutdown() {
    {
      absl::MutexLock lock(&mutex_);
      state_ = kShutdown;
    }
    cv_.Signal();
  }

 private:
  void Loop() {
    PythonExecCtx py_ctx;
    if (!py_ctx.Acquired()) {
      return;
    }
    py_ctx.RegisterOnExitCallback([self = Ref()] { self->Shutdown(); });
    for (;;) {
      GPR_DEBUG_ASSERT(PyThreadState_Get());
      for (auto& object : *deallocating_) {
        Py_DECREF(object);
      }
      auto nogil = gil::Release();
      absl::MutexLock lock{&mutex_};
      while (state_ != kShutdown && current_->empty()) {
        cv_.Wait(&mutex_);
      }
      if (state_ == kShutdown) {
        return;
      }
      deallocating_->clear();
      std::swap(current_, deallocating_);
    }
  }

  absl::Mutex mutex_;
  absl::CondVar cv_;
  enum {
    kNotStarted,
    kDeallocating,
    kShutdown,
  } state_;
  absl::InlinedVector<PyObject*, 128> queues_[2], *current_ = &queues_[0], *deallocating_ = &queues_[1];
};

RefCountedPtr<Deallocator> DefaultDeallocator() {
  static auto deallocator = MakeRefCounted<Deallocator>();
  return deallocator;
}

ExceptionDetails ExtractLastPythonException() {
  GPR_DEBUG_ASSERT(PyThreadState_Get());
  PyObject *type, *value, *traceback;
  PyErr_Fetch(&type, &value, &traceback);
  return ExceptionDetails(type, value, traceback);
}

template <typename... Args>
Expected<AutoReleasedObject, ExceptionDetails> CallPythonImpl(PyObject* receiver, Args&&... args) noexcept {
  AutoReleasedObject return_value{PyObject_CallFunctionObjArgs(receiver, std::forward<Args>(args)..., nullptr)};
  GPR_DEBUG_ASSERT(PyThreadState_Get());
  if (!PyErr_Occurred()) {
    if (!return_value.get()) {
      // Should not happen! Perhaps assert? but we don't want a broken Python
      // extension to crash the process...
      return AddRef(Py_None);
    }
    return return_value;
  }
  return MakeUnexpected(ExtractLastPythonException());
}

class PythonAtExitHandler {
 public:
  PythonAtExitHandler() noexcept {}

  // TODO(mehrdad): Use atomics instead of mutex
  bool IncrementExitLock() noexcept {
    absl::MutexLock lock{&mutex_};
    if (exiting_) {
      return false;
    }
    exit_locks_++;
    return true;
  }

  void DecrementExitLock() noexcept {
    absl::MutexLock lock{&mutex_};
    --exit_locks_;
  }

  absl::optional<size_t> InvokeOnExit(std::function<void()> tear_down_callback) {
    absl::ReleasableMutexLock lock{&mutex_};
    if (exiting_) {
      lock.Release();
      tear_down_callback();
      return absl::nullopt;
    } else {
      if (callback_free_list_.empty()) {
        tear_down_callbacks_.emplace_back(std::move(tear_down_callback));
        return tear_down_callbacks_.size() - 1;
      } else {
        const auto index = callback_free_list_.back();
        tear_down_callbacks_[index] = std::move(tear_down_callback);
        callback_free_list_.pop_back();
        return index;
      }
    }
  }

  bool UnregisterCallback(size_t index) {
    absl::MutexLock lock{&mutex_};
    if (exiting_) {
      return false;
    }
    tear_down_callbacks_[index] = nullptr;
    callback_free_list_.push_back(index);
    return true;
  }

  void OnExit() {
    auto nogil = gil::Release();
    {
      absl::MutexLock lock{&mutex_};
      if (exiting_) {
        return;
      }
      exiting_ = true;
    }
    for (auto& callback : tear_down_callbacks_) {
      if (callback) {
        callback();
        callback = nullptr;
      }
    }

    absl::MutexLock lock{&mutex_};
    // TODO(mehrdad): figure out why bazel is printing DEBUG messages
    // gpr_log(GPR_DEBUG, "OnExit: waiting for %ld threads to unlock", exit_locks_);
    mutex_.Await(absl::Condition(
        +[](decltype(exit_locks_)* locks) { return *locks == 0; }, &exit_locks_));
    // gpr_log(GPR_DEBUG, "OnExit: resuming shutdown");
  }

 private:
  absl::Mutex mutex_;
  size_t exit_locks_ = 0;
  bool exiting_ = false;
  absl::InlinedVector<std::function<void()>, 256> tear_down_callbacks_;
  absl::InlinedVector<size_t, 256> callback_free_list_;
};

PythonAtExitHandler& AtExitHandler() {
  static PythonAtExitHandler* singleton = new PythonAtExitHandler();
  return *singleton;
}

}  // namespace

namespace gil {

namespace detail {
void ReacquireGlobalInterpreterLockOnDelete::operator()(void* gil_state) const noexcept {
  PyEval_RestoreThread(static_cast<PyThreadState*>(gil_state));
}
}  // namespace detail

std::unique_ptr<void, detail::ReacquireGlobalInterpreterLockOnDelete> Release() noexcept {
  GPR_DEBUG_ASSERT(PyThreadState_Get());
  return decltype(Release()){reinterpret_cast<void*>(PyEval_SaveThread())};
}

namespace {
decltype(Release()) ReleaseIfSafe(bool barrier) noexcept {
  if (ABSL_PREDICT_TRUE(barrier)) {
    return Release();
  }
  return nullptr;
}
}  // namespace

}  // namespace gil

namespace interop {

bool CheckForSignals(decltype(gil::Release())& gil_handle) noexcept {
  const bool rerelease = gil_handle != nullptr;
  gil_handle.reset();
  if (Py_MakePendingCalls() < 0) {
    return true;
  }
  if (rerelease) {
    gil_handle = gil::Release();
  }
  return false;
}

void Initialize() noexcept {
  PyEval_InitThreads();
  AtExitHandler();
}

void Exit() noexcept { AtExitHandler().OnExit(); }

}  // namespace interop

ExitBarrier::ExitBarrier() noexcept : acquired_(AtExitHandler().IncrementExitLock()) {}
ExitBarrier::~ExitBarrier() {
  if (ABSL_PREDICT_TRUE(acquired_)) {
    if (callback_index_) {
      AtExitHandler().UnregisterCallback(*callback_index_);
    }
    AtExitHandler().DecrementExitLock();
  }
}
void ExitBarrier::RegisterOnExitCallback(std::function<void()> tear_down_callback) {
  if (ABSL_PREDICT_FALSE(!acquired_)) {
    return tear_down_callback();
  }
  callback_index_ = AtExitHandler().InvokeOnExit(std::move(tear_down_callback));
}

struct PythonExecCtx::PythonExecCtxImpl final {
  PythonExecCtxImpl() noexcept {
    if (barrier_) {
      state_ = PyGILState_Ensure();
    }
  }

  ~PythonExecCtxImpl() {
    if (barrier_) {
      PyGILState_Release(state_);
      if (tear_down_callback_index_) {
        AtExitHandler().UnregisterCallback(*tear_down_callback_index_);
      }
    }
  }

  ExitBarrier barrier_;
  PyGILState_STATE state_;
  absl::optional<size_t> tear_down_callback_index_;
};

PythonExecCtx::PythonExecCtx() noexcept : impl_(std::make_unique<PythonExecCtxImpl>()) {}
PythonExecCtx::~PythonExecCtx() = default;

bool PythonExecCtx::Acquired() noexcept { return impl_->barrier_; }

void PythonExecCtx::RegisterOnExitCallback(std::function<void()> tear_down_callback) {
  impl_->barrier_.RegisterOnExitCallback(std::move(tear_down_callback));
}

namespace detail {
void PyObject_Deleter::operator()(PyObject* p) const noexcept {
  GPR_DEBUG_ASSERT(PyThreadState_Get());
  Py_DECREF(p);
}
}  // namespace detail

AutoReleasedObject AddRef(PyObject* object) noexcept {
  // TODO: figure out why this fails in open source:
  // GPR_DEBUG_ASSERT(PyThreadState_Get());
  if (object) {
    Py_INCREF(object);
    return AutoReleasedObject{object};
  }
  return {};
}

AutoReleasedObject AddRef(const AutoReleasedObject& object) noexcept {
  auto ptr = object.get();
  GPR_DEBUG_ASSERT(PyThreadState_Get());
  if (ptr) {
    Py_INCREF(ptr);
    return AutoReleasedObject{ptr};
  }
  return {};
}

PyObject* ValueOrNone(const AutoReleasedObject& object) noexcept {
  GPR_DEBUG_ASSERT(PyThreadState_Get());
  auto ptr = object.get();
  if (!ptr) {
    ptr = Py_None;
  }
  Py_INCREF(ptr);
  return ptr;
}

PyObject* ObjectHandle::SafeReturn() const noexcept {
  GPR_DEBUG_ASSERT(PyThreadState_Get());
  auto obj = get();
  if (obj) {
    Py_INCREF(obj);
    return obj;
  }
  Py_RETURN_NONE;
}

ObjectHandle::ObjectHandle(PyObject* object)
    : object_(object != nullptr && object != Py_None
                  ? std::shared_ptr<PyObject>(object, [d = DefaultDeallocator()](auto obj) { (*d)(obj); })
                  : nullptr) {
  GPR_DEBUG_ASSERT(object == nullptr || PyThreadState_Get());
  Py_XINCREF(object_.get());
}

ObjectHandle::ObjectHandle(AutoReleasedObject&& object)
    : object_(object.get() != nullptr && object.get() != Py_None
                  ? std::shared_ptr<PyObject>(object.release(), [d = DefaultDeallocator()](auto obj) { (*d)(obj); })
                  : nullptr) {
  object.reset();
}

Expected<AutoReleasedObject, ExceptionDetails> CallPython(PyObject* receiver) noexcept {
  return CallPythonImpl(receiver);
}
Expected<AutoReleasedObject, ExceptionDetails> CallPython(PyObject* receiver, PyObject* a1) noexcept {
  return CallPythonImpl(receiver, a1);
}
Expected<AutoReleasedObject, ExceptionDetails> CallPython(PyObject* receiver, PyObject* a1, PyObject* a2) noexcept {
  return CallPythonImpl(receiver, a1, a2);
}
Expected<AutoReleasedObject, ExceptionDetails> CallPython(PyObject* receiver, PyObject* a1, PyObject* a2,
                                                          PyObject* a3) noexcept {
  return CallPythonImpl(receiver, a1, a2, a3);
}
Expected<AutoReleasedObject, ExceptionDetails> CallPython(PyObject* receiver, PyObject* a1, PyObject* a2, PyObject* a3,
                                                          PyObject* a4) noexcept {
  return CallPythonImpl(receiver, a1, a2, a3, a4);
}
Expected<AutoReleasedObject, ExceptionDetails> CallPython(PyObject* receiver, PyObject* a1, PyObject* a2, PyObject* a3,
                                                          PyObject* a4, PyObject* a5) noexcept {
  return CallPythonImpl(receiver, a1, a2, a3, a4, a5);
}
Expected<AutoReleasedObject, ExceptionDetails> CallPython(PyObject* receiver, PyObject* a1, PyObject* a2, PyObject* a3,
                                                          PyObject* a4, PyObject* a5, PyObject* a6) noexcept {
  return CallPythonImpl(receiver, a1, a2, a3, a4, a5, a6);
}

void ExceptionDetails::Restore() noexcept { PyErr_Restore(type.release(), value.release(), traceback.release()); }

bool ExceptionDetails::IsOfType(PyTypeObject* type_object) const noexcept {
  return reinterpret_cast<void*>(type.get()) == type_object;
}

namespace types {

namespace {

PyTypeObject* InitializeMetadatumTupleType() noexcept {
  static PyStructSequence_Field fields[] = {
      {.name = "key", .doc = nullptr},
      {.name = "value", .doc = nullptr},
      {nullptr},
  };
  static PyStructSequence_Desc desc = {
      .name = "grpc._Metadatum",
      .doc = nullptr,
      .fields = fields,
      .n_in_sequence = 2,
  };
  static PyTypeObject type;
  PyStructSequence_InitType(&type, &desc);
  return &type;
}

AutoReleasedObject NewMetadatumTuple() noexcept {
  static auto type = InitializeMetadatumTupleType();
  return AutoReleasedObject{PyStructSequence_New(type)};
}

}  // namespace

Metadata Metadata::FromMultimap(const std::multimap<::grpc::string_ref, ::grpc::string_ref>& map) {
  AutoReleasedObject metadata{PyTuple_New(map.size())};
  Py_ssize_t index = 0;
  for (auto& item : map) {
    absl::string_view key{item.first.data(), item.first.length()}, value{item.second.data(), item.second.length()};
    auto metadatum = NewMetadatumTuple();
    PyStructSequence_SET_ITEM(metadatum.get(), 0, types::Str::FromUtf8Bytes(key));
    PyStructSequence_SET_ITEM(
        metadatum.get(), 1,
        absl::EndsWith(key, "-bin") ? types::Bytes::FromUtf8Bytes(value) : types::Str::FromUtf8Bytes(value));
    PyTuple_SET_ITEM(metadata.get(), index++, metadatum.release());
  }
  return Metadata{metadata.release()};
}

bool Metadata::ApplyToClientContext(::grpc::ClientContext* context) noexcept {
  return Apply([&](const auto& metadatum) { context->AddMetadata(metadatum.first, metadatum.second); });
}

bool Metadata::Apply(const std::function<void(const std::pair<std::string, std::string>&)>& fn) noexcept {
  std::vector<std::pair<std::string, std::string>> staged_metadata;
  auto iteration_status = iterator::ForEach(object_, [&](auto metadatum) noexcept {
    static auto py_zero = PyLong_FromLong(0);
    static auto py_one = PyLong_FromLong(1);
    AutoReleasedObject key{PyObject_GetItem(metadatum.get(), py_zero)};
    if (ABSL_PREDICT_FALSE(!key)) {
      return false;
    }
    AutoReleasedObject value{PyObject_GetItem(metadatum.get(), py_one)};
    if (ABSL_PREDICT_FALSE(!value)) {
      return false;
    }
    auto utf8key = Str::GetUtf8Bytes(key.get());
    if (ABSL_PREDICT_FALSE(!utf8key)) {
      return false;
    }
    auto utf8value = Str::GetUtf8Bytes(value.get());
    if (ABSL_PREDICT_FALSE(!utf8value)) {
      return false;
    }
    auto validation_result = metadata::ValidateHeader(utf8key.value(), utf8value.value());
    if (ABSL_PREDICT_FALSE(!validation_result.IsValid())) {
      PyErr_SetString(PyExc_ValueError, validation_result.Error().c_str());
      return false;
    }
    staged_metadata.emplace_back(std::move(utf8key).value(), std::move(utf8value).value());
    return true;
  });
  if (ABSL_PREDICT_FALSE(!iteration_status.IsValid())) {
    iteration_status.Error().Restore();
    return false;
  }
  if (ABSL_PREDICT_TRUE(iteration_status.Value() == iterator::kEndOfSequence)) {
    ExitBarrier barrier;
    auto nogil = gil::ReleaseIfSafe(barrier);
    for (auto& metadatum : staged_metadata) {
      fn(metadatum);
    }
    return true;
  }
  return false;
}

Expected<Bytes, std::string> Bytes::FromByteBuffer(const ::grpc::ByteBuffer& byte_buffer) noexcept {
  GPR_DEBUG_ASSERT(PyThreadState_Get());
  auto py_bytes = PyBytes_FromStringAndSize(nullptr, byte_buffer.Length());
  if (!py_bytes) {
    PyErr_Clear();
    return MakeUnexpected<std::string>("Error creating Python buffer: PyBytes_FromStringAndSize failed");
  }
  auto destination = PyBytes_AsString(py_bytes);
  if (!destination || PyErr_Occurred()) {
    PyErr_Clear();
    Py_DECREF(py_bytes);
    return MakeUnexpected<std::string>("Error creating Python buffer: PyBytes_AsString failed");
  }

  ExitBarrier barrier;
  auto nogil = gil::ReleaseIfSafe(barrier);
  std::vector<::grpc::Slice> slices;
  byte_buffer.Dump(&slices);
  auto current = destination;
  for (const auto& slice : slices) {
    const auto size = slice.size();
    if (size) {
      std::copy(slice.begin(), slice.end(), current);
      current += size;
    }
  }
  return Bytes{py_bytes};
}

std::string Bytes::ToString() const {
  GPR_DEBUG_ASSERT(PyThreadState_Get());
  char* destination;
  Py_ssize_t len;
  if (PyBytes_AsStringAndSize(object_, &destination, &len) == -1 || PyErr_Occurred()) {
    gpr_log(GPR_DEBUG, "Bytes::ToString(): PyBytes_AsStringAndSize failed");
    PyErr_Clear();
    return {};
  }
  ExitBarrier barrier;
  auto nogil = gil::ReleaseIfSafe(barrier);
  return {destination, static_cast<size_t>(len)};
}

Expected<absl::string_view, Void> Bytes::ToStringView() const noexcept {
  GPR_DEBUG_ASSERT(PyThreadState_Get());
  char* destination;
  Py_ssize_t len;
  if (PyBytes_AsStringAndSize(object_, &destination, &len) == -1) {
    return MakeUnexpected<Void>({});
  }
  return absl::string_view{destination, static_cast<size_t>(len)};
}

absl::optional<std::string> Str::GetUtf8Bytes(PyObject* bytes_or_unicode) noexcept {
  Py_ssize_t size;
  if (ABSL_PREDICT_TRUE(PyBytes_Check(bytes_or_unicode))) {
    char* utf8;
    if (ABSL_PREDICT_FALSE(PyBytes_AsStringAndSize(bytes_or_unicode, &utf8, &size) == -1)) {
      return absl::nullopt;
    }
    return std::string{utf8, static_cast<size_t>(size)};
  }
  if (ABSL_PREDICT_TRUE(PyUnicode_Check(bytes_or_unicode))) {
#if PY_MAJOR_VERSION > 2
    auto utf8 = PyUnicode_AsUTF8AndSize(bytes_or_unicode, &size);
    if (ABSL_PREDICT_FALSE(!utf8)) {
      return absl::nullopt;
    }
#else   // PY_MAJOR_VERSION > 2
    AutoReleasedObject py_bytes{PyUnicode_AsUTF8String(bytes_or_unicode)};
    if (!py_bytes) {
      return absl::nullopt;
    }
    char* utf8;
    if (ABSL_PREDICT_FALSE(PyBytes_AsStringAndSize(py_bytes.get(), &utf8, &size) == -1)) {
      return absl::nullopt;
    }
#endif  // PY_MAJOR_VERSION > 2
    return std::string{utf8, static_cast<size_t>(size)};
  }
  PyErr_SetString(PyExc_TypeError, "expected bytes or str/unicode");
  return absl::nullopt;
}

#if PY_MAJOR_VERSION > 2  // Python 3:
std::string Unicode::ToString() const {
  GPR_DEBUG_ASSERT(PyThreadState_Get());
  Py_ssize_t len;
  const char* destination = PyUnicode_AsUTF8AndSize(object_, &len);
  if (!destination) {
    gpr_log(GPR_DEBUG, "Unicode::ToString(): PyUnicode_AsUTF8AndSize failed");
    PyErr_Clear();
    return {};
  }
  ExitBarrier barrier;
  auto nogil = gil::ReleaseIfSafe(barrier);
  return {destination, static_cast<size_t>(len)};
}

std::string Str::ToString() const { return Unicode{object_}.ToString(); }
Str Str::FromUtf8Bytes(absl::string_view string_view) { return Str{Unicode::FromUtf8Bytes(string_view).get()}; }

#else  // Python 2:

std::string Str::ToString() const { return Bytes{object_}.ToString(); }
Str Str::FromUtf8Bytes(absl::string_view string_view) { return Str{Bytes::FromUtf8Bytes(string_view).get()}; }

#endif

::grpc::ByteBuffer Bytes::ToByteBuffer() {
  GPR_DEBUG_ASSERT(PyThreadState_Get());
  if (!*this) {
    return {};
  }
  char* source;
  Py_ssize_t len;
  if (PyBytes_AsStringAndSize(object_, &source, &len) == -1 || PyErr_Occurred()) {
    PyErr_Clear();
    return {};
  }
  ExitBarrier barrier;
  auto nogil = gil::ReleaseIfSafe(barrier);
  ::grpc::Slice slice(source, len);
  return {&slice, 1};
}

Bytes Bytes::FromUtf8Bytes(absl::string_view byte_seq) {
  GPR_DEBUG_ASSERT(PyThreadState_Get());
  // Initialize the PyBytes first without passing the data
  // so that we can copy in the data when GIL is released.
  auto bytes = PyBytes_FromStringAndSize(nullptr, byte_seq.length());
  if (!bytes) {
    return Bytes{nullptr};
  }
  auto destination = PyBytes_AsString(bytes);
  ExitBarrier barrier;
  auto nogil = gil::ReleaseIfSafe(barrier);
  std::copy(byte_seq.begin(), byte_seq.end(), destination);
  return Bytes{bytes};
}

Unicode Unicode::FromUtf8Bytes(absl::string_view byte_seq) {
  GPR_DEBUG_ASSERT(PyThreadState_Get());
  return Unicode{PyUnicode_FromStringAndSize(byte_seq.data(), byte_seq.length())};
}

Float Float::FromDouble(double value) noexcept { return Float{PyFloat_FromDouble(value)}; }
Expected<double, ExceptionDetails> Float::ToDouble() noexcept {
  double converted = PyFloat_AsDouble(object_);
  if (!PyErr_Occurred()) {
    return converted;
  }
  return MakeUnexpected(ExtractLastPythonException());
}

Tuple Tuple::New(size_t size) noexcept { return Tuple{PyTuple_New(size)}; }
void Tuple::FillItem(size_t index, PyObject* object) noexcept { PyTuple_SET_ITEM(object_, index, object); }

Dict Dict::New() noexcept { return Dict{PyDict_New()}; }
void Dict::Set(PyObject* key, PyObject* value) noexcept { PyDict_SetItem(object_, key, value); }

}  // namespace types

namespace iterator {

namespace {

typedef struct {
  PyObject_HEAD iterator::Interface* iterator;
} CustomIterator;

PyObject* CustomIterator_Iter(PyObject* self) {
  Py_INCREF(self);
  return self;
}

PyObject* CustomIterator_Next(PyObject* py_self) {
  return reinterpret_cast<CustomIterator*>(py_self)->iterator->Next();
}

void CustomIterator_Dealloc(PyObject* py_self) {
  auto self = reinterpret_cast<CustomIterator*>(py_self);
  self->iterator->Dispose();
  delete self;
}

}  // namespace

AutoReleasedObject Create(Interface* iterator) {
  static PyTypeObject type = {
      PyVarObject_HEAD_INIT(NULL, 0).tp_name = "grpc._Iterator",
      .tp_basicsize = sizeof(CustomIterator),
      .tp_dealloc = (destructor)CustomIterator_Dealloc,
      .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_ITER,
      .tp_iter = CustomIterator_Iter,
      .tp_iternext = CustomIterator_Next,
  };
  auto object = absl::make_unique<CustomIterator>();
  object->iterator = iterator;
  if (PyObject_Init(reinterpret_cast<PyObject*>(object.get()), &type)) {
    return AutoReleasedObject{reinterpret_cast<PyObject*>(object.release())};
  }
  return nullptr;
}

Expected<AutoReleasedObject, ExceptionDetails> GetIterator(PyObject* iterable) noexcept {
  GPR_DEBUG_ASSERT(PyThreadState_Get());
  AutoReleasedObject iterator{PyObject_GetIter(iterable)};
  if (!iterator) {
    return MakeUnexpected(ExtractLastPythonException());
  }
  return iterator;
}

Expected<IterationStatus, ExceptionDetails> Next(PyObject* iterator,
                                                 const std::function<bool(AutoReleasedObject)>& fn) noexcept {
  GPR_DEBUG_ASSERT(PyThreadState_Get());
  AutoReleasedObject element{PyIter_Next(iterator)};
  if (element) {
    return fn(std::move(element)) ? kContinue : kBreak;
  } else if (PyErr_Occurred()) {
    return MakeUnexpected(ExtractLastPythonException());
  } else {
    return kEndOfSequence;
  }
}

Expected<IterationStatus, ExceptionDetails> ForEach(PyObject* iterable,
                                                    const std::function<bool(AutoReleasedObject)>& fn) noexcept {
  GPR_DEBUG_ASSERT(PyThreadState_Get());
  Expected<IterationStatus, ExceptionDetails> next_value;
  auto expected = GetIterator(iterable).Map([&](auto iterator) noexcept {
    for (;;) {
      next_value = Next(iterator.get(), fn);
      if (!next_value.IsValid() || next_value.Value() != kContinue) {
        return Void{};
      }
    }
  });
  if (!expected.IsValid()) {
    return std::move(expected).GetUnexpected();
  } else if (!next_value.IsValid()) {
    return std::move(next_value).GetUnexpected();
  } else {
    return std::move(next_value).Value();
  }
}

}  // namespace iterator

namespace time {

void SetClientContextTimeout(::grpc::ClientContext* context, PyObject* timeout) noexcept {
  if (timeout == Py_None) {
    return;
  }
  double timeout_value = PyFloat_AsDouble(timeout);
  if (!PyErr_Occurred()) {
    context->set_deadline(absl::Seconds(timeout_value) + absl::Now());
  }
}

}  // namespace time

PyObject* const kNone = Py_None;
PyObject* const kTrue = Py_True;
PyObject* const kFalse = Py_False;

}  // namespace python
}  // namespace grpc
}  // namespace dropbox
