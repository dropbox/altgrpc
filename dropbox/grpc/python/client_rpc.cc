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

#include <grpcpp/impl/codegen/async_stream.h>
#include <grpcpp/impl/codegen/async_unary_call.h>
#include <grpcpp/impl/codegen/client_unary_call.h>
#include <grpcpp/impl/codegen/sync_stream.h>

#include <condition_variable>
#include <mutex>

#include "absl/base/attributes.h"
#include "dropbox/grpc/internal.h"
#include "dropbox/grpc/python/client.h"
#include "dropbox/grpc/python/dispatch.h"
#include "dropbox/grpc/python/rendezvous.h"
#include "dropbox/grpc/python/semaphore.h"
#include "dropbox/grpc/time.h"

namespace dropbox {
namespace grpc {
namespace python {

namespace {

const ::grpc::Status kErrorSerializingRequest{::grpc::StatusCode::INTERNAL, "Exception serializing request!"};
const ::grpc::Status kErrorDeserializingResponse{::grpc::StatusCode::INTERNAL, "Exception deserializing response!"};
const ::grpc::Status kErrorCancelledOnShutdown{::grpc::StatusCode::CANCELLED, "RPC canceled due to process shutdown"};

const ::grpc::Status& kExceptionGettingIterator = kErrorSerializingRequest;
const ::grpc::Status& kExceptionCallingNext = kErrorSerializingRequest;

ABSL_ATTRIBUTE_NORETURN void SleepForever() noexcept {
  std::mutex mutex;
  std::condition_variable cv;
  std::unique_lock<std::mutex> lock(mutex);
  while (true) {
    cv.wait(lock);
  }
}

template <typename T>
void ConsumeRequestIterator(RefCountedPtr<T> self, AutoReleasedObject request_iterator,
                            AutoReleasedObject rendezvous_handle) {
  SetCurrentThreadName("requestiter");
  PythonExecCtx py_exec_ctx;
  if (!py_exec_ctx.Acquired()) {
    return;
  }
  auto request_serializer = std::move(self->request_serializer);
  auto iteration_status = iterator::ForEach(request_iterator.get(), [&](auto request) noexcept {
    auto serialized_request = (*request_serializer)(request.get());
    request.reset();
    if (!serialized_request.IsValid()) {
      self->rendezvous->Finalize(kErrorSerializingRequest);
      return false;
    }
    auto nogil = gil::Release();
    auto write_task = CreateTask();
    self->stream->Write(std::move(serialized_request).Value(), write_task->RefTag());
    write_task->Await();
    return write_task->ok();
  });
  request_iterator.reset();
  if (iteration_status.IsValid()) {
    if (std::move(iteration_status).Value() == iterator::kEndOfSequence) {
      auto nogil = gil::Release();
      auto write_task = CreateTask();
      self->stream->WritesDone(write_task->RefTag());
      write_task->Await();
    } else {
      self->rendezvous->context()->TryCancel();
    }
  } else {
    self->rendezvous->Finalize(kExceptionCallingNext);
    self->rendezvous->context()->TryCancel();
  }
  rendezvous_handle.reset();
  auto nogil = gil::Release();
  self.reset();
}

template <typename T>
struct ReaderState : public RefCountedCompletionQueueTag<ReaderState<T>> {
  ReaderState() noexcept {}
  Rendezvous* rendezvous;
  T* stream;
  std::shared_ptr<const MessageDeserializer> deserializer;
  void FailAndUnlockRead() noexcept {
    last_ok = false;
    read_unlocked.Increment();
  }
  PyObject* BlockingRead(PyObject* self) noexcept {
    ::grpc::ByteBuffer message;
    bool ok = false;
    if (!rendezvous->IsDone()) {
      ExitBarrier exit_barrier;
      if (ABSL_PREDICT_TRUE(exit_barrier)) {
        auto gil_acquirer = gil::Release();
        auto lock = read_unlocked.Decrement(gil_acquirer);
        if (ABSL_PREDICT_FALSE(!lock)) {
          return nullptr;
        }
        if (ABSL_PREDICT_TRUE(last_ok)) {
          stream->Read(&message, ReaderState<T>::RefTag());
          auto read_op_lock = result_available.Decrement(gil_acquirer);
          if (ABSL_PREDICT_FALSE(!read_op_lock)) {
            return nullptr;
          }
          // Do not unlock semaphore: next FinalizeResult will do so
          ok = last_ok;
          read_op_lock.release();
        }
      }
    }
    if (ABSL_PREDICT_FALSE(!ok)) {
      auto status_code = rendezvous->StatusCode();
      if (status_code) {
        if (status_code == ::grpc::StatusCode::OK) {
          Cython_RaiseStopIteration();
        } else {
          Cython_Raise(self);
        }
      }
      return nullptr;
    }
    auto deserialized_response = (*deserializer)(message);
    if (!deserialized_response.IsValid()) {
      rendezvous->Finalize(kErrorDeserializingResponse);
      rendezvous->context()->TryCancel();
      Cython_Raise(self);
      return nullptr;
    }
    return std::move(deserialized_response).Value().release();
  }

 private:
  Semaphore read_unlocked, result_available;
  bool last_ok{};
  enum {
    kReadingInitialMetadata,
    kReadingMessages,
  } state_ = kReadingInitialMetadata;

  void ProceedReffed(bool ok) noexcept override {
    last_ok = ok;
    switch (state_) {
      case kReadingInitialMetadata:
        state_ = kReadingMessages;
        rendezvous->UnblockInitialMetadata();
        read_unlocked.Increment();
        break;
      case kReadingMessages:
        result_available.Increment();
        break;
      default:
        abort();
    }
  }
};

}  // namespace

bool Rendezvous::PrepareResult(PyObject* timeout) noexcept {
  if (!done_) {
    absl::optional<absl::Time> deadline;
    if (timeout && timeout != kNone) {
      auto converted_value = types::Float{timeout}.ToDouble();
      if (converted_value.IsValid()) {
        deadline = absl::Seconds(converted_value.Value()) + absl::Now();
      } else {
        std::move(converted_value).Error().Restore();
        return false;
      }
    }
    ExitBarrier exit_barrier;
    if (ABSL_PREDICT_FALSE(!exit_barrier)) {
      Finalize(kErrorCancelledOnShutdown);
      return true;
    }
    auto gil_acquirer = gil::Release();
    if (deadline) {
      bool timed_out;
      if (!done_semaphore_.DecrementWithDeadline(gil_acquirer, *deadline, &timed_out)) {
        if (!timed_out) {
          return false;
        }
        gil_acquirer.reset();
        Cython_RaiseFutureTimeoutError();
        return false;
      }
    } else {
      if (!done_semaphore_.Decrement(gil_acquirer)) {
        return false;
      }
    }
  }
  return true;
}

PyObject* Rendezvous::Result(PyObject* self, PyObject* timeout) noexcept {
  if (ABSL_PREDICT_FALSE(!PrepareResult(timeout))) {
    return nullptr;
  }
  if (ABSL_PREDICT_TRUE(status_.ok())) {
    return AddRef(result_).release();
  }
  if (status_.error_code() == ::grpc::StatusCode::CANCELLED) {
    Cython_RaiseFutureCancelledError();
  } else {
    Cython_Raise(self);
  }
  return nullptr;
}

void RpcInvoker::InvokeUnaryUnary(PyObject* py_rendezvous, Rendezvous* rendezvous, PyObject* request) noexcept {
  auto serialized_request = (*request_serializer_)(request);
  if (!serialized_request.IsValid()) {
    rendezvous->Finalize(kErrorSerializingRequest);
    return;
  }

  struct CallState : CompletionQueueTag {
    std::shared_ptr<CompletionQueueWatcher> watcher;
    std::shared_ptr<Dispatcher> dispatcher;

    AutoReleasedObject rendezvous_handle, rendezvous_handle_for_write;
    Rendezvous* rendezvous;
    ::grpc::ByteBuffer request;
    std::shared_ptr<const MessageDeserializer> response_deserializer;
    ::grpc::ClientAsyncWriter<::grpc::ByteBuffer>* stream;
    ::grpc::Status status;
    ::grpc::ByteBuffer response;

   private:
    enum { kStartCall, kFinishCall } state = kStartCall;
    void Proceed(bool ok) noexcept override {
      switch (state) {
        case kStartCall:
          state = kFinishCall;
          if (ok) {
            auto write_task = CreateTask();
            dispatcher->RegisterCallback(write_task.get(), [rendezvous = rendezvous_handle_for_write.release()]() {
              AutoReleasedObject{rendezvous}.reset();
            });
            stream->WriteLast(std::move(request), {}, write_task->RefTag());
          }
          stream->Finish(&status, this);
          return;
        case kFinishCall:
          rendezvous->UnblockInitialMetadata();
          Dispatcher::RegisterCallbackInPool(dispatcher, [this]() noexcept {
            if (status.ok()) {
              auto response_object = (*response_deserializer)(response);
              if (response_object.IsValid()) {
                rendezvous->Finalize(std::move(status), std::move(response_object).Value());
              } else {
                rendezvous->Finalize(kErrorDeserializingResponse);
              }
            } else {
              rendezvous->Finalize(std::move(status));
            }
            rendezvous_handle.reset();
            rendezvous_handle_for_write.reset();
            auto nogil = gil::Release();
            delete this;
          });
          return;
      }
      abort();
    }
  };

  auto rendezvous_handle = AddRef(py_rendezvous);
  auto rendezvous_handle_for_write = AddRef(py_rendezvous);
  auto nogil = gil::Release();
  auto call = std::make_unique<CallState>();
  call->watcher = ClientCompletionQueueWatcher();
  call->dispatcher = ClientQueue();
  call->rendezvous_handle = std::move(rendezvous_handle);
  call->rendezvous_handle_for_write = std::move(rendezvous_handle_for_write);
  call->rendezvous = rendezvous;
  call->request = std::move(serialized_request).Value();
  call->response_deserializer = response_deserializer_;
  rendezvous->SetChannel(channel_);
  auto channel = channel_->channel();
  if (channel) {
    call->stream = CreateClientAsyncWriter(channel, call->watcher->CompletionQueue(), *method_, rendezvous->context(),
                                           &call->response);
    call->stream->StartCall(call.release());
    channel_->Register(rendezvous->context()->c_call());
  } else {
    SleepForever();
    // rendezvous->Finalize(kErrorCancelledOnShutdown);
  }
}

void RpcInvoker::InvokeStreamUnary(PyObject* py_rendezvous, Rendezvous* rendezvous,
                                   PyObject* request_iterator) noexcept {
  struct CallState : RefCounted<CallState>, CompletionQueueTag {
    std::shared_ptr<CompletionQueueWatcher> watcher;
    std::shared_ptr<Dispatcher> dispatcher;

    AutoReleasedObject rendezvous_handle, rendezvous_handle_for_iterator_thread;
    Rendezvous* rendezvous;
    AutoReleasedObject request_iterator;
    std::shared_ptr<const MessageSerializer> request_serializer;
    std::shared_ptr<const MessageDeserializer> response_deserializer;
    ::grpc::ClientAsyncWriter<::grpc::ByteBuffer>* stream;
    ::grpc::Status status;
    ::grpc::ByteBuffer response;

   private:
    enum { kStartCall, kFinishCall } state = kStartCall;
    void Proceed(bool ok) noexcept override {
      switch (state) {
        case kStartCall:
          state = kFinishCall;
          if (ok) {
            std::thread{ConsumeRequestIterator<CallState>, Ref(), std::move(request_iterator),
                        std::move(rendezvous_handle_for_iterator_thread)}
                .detach();
          }
          stream->Finish(&status, this);
          return;
        case kFinishCall:
          rendezvous->UnblockInitialMetadata();
          Dispatcher::RegisterCallbackInPool(dispatcher, [this]() noexcept {
            if (status.ok()) {
              auto response_object = (*response_deserializer)(response);
              if (response_object.IsValid()) {
                rendezvous->Finalize(std::move(status), std::move(response_object).Value());
              } else {
                rendezvous->Finalize(kErrorDeserializingResponse);
              }
            } else {
              rendezvous->Finalize(std::move(status));
            }
            rendezvous_handle.reset();
            rendezvous_handle_for_iterator_thread.reset();
            request_iterator.reset();
            auto nogil = gil::Release();
            Unref();
          });
          return;
      }
      abort();
    }
  };

  auto request_stream = iterator::GetIterator(request_iterator);
  if (!request_stream.IsValid()) {
    rendezvous->Finalize(kExceptionGettingIterator);
    return;
  }

  auto rendezvous_handle = AddRef(py_rendezvous);
  auto rendezvous_handle_for_iterator_thread = AddRef(py_rendezvous);

  auto nogil = gil::Release();
  auto call = MakeRefCounted<CallState>();
  call->watcher = ClientCompletionQueueWatcher();
  call->dispatcher = ClientQueue();
  call->rendezvous_handle = std::move(rendezvous_handle);
  call->rendezvous_handle_for_iterator_thread = std::move(rendezvous_handle_for_iterator_thread);
  call->rendezvous = rendezvous;
  call->request_iterator = std::move(request_stream).Value();
  call->request_serializer = request_serializer_;
  call->response_deserializer = response_deserializer_;
  rendezvous->SetChannel(channel_);
  auto channel = channel_->channel();
  if (channel) {
    call->stream = CreateClientAsyncWriter(channel, call->watcher->CompletionQueue(), *method_, rendezvous->context(),
                                           &call->response);
    call->stream->StartCall(call.release());
    channel_->Register(rendezvous->context()->c_call());
  } else {
    SleepForever();
    // rendezvous->Finalize(kErrorCancelledOnShutdown);
  }
}

void RpcInvoker::InvokeUnaryStream(PyObject* py_rendezvous, Rendezvous* rendezvous, PyObject* request) noexcept {
  auto serialized_request = (*request_serializer_)(request);
  if (!serialized_request.IsValid()) {
    rendezvous->Finalize(kErrorSerializingRequest);
    return;
  }

  struct CallState : CompletionQueueTag {
    std::shared_ptr<CompletionQueueWatcher> watcher;
    std::shared_ptr<Dispatcher> dispatcher;

    AutoReleasedObject rendezvous_handle;
    ::grpc::Status status;

    RefCountedPtr<ReaderState<::grpc::ClientAsyncReader<::grpc::ByteBuffer>>> reader =
        MakeRefCounted<ReaderState<::grpc::ClientAsyncReader<::grpc::ByteBuffer>>>();

    Rendezvous*& rendezvous = reader->rendezvous;
    ::grpc::ClientAsyncReader<::grpc::ByteBuffer>*& stream = reader->stream;

   private:
    enum { kStartCall, kFinishCall } state = kStartCall;
    void Proceed(bool ok) noexcept override {
      switch (state) {
        case kStartCall:
          state = kFinishCall;
          if (ok) {
            stream->ReadInitialMetadata(reader->RefTag());
          } else {
            reader->FailAndUnlockRead();
          }
          stream->Finish(&status, this);
          return;
        case kFinishCall:
          Dispatcher::RegisterCallbackInPool(dispatcher, [this]() noexcept {
            rendezvous->Finalize(std::move(status));
            rendezvous_handle.reset();
            auto nogil = gil::Release();
            delete this;
          });
          return;
      }
      abort();
    }
  };

  auto rendezvous_handle = AddRef(py_rendezvous);
  auto nogil = gil::Release();
  auto call = std::make_unique<CallState>();
  call->watcher = ClientCompletionQueueWatcher();
  call->dispatcher = ClientQueue();
  call->rendezvous_handle = std::move(rendezvous_handle);
  call->reader->rendezvous = rendezvous;
  call->reader->deserializer = response_deserializer_;
  rendezvous->SetBlockingReader([reader = call->reader](PyObject* self) { return reader->BlockingRead(self); });
  rendezvous->SetChannel(channel_);
  auto channel = channel_->channel();
  if (channel) {
    call->reader->stream = CreateClientAsyncReader(channel, call->watcher->CompletionQueue(), *method_,
                                                   rendezvous->context(), std::move(serialized_request).Value());
    call->reader->stream->StartCall(call.release());
    channel_->Register(rendezvous->context()->c_call());
  } else {
    SleepForever();
    // rendezvous->Finalize(kErrorCancelledOnShutdown);
  }
}

void RpcInvoker::InvokeStreamStream(PyObject* py_rendezvous, Rendezvous* rendezvous,
                                    PyObject* request_iterator) noexcept {
  auto request_stream = iterator::GetIterator(request_iterator);
  if (!request_stream.IsValid()) {
    rendezvous->Finalize(kExceptionGettingIterator);
    return;
  }

  struct CallState : RefCounted<CallState>, CompletionQueueTag {
    std::shared_ptr<CompletionQueueWatcher> watcher;
    std::shared_ptr<Dispatcher> dispatcher;

    AutoReleasedObject rendezvous_handle, rendezvous_handle_for_iterator_thread;
    AutoReleasedObject request_iterator;
    std::shared_ptr<const MessageSerializer> request_serializer;
    ::grpc::Status status;

    RefCountedPtr<ReaderState<::grpc::ClientAsyncReaderWriter<::grpc::ByteBuffer, ::grpc::ByteBuffer>>> reader =
        MakeRefCounted<ReaderState<::grpc::ClientAsyncReaderWriter<::grpc::ByteBuffer, ::grpc::ByteBuffer>>>();

    Rendezvous*& rendezvous = reader->rendezvous;
    ::grpc::ClientAsyncReaderWriter<::grpc::ByteBuffer, ::grpc::ByteBuffer>*& stream = reader->stream;

   private:
    enum { kStartCall, kFinishCall } state = kStartCall;
    void Proceed(bool ok) noexcept override {
      switch (state) {
        case kStartCall:
          state = kFinishCall;
          if (ok) {
            std::thread{ConsumeRequestIterator<CallState>, Ref(), std::move(request_iterator),
                        std::move(rendezvous_handle_for_iterator_thread)}
                .detach();
            stream->ReadInitialMetadata(reader->RefTag());
          } else {
            reader->FailAndUnlockRead();
          }
          stream->Finish(&status, this);
          return;
        case kFinishCall:
          Dispatcher::RegisterCallbackInPool(dispatcher, [this]() noexcept {
            rendezvous->Finalize(std::move(status));
            request_iterator.reset();
            rendezvous_handle.reset();
            rendezvous_handle_for_iterator_thread.reset();
            auto nogil = gil::Release();
            Unref();
          });
          return;
      }
      abort();
    }
  };

  auto rendezvous_handle = AddRef(py_rendezvous);
  auto rendezvous_handle_for_iterator_thread = AddRef(py_rendezvous);
  auto nogil = gil::Release();
  auto call = MakeRefCounted<CallState>();
  call->watcher = ClientCompletionQueueWatcher();
  call->dispatcher = ClientQueue();
  call->rendezvous_handle = std::move(rendezvous_handle);
  call->rendezvous_handle_for_iterator_thread = std::move(rendezvous_handle_for_iterator_thread);
  call->rendezvous = rendezvous;
  call->request_iterator = std::move(request_stream).Value();
  call->request_serializer = request_serializer_;
  call->reader->deserializer = response_deserializer_;
  rendezvous->SetBlockingReader([reader = call->reader](PyObject* self) { return reader->BlockingRead(self); });
  rendezvous->SetChannel(channel_);
  auto channel = channel_->channel();
  if (channel) {
    call->stream =
        CreateClientAsyncReaderWriter(channel, call->watcher->CompletionQueue(), *method_, rendezvous->context());
    call->stream->StartCall(call.release());
    channel_->Register(rendezvous->context()->c_call());
  } else {
    SleepForever();
    // rendezvous->Finalize(kErrorCancelledOnShutdown);
  }
}

}  // namespace python
}  // namespace grpc
}  // namespace dropbox
