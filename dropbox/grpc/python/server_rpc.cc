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
#include "dropbox/grpc/python/server.h"
#include "dropbox/types/expected.h"

namespace dropbox {
namespace grpc {
namespace python {

namespace {

const ::grpc::Status kUnimplementedStatus{::grpc::StatusCode::UNIMPLEMENTED, "Method not found!"};
const ::grpc::Status kErrorInServiceHandlerStatus{::grpc::StatusCode::UNKNOWN, "Error in service handler!"};
const ::grpc::Status kFailedDeserializingRequest{::grpc::StatusCode::INTERNAL, "Exception deserializing request!"};

// Do not leak the exact info to the client:
const ::grpc::Status kStatusErrorCallingApplication{::grpc::StatusCode::UNKNOWN, "Exception calling application"};
const ::grpc::Status& kStatusErrorSerializingResponse = kStatusErrorCallingApplication;

// TODO(mehrdad): Move this to C++
extern "C" void _abort_with_abortion_exception(PyObject* servicer_context, PyObject* abortion_exception);

void HandleError(ObjectHandle servicer_context, ServicerContext* call, ExceptionDetails error) noexcept {
  if (error.IsOfType(&AbortionExceptionType)) {
    _abort_with_abortion_exception(servicer_context.get(), error.value.get());
    call->End();
  } else if (error.IsOfType(&RpcErrorType)) {
    call->End();
  } else {
    call->Abort(kStatusErrorCallingApplication);
  }
}

}  // namespace

void ServicerContext::Start(RefCountedPtr<ServerCall> grpc_call, std::shared_ptr<Dispatcher> dispatcher,
                            AutoReleasedObject servicer_context, ObjectHandle service_pipeline) noexcept {
  call_ = std::move(grpc_call);
  dispatcher_ = std::move(dispatcher);
  auto call = this;
  call->dispatcher()->RunInPool(
      [servicer_context = servicer_context.release(), service_pipeline = std::move(service_pipeline)] {
        AutoReleasedObject ctx{servicer_context};
        CallPython(service_pipeline.get(), ctx.get());
      });
}

PyObject* ServicerContext::Read() {
  if (!request_deserializer_ || !IsActive()) {
    Cython_RaiseRpcError();
  }
  RefCountedPtr<Future<::grpc::ByteBuffer>> future;
  {
    auto nogil = gil::Release();
    future = call_->Read();
    future->Await();
  }
  if (future->ok()) {
    auto message = request_deserializer_(future->result());
    if (message.IsValid()) {
      return std::move(message).Value().release();
    }
    Cython_RaiseRpcError();
    return nullptr;
  } else {
    return AddRef(kNone).release();
  }
}

void ServicerContext::Write(PyObject* message) {
  if (!response_serializer_ || !IsActive()) {
    Cython_RaiseRpcError();
    return;
  }
  auto buffer = response_serializer_(message);
  if (!buffer.IsValid()) {
    Cython_RaiseRpcError();
    return;
  }
  RefCountedPtr<Task> write_task;
  {
    auto nogil = gil::Release();
    write_task = call_->Write(std::move(buffer).Value());
    write_task->Await();
  }
  if (!write_task->ok()) {
    Cython_RaiseRpcError();
    return;
  }
}

void ServicerContext::AbortHandlerLookupFailed() noexcept {
  auto nogil = gil::Release();
  call_->Finish(kErrorInServiceHandlerStatus);
}

void ServicerContext::AbortUnrecognizedMethod() noexcept {
  auto nogil = gil::Release();
  call_->Finish(kUnimplementedStatus);
}

namespace rpc {

class MethodHandler::Private {};

std::shared_ptr<MethodHandler> MethodHandler::Create(bool request_streaming, bool response_streaming, bool iterators,
                                                     ObjectHandle handler, MessageDeserializer request_deserializer,
                                                     MessageSerializer response_serializer) {
  return std::make_shared<MethodHandler>(Private{}, request_streaming, response_streaming, iterators,
                                         std::move(request_deserializer), std::move(response_serializer),
                                         std::move(handler));
}

void MethodHandler::Handle(ObjectHandle servicer_context, ServicerContext* call) const {
  if (!iterators_) {
    call->SetSerDes(std::move(response_serializer_), std::move(request_deserializer_));
    HandleIteratorLessCall(std::move(servicer_context), call);
  } else if (request_streaming_) {
    HandleStreamingRequest(std::move(servicer_context), call);
  } else {
    HandleUnaryRequest(std::move(servicer_context), call);
  }
}

void MethodHandler::Continue(ObjectHandle servicer_context, ServicerContext* call,
                             AutoReleasedObject request_or_iterator) const {
  if (response_streaming_) {
    ContinueStreamingResponse(std::move(servicer_context), call, std::move(request_or_iterator));
  } else {
    ContinueUnaryResponse(std::move(servicer_context), call, std::move(request_or_iterator));
  }
}

void MethodHandler::HandleUnaryRequest(ObjectHandle servicer_context, ServicerContext* call) const {
  auto future = call->call()->Read();
  call->dispatcher()->RegisterCallback(
      future.get(), [ctx = std::move(servicer_context), call, future, self = shared_from_this()] {
        call->dispatcher()->RunInPool([ctx = std::move(ctx), call, future = std::move(future), self = std::move(self)] {
          if (!call->IsActive()) {
            return;
          }
          if (future->ok()) {
            auto request = self->request_deserializer_(future->result());
            if (request.IsValid()) {
              self->Continue(std::move(ctx), call, std::move(request).Value());
              return;
            }
          }
          call->call()->Finish(kFailedDeserializingRequest);
        });
      });
}

void MethodHandler::HandleStreamingRequest(ObjectHandle servicer_context, ServicerContext* call) const {
  struct RequestIterator : iterator::Interface {
    RequestIterator(RefCountedPtr<ServerCall> call, MessageDeserializer deserializer) noexcept
        : call_(std::move(call)), deserializer_(deserializer) {}
    virtual ~RequestIterator() {}

    PyObject* Next() override {
      if (error_) {
        Cython_RaiseRpcError();
        return nullptr;
      }
      if (!call_.get()) {
        return nullptr;
      }
      RefCountedPtr<Future<::grpc::ByteBuffer>> future;
      {
        auto nogil = gil::Release();
        future = call_->Read();
        future->Await();
      }
      if (future->ok()) {
        auto request_item = deserializer_(future->result());
        if (request_item.IsValid()) {
          return std::move(request_item).Value().release();
        }
        error_ = true;
      }
      call_.reset();
      deserializer_ = nullptr;
      if (error_) {
        Cython_RaiseRpcError();
      }
      return nullptr;
    }

    void Dispose() override { delete this; }

   private:
    RefCountedPtr<ServerCall> call_;
    MessageDeserializer deserializer_;
    bool error_ = false;
  };

  call->dispatcher()->RunInPool([ctx = std::move(servicer_context), call, self = shared_from_this()] {
    if (!call->IsActive()) {
      return;
    }
    self->Continue(std::move(ctx), call,
                   iterator::Create(new RequestIterator(call->call()->Ref(), self->request_deserializer_)));
  });
}

void MethodHandler::HandleIteratorLessCall(ObjectHandle servicer_context, ServicerContext* call) const {
  call->dispatcher()->RunInPool([ctx = std::move(servicer_context), call, self = shared_from_this()] {
    if (!call->IsActive()) {
      return;
    }

    auto response_object = CallPython(self->handler_.get(), kNone, ctx.get());
    if (!response_object.IsValid()) {
      gpr_log(GPR_DEBUG, "Exception calling application");
      call->Abort(kStatusErrorCallingApplication);
      return;
    }

    call->End();
  });
}

void MethodHandler::ContinueUnaryResponse(ObjectHandle servicer_context, ServicerContext* call,
                                          AutoReleasedObject request_or_iterator) const {
  if (!call->IsActive()) {
    return;
  }

  // Handle AbortException
  auto response_object = CallPython(handler_.get(), request_or_iterator.get(), servicer_context.get());
  if (!response_object.IsValid()) {
    gpr_log(GPR_DEBUG, "Exception calling application");
    call->Abort(kStatusErrorCallingApplication);
    return;
  }
  if (response_object.Value().get() == kNone) {
    call->End();
    return;
  }

  if (!call->IsActive()) {
    return;
  }

  auto response = response_serializer_(response_object.Value().get());
  if (response.IsValid()) {
    call->End(std::move(response).Value());
  } else {
    call->Abort(kStatusErrorSerializingResponse);
  }
}

void MethodHandler::StreamResponses(ObjectHandle servicer_context, ServicerContext* call,
                                    AutoReleasedObject response_iterator) const {
  for (;;) {
    if (!call->IsActive()) {
      break;
    }
    RefCountedPtr<Task> write_task;
    auto status = iterator::Next(response_iterator.get(), [&](auto response) noexcept {
      if (response.get() == kNone) {
        call->End();
        return false;
      }
      auto write_buffer = response_serializer_(response.get());
      if (!write_buffer.IsValid()) {
        call->Abort(kStatusErrorSerializingResponse);
        return false;
      }

      write_task = call->call()->Write(std::move(write_buffer).Value());
      return true;
    });

    if (!status.IsValid()) {
      HandleError(servicer_context, call, std::move(status).Error());
      break;
    } else if (status.Value() == iterator::kContinue) {
      write_task->Await();
      if (!write_task->ok()) {
        call->End();
        break;
      }
    } else {
      call->End();
      break;
    }
  }
}

void MethodHandler::StreamResponsesAsync(ObjectHandle servicer_context, ServicerContext* call,
                                         AutoReleasedObject response_iterator) const {
  auto status = iterator::Next(response_iterator.get(), [&](auto response) noexcept {
    if (response.get() == kNone) {
      call->End();
      return false;
    }
    auto write_buffer = response_serializer_(response.get());
    if (!write_buffer.IsValid()) {
      call->Abort(kStatusErrorSerializingResponse);
      return false;
    }

    auto write_task = call->call()->Write(std::move(write_buffer).Value());
    call->dispatcher()->RegisterCallback(
        write_task.get(), [write_task, ctx = std::move(servicer_context), call,
                           response_iterator_obj = response_iterator.release(), self = shared_from_this()] {
          AutoReleasedObject response_iterator{response_iterator_obj};
          if (!write_task->ok()) {
            call->End();
            return;
          }
          call->dispatcher()->RunInPool(
              [ctx = std::move(ctx), call, response_iterator = response_iterator.release(), self = std::move(self)] {
                self->StreamResponsesAsync(std::move(ctx), call, AutoReleasedObject{response_iterator});
              });
        });
    return true;
  });

  if (!status.IsValid()) {
    HandleError(servicer_context, call, std::move(status).Error());
  } else if (status.Value() == iterator::kEndOfSequence) {
    call->End();
  }
}

void MethodHandler::ContinueStreamingResponse(ObjectHandle servicer_context, ServicerContext* call,
                                              AutoReleasedObject request_iterator) const {
  if (!call->IsActive()) {
    return;
  }

  auto handler_result = CallPython(handler_.get(), request_iterator.get(), servicer_context.get());
  if (handler_result.IsValid()) {
    auto response_iterator = iterator::GetIterator(handler_result.Value().get());
    if (response_iterator.IsValid()) {
      StreamResponses(std::move(servicer_context), call, std::move(response_iterator).Value());
    } else {
      HandleError(servicer_context, call, std::move(response_iterator).Error());
    }
  } else {
    call->Abort(kStatusErrorCallingApplication);
  }
}

}  // namespace rpc
}  // namespace python
}  // namespace grpc
}  // namespace dropbox
