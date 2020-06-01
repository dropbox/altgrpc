# Copyright (c) 2020 Dropbox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from libcpp.string cimport string as std_string
from libcpp.memory cimport shared_ptr
cimport grpcpp as grpc

cdef extern from "dropbox/grpc/python/interop.h" namespace "dropbox::grpc::python::interop" nogil:
    void Initialize()
    void Exit()

cdef extern from "dropbox/grpc/python/common.h" namespace "dropbox::grpc::python" nogil:
    cppclass MessageDeserializer:
        pass
    cppclass MessageSerializer:
        pass
    MessageDeserializer GenericDeserializer(deserializer)
    MessageSerializer GenericSerializer(serializer)

    void InitializeConnectivityObjects(idle, connecting, ready, transient_failure, shutdown)

cdef extern from "dropbox/grpc/python/dispatch.h" namespace "dropbox::grpc::python" nogil:
    cppclass DispatchCallback "dropbox::grpc::python::Dispatcher::DispatchCallback":
        void operator()()
    cppclass Dispatcher:
        pass
    shared_ptr[Dispatcher] ClientQueue()

cdef extern from "dropbox/grpc/server.h" namespace "dropbox::grpc" nogil:
    void AddPointerArgument(grpc.ServerBuilder* builder, const std_string& name, void* value) except +

cdef extern from "dropbox/grpc/python/server.h" namespace "dropbox::grpc::python" nogil:
    cppclass RequestHandler:
        RequestHandler()
        RequestHandler(thread_pool_submit, service_pipeline)
        RequestHandler(thread_pool_submit, service_pipeline, size_t concurrency_limit)
        void AddMethod(std_string full_method_path)

    cppclass RequestHandlers:
        RequestHandlers()
        RequestHandlers(RequestHandler default_handler)
        void AddHandler(RequestHandler handler)

    cppclass Server:
        bint BuildAndStart(grpc.ServerBuilder* builder, RequestHandlers handlers, size_t thread_count)
        object Stop()
        object Stop(double grace)
        void Loop()

    cppclass ServicerContext:
        str Method()
        bint IsActive()
        TimeRemaining()
        void Cancel()
        bint AddCallback(void())
        tuple InvocationMetadata()
        str Peer()
        tuple PeerIdentities()
        str PeerIdentityKey()
        dict AuthContext()
        void SetCompression(grpc.grpc_compression_algorithm)
        void DisableNextMessageCompression()
        void SetCode(grpc.grpc_status_code)
        void SetDetails(std_string details)
        void AddInitialMetadata(std_string key, std_string value)
        void AddTrailingMetadata(std_string key, std_string value)
        void SendInitialMetadataBlocking()
        object Read()
        void Write(message)
        void AbortUnrecognizedMethod()
        void AbortHandlerLookupFailed()

cdef extern from "dropbox/grpc/python/server.h" namespace "dropbox::grpc::python::rpc" nogil:
    cppclass MethodHandler:
        void Handle(servicer_context, ServicerContext* server_call) except +
        @staticmethod
        shared_ptr[MethodHandler] Create(bint request_streaming, bint response_streaming, bint iterators, handler, MessageDeserializer request_deserializer, MessageSerializer response_serializer) except +

cdef extern from "dropbox/grpc/python/dummy_server.h" namespace "dropbox::grpc::python" nogil:
    cppclass DummyServer:
        int Bind(const char* addr) nogil except +

cdef extern from "dropbox/grpc/python/channel.h" namespace "dropbox::grpc::python" nogil:
    cppclass Channel:
        void Initialize(std_string target, grpc.ChannelArguments* args, shared_ptr[grpc.ChannelCredentials])
        UnaryUnary(method, request_serializer, response_deserializer)
        AsyncUnaryUnary(method, request_serializer, response_deserializer)
        UnaryStream(method, request_serializer, response_deserializer)
        AsyncUnaryStream(method, request_serializer, response_deserializer)
        StreamUnary(method, request_serializer, response_deserializer)
        AsyncStreamUnary(method, request_serializer, response_deserializer)
        StreamStream(method, request_serializer, response_deserializer)
        AsyncStreamStream(method, request_serializer, response_deserializer)
        WaitForConnected(timeout)
        GetState(bint try_to_connect)
        void Subscribe(callback, bint try_to_connect)
        void Unsubscribe(callback)
        void Close()

cdef extern from "dropbox/grpc/python/rendezvous.h" namespace "dropbox::grpc::python" nogil:
    void SetClientContextMetadata(grpc.ClientContext*, metadata) except *
    void SetClientContextTimeout(grpc.ClientContext*, timeout) except *
    cppclass Rendezvous:
        grpc.ClientContext* context()
        bint IsActive()
        bint IsRunning()
        bint IsDone()
        bint IsCancelled()
        TimeRemaining()
        bint Cancel()
        InitialMetadata()
        TrailingMetadata()
        Code()
        Details()
        bint AddCallback(callback)
        DebugErrorString()
        Result(self, timeout)
        Exception(self, timeout)
        Traceback(self, timeout)
        Next(self)
        StringRepresentation()

cdef extern from "dropbox/grpc/python/client.h" namespace "dropbox::grpc::python" nogil:
    cppclass RpcInvoker:
        void InvokeUnaryUnary(rendezvous_handle, Rendezvous* Rendezvous, request)
        void InvokeStreamUnary(rendezvous_handle, Rendezvous* Rendezvous, request_iterator)
        void InvokeUnaryStream(rendezvous_handle, Rendezvous* Rendezvous, request)
        void InvokeStreamStream(rendezvous_handle, Rendezvous* Rendezvous, request_iterator)

cdef extern from "dropbox/grpc/python/auth_metadata_plugin.h" namespace "dropbox::grpc::python":
    shared_ptr[grpc.CallCredentials] MetadataCallCredentials(plugin, std_string name) except *

cdef extern from "dropbox/grpc/servicer.h" namespace "dropbox::grpc" nogil:
    cppclass Servicer:
        void Register(grpc.ServerBuilder* builder)
        void Start()
        void Shutdown()
