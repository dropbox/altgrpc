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

from libc.stdint cimport uint8_t, int32_t, uint32_t, int64_t
from libcpp.memory cimport shared_ptr
from libcpp.string cimport string as std_string
from libcpp.vector cimport vector


cdef extern from "grpc/grpc_security_constants.h" nogil:
   ctypedef enum LocalConnectionType "grpc_local_connect_type":
      UDS
      LOCAL_TCP

cdef extern from "grpcpp/security/credentials.h" namespace "::grpc" nogil:
    cppclass CallCredentials:
        pass
    cppclass ChannelCredentials:
        pass
    cppclass SslChannelCredentialsOptions "::grpc::SslCredentialsOptions":
        std_string pem_root_certs
        std_string pem_private_key
        std_string pem_cert_chain
    shared_ptr[ChannelCredentials] InsecureChannelCredentials()
    shared_ptr[ChannelCredentials] SslChannelCredentials "::grpc::SslCredentials"(const SslChannelCredentialsOptions& options)
    shared_ptr[ChannelCredentials] LocalChannelCredentials "::grpc::experimental::LocalCredentials"(LocalConnectionType connection_type)
    shared_ptr[CallCredentials] AccessTokenCallCredentials "::grpc::AccessTokenCredentials"(const std_string& access_token)
    shared_ptr[CallCredentials] CompositeCallCredentials(const shared_ptr[CallCredentials] &creds1, const shared_ptr[CallCredentials] &creds2)
    shared_ptr[ChannelCredentials] CompositeChannelCredentials(const shared_ptr[ChannelCredentials] &creds1, const shared_ptr[CallCredentials] &creds2)

cdef extern from "grpcpp/security/server_credentials.h" namespace "::grpc" nogil:
    cppclass ServerCredentials:
        pass
    shared_ptr[ServerCredentials] InsecureServerCredentials()
    shared_ptr[ServerCredentials] LocalServerCredentials "::grpc::experimental::LocalServerCredentials"(LocalConnectionType connection_type)

cdef extern from "grpcpp/grpcpp.h" namespace "::grpc" nogil:
    std_string Version()

    cppclass ServerBuilder:
        ServerBuilder& AddListeningPort(const std_string& addr_uri, shared_ptr[ServerCredentials] creds, int* selected_port)
        ServerBuilder& AddIntegerArgument "AddChannelArgument"(const std_string& name, const int& value)
        ServerBuilder& AddStringArgument "AddChannelArgument"(const std_string& name, const std_string& value)
        ServerBuilder& SetMaxReceiveMessageSize(int max_receive_message_size)
        ServerBuilder& SetMaxSendMessageSize(int max_send_message_size)

    cppclass Channel:
        pass

    cppclass ChannelArguments:
        void SetInt(const std_string& key, int value)
        void SetString(const std_string&, const std_string& value)
        void SetPointer(const std_string&, void*)
        void SetCompressionAlgorithm(grpc_compression_algorithm algorithm)

    cppclass ClientContext:
        void TryCancel()
        void set_wait_for_ready(bint wait_for_ready) except +
        void set_credentials(const shared_ptr[CallCredentials]& creds) except +
        void set_compression_algorithm(grpc_compression_algorithm) except +
        std_string debug_error_string() except +
        void AddMetadata(const std_string& meta_key, const std_string& meta_value) except+

    ctypedef enum StatusCode:
        OK
        CANCELLED
        UNKNOWN
        INTERNAL

    cppclass Status:
        StatusCode error_code()
        bint ok()
        std_string error_details()
        std_string error_message()

    cppclass ClientAsyncWriter[T]:
        void Write(const T&, void*)
        void WritesDone(void* tag)
        void Finish(Status*, void*)

    cppclass ClientAsyncReaderWriter[T,U](ClientAsyncWriter[U]):
        void Read(T*, void*)

cdef extern from "grpc/compression.h":
  ctypedef enum grpc_compression_algorithm:
    GRPC_COMPRESS_NONE
    GRPC_COMPRESS_DEFLATE
    GRPC_COMPRESS_GZIP
    GRPC_COMPRESS_STREAM_GZIP
    GRPC_COMPRESS_ALGORITHMS_COUNT

  ctypedef enum grpc_compression_level:
    GRPC_COMPRESS_LEVEL_NONE
    GRPC_COMPRESS_LEVEL_LOW
    GRPC_COMPRESS_LEVEL_MED
    GRPC_COMPRESS_LEVEL_HIGH
    GRPC_COMPRESS_LEVEL_COUNT

cdef extern from "grpc/grpc.h" nogil:
  ctypedef enum grpc_status_code:
    GRPC_STATUS_OK
    GRPC_STATUS_CANCELLED
    GRPC_STATUS_UNKNOWN
    GRPC_STATUS_INVALID_ARGUMENT
    GRPC_STATUS_DEADLINE_EXCEEDED
    GRPC_STATUS_NOT_FOUND
    GRPC_STATUS_ALREADY_EXISTS
    GRPC_STATUS_PERMISSION_DENIED
    GRPC_STATUS_UNAUTHENTICATED
    GRPC_STATUS_RESOURCE_EXHAUSTED
    GRPC_STATUS_FAILED_PRECONDITION
    GRPC_STATUS_ABORTED
    GRPC_STATUS_OUT_OF_RANGE
    GRPC_STATUS_UNIMPLEMENTED
    GRPC_STATUS_INTERNAL
    GRPC_STATUS_UNAVAILABLE
    GRPC_STATUS_DATA_LOSS

  ctypedef enum grpc_connectivity_state:
    GRPC_CHANNEL_IDLE
    GRPC_CHANNEL_CONNECTING
    GRPC_CHANNEL_READY
    GRPC_CHANNEL_TRANSIENT_FAILURE
    GRPC_CHANNEL_SHUTDOWN

cdef extern from "grpcpp/security/server_credentials.h" namespace "::grpc::SslServerCredentialsOptions" nogil:
    cppclass PemKeyCertPair:
        std_string private_key
        std_string cert_chain

cdef extern from "grpcpp/security/server_credentials.h" namespace "::grpc" nogil:
    cppclass SslServerCredentialsOptions:
        std_string pem_root_certs
        vector[PemKeyCertPair] pem_key_cert_pairs
        bint force_client_auth
    cdef shared_ptr[ServerCredentials] SslServerCredentials(const SslServerCredentialsOptions& options) except +

