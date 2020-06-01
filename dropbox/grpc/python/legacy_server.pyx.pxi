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

cdef class server:
    cdef unique_ptr[altgrpc.DummyServer] x_dummy_server
    cdef list bound_addresses
    cdef ServerBuilder builder
    cdef _Server running_server

    def __cinit__(self, thread_pool, handlers=None, interceptors=None, options=None, maximum_concurrent_rpcs=None, compression=None):
        self.x_dummy_server = make_unique[altgrpc.DummyServer]()
        self.bound_addresses = []
        self.builder = ServerBuilder()
        self.running_server = None
        if handlers:
            self.builder.add_generic_rpc_handlers(handlers)
        if interceptors:
            self.builder.set_interceptors(interceptors)
        if options:
            for option, value in options:
                self.builder.set_option(
                    option if isinstance(option, bytes) else option.encode('utf8'),
                    value if PY_MAJOR_VERSION < 3 or not isinstance(value, str) else value.encode('utf8'))
        if compression:
            self.builder.set_option(b'grpc-internal-encoding-request', compression.value)
        if maximum_concurrent_rpcs is not None:
            self.builder.set_maximum_concurrent_rpcs(maximum_concurrent_rpcs)
        self.builder.set_thread_pool(thread_pool)

    def add_generic_rpc_handlers(server self, generic_rpc_handlers):
        self.builder.add_generic_rpc_handlers(generic_rpc_handlers)

    def add_insecure_port(server self, address):
        return _jeearrpeesee_server_add_http2_port(
            self, address, grpc.InsecureServerCredentials())

    def add_secure_port(server self, address, _ServerCredentials server_credentials):
        return _jeearrpeesee_server_add_http2_port(
            self, address, server_credentials.credentials)

    def start(server self):
        with nogil:
            self.x_dummy_server.reset()
        self.running_server = self.builder.build_and_start()
        for expected, actual in self.bound_addresses:
            actual_val = actual()
            if expected != actual_val:
                # TODO: Stop the newly started server?
                raise Exception('Failed to start server: race between dummy server and main server initialization: previously bound port[{}]!=newly bound port[{}]'.format(expected, actual_val))
        self.bound_addresses = None

    def stop(server self, grace):
        with nogil:
            self.x_dummy_server.reset()
        if self.running_server is None:
            e = _threading.Event()
            e.set()
            return e
        return self.running_server.stop(grace)

    def __dealloc__(server self):
        with nogil:
            self.x_dummy_server.reset()

cdef int _jeearrpeesee_server_add_http2_port(server server, address, shared_ptr[grpc.ServerCredentials] creds) except *:
    if not isinstance(address, bytes):
        address = address.encode('utf8')
    cdef int bound_port = server.x_dummy_server.get().Bind(address)
    if bound_port <= 0:
        return 0
    cdef bytes adapted_address = address.rsplit(b':', 1)[0] + (b':%d' % bound_port)
    cdef _BoundPort actual_port_fetcher = _server_builder_add_port(server.builder, adapted_address, creds)
    server.bound_addresses.append((bound_port, actual_port_fetcher))
    return bound_port
