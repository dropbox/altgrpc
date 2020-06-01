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

# Eliminate this dependency by relying on a native thread pool
# in PyRequestHandler.
from concurrent.futures import ThreadPoolExecutor as _ThreadPoolExecutor

###############################################################################
## PUBLIC API:

include "legacy_server.pyx.pxi"

@cython.internal
cdef class PyRequestHandler:
    cdef object submit
    cdef dict dict_handler
    cdef object concurrency_limit

cdef class ServerBuilder:
    cdef grpc.ServerBuilder x
    cdef vector[shared_ptr[altgrpc.Servicer]] _native_servicers
    cdef list bound_ports
    cdef list additional_handlers
    cdef list generic_handlers
    cdef dict dict_handler
    cdef list interceptors
    cdef object submit
    cdef object maximum_concurrent_rpcs
    cdef uint16_t thread_count

    def __cinit__(self):
        self.bound_ports = []
        self.additional_handlers = []
        self.generic_handlers = []
        self.dict_handler = {}
        self.interceptors = []
        self.thread_count = 0

    cpdef _BoundPort add_insecure_port(ServerBuilder self, bytes address):
        return _server_builder_add_port(self, address, grpc.InsecureServerCredentials())

    cpdef _BoundPort add_secure_port(ServerBuilder self, bytes address, _ServerCredentials credentials):
        return _server_builder_add_port(self, address, credentials.credentials)

    cpdef void add_native_servicer(ServerBuilder self, _NativeServicer servicer):
        self._native_servicers.push_back(servicer._servicer)
        servicer._servicer.get().Register(&self.x)

    cpdef void add_generic_rpc_handlers(ServerBuilder self, generic_rpc_handlers, max_workers=None, max_queue_size=0) except *:
        cdef list generic_handlers = []
        cdef dict dict_handler = {}
        cdef _DictionaryGenericHandler typed_dict_handler
        cdef PyRequestHandler additional_handler
        for generic_rpc_handler in generic_rpc_handlers:
            if isinstance(generic_rpc_handler, _DictionaryGenericHandler):
                typed_dict_handler = generic_rpc_handler
                for path, handler in typed_dict_handler.method_handlers.iteritems():
                    dict_handler[path] = handler
            else:
                if max_workers is not None:
                    raise AttributeError(
                        '"{}" must be created using grpc.method_handlers_generic_handler() '
                        'if max_workers is specified on per-service basis'.format(generic_rpc_handler))
                generic_handlers.append(generic_rpc_handler)
                service_attribute = getattr(generic_rpc_handler, 'service', None)
                if service_attribute is None:
                    raise AttributeError(
                        '"{}" must conform to grpc.GenericRpcHandler type but does '
                        'not have "service" method!'.format(generic_rpc_handler))
        if max_workers is not None:
            additional_handler = PyRequestHandler()
            additional_handler.concurrency_limit = max_workers + max_queue_size
            additional_handler.dict_handler = dict_handler
            additional_handler.submit = _ThreadPoolExecutor(max_workers=max_workers).submit
            self.additional_handlers.append(additional_handler)
        else:
            self.dict_handler.update(dict_handler)
        self.generic_handlers.extend(generic_handlers)

    cpdef void set_interceptors(ServerBuilder self, interceptors) except *:
        if interceptors is not None:
            self.interceptors[:] = list(interceptors)

    def set_max_send_message_length(self, int max_send_message_length):
        # type: (int) -> None
        self.x.SetMaxSendMessageSize(max_send_message_length)

    def set_max_receive_message_length(self, int max_receive_message_length):
        # type: (int) -> None
        self.x.SetMaxReceiveMessageSize(max_receive_message_length)

    def set_reuse_port(self, bint reuse_port):
        # type: (bool) -> None
        cdef int value = 1 if reuse_port else 0
        self.x.AddIntegerArgument("grpc.so_reuseport", value)

    cpdef void set_option(ServerBuilder self, name, value) except *:
        if isinstance(name, (str, unicode)):
            name = name.encode('utf8')
        if isinstance(value, int):
            self.x.AddIntegerArgument(name, value)
        elif isinstance(value, (bytes, str, unicode)):
            self.x.AddStringArgument(name, value)
        elif hasattr(value, '__int__'):
            altgrpc.AddPointerArgument(&self.x, name, <void*>(<intptr_t>int(value)))
        else:
          raise ValueError("Invalid value for options '{}'".format(name))

    cpdef void set_maximum_concurrent_rpcs(ServerBuilder self, maximum_concurrent_rpcs) except *:
        if maximum_concurrent_rpcs is not None:
            self.maximum_concurrent_rpcs = <size_t>maximum_concurrent_rpcs
        else:
            self.maximum_concurrent_rpcs = None

    cpdef void set_thread_pool(ServerBuilder self, thread_pool) except *:
        self.submit = thread_pool.submit

    cpdef void set_thread_count(ServerBuilder self, size_t thread_count) except *:
        self.thread_count = thread_count

    cpdef _Server build_and_start(ServerBuilder self):
        cdef _Server server = _Server()
        cdef default_service_pipeline = _service_pipeline(self.dict_handler, self.generic_handlers, self.interceptors)
        cdef altgrpc.RequestHandler default_handler
        if self.maximum_concurrent_rpcs is not None:
            default_handler = altgrpc.RequestHandler(self.submit, default_service_pipeline, self.maximum_concurrent_rpcs)
        else:
            default_handler = altgrpc.RequestHandler(self.submit, default_service_pipeline)
        cdef altgrpc.RequestHandlers handlers = altgrpc.RequestHandlers(default_handler)
        cdef service_pipeline
        cdef altgrpc.RequestHandler handler
        cdef PyRequestHandler additional_handler
        for additional_handler in self.additional_handlers:
            service_pipeline = _service_pipeline(additional_handler.dict_handler, [], self.interceptors)
            handler = altgrpc.RequestHandler(additional_handler.submit, service_pipeline, additional_handler.concurrency_limit)
            for method in additional_handler.dict_handler.keys():
                if isinstance(method, unicode):
                    handler.AddMethod(method.encode('utf-8'))
                else:
                    handler.AddMethod(method)
            handlers.AddHandler(handler)
        if not server.x.BuildAndStart(&self.x, handlers, self.thread_count):
            raise Exception('Unable to start server')
        for bound_port in self.bound_ports:
            _bound_port_load(bound_port)
        for native_servicer in self._native_servicers:
            native_servicer.get().Start()
        self.bound_ports = None
        self.dict_handler = None
        self.generic_handlers = None
        self.interceptors = None
        server.x.Loop()
        return server

def unary_unary_rpc_method_handler(behavior,
                                   request_deserializer=None,
                                   response_serializer=None):
    return _RpcMethodHandler(False, False, True, behavior, request_deserializer, response_serializer)

def unary_stream_rpc_method_handler(behavior,
                                    request_deserializer=None,
                                    response_serializer=None,
                                    iterators=True):
    return _RpcMethodHandler(False, True, iterators, behavior, request_deserializer, response_serializer)

def stream_unary_rpc_method_handler(behavior,
                                    request_deserializer=None,
                                    response_serializer=None,
                                    iterators=True):
    return _RpcMethodHandler(True, False, iterators, behavior, request_deserializer, response_serializer)

def stream_stream_rpc_method_handler(behavior,
                                     request_deserializer=None,
                                     response_serializer=None,
                                    iterators=True):
    return _RpcMethodHandler(True, True, iterators, behavior, request_deserializer, response_serializer)

def method_handlers_generic_handler(service, method_handlers):
    return _DictionaryGenericHandler(service, method_handlers)

def local_server_credentials(local_connect_type=LocalConnectionType.LOCAL_TCP):
    cdef _ServerCredentials credentials = _ServerCredentials.__new__(_ServerCredentials)
    credentials.credentials = grpc.LocalServerCredentials(local_connect_type.value)
    return credentials

def ssl_server_credentials(private_key_certificate_chain_pairs, root_certificates=None, bint require_client_auth=False):
    cdef grpc.SslServerCredentialsOptions opts
    cdef grpc.PemKeyCertPair key_pair
    cdef _ServerCredentials credentials = _ServerCredentials.__new__(_ServerCredentials)
    if not private_key_certificate_chain_pairs:
        raise ValueError(
            'At least one private key-certificate chain pair is required!')
    if require_client_auth and root_certificates is None:
        raise ValueError(
            'Illegal to require client auth without providing root certificates!')
    if root_certificates:
        opts.pem_root_certs = root_certificates
    for key, pem in private_key_certificate_chain_pairs:
        key_pair.private_key = key
        key_pair.cert_chain = pem
        opts.pem_key_cert_pairs.push_back(key_pair)
    opts.force_client_auth = require_client_auth
    credentials.credentials = grpc.SslServerCredentials(opts)
    return credentials


### Compatibility Interfaces ###
cdef class Server(object):
    pass
cdef class ServerCredentials(object):
    pass
cdef class ServerInterceptor(object):
    pass
cdef class HandlerCallDetails(object):
    pass
cdef class ServicerContext(object):
    pass
cdef class RpcMethodHandler(object):
    pass
cdef class GenericRpcHandler(object):
    pass
cdef class ServiceRpcHandler(GenericRpcHandler):
    pass

# TODO(mehrdad): implement for 100% compatibility with upstream
#   ServerCertificateConfiguration
#     ssl_server_certificate_configuration
#     dynamic_ssl_server_credentials
#   experimental:
#     compression (ensure it works)
#     SSLSessionCache
#############################################################################

@cython.internal
cdef class _Server(Server):
    cdef altgrpc.Server x
    def stop(_Server self, grace):
        if grace is None:
            return self.x.Stop()
        else:
            return self.x.Stop(<double>grace)

@cython.internal
cdef class _NativeServicer:
    cdef shared_ptr[altgrpc.Servicer] _servicer

cdef _BoundPort _server_builder_add_port(ServerBuilder self, bytes address, shared_ptr[grpc.ServerCredentials] creds):
    cdef _BoundPort port = _BoundPort()
    self.bound_ports.append(port)
    self.x.AddListeningPort(address, creds, port.x.get())
    return port

@cython.internal
cdef class _BoundPort:
    cdef shared_ptr[int] x
    cdef object loaded_port
    def __cinit__(_BoundPort self):
        self.x = make_shared[int]()
        self.loaded_port = None
    def __call__(_BoundPort self):
        return self.loaded_port

cdef void _bound_port_load(_BoundPort port) except *:
    port.loaded_port = port.x.get()[0]
    port.x.reset()

cdef _service_call(service_pipeline):
    def f(_ServicerContext call):
        cdef object method_handler
        if call.x.Method():
            try:
                method_handler = service_pipeline(
                    _HandlerCallDetails(call.x.Method(), call.x.InvocationMetadata()))
                if method_handler is not None:
                    _wire_method_handler(method_handler).get().Handle(call, &call.x)
                else:
                    call.x.AbortUnrecognizedMethod()
                return
            except Exception as exception:  # pylint: disable=broad-except
                details = 'Exception servicing handler: {}'.format(exception)
                _LOGGER.exception(details)
        call.x.AbortHandlerLookupFailed()
    return f

cdef object _service_pipeline(dict_handler, handlers, interceptors):
    cdef pipeline = _GenericHandlerLookup(dict_handler, handlers)
    if interceptors:
        for interceptor in reversed(interceptors):
            pipeline = _ServerInterceptorContinuation(interceptor, pipeline)
    return _service_call(pipeline)

@cython.internal
cdef class _HandlerCallDetails:
    cdef readonly method
    cdef readonly tuple invocation_metadata
    def __cinit__(_HandlerCallDetails self, method, tuple invocation_metadata):
        self.method = method
        self.invocation_metadata = invocation_metadata

@cython.internal
cdef class _GenericHandlerLookup:
    cdef list generic_handlers
    cdef dict dict_handler
    def __cinit__(_GenericHandlerLookup self, dict dict_handler, handlers):
        self.dict_handler = dict_handler
        self.generic_handlers = handlers if handlers else None
    def __call__(_GenericHandlerLookup self, handler_call_details):
        cdef method_handler = self.dict_handler.get(handler_call_details.method)
        if method_handler is None and self.generic_handlers is not None:
            for handler in self.generic_handlers:
                method_handler = handler.service(handler_call_details)
                if method_handler is not None:
                    return method_handler
        return method_handler

@cython.internal
cdef class _ServerInterceptorContinuation:
    cdef interceptor
    cdef thunk
    def __cinit__(_ServerInterceptorContinuation self, interceptor, thunk):
        self.interceptor = interceptor
        self.thunk = thunk
    def __call__(_ServerInterceptorContinuation self, context):
        return self.interceptor.intercept_service(self.thunk, context)

@cython.internal
cdef class _ServicerContext(ServicerContext):
  cdef altgrpc.ServicerContext x
  def is_active(_ServicerContext self):
    return self.x.IsActive()
  def time_remaining(_ServicerContext self):
    return self.x.TimeRemaining()
  def cancel(_ServicerContext self):
    self.x.Cancel()
  def invocation_metadata(_ServicerContext self):
    return self.x.InvocationMetadata()
  def peer(_ServicerContext self):
    return self.x.Peer()
  def peer_identities(_ServicerContext self):
    return self.x.PeerIdentities()
  def peer_identity_key(_ServicerContext self):
    return self.x.PeerIdentityKey()
  def auth_context(_ServicerContext self):
    return self.x.AuthContext()
  def abort(_ServicerContext self, code, details):
    raise _new_abortion_exception(code, details)
  def abort_with_status(_ServicerContext self, status):
    cdef AbortionException exception = _new_abortion_exception(status.code, status.details)
    exception.trailing_metadata = status.trailing_metadata
    raise exception
  def set_code(_ServicerContext self, code):
    self.x.SetCode(_grpc_status_code_int(code))
  def set_details(_ServicerContext self, details):
    self.x.SetDetails(details if isinstance(details, bytes) else details.encode('utf8'))
  def set_compression(_ServicerContext self, compression):
    self.x.SetCompression(compression.value[0])
  def disable_next_message_compression(_ServicerContext self):
    self.x.DisableNextMessageCompression()
  def send_initial_metadata(_ServicerContext self, initial_metadata):
    for key, value in initial_metadata:
        self.x.AddInitialMetadata(key if isinstance(key, bytes) else key.encode('ascii'), <bytes>value if isinstance(value, bytes) else value.encode('utf8'))
    self.x.SendInitialMetadataBlocking()
  cpdef set_trailing_metadata(_ServicerContext self, trailing_metadata):
    # NOTE(compatibility): set_trailing_metadata appends, as opposed to overwriting previous ones
    for key, value in trailing_metadata:
        self.x.AddTrailingMetadata(key if isinstance(key, bytes) else key.encode('ascii'), <bytes>value if isinstance(value, bytes) else value.encode('utf8'))
  cpdef read(_ServicerContext self):
    return self.x.Read()
  cpdef write(_ServicerContext self, message):
    self.x.Write(message)

cdef public object Cython_NewServicerContext(altgrpc.ServicerContext** x) with gil:
  cdef _ServicerContext ctx = _ServicerContext()
  x[0] = &ctx.x
  return ctx

cdef grpc.grpc_status_code _grpc_status_code_int(code):
    cdef grpc.grpc_status_code native_code
    if isinstance(code, StatusCode):
        try:
            native_code = code.value[0]
            if _lookup_status_from_code(<grpc.StatusCode>native_code) is not StatusCode.UNKNOWN:
                return native_code
        except:
            pass
    return grpc.GRPC_STATUS_UNKNOWN

cdef logged_exception(func):
    if func is None:
        return None
    def f(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as exception:
            _LOGGER.exception('Exception: {}'.format(exception))
            raise
    return f

cdef _log_exception_in_serializer(func):
    if func is None:
        return None
    def f(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as exception:
            _LOGGER.exception('Exception serializing response: {}, {}'.format(exception, *args))
            raise
    return f

cdef public class AbortionException(RpcError)[object AbortionExceptionStruct, type AbortionExceptionType]:
  cdef grpc.grpc_status_code code
  cdef std_string details
  cdef object trailing_metadata

cdef AbortionException _new_abortion_exception(code, details):
    cdef AbortionException exception = AbortionException()
    exception.code = _grpc_status_code_int(code)
    if exception.code is grpc.GRPC_STATUS_OK:
        exception.code = grpc.GRPC_STATUS_UNKNOWN
    else:
        exception.details = <bytes>details if isinstance(details, bytes) else details.encode('utf8')
    return exception

cdef public void _abort_with_abortion_exception(context_obj, exception_obj):
    if not isinstance(exception_obj, AbortionException):
        return
    cdef AbortionException exception = <AbortionException>exception_obj
    cdef _ServicerContext context = <_ServicerContext>context_obj
    if exception.code is grpc.GRPC_STATUS_OK:
        exception.code = grpc.GRPC_STATUS_UNKNOWN
    context.x.SetCode(exception.code)
    context.x.SetDetails(exception.details)
    try:
        if exception.trailing_metadata is not None:
            context.set_trailing_metadata(exception.trailing_metadata)
    except:
        _LOGGER.exception('set_trailing_metadata failed: {}'.format(exception.trailing_metadata))

cdef _log_exception_in_handler(func):
    def f(request_iterator, _ServicerContext context):
        try:
            return func(request_iterator, context)
        except AbortionException as abortion_exception:
            _abort_with_abortion_exception(context, abortion_exception)
            return None
        except RpcError as rpc_error:
            context.x.SetCode(grpc.GRPC_STATUS_UNKNOWN)
            context.x.SetDetails('Error in service handler')
            return None
        except Exception as exception:
            _LOGGER.exception('Exception servicing handler: {}'.format(exception))
            raise
    return f

cdef shared_ptr[altgrpc.MethodHandler] _wire_method_handler(handler):
    if handler.request_streaming:
        if handler.response_streaming:
            handler_method = handler.stream_stream
        else:
            handler_method = handler.stream_unary
    else:
        if handler.response_streaming:
            handler_method = handler.unary_stream
        else:
            handler_method = handler.unary_unary
    use_iterators = getattr(handler, "use_iterators", True)

    return altgrpc.MethodHandler.Create(handler.request_streaming, handler.response_streaming,
        use_iterators, _log_exception_in_handler(handler_method),
        altgrpc.GenericDeserializer(logged_exception(handler.request_deserializer)),
        altgrpc.GenericSerializer(_log_exception_in_serializer(handler.response_serializer)))

@cython.internal
cdef class _RpcMethodHandler(RpcMethodHandler):
    cdef readonly bint request_streaming
    cdef readonly bint response_streaming
    cdef readonly bint iterators
    cdef readonly object request_deserializer
    cdef readonly object response_serializer
    cdef object handler
    def __cinit__(_RpcMethodHandler self, bint request_streaming, bint response_streaming, bint iterators, handler, request_deserializer, response_serializer):
        self.request_streaming = request_streaming
        self.response_streaming = response_streaming
        self.iterators = iterators
        self.handler = handler
        self.request_deserializer = request_deserializer
        self.response_serializer = response_serializer
    @property
    def unary_unary(_RpcMethodHandler self):
        return None if self.request_streaming or self.response_streaming else self.handler
    @property
    def unary_stream(_RpcMethodHandler self):
        return None if self.request_streaming or not self.response_streaming else self.handler
    @property
    def stream_unary(_RpcMethodHandler self):
        return None if not self.request_streaming or self.response_streaming else self.handler
    @property
    def stream_stream(_RpcMethodHandler self):
        return self.handler if self.request_streaming and self.response_streaming else None
    @property
    def use_iterators(_RpcMethodHandler self):
        return self.iterators

cdef str _canonicalize_route_path(service_name, method_name):
    if not isinstance(service_name, str):
        service_name = service_name.decode('utf8')
    if not isinstance(method_name, str):
        method_name = method_name.decode('utf8')
    return '/{}/{}'.format(service_name, method_name)

@cython.internal
cdef class _DictionaryGenericHandler(ServiceRpcHandler):
    cdef readonly object service_name
    cdef dict method_handlers
    def __cinit__(_DictionaryGenericHandler self, service, method_handlers):
        self.service_name = service
        self.method_handlers = {
            _canonicalize_route_path(service, method): method_handler
            for method, method_handler in method_handlers.iteritems()
        }
    def service(_DictionaryGenericHandler self, handler_call_details):
        return self.method_handlers.get(handler_call_details.method)

@cython.internal
cdef class _ServerCredentials(ServerCredentials):
    cdef shared_ptr[grpc.ServerCredentials] credentials
    def __init__(self):
        raise Exception('Cannot create instances of _ServerCredentials')

