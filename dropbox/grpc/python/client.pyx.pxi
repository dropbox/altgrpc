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

###############################################################################
## PUBLIC API:

def insecure_channel(target, options=None, compression=None):
  cdef _Channel channel = _Channel.__new__(_Channel)
  _initialize_channel(channel, _utf8_bytes(target), grpc.InsecureChannelCredentials(), options, compression)
  return channel

def secure_channel(target, _ChannelCredentials credentials, options=None, compression=None):
  cdef _Channel channel = _Channel.__new__(_Channel)
  _initialize_channel(channel, _utf8_bytes(target), credentials.credentials, options, compression)
  return channel

def local_channel_credentials(local_connect_type=LocalConnectionType.LOCAL_TCP):
  cdef _ChannelCredentials credentials = _ChannelCredentials.__new__(_ChannelCredentials)
  credentials.credentials = grpc.LocalChannelCredentials(local_connect_type.value)
  return credentials

def ssl_channel_credentials(root_certificates=None, private_key=None, certificate_chain=None):
  cdef grpc.SslChannelCredentialsOptions opts
  cdef _ChannelCredentials credentials = _ChannelCredentials.__new__(_ChannelCredentials)
  if root_certificates:
      opts.pem_root_certs = root_certificates
  if private_key:
      opts.pem_private_key = private_key
  if certificate_chain:
      opts.pem_cert_chain = certificate_chain
  credentials.credentials = grpc.SslChannelCredentials(opts)
  return credentials

def access_token_call_credentials(access_token):
  cdef _CallCredentials credentials = _CallCredentials.__new__(_CallCredentials)
  credentials.credentials = grpc.AccessTokenCallCredentials(_utf8_bytes(access_token))
  return credentials

def metadata_call_credentials(metadata_plugin, name=None):
  cdef _CallCredentials credentials = _CallCredentials.__new__(_CallCredentials)
  # TODO(mehrdad): pass name correctly
  credentials.credentials = altgrpc.MetadataCallCredentials(metadata_plugin, '')
  return credentials

def composite_channel_credentials(channel_credentials, *call_credentials):
  cdef size_t i = len(call_credentials)
  if i == 0:
    return channel_credentials
  i-=1
  cdef shared_ptr[grpc.CallCredentials] credentials = (<_CallCredentials>call_credentials[i]).credentials
  while i > 0:
    i-=1
    credentials = grpc.CompositeCallCredentials((<_CallCredentials>call_credentials[i]).credentials, credentials)
  cdef _ChannelCredentials composite_credentials = _ChannelCredentials.__new__(_ChannelCredentials)
  composite_credentials.credentials = grpc.CompositeChannelCredentials((<_ChannelCredentials>channel_credentials).credentials, credentials)
  return composite_credentials

def composite_call_credentials(call_credentials0, *call_credentials):
  cdef size_t i = len(call_credentials)
  if i == 0:
    return call_credentials0
  cdef _CallCredentials credentials = _CallCredentials.__new__(_CallCredentials)
  i-=1
  credentials.credentials = (<_CallCredentials>call_credentials[i]).credentials
  while i > 0:
    i-=1
    credentials.credentials = grpc.CompositeCallCredentials((<_CallCredentials>call_credentials[i]).credentials, credentials.credentials)
  credentials.credentials = grpc.CompositeCallCredentials((<_CallCredentials>call_credentials0).credentials, credentials.credentials)
  return credentials

def channel_ready_future(channel):
  return _ChannelReadyFuture(channel)

### Compatibility Interfaces ###
class Call(RpcContext):
  __slots__ = []
cdef class Channel:
  def __init__(self):
    if type(self) == Channel:
      raise TypeError('Cannot initialize Channel interface directly, use secure_channel and insecure_channel instead')
cdef class ChannelCredentials:
  pass
cdef class CallCredentials:
  pass
cdef class UnaryUnaryMultiCallable:
  pass
cdef class UnaryStreamMultiCallable:
  pass
cdef class StreamUnaryMultiCallable:
  pass
cdef class StreamStreamMultiCallable:
  pass
cdef class ClientCallDetails(object):
  pass
cdef class AuthMetadataPlugin(object):
  pass
cdef class AuthMetadataContext(object):
  pass
cdef class AuthMetadataPluginCallback(object):
  pass

include "client_interceptor.pyx.pxi"

# TODO(mehrdad): implement for 100% compatibility with upstream
# experimental:
#  compression (ensure it works)
#  SSLSessionCache
#############################################################################

@cython.internal
cdef class _ChannelCredentials(ChannelCredentials):
  cdef shared_ptr[grpc.ChannelCredentials] credentials

@cython.internal
cdef class _CallCredentials(CallCredentials):
  cdef shared_ptr[grpc.CallCredentials] credentials

@cython.internal
cdef class _PointerArgument:
  cdef void* pointer

@cython.internal
cdef class _Channel(Channel):
  cdef altgrpc.Channel x
  def unary_unary(_Channel self, method, request_serializer=None, response_deserializer=None):
    return self.x.UnaryUnary(method, request_serializer, response_deserializer)
  def async_unary_unary(_Channel self, method, request_serializer=None, response_deserializer=None):
    return self.x.AsyncUnaryUnary(method, request_serializer, response_deserializer)
  def unary_stream(_Channel self, method, request_serializer=None, response_deserializer=None):
    return self.x.UnaryStream(method, request_serializer, response_deserializer)
  def async_unary_stream(_Channel self, method, request_serializer=None, response_deserializer=None):
    return self.x.AsyncUnaryStream(method, request_serializer, response_deserializer)
  def stream_unary(_Channel self, method, request_serializer=None, response_deserializer=None):
    return self.x.StreamUnary(method, request_serializer, response_deserializer)
  def async_stream_unary(_Channel self, method, request_serializer=None, response_deserializer=None):
    return self.x.AsyncStreamUnary(method, request_serializer, response_deserializer)
  def stream_stream(_Channel self, method, request_serializer=None, response_deserializer=None):
    return self.x.StreamStream(method, request_serializer, response_deserializer)
  def async_stream_stream(_Channel self, method, request_serializer=None, response_deserializer=None):
    return self.x.AsyncStreamStream(method, request_serializer, response_deserializer)
  def wait_for_connected(_Channel self, timeout=None):
    return self.x.WaitForConnected(timeout)
  def state(_Channel self, try_to_connect=False):
    return self.x.GetState(try_to_connect)
  def subscribe(_Channel self, callback, try_to_connect=False):
    self.x.Subscribe(callback, try_to_connect)
  def unsubscribe(_Channel self, callback):
    self.x.Unsubscribe(callback)
  def close(_Channel self):
    self.x.Close()
  def __enter__(_Channel self):
    return self
  def __exit__(self, exc_type, exc_val, exc_tb):
    self.x.Close()


cdef void _initialize_channel(_Channel self, bytes target, shared_ptr[grpc.ChannelCredentials] credentials, options, compression) except *:
  cdef grpc.ChannelArguments args
  if options:
    for name, value in options:
      if not isinstance(name, bytes):
        name = name.encode('utf8')
      if isinstance(value, int):
        args.SetInt(name, value)
      elif isinstance(value, bytes):
        args.SetString(name, <bytes>value)
      elif isinstance(value, (str, unicode,)):
        args.SetString(name, value.encode('utf8'))
      elif isinstance(value, _PointerArgument):
        args.SetPointer(name, (<_PointerArgument>value).pointer)
      elif hasattr(value, '__int__'):
        args.SetPointer(name, <void*>(<intptr_t>int(value)))
      else:
        _LOGGER.exception(ValueError("Invalid value for options '{}'".format(name))) # raise
  if compression is not None:
    args.SetCompressionAlgorithm(compression.value)
  self.x.Initialize(target, &args, credentials)


cdef void _populate_client_context(grpc.ClientContext* context, timeout, metadata, _CallCredentials credentials, wait_for_ready, compression) except *:
  altgrpc.SetClientContextTimeout(context, timeout)
  altgrpc.SetClientContextMetadata(context, metadata)
  if credentials is not None:
    context.set_credentials(credentials.credentials)
  if wait_for_ready is not None:
    context.set_wait_for_ready(wait_for_ready)
  if compression is not None:
    context.set_compression_algorithm(compression.value)


# NOTE: should really be internal, but we have code that explicitly checks for _Rendezvous
# so we leave it a publicly visible symbol for now.
# @cython.internal
cdef class _Rendezvous(RpcError, Call):
  cdef altgrpc.Rendezvous x
  def is_active(_Rendezvous self):
    return self.x.IsActive()
  def running(_Rendezvous self):
    return self.x.IsRunning()
  def done(_Rendezvous self):
    return self.x.IsDone()
  def cancelled(_Rendezvous self):
    return self.x.IsCancelled()
  def time_remaining(_Rendezvous self):
    return self.x.TimeRemaining()
  def cancel(_Rendezvous self):
    return self.x.Cancel()
  def initial_metadata(_Rendezvous self):
    return self.x.InitialMetadata()
  def trailing_metadata(_Rendezvous self):
    return self.x.TrailingMetadata()
  def code(_Rendezvous self):
    return self.x.Code()
  def details(_Rendezvous self):
    return self.x.Details()
  def debug_error_string(_Rendezvous self):
    return self.x.DebugErrorString()
  def add_callback(_Rendezvous self, callback):
    def exception_safe_callback():
      try:
        callback()
      except Exception as exception:
        _LOGGER.error("Error executing callback: {}".format(exception))
    return self.x.AddCallback(exception_safe_callback)
  # NOTE: this is part of the Future interface, so should not
  # be necessary to lift it generally to conform to the streaming
  # API, but some users may have existing code that is
  # targeted at _Rendezvous and abuses the API.
  def add_done_callback(_Rendezvous self, fn):
    def exception_safe_callback():
      try:
        fn(self)
      except Exception as exception:
        _LOGGER.error("Error executing callback: {}".format(exception))
    if not self.x.AddCallback(exception_safe_callback):
      fn(self)
  def __str__(_Rendezvous self):
    return self.x.StringRepresentation()
  def __repr__(_Rendezvous self):
    return self.x.StringRepresentation()


@cython.internal
cdef class _StreamRendezvous(_Rendezvous):
  def __iter__(_StreamRendezvous self):
    return self
  def __next__(_StreamRendezvous self):
    return self.x.Next(self)
  # NOTE: the following should not strictly be necessary, but there are
  # users relying on these undocumented APIs that we rather not break.
  def next(_StreamRendezvous self):
    return self.x.Next(self)
  def _next(_StreamRendezvous self):
    return self.x.Next(self)


@cython.internal
cdef class _UnaryRendezvous(_Rendezvous, Future):
  cpdef result(_UnaryRendezvous self, timeout=None):
    return self.x.Result(self, timeout)
  def exception(_UnaryRendezvous self, timeout=None):
    return self.x.Exception(self, timeout)
  def traceback(_UnaryRendezvous self, timeout=None):
    return self.x.Traceback(self, timeout)


@cython.internal
cdef class _UnaryUnaryMultiCallable(UnaryUnaryMultiCallable):
  cdef altgrpc.RpcInvoker x

  def __call__(_UnaryUnaryMultiCallable self, request, timeout=None, metadata=None, credentials=None, wait_for_ready=None, compression=None):
    cdef _UnaryRendezvous rendezvous = _UnaryRendezvous.__new__(_UnaryRendezvous)
    _populate_client_context(rendezvous.x.context(), timeout, metadata, credentials, wait_for_ready, compression)
    self.x.InvokeUnaryUnary(rendezvous, &rendezvous.x, request)
    return rendezvous.result()

  def with_call(_UnaryUnaryMultiCallable self, request, timeout=None, metadata=None, credentials=None, wait_for_ready=None, compression=None):
    cdef _UnaryRendezvous rendezvous = _UnaryRendezvous.__new__(_UnaryRendezvous)
    _populate_client_context(rendezvous.x.context(), timeout, metadata, credentials, wait_for_ready, compression)
    self.x.InvokeUnaryUnary(rendezvous, &rendezvous.x, request)
    return rendezvous.result(), rendezvous

  def future(_UnaryUnaryMultiCallable self, request, timeout=None, metadata=None, credentials=None, wait_for_ready=None, compression=None):
    cdef _UnaryRendezvous rendezvous = _UnaryRendezvous.__new__(_UnaryRendezvous)
    _populate_client_context(rendezvous.x.context(), timeout, metadata, credentials, wait_for_ready, compression)
    self.x.InvokeUnaryUnary(rendezvous, &rendezvous.x, request)
    return rendezvous


@cython.internal
cdef class _StreamUnaryMultiCallable(StreamUnaryMultiCallable):
  cdef altgrpc.RpcInvoker x

  def __call__(_StreamUnaryMultiCallable self, request_iterator, timeout=None, metadata=None, credentials=None, wait_for_ready=None, compression=None):
    cdef _UnaryRendezvous rendezvous = _UnaryRendezvous.__new__(_UnaryRendezvous)
    _populate_client_context(rendezvous.x.context(), timeout, metadata, credentials, wait_for_ready, compression)
    self.x.InvokeStreamUnary(rendezvous, &rendezvous.x, request_iterator)
    return rendezvous.result()

  def with_call(_StreamUnaryMultiCallable self, request_iterator, timeout=None, metadata=None, credentials=None, wait_for_ready=None, compression=None):
    cdef _UnaryRendezvous rendezvous = _UnaryRendezvous.__new__(_UnaryRendezvous)
    _populate_client_context(rendezvous.x.context(), timeout, metadata, credentials, wait_for_ready, compression)
    self.x.InvokeStreamUnary(rendezvous, &rendezvous.x, request_iterator)
    return rendezvous.result(), rendezvous

  def future(_StreamUnaryMultiCallable self, request_iterator, timeout=None, metadata=None, credentials=None, wait_for_ready=None, compression=None):
    cdef _UnaryRendezvous rendezvous = _UnaryRendezvous.__new__(_UnaryRendezvous)
    _populate_client_context(rendezvous.x.context(), timeout, metadata, credentials, wait_for_ready, compression)
    self.x.InvokeStreamUnary(rendezvous, &rendezvous.x, request_iterator)
    return rendezvous


@cython.internal
cdef class _UnaryStreamMultiCallable(UnaryStreamMultiCallable):
  cdef altgrpc.RpcInvoker x

  def __call__(_UnaryStreamMultiCallable self, request, timeout=None, metadata=None, credentials=None, wait_for_ready=None, compression=None):
    cdef _StreamRendezvous rendezvous = _StreamRendezvous.__new__(_StreamRendezvous)
    _populate_client_context(rendezvous.x.context(), timeout, metadata, credentials, wait_for_ready, compression)
    self.x.InvokeUnaryStream(rendezvous, &rendezvous.x, request)
    return rendezvous

@cython.internal
cdef class _StreamStreamMultiCallable(StreamStreamMultiCallable):
  cdef altgrpc.RpcInvoker x

  def __call__(_StreamStreamMultiCallable self, request_iterator, timeout=None, metadata=None, credentials=None, wait_for_ready=None, compression=None):
    cdef _StreamRendezvous rendezvous = _StreamRendezvous.__new__(_StreamRendezvous)
    _populate_client_context(rendezvous.x.context(), timeout, metadata, credentials, wait_for_ready, compression)
    self.x.InvokeStreamStream(rendezvous, &rendezvous.x, request_iterator)
    return rendezvous

@cython.internal
cdef class _ChannelReadyFuture(_NoBaseClass, Future):
  cdef object lock, ready, channel
  cdef bint is_cancelled
  cdef list callbacks
  def __init__(self, channel):
    self.lock = _threading.Lock()
    self.ready = _threading.Event()
    self.channel = channel
    self.is_cancelled = False
    self.callbacks = []
    channel.subscribe(self._watch, try_to_connect=True)
  def _unsubscribe(self):
    self.channel.unsubscribe(self._watch)
    self.channel = None
  def _watch(self, state):
    if state == ChannelConnectivity.READY:
      self.ready.set()
      self._unsubscribe()
      self._execute_callbacks()
  def cancel(self):
    if not self.ready.is_set():
      self.is_cancelled = True
      self.ready.set()
      self._unsubscribe()
      self._execute_callbacks()
    return self.is_cancelled
  def cancelled(self):
    return self.is_cancelled
  def running(self):
    return not self.ready.is_set()
  def done(self):
    return self.ready.is_set()
  def result(self, timeout=None):
    self.ready.wait(timeout=timeout)
    if self.is_cancelled:
      raise FutureCancelledError()
    if not self.ready.is_set():
      raise FutureTimeoutError()
  def exception(self, timeout=None):
    self.result(timeout=timeout)
  def traceback(self, timeout=None):
    self.result(timeout=timeout)
  def add_done_callback(self, fn):
    with self.lock:
      if self.callbacks is not None:
        self.callbacks.append(fn)
        return
    fn(self)
  def _execute_callbacks(self):
    cdef list callbacks
    with self.lock:
      callbacks = self.callbacks
      self.callbacks = None
    for callback in callbacks:
      try:
        callback(self)
      except Exception as exception:
        _LOGGER.error("Error calling channel connectivity future callback: {}".format(exception))

# Internal helpers:
cdef public object Cython_NewUnaryUnaryMultiCallable(altgrpc.RpcInvoker** invoker):
  cdef _UnaryUnaryMultiCallable x = _UnaryUnaryMultiCallable.__new__(_UnaryUnaryMultiCallable)
  invoker[0] = &x.x
  return x

cdef public object Cython_NewUnaryStreamMultiCallable(altgrpc.RpcInvoker** invoker):
  cdef _UnaryStreamMultiCallable x = _UnaryStreamMultiCallable.__new__(_UnaryStreamMultiCallable)
  invoker[0] = &x.x
  return x

cdef public object Cython_NewStreamUnaryMultiCallable(altgrpc.RpcInvoker** invoker):
  cdef _StreamUnaryMultiCallable x = _StreamUnaryMultiCallable.__new__(_StreamUnaryMultiCallable)
  invoker[0] = &x.x
  return x

cdef public object Cython_NewStreamStreamMultiCallable(altgrpc.RpcInvoker** invoker):
  cdef _StreamStreamMultiCallable x = _StreamStreamMultiCallable.__new__(_StreamStreamMultiCallable)
  invoker[0] = &x.x
  return x

