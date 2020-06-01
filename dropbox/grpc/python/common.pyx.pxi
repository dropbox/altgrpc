# Copyright (c) 2015 The gRPC Authors
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

##### Enums #####
import enum as _enum

@_enum.unique
class StatusCode(_enum.Enum):
    OK = (grpc.GRPC_STATUS_OK, 'ok')
    CANCELLED = (grpc.GRPC_STATUS_CANCELLED, 'cancelled')
    UNKNOWN = (grpc.GRPC_STATUS_UNKNOWN, 'unknown')
    INVALID_ARGUMENT = (grpc.GRPC_STATUS_INVALID_ARGUMENT, 'invalid argument')
    DEADLINE_EXCEEDED = (grpc.GRPC_STATUS_DEADLINE_EXCEEDED,
                         'deadline exceeded')
    NOT_FOUND = (grpc.GRPC_STATUS_NOT_FOUND, 'not found')
    ALREADY_EXISTS = (grpc.GRPC_STATUS_ALREADY_EXISTS, 'already exists')
    PERMISSION_DENIED = (grpc.GRPC_STATUS_PERMISSION_DENIED,
                         'permission denied')
    RESOURCE_EXHAUSTED = (grpc.GRPC_STATUS_RESOURCE_EXHAUSTED,
                          'resource exhausted')
    FAILED_PRECONDITION = (grpc.GRPC_STATUS_FAILED_PRECONDITION,
                           'failed precondition')
    ABORTED = (grpc.GRPC_STATUS_ABORTED, 'aborted')
    OUT_OF_RANGE = (grpc.GRPC_STATUS_OUT_OF_RANGE, 'out of range')
    UNIMPLEMENTED = (grpc.GRPC_STATUS_UNIMPLEMENTED, 'unimplemented')
    INTERNAL = (grpc.GRPC_STATUS_INTERNAL, 'internal')
    UNAVAILABLE = (grpc.GRPC_STATUS_UNAVAILABLE, 'unavailable')
    DATA_LOSS = (grpc.GRPC_STATUS_DATA_LOSS, 'data loss')
    UNAUTHENTICATED = (grpc.GRPC_STATUS_UNAUTHENTICATED, 'unauthenticated')

@_enum.unique
class Compression(_enum.IntEnum):
    NoCompression = grpc.GRPC_COMPRESS_NONE
    Deflate = grpc.GRPC_COMPRESS_DEFLATE
    Gzip = grpc.GRPC_COMPRESS_GZIP

@_enum.unique
class ChannelConnectivity(_enum.Enum):
    IDLE = (grpc.GRPC_CHANNEL_IDLE, 'idle')
    CONNECTING = (grpc.GRPC_CHANNEL_CONNECTING, 'connecting')
    READY = (grpc.GRPC_CHANNEL_READY, 'ready')
    TRANSIENT_FAILURE = (grpc.GRPC_CHANNEL_TRANSIENT_FAILURE, 'transient failure')
    SHUTDOWN = (grpc.GRPC_CHANNEL_SHUTDOWN, 'shutdown')

altgrpc.InitializeConnectivityObjects(ChannelConnectivity.IDLE, ChannelConnectivity.CONNECTING, ChannelConnectivity.READY, ChannelConnectivity.TRANSIENT_FAILURE, ChannelConnectivity.SHUTDOWN)

@_enum.unique
class LocalConnectionType(_enum.Enum):
    UDS = grpc.UDS
    LOCAL_TCP = grpc.LOCAL_TCP

##### Compatibility type definitions #####
cdef public void Cython_RaiseFutureTimeoutError() except *:
    raise FutureTimeoutError()
cdef public void Cython_RaiseFutureCancelledError() except *:
    raise FutureCancelledError()
cdef public void Cython_RaiseStopIteration() except *:
    raise StopIteration()
cdef public void Cython_RaiseRpcError() except *:
    raise RpcError()
cdef public void Cython_RaiseSystemExit() except *:
    _sys.exit()
cdef public void Cython_RaiseChannelClosed() except *:
    raise ValueError('Channel closed')
cdef public void Cython_Raise(exception) except *:
    raise exception
cdef public Cython_CaptureTraceback(exception):
    try:
        raise exception
    except:
        return _sys.exc_info()[2]
@cython.internal
cdef class _NoBaseClass:
  pass
class RpcContext(object):
  __slots__ = []
class Status(object):
  __slots__ = []
class Future(object):
  __slots__ = []
cdef class FutureTimeoutError(Exception):
    pass
cdef class FutureCancelledError(Exception):
    pass
cdef public class RpcError(Exception)[object RpcErrorStruct, type RpcErrorType]:
    pass

cdef public void Cython_SubmitToThreadPool(thread_pool_submit, altgrpc.DispatchCallback callback):
    thread_pool_submit(lambda: callback())

cdef public object Cython_NewThreadingEvent():
    return _threading.Event()

cdef public void Cython_SetThreadingEvent(event):
    event.set()


cdef tuple _initialize_status_lookup_table():
    cdef dict map = { x.value[0]: x for x in StatusCode }
    cdef list lookup_table = [StatusCode.UNKNOWN] * (max([x.value[0] for x in StatusCode]) + 1)
    for i in xrange(len(lookup_table)):
        if i in map:
            lookup_table[i] = map[i]
    return tuple(lookup_table)

cdef tuple _STATUS_CODE_LOOKUP = _initialize_status_lookup_table()

cdef public object _lookup_status_from_code(grpc.StatusCode code):
    cdef uint32_t status_code = code
    return _STATUS_CODE_LOOKUP[status_code] if status_code < len(_STATUS_CODE_LOOKUP) else StatusCode.UNKNOWN

cdef public _utf8_bytes(str_or_bytes):
    return str_or_bytes if isinstance(str_or_bytes, bytes) else str_or_bytes.encode('utf8')


