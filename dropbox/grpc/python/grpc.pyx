# distutils: language=c++

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


from libc.stdint cimport intptr_t, uint16_t, uint32_t
from libcpp.memory cimport shared_ptr, unique_ptr, make_shared, make_unique
from libcpp.vector cimport vector
from libcpp.string cimport string as std_string
cimport libcpp

cimport cpython
from cpython cimport PyObject
from cpython.version cimport PY_MAJOR_VERSION
cimport cython

cimport grpcpp as grpc
cimport altgrpc

import atexit as _atexit
import logging as _logging
import sys as _sys
import threading as _threading

__version__ = b"altgrpc_" + bytes(grpc.Version())

_LOGGER = _logging.getLogger(__name__)
_LOGGER.addHandler(_logging.NullHandler())

altgrpc.Initialize()
_atexit.register(lambda:altgrpc.Exit())

include "common.pyx.pxi"
include "client.pyx.pxi"
include "server.pyx.pxi"

include "plugins/plugins.pyx.pxi"
