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
#endif

#include <map>
#include <utility>

#include "absl/synchronization/notification.h"
#include "dropbox/grpc/python/auth_metadata_plugin.h"
#include "dropbox/types/expected.h"

namespace dropbox {
namespace grpc {
namespace python {

namespace {

class Plugin : public ::grpc::MetadataCredentialsPlugin {
 public:
  Plugin(ObjectHandle plugin, std::string type) : plugin_(std::move(plugin)), type_(std::move(type)) {}
  const char* GetType() const override { return type_.c_str(); }
  ::grpc::Status GetMetadata(::grpc::string_ref service_url, ::grpc::string_ref method_name,
                             const ::grpc::AuthContext& channel_auth_context,
                             std::multimap<std::string, std::string>* metadata) override;

 private:
  const ObjectHandle plugin_;
  const std::string type_;
};

struct CallbackResult {
  std::multimap<std::string, std::string>* metadata;
  absl::Notification ready;
  absl::optional<std::string> error;
};

typedef struct {
  PyObject_HEAD std::atomic<CallbackResult*> result;
} AuthMetadataPluginCallback;

PyObject* Call_AuthMetadataPluginCallback(AuthMetadataPluginCallback* self, PyObject* args, PyObject* kwargs) {
  PyObject* metadata;
  PyObject* exception;
  if (!PyArg_ParseTuple(args, "oo", &metadata, &exception)) {
    return nullptr;
  }
  auto result = self->result.exchange(nullptr, std::memory_order_relaxed);
  AutoReleasedObject metadata_deref{metadata};
  AutoReleasedObject exception_deref{exception};
  if (!result) {
    PyErr_SetString(PyExc_RuntimeError, "AuthMetadataPlugin raised or callback invoked more than once.");
    return nullptr;
  }
  if (exception == Py_None) {
    if (!types::Metadata{metadata}.Apply(
            [metadata = result->metadata](const auto& metadatum) { metadata->emplace(metadatum); })) {
      result->error = "Exception adding metadata";
      return nullptr;
    }
  } else {
    result->error = "AuthMetadataPluginCallback raised: TODO: str(Exception)";
  }
  result->ready.Notify();
  Py_RETURN_NONE;
}

PyTypeObject AuthMetadataPluginCallbackType = {
    PyVarObject_HEAD_INIT(NULL, 0).tp_name = "grpc._AuthMetadataPluginCallback",
    .tp_basicsize = sizeof(AuthMetadataPluginCallback),
    .tp_itemsize = 0,
    .tp_call = reinterpret_cast<ternaryfunc>(Call_AuthMetadataPluginCallback),
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_new = PyType_GenericNew,
};

PyTypeObject* AuthMetadataContextType() {
  static PyStructSequence_Field fields[] = {
      {.name = "service_url", .doc = nullptr},
      {.name = "method_name", .doc = nullptr},
      {nullptr},
  };
  static PyStructSequence_Desc desc = {
      .name = "grpc._AuthMetadataContext",
      .doc = nullptr,
      .fields = fields,
      .n_in_sequence = 2,
  };
  static PyTypeObject type;
  PyStructSequence_InitType(&type, &desc);
  return &type;
}

::grpc::Status Plugin::GetMetadata(::grpc::string_ref service_url, ::grpc::string_ref method_name,
                                   const ::grpc::AuthContext& channel_auth_context,
                                   std::multimap<std::string, std::string>* metadata) {
  CallbackResult result = {.metadata = metadata};
  auto gstate = PyGILState_Ensure();
  static auto auth_metadata_context_type = (PyType_Ready(&AuthMetadataPluginCallbackType), AuthMetadataContextType());
  auto callback = PyObject_New(AuthMetadataPluginCallback, &AuthMetadataPluginCallbackType);
  callback->result = &result;
  auto context = PyStructSequence_New(auth_metadata_context_type);
  PyStructSequence_SET_ITEM(context, 0,
                            types::Str::FromUtf8Bytes(absl::string_view{service_url.data(), service_url.length()}));
  PyStructSequence_SET_ITEM(context, 1,
                            types::Str::FromUtf8Bytes(absl::string_view{method_name.data(), method_name.length()}));
  auto plugin_result = CallPython(plugin_.get(), context, reinterpret_cast<PyObject*>(callback));
  Py_DECREF(reinterpret_cast<PyObject*>(context));
  Py_DECREF(reinterpret_cast<PyObject*>(callback));
  PyGILState_Release(gstate);
  if (plugin_result.IsValid()) {
    result.ready.WaitForNotification();
    gstate = PyGILState_Ensure();
    return ::grpc::Status::OK;
  }
  if (callback->result.exchange(nullptr) == nullptr) {
    result.ready.WaitForNotification();  // alternatively heap-allocate ready and no do not wait.
  }
  return {::grpc::StatusCode::INTERNAL, std::move(result.error).value()};
}

}  // namespace

std::shared_ptr<::grpc::CallCredentials> MetadataCallCredentials(PyObject* plugin, std::string name) noexcept {
  return MetadataCredentialsFromPlugin(
      std::unique_ptr<::grpc::MetadataCredentialsPlugin>(new Plugin(AddRef(plugin), std::move(name))));
}

}  // namespace python
}  // namespace grpc
}  // namespace dropbox
