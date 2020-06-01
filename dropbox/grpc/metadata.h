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

#ifndef DROPBOX_GRPC_METADATA_H_
#define DROPBOX_GRPC_METADATA_H_

#include <string>

#include "absl/strings/string_view.h"
#include "dropbox/types/expected.h"

namespace dropbox {
namespace grpc {
namespace metadata {

Expected<Void, std::string> ValidateHeaderKey(absl::string_view header_key);
Expected<Void, std::string> ValidateHeaderNonBinaryValue(absl::string_view header_nonbin_value);
Expected<Void, std::string> ValidateHeader(absl::string_view key, absl::string_view value);

}  // namespace metadata
}  // namespace grpc
}  // namespace dropbox

#endif  // DROPBOX_GRPC_METADATA_H_
