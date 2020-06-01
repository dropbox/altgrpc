// Copyright (c) 2016 The gRPC Authors
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

#include "dropbox/grpc/metadata.h"

#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"

namespace dropbox {
namespace grpc {
namespace metadata {

namespace {

constexpr absl::string_view kMetadataKeyIsEmpty = "Metadata keys cannot be zero length";
constexpr absl::string_view kMetadataKeyIsTooLong = "Metadata keys cannot be larger than UINT32_MAX";
constexpr absl::string_view kMetadataKeyStartsWithColon = "Metadata keys cannot start with :";
constexpr absl::string_view kMetadataInvalidHeaderKey = "Invalid metadata key: ";
constexpr absl::string_view kMetadataInvalidHeaderValue = "Invalid non-binary metadata value: ";

bool ConformsTo(absl::string_view value, const uint8_t* legal_bits) {
  const uint8_t* p = reinterpret_cast<const uint8_t*>(value.data());
  const uint8_t* e = p + value.length();
  for (; p != e; p++) {
    int idx = *p;
    int byte = idx / 8;
    int bit = idx % 8;
    if (ABSL_PREDICT_FALSE((legal_bits[byte] & (1 << bit)) == 0)) {
      return false;
    }
  }
  return true;
}

bool IsBinaryHeaderKey(absl::string_view header_key) { return absl::EndsWith(header_key, "-bin"); }

}  // namespace

Expected<Void, std::string> ValidateHeaderKey(absl::string_view header_key) {
  static const uint8_t legal_header_bits[256 / 8] = {0x00, 0x00, 0x00, 0x00, 0x00, 0x60, 0xff, 0x03, 0x00, 0x00, 0x00,
                                                     0x80, 0xfe, 0xff, 0xff, 0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                                                     0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
  if (ABSL_PREDICT_FALSE(header_key.empty())) {
    return MakeUnexpected(std::string{kMetadataKeyIsEmpty});
  }
  if (ABSL_PREDICT_FALSE(header_key.length() > UINT32_MAX)) {
    return MakeUnexpected(std::string{kMetadataKeyIsTooLong});
  }
  if (ABSL_PREDICT_FALSE(header_key[0] == ':')) {
    return MakeUnexpected(std::string{kMetadataKeyStartsWithColon});
  }
  if (ABSL_PREDICT_FALSE(!ConformsTo(header_key, legal_header_bits))) {
    return MakeUnexpected(std::string{absl::StrCat(kMetadataInvalidHeaderKey, header_key)});
  }
  return Void{};
}

Expected<Void, std::string> ValidateHeaderNonBinaryValue(absl::string_view header_value) {
  static const uint8_t legal_header_bits[256 / 8] = {0x00, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
                                                     0xff, 0xff, 0xff, 0xff, 0x7f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                                                     0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
  if (ABSL_PREDICT_TRUE(ConformsTo(header_value, legal_header_bits))) {
    return Void{};
  }
  return MakeUnexpected(std::string{absl::StrCat(kMetadataInvalidHeaderValue, header_value)});
}

Expected<Void, std::string> ValidateHeader(absl::string_view key, absl::string_view value) {
  auto validation = ValidateHeaderKey(key);
  if (ABSL_PREDICT_TRUE(validation.IsValid())) {
    if (IsBinaryHeaderKey(key)) {
      return Void{};
    }
    return ValidateHeaderNonBinaryValue(value);
  }
  return validation.GetUnexpected();
}

}  // namespace metadata
}  // namespace grpc
}  // namespace dropbox
