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

#include <chrono>
#include <utility>

#include "absl/container/flat_hash_map.h"
#include "absl/strings/match.h"
#include "dropbox/grpc/python/server.h"

namespace dropbox {
namespace grpc {
namespace python {

bool ServicerContext::IsActive() noexcept {
  auto nogil = gil::Release();
  return !call_->done()->done();
}

types::Float ServicerContext::TimeRemaining() noexcept {
  const auto deadline = call_->context().deadline();
  if (deadline != std::chrono::system_clock::time_point::max()) {
    const auto now = std::chrono::system_clock::now();
    return types::Float::FromDouble(deadline <= now ? 0 : std::chrono::duration<double>(deadline - now).count());
  }
  return types::Float{AddRef(kNone).release()};
}

void ServicerContext::Cancel() noexcept {
  auto nogil = gil::Release();
  call_->context().TryCancel();
}

bool ServicerContext::AddCallback(std::function<void()> callback) noexcept {
  return call_->done()->AddCallback(
      [callback, dispatcher = dispatcher()](auto) mutable { dispatcher->RunInPool(std::move(callback)); });
}

void ServicerContext::EnsureInitialMetadata() noexcept {
  std::call_once(initial_metadata_, [&] {
    method_ = AutoReleasedObject{types::Str::FromUtf8Bytes(call_->context().method())};
    invocation_metadata_ = AutoReleasedObject{types::Metadata::FromMultimap(call_->context().client_metadata())};
  });
}

types::Str ServicerContext::Method() noexcept {
  EnsureInitialMetadata();
  return types::Str{ValueOrNone(method_)};
}

types::Tuple ServicerContext::InvocationMetadata() noexcept {
  EnsureInitialMetadata();
  return types::Tuple{ValueOrNone(invocation_metadata_)};
}

types::Str ServicerContext::Peer() noexcept {
  std::call_once(peer_loaded_, [&] {
    std::string peer;
    {
      auto nogil = gil::Release();
      peer = call_->context().peer();
      if (peer.empty()) {
        return;
      }
    }
    peer_ = AutoReleasedObject{types::Str::FromUtf8Bytes(peer)};
  });
  return types::Str{ValueOrNone(peer_)};
}

types::Tuple ServicerContext::PeerIdentities() noexcept {
  std::call_once(peer_identities_loaded_, [&] {
    std::vector<::grpc::string_ref> identities;
    {
      auto nogil = gil::Release();
      auto auth_context = call_->context().auth_context();
      if (!auth_context) {
        return;
      }
      identities = auth_context->GetPeerIdentity();
    }
    if (identities.empty()) {
      return;
    }
    auto tuple = types::Tuple::New(identities.size());
    size_t index = 0;
    for (auto& identity : identities) {
      tuple.FillItem(index++, types::Bytes::FromUtf8Bytes(absl::string_view(identity.data(), identity.length())));
    }
    peer_identities_ = AutoReleasedObject{tuple};
  });
  return types::Tuple{ValueOrNone(peer_identities_)};
}

types::Str ServicerContext::PeerIdentityKey() noexcept {
  std::call_once(peer_identity_key_loaded_, [&] {
    std::string key;
    {
      auto nogil = gil::Release();
      auto auth_context = call_->context().auth_context();
      if (!auth_context) {
        return;
      }
      key = auth_context->GetPeerIdentityPropertyName();
    }
    if (!key.empty()) {
      peer_identity_key_ = AutoReleasedObject{types::Str::FromUtf8Bytes(key)};
    }
  });
  return types::Str{ValueOrNone(peer_identity_key_)};
}

types::Dict ServicerContext::AuthContext() noexcept {
  std::call_once(auth_context_loaded_, [&] {
    absl::flat_hash_map<std::string, std::vector<::grpc::string_ref>> map;
    {
      auto nogil = gil::Release();
      auto auth_context = call_->context().auth_context();
      if (!auth_context) {
        auth_context_ = AutoReleasedObject{types::Dict::New()};
        return;
      }
      for (auto& auth_property : *auth_context) {
        map.try_emplace(absl::string_view{auth_property.first.data(), auth_property.first.length()})
            .first->second.push_back(auth_property.second);
      }
    }
    if (map.empty()) {
      auth_context_ = AutoReleasedObject{types::Dict::New()};
      return;
    }
    auto dict = types::Dict::New();
    for (auto& entry : map) {
      AutoReleasedObject key{types::Str::FromUtf8Bytes(entry.first).get()};
      auto value = types::Tuple::New(entry.second.size());
      AutoReleasedObject value_releaser{value.get()};
      size_t index = 0;
      for (auto& subvalue : entry.second) {
        value.FillItem(index++, types::Bytes::FromUtf8Bytes({subvalue.begin(), subvalue.length()}));
      }
      dict.Set(key.get(), value.get());
    }
    auth_context_ = AutoReleasedObject{dict};
  });
  return types::Dict{ValueOrNone(auth_context_)};
}

void ServicerContext::SetCompression(grpc_compression_algorithm algorithm) noexcept {
  auto nogil = gil::Release();
  call_->context().set_compression_algorithm(algorithm);
}

void ServicerContext::DisableNextMessageCompression() noexcept {
  // TODO(mehrdad): implement
}

void ServicerContext::SetCode(grpc_status_code code) noexcept {
  auto nogil = gil::Release();
  absl::MutexLock lock{&status_mutex_};
  status_code_ = static_cast<::grpc::StatusCode>(code);
}

void ServicerContext::SetDetails(std::string details) noexcept {
  auto nogil = gil::Release();
  absl::MutexLock lock{&status_mutex_};
  details_ = std::move(details);
}

void ServicerContext::AddInitialMetadata(std::string key, std::string value) noexcept {
  auto nogil = gil::Release();
  call_->context().AddInitialMetadata(std::move(key), std::move(value));
}

bool ServicerContext::SendInitialMetadataBlocking() noexcept {
  auto nogil = gil::Release();
  auto task = call_->SendInitialMetadata();
  task->Await();
  return task->ok();
}

void ServicerContext::AddTrailingMetadata(std::string key, std::string value) noexcept {
  auto nogil = gil::Release();
  call_->context().AddTrailingMetadata(std::move(key), std::move(value));
}

void ServicerContext::End() noexcept {
  auto nogil = gil::Release();
  absl::MutexLock lock{&status_mutex_};
  call_->Finish({status_code_.value_or(::grpc::StatusCode::OK), details_});
}

void ServicerContext::End(const ::grpc::ByteBuffer& buffer) noexcept {
  auto nogil = gil::Release();
  absl::MutexLock lock{&status_mutex_};
  call_->WriteAndFinish(buffer, {status_code_.value_or(::grpc::StatusCode::OK), details_});
}

void ServicerContext::Abort(::grpc::Status abortion_status) noexcept {
  auto nogil = gil::Release();
  absl::MutexLock lock{&status_mutex_};
  if (status_code_.has_value() && status_code_.value() != ::grpc::StatusCode::OK) {
    call_->Finish({status_code_.value(), details_});
  } else {
    call_->Finish(std::move(abortion_status));
  }
}

ServicerContext::~ServicerContext() {
  auto nogil = gil::Release();
  call_.reset();
}

}  // namespace python
}  // namespace grpc
}  // namespace dropbox
