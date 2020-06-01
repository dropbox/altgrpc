// Copyright (c) 2017 The gRPC Authors
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

#ifndef DROPBOX_GRPC_REFCOUNT_H_
#define DROPBOX_GRPC_REFCOUNT_H_

#include <atomic>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"

namespace dropbox {
namespace grpc {

template <typename T>
class RefCountedPtr;

class RefCount {
 public:
  using Value = intptr_t;
  constexpr explicit RefCount(Value init = 1) : value_(init) {}
  void Ref(Value n = 1) { value_.fetch_add(n, std::memory_order_relaxed); }

  bool Unref() {
    const Value prior = value_.fetch_sub(1, std::memory_order_acq_rel);
    return prior == 1;
  }

 private:
  Value get() const { return value_.load(std::memory_order::memory_order_relaxed); }
  std::atomic<Value> value_;
};

template <typename Child>
class RefCounted {
 public:
  ABSL_MUST_USE_RESULT RefCountedPtr<Child> Ref() {
    IncrementRefCount();
    return RefCountedPtr<Child>(static_cast<Child*>(this));
  }

  void Unref() {
    if (ABSL_PREDICT_FALSE(refs_.Unref())) {
      delete this;
    }
  }

  RefCounted() = default;

  RefCounted(const RefCounted&) = delete;
  RefCounted& operator=(const RefCounted&) = delete;

  virtual ~RefCounted() = default;

 private:
  template <typename T>
  friend class RefCountedPtr;

  void IncrementRefCount() { refs_.Ref(); }
  RefCount refs_;
};

template <typename T>
class RefCountedPtr {
 public:
  RefCountedPtr() {}

  // If value is non-null, we take ownership of a ref to it.
  template <typename Y>
  explicit RefCountedPtr(Y* value) {
    value_ = value;
  }

  // Move ctors.
  RefCountedPtr(RefCountedPtr&& other) {
    value_ = other.value_;
    other.value_ = nullptr;
  }
  template <typename Y>
  RefCountedPtr(RefCountedPtr<Y>&& other) {
    value_ = static_cast<T*>(other.value_);
    other.value_ = nullptr;
  }

  // Move assignment.
  RefCountedPtr& operator=(RefCountedPtr&& other) {
    reset(other.value_);
    other.value_ = nullptr;
    return *this;
  }
  template <typename Y>
  RefCountedPtr& operator=(RefCountedPtr<Y>&& other) {
    reset(other.value_);
    other.value_ = nullptr;
    return *this;
  }

  // Copy ctors.
  RefCountedPtr(const RefCountedPtr& other) {
    if (other.value_ != nullptr) other.value_->IncrementRefCount();
    value_ = other.value_;
  }
  template <typename Y>
  RefCountedPtr(const RefCountedPtr<Y>& other) {
    static_assert(std::has_virtual_destructor<T>::value, "T does not have a virtual dtor");
    if (other.value_ != nullptr) other.value_->IncrementRefCount();
    value_ = static_cast<T*>(other.value_);
  }

  // Copy assignment.
  RefCountedPtr& operator=(const RefCountedPtr& other) {
    // Note: Order of reffing and unreffing is important here in case value_
    // and other.value_ are the same object.
    if (other.value_ != nullptr) other.value_->IncrementRefCount();
    reset(other.value_);
    return *this;
  }
  template <typename Y>
  RefCountedPtr& operator=(const RefCountedPtr<Y>& other) {
    static_assert(std::has_virtual_destructor<T>::value, "T does not have a virtual dtor");
    // Note: Order of reffing and unreffing is important here in case value_
    // and other.value_ are the same object.
    if (other.value_ != nullptr) other.value_->IncrementRefCount();
    reset(other.value_);
    return *this;
  }

  ~RefCountedPtr() {
    if (value_ != nullptr) value_->Unref();
  }

  // If value is non-null, we take ownership of a ref to it.
  void reset(T* value = nullptr) {
    if (value_ != nullptr) value_->Unref();
    value_ = value;
  }
  template <typename Y>
  void reset(Y* value = nullptr) {
    static_assert(std::has_virtual_destructor<T>::value, "T does not have a virtual dtor");
    if (value_ != nullptr) value_->Unref();
    value_ = static_cast<T*>(value);
  }

  T* release() {
    T* value = value_;
    value_ = nullptr;
    return value;
  }

  T* get() const { return value_; }

  T& operator*() const { return *value_; }
  T* operator->() const { return value_; }

  template <typename Y>
  bool operator==(const RefCountedPtr<Y>& other) const {
    return value_ == other.value_;
  }

  template <typename Y>
  bool operator==(const Y* other) const {
    return value_ == other;
  }

  bool operator==(std::nullptr_t) const { return value_ == nullptr; }

  template <typename Y>
  bool operator!=(const RefCountedPtr<Y>& other) const {
    return value_ != other.value_;
  }

  template <typename Y>
  bool operator!=(const Y* other) const {
    return value_ != other;
  }

  bool operator!=(std::nullptr_t) const { return value_ != nullptr; }

 private:
  template <typename Y>
  friend class RefCountedPtr;

  T* value_ = nullptr;
};

template <typename T, typename... Args>
inline RefCountedPtr<T> MakeRefCounted(Args&&... args) {
  return RefCountedPtr<T>(new T(std::forward<Args>(args)...));
}

}  // namespace grpc
}  // namespace dropbox

#endif  // DROPBOX_GRPC_REFCOUNT_H_
