#ifndef CPP_DROPBOX_TYPES_EXPECTED_H_
#define CPP_DROPBOX_TYPES_EXPECTED_H_

#include <string>
#include <utility>

#include "absl/types/variant.h"

namespace dropbox {

namespace detail {
// Wrapper type to help absl::variant distinguish error and value cases for
// Expected<int, int>.
template <typename E>
struct Error {
  E error;
};
}  // namespace detail

template <typename E>
class Unexpected {
 public:
  constexpr explicit Unexpected(const E& e) : error_(e) {}
  constexpr explicit Unexpected(E&& e) : error_(std::move(e)) {}

  constexpr const E& Value() const& { return error_; }
  constexpr E& Value() & { return error_; }
  constexpr E&& Value() && { return std::move(error_); }

 private:
  E error_;
};

template <typename E>
constexpr Unexpected<std::decay_t<E>> MakeUnexpected(E&& error) {
  return Unexpected<std::decay_t<E>>(std::forward<E>(error));
}

template <typename V, typename E>
class Expected {
  using ErrorWrapper = detail::Error<E>;

 public:
  using ValueType = V;
  using ErrorType = E;

  constexpr Expected() : value_(ValueType()) {}  // NOLINT(runtime/explicit)
  constexpr Expected(ValueType&& value)          // NOLINT(runtime/explicit)
      : value_(std::move(value)) {}
  constexpr Expected(const ValueType& value)  // NOLINT(runtime/explicit)
      : value_(value) {}
  constexpr Expected(Unexpected<ErrorType>&& error)  // NOLINT(runtime/explicit)
      : value_(ErrorWrapper{std::move(error).Value()}) {}
  constexpr Expected(const Unexpected<ErrorType>& error)  // NOLINT(runtime/explicit)
      : value_(ErrorWrapper{error.Value()}) {}

  constexpr bool IsValid() const { return value_.index() == 0; }

  constexpr const ValueType& Value() const& { return absl::get<0>(value_); }
  constexpr ValueType& Value() & { return absl::get<0>(value_); }
  constexpr ValueType&& Value() && { return absl::get<0>(std::move(value_)); }

  constexpr const ValueType& operator*() const& { return Value(); }
  constexpr ValueType& operator*() & { return Value(); }
  constexpr ValueType&& operator*() && { return std::move(*this).Value(); }

  constexpr const ValueType* operator->() const { return &Value(); }

  constexpr const ErrorType& Error() const& { return absl::get<1>(value_).error; }
  constexpr ErrorType& Error() & { return absl::get<1>(value_).error; }
  constexpr ErrorType&& Error() && { return absl::get<1>(std::move(value_)).error; }

  constexpr Unexpected<ErrorType> GetUnexpected() const& { return Unexpected<ErrorType>(absl::get<1>(value_).error); }
  constexpr Unexpected<ErrorType> GetUnexpected() && {
    return Unexpected<ErrorType>(absl::get<1>(std::move(value_)).error);
  }

  template <typename F>
  auto Map(F&& func) -> Expected<decltype(func(std::declval<ValueType>())), ErrorType> {
    if (IsValid()) {
      return func(std::move(*this).Value());
    }
    return std::move(*this).GetUnexpected();
  }

  template <typename F>
  auto MapErr(F&& func) -> Expected<ValueType, decltype(func(std::declval<ErrorType>()))> {
    if (!IsValid()) {
      return MakeUnexpected(func(std::move(*this).Error()));
    }
    return std::move(*this).Value();
  }

 private:
  absl::variant<ValueType, ErrorWrapper> value_;
};

using Void = absl::monostate;

}  // namespace dropbox

#endif  // CPP_DROPBOX_TYPES_EXPECTED_H_
