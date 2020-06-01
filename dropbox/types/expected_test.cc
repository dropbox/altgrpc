#include "dropbox/types/expected.h"

#include <memory>

#include "gtest/gtest.h"

namespace dropbox {
namespace {

TEST(Expected, Simple) {
  {
    Expected<int, std::string> expected;
    ASSERT_TRUE(expected.IsValid());
    ASSERT_EQ(expected.Value(), 0);

    Expected<int, std::string> moved = std::move(expected);
    ASSERT_TRUE(moved.IsValid());
    ASSERT_EQ(moved.Value(), 0);

    auto value = moved.Map([](int value) { return value + 1; })
                     .MapErr([](std::string value) { return value + "4"; })
                     .Map([](int value) { return value + 2; })
                     .Value();
    EXPECT_EQ(value, 3);
  }
  {
    Expected<int, std::string> expected = 5;
    ASSERT_TRUE(expected.IsValid());
    ASSERT_EQ(expected.Value(), 5);
    ASSERT_EQ(*expected, 5);
  }
  {
    Expected<int, std::string> expected = MakeUnexpected(std::string("123"));
    ASSERT_FALSE(expected.IsValid());
    ASSERT_EQ(expected.Error(), "123");

    auto value =
        expected.Map([](int value) { return value + 1; }).MapErr([](std::string value) { return value + "4"; }).Error();
    EXPECT_EQ(value, "1234");
  }
}

using UncopyableType = std::unique_ptr<std::string>;

TEST(Expected, Uncopyable) {
  {
    auto original_value = std::make_unique<std::string>("hello, world");
    auto value_ptr = original_value.get();
    Expected<UncopyableType, std::string> expected = std::move(original_value);
    ASSERT_TRUE(expected.IsValid());
    ASSERT_EQ(expected.Value().get(), value_ptr);

    Expected<UncopyableType, std::string> moved = std::move(expected);
    ASSERT_TRUE(moved.IsValid());
    ASSERT_EQ(moved.Value().get(), value_ptr);

    auto value = moved.Map([](auto value) { return std::make_unique<std::string>(*value + "1"); })
                     .MapErr([](auto error) { return error + "4"; })
                     .Map([](auto value) { return std::make_unique<std::string>(*value + "7"); })
                     .Value();
    EXPECT_EQ(*value, "hello, world17");
  }
  {
    Expected<int, UncopyableType> expected = 5;
    ASSERT_TRUE(expected.IsValid());
    ASSERT_EQ(expected.Value(), 5);
    ASSERT_EQ(*expected, 5);
  }
  {
    Expected<int, UncopyableType> expected = MakeUnexpected(std::make_unique<std::string>("123"));
    ASSERT_FALSE(expected.IsValid());
    ASSERT_EQ(*expected.Error(), std::string{"123"});

    auto value = expected.Map([](int value) { return value + 1; })
                     .MapErr([](auto error) { return std::make_unique<std::string>(*error + "4"); })
                     .Error();
    EXPECT_EQ(*value, std::string{"1234"});
  }
}

}  // namespace
}  // namespace dropbox
