/*********************************************************************************
 * Modifications Copyright 2026 eBay Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 *********************************************************************************/

// craft_types smoke test: the vocabulary compiles + links off sisl alone (no engine), the craft_error
// std::error_condition machinery round-trips, and the async aliases ARE sisl's canonical result types.

#include <gtest/gtest.h>

#include <craft/replica.hpp> // pulls craft/types.hpp + the async aliases + the interface

// The client-facing async aliases are exactly sisl's canonical result carrier (no fork).
static_assert(std::is_same_v< craft::async_result< int >, sisl::async::result< int > >);
static_assert(std::is_same_v< craft::async_status, sisl::async::status >);

TEST(CraftTypes, ErrorConditionRoundTrips) {
    std::error_condition const ec = craft::make_error_condition(craft::craft_error::STALE_TERM);
    EXPECT_EQ(ec, craft::craft_error::STALE_TERM); // branchable via is_error_condition_enum
    EXPECT_NE(ec, craft::craft_error::NOT_LEADER);
    EXPECT_EQ(std::string{ec.category().name()}, "craft");
    EXPECT_EQ(ec.message(), "STALE_TERM"); // enum_name via the category
}

TEST(CraftTypes, DataStructDefaults) {
    craft::lsn_pair const p;
    EXPECT_EQ(p.commit_lsn, -1);
    EXPECT_EQ(p.last_append_lsn, -1);
    craft::io_extent const e{100, 512, true};
    EXPECT_TRUE(e.hole);
    craft::client_hdr const h;
    EXPECT_EQ(h.term, 0u);
    EXPECT_EQ(h.commit_lsn, -1);
}

// Compile-only: a domain code rides the type-erased result carrier -- the whole point of the sisl vocabulary.
craft::async_status ok_coro() { co_return sisl::ok(); }
craft::async_result< craft::lsn_pair > fenced_coro() {
    co_return std::unexpected(craft::make_error_condition(craft::craft_error::STALE_TERM));
}
