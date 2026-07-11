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
#pragma once

// The ONE place that bridges the packed wire status byte and the domain craft_error facility. Homeblocks-
// coupled (craft_error) and depends on the wire ONE-WAY -- the wire never depends on this, so craft_wire
// stays an extractable std-only leaf. Used by the transport's coupled TUs (the servers translate a core's
// error to a status byte; the proxy translates a reply's status byte back). The wire-only client never
// includes it -- it surfaces wire::status raw.

#include <system_error>

#include <craft/types.hpp> // craft_error, make_error_condition
#include <craft/wire.hpp>  // wire::status

namespace craft {

// A core's std::error_condition (craft_error / std::errc) -> the response status byte. Server side.
inline wire::status to_wire_status(std::error_condition const& e) {
    if (e == craft_error::STALE_TERM) return wire::status::stale_term;
    if (e == craft_error::NOT_LEADER) return wire::status::not_leader;
    if (e == craft_error::NO_QUORUM) return wire::status::no_quorum;
    if (e == craft_error::WRONG_TOKEN) return wire::status::wrong_token;
    if (e == craft_error::NOT_ELIGIBLE) return wire::status::not_eligible;
    if (e == craft_error::REPLICA_DOWN) return wire::status::replica_down;
    if (e == std::errc::invalid_argument) return wire::status::invalid_argument;
    return wire::status::internal;
}

// A reply's status byte -> the domain error. Client/proxy side. `ok` is not an error and has no mapping here.
inline std::error_condition status_to_error(wire::status s) {
    switch (s) {
    case wire::status::stale_term:
        return make_error_condition(craft_error::STALE_TERM);
    case wire::status::not_leader:
        return make_error_condition(craft_error::NOT_LEADER);
    case wire::status::no_quorum:
        return make_error_condition(craft_error::NO_QUORUM);
    case wire::status::wrong_token:
        return make_error_condition(craft_error::WRONG_TOKEN);
    case wire::status::not_eligible:
        return make_error_condition(craft_error::NOT_ELIGIBLE);
    case wire::status::replica_down:
        return make_error_condition(craft_error::REPLICA_DOWN);
    case wire::status::invalid_argument:
        return std::make_error_condition(std::errc::invalid_argument);
    default:
        return make_error_condition(craft_error::REPLICA_DOWN);
    }
}

} // namespace craft
