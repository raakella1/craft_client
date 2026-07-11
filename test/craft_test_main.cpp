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

// Shared gtest entry point for the craft_client suites (all link sisl -> boost, so main() must be a direct
// object, not libgtest_main.a; see test_main.cpp). Sets up sisl logging (the coro detach path logs on error).

#include <gtest/gtest.h>
#include <sisl/logging/logging.h>
#include <sisl/options/options.h>

SISL_LOGGING_INIT(craft)
SISL_OPTIONS_ENABLE(logging)

int main(int argc, char* argv[]) {
    int parsed_argc = argc;
    ::testing::InitGoogleTest(&parsed_argc, argv);
    SISL_OPTIONS_LOAD(parsed_argc, argv, logging);
    sisl::logging::SetLogger(std::string(argv[0]));
    sisl::logging::SetLogPattern("[%D %T%z] [%^%L%$] [%t] %v");
    return RUN_ALL_TESTS();
}
