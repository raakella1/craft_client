cmake_minimum_required(VERSION 3.16)
project(craft_client LANGUAGES CXX)
enable_testing()

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if((DEFINED THREAD_SANITIZER_ON) AND (${THREAD_SANITIZER_ON}))
  add_compile_options(-fsanitize=thread)
  add_link_options(-fsanitize=thread)
elseif((DEFINED ADDRESS_SANITIZER_ON) AND (${ADDRESS_SANITIZER_ON}))
  add_compile_options(-fsanitize=address -fsanitize=undefined -fno-sanitize-recover=all)
  add_link_options(-fsanitize=address -fsanitize=undefined)
endif()

find_package(GTest REQUIRED)
find_package(sisl REQUIRED)
find_package(liburing REQUIRED)

# ── craft_wire: the std-only wire codec LEAF (the future standalone dependency) ──
add_library(craft_wire STATIC src/wire.cpp)
target_include_directories(craft_wire PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_compile_features(craft_wire PUBLIC cxx_std_23)

# ── craft_types: the domain vocabulary + craft_replica interface (header-only; result carrier from sisl) ──
add_library(craft_types INTERFACE)
target_include_directories(craft_types INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(craft_types INTERFACE sisl::sisl)
target_compile_features(craft_types INTERFACE cxx_std_23)

# ── craft_client: the CRAFT client + io_uring transport (the production surface ublkpp drives) ──
add_library(craft_client STATIC
  src/client.cpp
  src/dlsn_tracker.cpp
  src/net/conn.cpp
  src/net/tcp_client.cpp
  src/net/tcp_replica.cpp
  src/net/tcp_cluster.cpp) # craft/tcp.hpp: the public "connect to remote servers" backend builder
target_include_directories(craft_client PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_include_directories(craft_client PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src) # internal headers (dlsn_tracker, ...)
target_link_libraries(craft_client PUBLIC craft_wire craft_types sisl::sisl liburing::liburing)
target_compile_features(craft_client PUBLIC cxx_std_23)

# ── craft_reference: the in-memory reference model + loopback cluster server (test-support only) ──
add_library(craft_reference STATIC
  src/mem/replica.cpp
  src/mem/cluster.cpp
  src/local.cpp # craft/local.hpp: the public "no-remote, in-process" backend builder over the mem model
  src/net/cluster_server.cpp
  src/net/tcp_server.cpp)
target_include_directories(craft_reference PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(craft_reference PUBLIC craft_client)
target_compile_features(craft_reference PUBLIC cxx_std_23)

# ── tests ──
add_executable(test_wire test/test_wire.cpp)
target_link_libraries(test_wire PRIVATE craft_wire GTest::gtest GTest::gtest_main)
add_test(NAME CraftWire COMMAND test_wire)

# sisl-linking tests provide their own main (test_main.cpp) so boost's test_exec_monitor is not pulled.
add_executable(test_types test/test_types.cpp test/test_main.cpp)
target_link_libraries(test_types PRIVATE craft_types GTest::gtest)
add_test(NAME CraftTypes COMMAND test_types)

# Behavioral suites: link the libs under test + the shared sisl-logging main (direct object, not gtest_main).
function(craft_test name)
  add_executable(${name} test/${name}.cpp test/craft_test_main.cpp)
  target_link_libraries(${name} PRIVATE ${ARGN} GTest::gmock)
  add_test(NAME ${name} COMMAND ${name})
endfunction()

craft_test(test_api          craft_reference) # the PUBLIC free-function surface (no internal headers)
craft_test(test_tcp_builder  craft_reference) # craft/tcp.hpp: connect() a client to a reference server, public-only
craft_test(test_memory       craft_reference)
craft_test(test_dlsn_tracker craft_client)
target_include_directories(test_dlsn_tracker PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src) # white-box: internal dlsn_tracker.hpp
craft_test(test_cluster      craft_reference)
craft_test(test_tcp          craft_reference)
craft_test(test_tcp_replica  craft_reference)

# test_client / test_client_tcp carry their own main() + SISL_LOGGING_DEF (no shared main).
function(craft_test_ownmain name)
  add_executable(${name} test/${name}.cpp)
  target_link_libraries(${name} PRIVATE ${ARGN} GTest::gmock)
  add_test(NAME ${name} COMMAND ${name})
endfunction()

craft_test_ownmain(test_client     craft_reference)
craft_test_ownmain(test_client_tcp craft_reference)
# white-box: these two reach the concrete craft_client + read_route_map (internal headers under src/)
target_include_directories(test_client     PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_include_directories(test_client_tcp PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)

# Under TSAN, point every test at the suppression file (a benign sisl NamedCounter / StreamTracker-metrics
# registration race; see tsan.supp). Applied after all add_test() calls so it covers the whole suite.
if((DEFINED THREAD_SANITIZER_ON) AND (${THREAD_SANITIZER_ON}))
  get_property(_craft_tests DIRECTORY PROPERTY TESTS)
  set_tests_properties(${_craft_tests} PROPERTIES
    ENVIRONMENT "TSAN_OPTIONS=suppressions=${CMAKE_SOURCE_DIR}/tsan.supp")
endif()
