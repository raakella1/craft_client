cmake_minimum_required(VERSION 3.16)
project(craft_client LANGUAGES CXX)
enable_testing()

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if ((DEFINED BUILD_COVERAGE) AND (${BUILD_COVERAGE}))
  enable_testing()
  include (cmake/coverage.cmake)
  APPEND_COVERAGE_COMPILER_FLAGS()
  SETUP_TARGET_FOR_COVERAGE_GCOVR_XML(NAME coverage EXECUTABLE ctest DEPENDENCIES )
elseif ((DEFINED THREAD_SANITIZER_ON) AND (${THREAD_SANITIZER_ON}))
  set(SANITIZER_TYPE "thread")
  include (cmake/sanitize.cmake)
elseif ((DEFINED ADDRESS_SANITIZER_ON) AND (${ADDRESS_SANITIZER_ON}))
  set(SANITIZER_TYPE "address")
  include (cmake/sanitize.cmake)
endif()

find_program(CCACHE_FOUND ccache)
if (CCACHE_FOUND)
    set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE ccache)
    set_property(GLOBAL PROPERTY RULE_LAUNCH_LINK ccache)
endif ()

find_package(GTest REQUIRED)
find_package(sisl REQUIRED)
find_package(liburing REQUIRED)
find_package(NuraftMesg REQUIRED)

# ── craft_wire: the std-only wire codec LEAF (the future standalone dependency) ──
add_library(craft_wire STATIC src/wire.cpp)
target_include_directories(craft_wire PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_compile_features(craft_wire PUBLIC cxx_std_23)

# ── craft_types: the domain vocabulary ONLY (header-only; result carrier from sisl). A storage
# backend also needs craft_client for craft_peer and the peer codec. ──
add_library(craft_types INTERFACE)
target_include_directories(craft_types INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(craft_types INTERFACE sisl::sisl)
target_compile_features(craft_types INTERFACE cxx_std_23)

# ── craft_client: the CRAFT client + io_uring transport (the production surface ublkpp drives) ──
add_library(craft_client STATIC
  src/client.cpp
  src/dlsn_tracker.cpp
  src/peer_codec.cpp
  src/net/conn.cpp
  src/net/wire_client.cpp
  src/net/async_conn.cpp # the P2 on-ring TCP data path (recv-demux pump); internal, behind tcp_replica
  src/net/session_mgr.cpp # the ONE process-wide admin thread every proxy shares (login/logout + the no-ring tier)
  src/net/tcp_replica.cpp
  src/net/tcp_cluster.cpp) # craft/tcp.hpp: the public "connect to remote servers" backend builder
target_include_directories(craft_client PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_include_directories(craft_client PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src) # internal headers (dlsn_tracker, ...)
target_link_libraries(craft_client PUBLIC craft_wire craft_types sisl::sisl liburing::liburing)
target_compile_features(craft_client PUBLIC cxx_std_23)

# ── craft_replica_mgr: the internal replica manager (testing only) ──
add_library(craft_replica_mgr STATIC
  src/replica_mgr.cpp
  src/net/tcp_peer.cpp)
target_include_directories(craft_replica_mgr PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_include_directories(craft_replica_mgr PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(craft_replica_mgr PUBLIC craft_client)
target_compile_features(craft_replica_mgr PUBLIC cxx_std_23)

# ── craft_reference: the in-memory reference model + loopback cluster server (test-support only) ──
add_library(raft_service STATIC
  src/raft/raft_service.cpp
  src/raft/in_memory_log_store.cpp
  src/raft/raft_state_manager.cpp)
target_include_directories(raft_service PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_include_directories(raft_service PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(raft_service PUBLIC nuraft_mesg::proto craft_replica_mgr)
target_compile_features(raft_service PUBLIC cxx_std_23)

add_library(craft_reference STATIC
  src/mem/replica.cpp
  src/mem/cluster.cpp
  src/local.cpp # craft/local.hpp: the public "no-remote, in-process" backend builder over the mem model
  src/net/cluster_server.cpp
  src/net/tcp_server.cpp)
target_include_directories(craft_reference PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_include_directories(craft_reference PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src) # the internal headers it implements
target_link_libraries(craft_reference PUBLIC raft_service)
target_compile_features(craft_reference PUBLIC cxx_std_23)

# ── craft_reference_tcp_srv: a STANDALONE single-replica reference server. Run N of them on different ports to
# form a multi-process cluster; point a client (ublkpp_disk --craft-tcp) at them. Test/dev support, not installed.
add_executable(craft_reference_tcp_srv tools/craft_reference_tcp_srv.cpp)
target_link_libraries(craft_reference_tcp_srv PRIVATE craft_reference)
target_include_directories(craft_reference_tcp_srv PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src) # net/tcp_server.hpp
target_compile_features(craft_reference_tcp_srv PRIVATE cxx_std_23)

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

craft_test(test_api          craft_reference) # the PUBLIC surface, and ONLY it -- the canary that include/ suffices
craft_test(test_tcp_builder  craft_reference) # craft/tcp.hpp: connect() a client to a reference server
craft_test(test_memory       craft_reference)
craft_test(test_dlsn_tracker craft_client)
craft_test(test_cluster      craft_reference)
craft_test(test_tcp          craft_reference)
craft_test(test_tcp_replica  craft_reference)
craft_test(test_async_mem    craft_reference) # the on-ring async transport (prepare_for_async) driven at depth
craft_test(test_async_tcp    craft_reference) # the on-ring TCP transport (recv-demux pump) over real loopback sockets

# Every suite EXCEPT test_wire / test_types / test_api reaches an internal header under src/ -- the reference
# model, the TCP server/proxy, the craft_replica interface, the dlsn_tracker. That is the point of the split:
# those types are NOT on the public surface, so only in-repo code may name them. test_api is deliberately left
# out: it must compile against include/ alone, and it fails loudly if the public surface ever stops sufficing.
foreach(t test_tcp_builder test_memory test_dlsn_tracker test_cluster test_tcp test_tcp_replica
          test_async_mem test_async_tcp)
  target_include_directories(${t} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
endforeach()

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
# registration race; see tsan.supp). Applied after all add_test() calls so it covers the whole suite. The
# EXISTS guard matters: TSAN treats a missing suppressions file as fatal and aborts EVERY test on startup, so a
# path/packaging slip must degrade to "no suppression" (one truthful race failure), not "all tests fail".
set(_craft_tsan_supp "${CMAKE_SOURCE_DIR}/tsan.supp")
if((DEFINED THREAD_SANITIZER_ON) AND (${THREAD_SANITIZER_ON}) AND (EXISTS "${_craft_tsan_supp}"))
  get_property(_craft_tests DIRECTORY PROPERTY TESTS)
  set_tests_properties(${_craft_tests} PROPERTIES ENVIRONMENT "TSAN_OPTIONS=suppressions=${_craft_tsan_supp}")
endif()
