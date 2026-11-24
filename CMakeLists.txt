cmake_minimum_required(VERSION 3.20)
project(ParallelTaskScheduler LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(SCHEDULER_SANITIZER "NONE" CACHE STRING "Sanitizer mode: NONE, TSAN, ASAN")
set_property(CACHE SCHEDULER_SANITIZER PROPERTY STRINGS NONE TSAN ASAN)
string(TOUPPER "${SCHEDULER_SANITIZER}" _SCHEDULER_SANITIZER_UPPER)

set(_SCHEDULER_ALLOWED_SANITIZERS NONE TSAN ASAN)
if(NOT _SCHEDULER_SANITIZER_UPPER IN_LIST _SCHEDULER_ALLOWED_SANITIZERS)
    message(FATAL_ERROR "Invalid SCHEDULER_SANITIZER='${SCHEDULER_SANITIZER}'. Expected NONE, TSAN, or ASAN.")
endif()

add_library(scheduler_sanitizer_flags INTERFACE)
if(_SCHEDULER_SANITIZER_UPPER STREQUAL "TSAN" OR _SCHEDULER_SANITIZER_UPPER STREQUAL "ASAN")
    if(NOT (CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang") OR MINGW)
        message(FATAL_ERROR "SCHEDULER_SANITIZER=${_SCHEDULER_SANITIZER_UPPER} requires Linux/macOS GCC or Clang (not MSVC/MinGW).")
    endif()
    if(_SCHEDULER_SANITIZER_UPPER STREQUAL "TSAN")
        target_compile_options(scheduler_sanitizer_flags INTERFACE -fsanitize=thread -fno-omit-frame-pointer -g -O1)
        target_link_options(scheduler_sanitizer_flags INTERFACE -fsanitize=thread)
    else()
        target_compile_options(scheduler_sanitizer_flags INTERFACE -fsanitize=address,undefined -fno-omit-frame-pointer -g -O1)
        target_link_options(scheduler_sanitizer_flags INTERFACE -fsanitize=address,undefined)
    endif()
endif()

find_package(Threads REQUIRED)

add_library(scheduler_headers INTERFACE)
target_include_directories(scheduler_headers INTERFACE "${CMAKE_CURRENT_SOURCE_DIR}")
target_link_libraries(scheduler_headers INTERFACE Threads::Threads)
target_compile_features(scheduler_headers INTERFACE cxx_std_23)

if(MSVC)
    target_compile_options(scheduler_headers INTERFACE /W4 /utf-8 /permissive-)
elseif(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(scheduler_headers INTERFACE
        -Wall
        -Wextra
        $<$<OR:$<CONFIG:Release>,$<CONFIG:RelWithDebInfo>>:-fno-omit-frame-pointer>
    )
endif()

add_executable(scheduler_tests tests/scheduler_tests.cpp)
target_link_libraries(scheduler_tests PRIVATE scheduler_headers scheduler_sanitizer_flags)

add_executable(bench_scheduler bench_scheduler.cpp)
target_link_libraries(bench_scheduler PRIVATE scheduler_headers scheduler_sanitizer_flags)

add_executable(gemm_benchmark gemm_benchmark.cpp)
target_link_libraries(gemm_benchmark PRIVATE scheduler_headers scheduler_sanitizer_flags)

include(CTest)
if(BUILD_TESTING)
    add_test(NAME core_conformance COMMAND scheduler_tests --suite core)
    set_tests_properties(core_conformance PROPERTIES LABELS "core")

    add_test(NAME v25_tsan_race COMMAND scheduler_tests --suite v25_race)
    set_tests_properties(v25_tsan_race PROPERTIES LABELS "tsan;red_light")
endif()
