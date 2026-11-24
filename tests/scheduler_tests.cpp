#include <algorithm>
#include <atomic>
#include <barrier>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <future>
#include <iostream>
#include <latch>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "SchedulerContract.h"
#include "ThreadPool.h"
#include "ThreadPoolV25.h"

namespace {

	struct TestFailure : std::runtime_error {
		using std::runtime_error::runtime_error;
	};

#define TEST_CHECK(cond)                                                           \
	do {                                                                           \
		if (!(cond)) {                                                             \
			std::ostringstream _oss;                                               \
			_oss << "Check failed: (" #cond ") at " << __FILE__ << ':' << __LINE__; \
			throw TestFailure(_oss.str());                                         \
		}                                                                          \
	} while (false)

#define TEST_CHECK_EQ(actual, expected)                                            \
	do {                                                                           \
		const auto& _actual_val = (actual);                                        \
		const auto& _expected_val = (expected);                                    \
		if (!(_actual_val == _expected_val)) {                                     \
			std::ostringstream _oss;                                               \
			_oss << "Check failed: (" #actual ") == (" #expected "), got ["        \
				 << _actual_val << "] vs [" << _expected_val << "] at "            \
				 << __FILE__ << ':' << __LINE__;                                   \
			throw TestFailure(_oss.str());                                         \
		}                                                                          \
	} while (false)

#define TEST_CHECK_NEAR(actual, expected, eps)                                     \
	do {                                                                           \
		const double _actual_val = static_cast<double>(actual);                    \
		const double _expected_val = static_cast<double>(expected);                \
		const double _eps_val = static_cast<double>(eps);                          \
		if (!(std::abs(_actual_val - _expected_val) <= _eps_val)) {                \
			std::ostringstream _oss;                                               \
			_oss << "Check failed: |" #actual " - " #expected "| <= " << _eps_val  \
				 << ", got [" << _actual_val << "] vs [" << _expected_val          \
				 << "] at " << __FILE__ << ':' << __LINE__;                        \
			throw TestFailure(_oss.str());                                         \
		}                                                                          \
	} while (false)

#define TEST_CHECK_THROWS(expr, ExcType)                                           \
	do {                                                                           \
		bool _threw_expected = false;                                              \
		try {                                                                      \
			(void)(expr);                                                          \
		} catch (const ExcType&) {                                                 \
			_threw_expected = true;                                                \
		} catch (const std::exception& _ex) {                                      \
			std::ostringstream _oss;                                               \
			_oss << "Unexpected std::exception in (" #expr "): " << _ex.what()     \
				 << " at " << __FILE__ << ':' << __LINE__;                         \
			throw TestFailure(_oss.str());                                         \
		} catch (...) {                                                            \
			std::ostringstream _oss;                                               \
			_oss << "Unexpected non-std exception in (" #expr ") at "              \
				 << __FILE__ << ':' << __LINE__;                                   \
			throw TestFailure(_oss.str());                                         \
		}                                                                          \
		if (!_threw_expected) {                                                    \
			std::ostringstream _oss;                                               \
			_oss << "Expected exception " #ExcType " from (" #expr ") at "         \
				 << __FILE__ << ':' << __LINE__;                                   \
			throw TestFailure(_oss.str());                                         \
		}                                                                          \
	} while (false)

	// Compile-time contract verification for v2.4 and v2.5
	static_assert(szz::contract::TaskScheduler<szz::ThreadPool>);
	static_assert(szz::contract::DetachedSubmittable<szz::ThreadPool, void (*)()>);
	static_assert(szz::contract::FutureSubmittable<szz::ThreadPool, int (*)(int), int>);
	static_assert(!szz::contract::BatchSubmittable<szz::ThreadPool>);
	static_assert(szz::contract::HasStats<szz::ThreadPool>);
	static_assert(!szz::contract::HasBatchStats<szz::ThreadPool>);

	static_assert(szz::contract::TaskScheduler<szz::batch::ThreadPool>);
	static_assert(szz::contract::DetachedSubmittable<szz::batch::ThreadPool, void (*)()>);
	static_assert(szz::contract::FutureSubmittable<szz::batch::ThreadPool, int (*)(int), int>);
	static_assert(szz::contract::BatchSubmittable<szz::batch::ThreadPool>);
	static_assert(szz::contract::HasStats<szz::batch::ThreadPool>);
	static_assert(szz::contract::HasBatchStats<szz::batch::ThreadPool>);

	template <class Pool>
	void test_zero_threads_throws() {
		TEST_CHECK_THROWS(Pool(0), std::invalid_argument);
	}

	template <class Pool>
	void test_enqueue_return_move_only_and_exception() {
		Pool pool(4);

		auto f_int = pool.enqueue([](int x, int y) { return x + y; }, 19, 23);
		TEST_CHECK_EQ(f_int.get(), 42);

		struct Pair {
			int a;
			double b;
		};
		auto f_struct = pool.enqueue([] { return Pair{ 7, 2.5 }; });
		const Pair res = f_struct.get();
		TEST_CHECK_EQ(res.a, 7);
		TEST_CHECK_NEAR(res.b, 2.5, 1e-12);

		auto cap = std::make_unique<int>(100);
		auto arg = std::make_unique<int>(23);
		auto f_move_only = pool.enqueue(
			[c = std::move(cap)](std::unique_ptr<int> a) {
				return *c + *a;
			},
			std::move(arg));
		TEST_CHECK_EQ(f_move_only.get(), 123);

		auto f_void = pool.enqueue([] {});
		f_void.get();

		auto f_err = pool.enqueue([]() -> int {
			throw std::runtime_error("expected_future_error");
		});
		TEST_CHECK_THROWS(f_err.get(), std::runtime_error);

		TEST_CHECK_EQ(pool.enqueue([] { return 99; }).get(), 99);
		pool.wait_all();
	}

	template <class Pool>
	void test_enqueue_detached_exception_containment() {
		Pool pool(4);
		std::atomic<int> ok_count{ 0 };

		for (int i = 0; i < 100; ++i) {
			if (i % 2 == 0) {
				auto token = std::make_unique<int>(1);
				pool.enqueue_detached(
					[&ok_count](std::unique_ptr<int> t) {
						ok_count.fetch_add(*t, std::memory_order_relaxed);
					},
					std::move(token));
			}
			else {
				pool.enqueue_detached([] {
					throw std::runtime_error("detached_error");
				});
			}
		}

		pool.wait_all();
		TEST_CHECK_EQ(ok_count.load(std::memory_order_relaxed), 50);
	}

	template <class Pool>
	void test_wait_all_inflight_barrier() {
		Pool pool(4);
		std::latch started(1);
		std::latch release(1);
		std::atomic<int> finished{ 0 };

		pool.enqueue_detached([&] {
			started.count_down();
			release.wait();
			finished.fetch_add(1, std::memory_order_relaxed);
		});

		started.wait();
		TEST_CHECK_EQ(pool.pending_tasks(), std::size_t{ 0 });

		std::atomic<bool> wait_done{ false };
		std::jthread waiter([&] {
			pool.wait_all();
			wait_done.store(true, std::memory_order_release);
		});

		std::this_thread::sleep_for(std::chrono::milliseconds(15));
		TEST_CHECK(!wait_done.load(std::memory_order_acquire));

		release.count_down();
		waiter.join();
		TEST_CHECK(wait_done.load(std::memory_order_acquire));
		TEST_CHECK_EQ(finished.load(std::memory_order_relaxed), 1);

		if constexpr (szz::contract::BatchSubmittable<Pool>) {
			std::latch batch_started(1);
			std::latch batch_release(1);
			std::atomic<int> batch_finished{ 0 };
			{
				auto batch = pool.make_batch(2);
				batch.submit([&] {
					batch_started.count_down();
					batch_release.wait();
					batch_finished.fetch_add(1, std::memory_order_relaxed);
				});
				batch.submit([&] {
					batch_finished.fetch_add(1, std::memory_order_relaxed);
				});
			}

			batch_started.wait();
			std::atomic<bool> batch_wait_done{ false };
			std::jthread batch_waiter([&] {
				pool.wait_all();
				batch_wait_done.store(true, std::memory_order_release);
			});

			std::this_thread::sleep_for(std::chrono::milliseconds(15));
			TEST_CHECK(!batch_wait_done.load(std::memory_order_acquire));

			batch_release.count_down();
			batch_waiter.join();
			TEST_CHECK(batch_wait_done.load(std::memory_order_acquire));
			TEST_CHECK_EQ(batch_finished.load(std::memory_order_relaxed), 2);
		}
	}

	template <class Pool>
	void test_shutdown_idempotent_and_concurrent() {
		Pool pool(4);
		std::atomic<int> completed{ 0 };

		for (int i = 0; i < 64; ++i) {
			pool.enqueue_detached([&completed] {
				completed.fetch_add(1, std::memory_order_relaxed);
			});
		}

		{
			std::barrier gate(4);
			std::vector<std::jthread> stoppers;
			stoppers.reserve(4);
			for (int i = 0; i < 4; ++i) {
				stoppers.emplace_back([&] {
					gate.arrive_and_wait();
					pool.shutdown();
				});
			}
		}

		TEST_CHECK(pool.is_stopped());
		pool.shutdown();
		TEST_CHECK(pool.is_stopped());
		TEST_CHECK_EQ(completed.load(std::memory_order_acquire), 64);

		TEST_CHECK_THROWS(pool.enqueue([] { return 1; }), std::runtime_error);
		TEST_CHECK_THROWS(pool.enqueue_detached([] {}), std::runtime_error);
	}

	template <class Pool>
	void test_shutdown_from_worker_self_detach() {
		std::latch shutdown_returned(1);
		{
			Pool pool(4);
			pool.enqueue_detached([&pool, &shutdown_returned] {
				pool.shutdown();
				shutdown_returned.count_down();
			});

			shutdown_returned.wait();
			TEST_CHECK(pool.is_stopped());
			pool.wait_all();
			std::this_thread::sleep_for(std::chrono::milliseconds(20));
		}
	}

	template <class Pool>
	void test_multi_producer_correctness() {
		constexpr std::size_t kProducers = 4;
		constexpr std::size_t kPerProducer = 2000;
		constexpr std::size_t kTotal = kProducers * kPerProducer;

		{
			Pool pool(4);
			std::vector<int> slots(kTotal, 0);
			{
				std::vector<std::jthread> producers;
				producers.reserve(kProducers);
				for (std::size_t p = 0; p < kProducers; ++p) {
					producers.emplace_back([&, p] {
						const std::size_t base = p * kPerProducer;
						for (std::size_t i = 0; i < kPerProducer; ++i) {
							const std::size_t idx = base + i;
							pool.enqueue_detached([&slots, idx] {
								slots[idx] = static_cast<int>(idx * 3 + 1);
							});
						}
					});
				}
			}
			pool.wait_all();
			for (std::size_t i = 0; i < kTotal; ++i) {
				TEST_CHECK_EQ(slots[i], static_cast<int>(i * 3 + 1));
			}
		}

		if constexpr (szz::contract::BatchSubmittable<Pool>) {
			Pool pool(4);
			std::vector<int> slots(kTotal, 0);
			{
				std::vector<std::jthread> producers;
				producers.reserve(kProducers);
				for (std::size_t p = 0; p < kProducers; ++p) {
					producers.emplace_back([&, p] {
						const std::size_t base = p * kPerProducer;
						auto batch = pool.make_batch();
						for (std::size_t i = 0; i < kPerProducer; ++i) {
							const std::size_t idx = base + i;
							batch.submit([&slots, idx] {
								slots[idx] = static_cast<int>(idx * 5 + 2);
							});
						}
					});
				}
			}
			pool.wait_all();
			for (std::size_t i = 0; i < kTotal; ++i) {
				TEST_CHECK_EQ(slots[i], static_cast<int>(i * 5 + 2));
			}
		}
	}

	template <class Pool>
	void test_batch_tail_flush_and_post_stop() {
		if constexpr (szz::contract::BatchSubmittable<Pool>) {
			for (std::size_t count : { std::size_t{ 1 }, std::size_t{ 63 }, std::size_t{ 64 },
									   std::size_t{ 65 }, std::size_t{ 130 } }) {
				Pool pool(4);
				std::vector<int> done(count, 0);
				{
					auto batch = pool.make_batch();
					for (std::size_t i = 0; i < count; ++i) {
						batch.submit([&done, i] { done[i] = 1; });
					}
				}
				pool.wait_all();
				for (std::size_t i = 0; i < count; ++i) {
					TEST_CHECK_EQ(done[i], 1);
				}
			}

			{
				Pool pool(4);
				std::vector<int> done(20, 0);
				{
					auto batch = pool.make_batch(64);
					for (std::size_t i = 0; i < 10; ++i) {
						batch.submit([&done, i] { done[i] = 2; });
					}
					batch.flush();
					for (std::size_t i = 10; i < 20; ++i) {
						batch.submit([&done, i] { done[i] = 2; });
					}
				}
				pool.wait_all();
				for (std::size_t i = 0; i < 20; ++i) {
					TEST_CHECK_EQ(done[i], 2);
				}
			}

			{
				Pool pool(2);
				pool.shutdown();
				{
					auto batch = pool.make_batch(8);
					batch.submit([] {});
					TEST_CHECK_THROWS(batch.flush(), std::runtime_error);
				}
				{
					auto batch = pool.make_batch(8);
					batch.submit([] {});
				}
			}
		}
	}

	template <class Pool>
	void test_numerical_elementwise_and_gemm() {
		{
			constexpr std::size_t kVecN = 4096;
			std::vector<double> a(kVecN, 1.5);
			std::vector<double> b(kVecN, 2.0);
			std::vector<double> c(kVecN, 0.0);

			Pool pool(4);
			const std::size_t chunk = 128;
			if constexpr (szz::contract::BatchSubmittable<Pool>) {
				auto batch = pool.make_batch();
				for (std::size_t begin = 0; begin < kVecN; begin += chunk) {
					const std::size_t end = std::min(begin + chunk, kVecN);
					batch.submit([&a, &b, &c, begin, end] {
						for (std::size_t i = begin; i < end; ++i) {
							c[i] = a[i] * b[i] + 1.0;
						}
					});
				}
			}
			else {
				for (std::size_t begin = 0; begin < kVecN; begin += chunk) {
					const std::size_t end = std::min(begin + chunk, kVecN);
					pool.enqueue_detached([&a, &b, &c, begin, end] {
						for (std::size_t i = begin; i < end; ++i) {
							c[i] = a[i] * b[i] + 1.0;
						}
					});
				}
			}
			pool.wait_all();
			for (std::size_t i = 0; i < kVecN; ++i) {
				TEST_CHECK_NEAR(c[i], 4.0, 1e-12);
			}
		}

		{
			constexpr std::size_t N = 64;
			std::vector<double> A(N * N, 1.5);
			std::vector<double> B(N * N, 2.0);
			std::vector<double> C(N * N, 0.0);

			Pool pool(4);
			std::vector<std::future<void>> futs;
			futs.reserve(N / 4);
			for (std::size_t row = 0; row < N; row += 4) {
				const std::size_t end_row = std::min(row + 4, N);
				futs.emplace_back(pool.enqueue([&A, &B, &C, N, row, end_row] {
					for (std::size_t i = row; i < end_row; ++i) {
						for (std::size_t k = 0; k < N; ++k) {
							const double temp = A[i * N + k];
							for (std::size_t j = 0; j < N; ++j) {
								C[i * N + j] += temp * B[k * N + j];
							}
						}
					}
				}));
			}
			for (auto& f : futs) {
				f.get();
			}
			const double expected = 3.0 * static_cast<double>(N);
			for (std::size_t i = 0; i < C.size(); ++i) {
				TEST_CHECK_NEAR(C[i], expected, 1e-9);
			}
		}
	}

	// Adversarial TSan red-light test targeting ThreadPoolV25.h:297 (unlocked read of
	// workers_.size() inside enqueue_bulk) racing with line 324 (local_workers.swap(workers_)
	// inside shutdown). Deliberately uses memory_order_relaxed so no happens-before edge hides
	// the unsynchronized read/write access on workers_.
	void test_v25_enqueue_bulk_shutdown_race() {
		constexpr int kRounds = 128;
		int post_flush_shutdown_rounds = 0;

		for (int round = 0; round < kRounds; ++round) {
			szz::batch::ThreadPool pool(4);
			std::barrier start_gate(2);
			std::atomic<int> batches_flushed{ 0 };

			std::jthread producer([&] {
				start_gate.arrive_and_wait();
				try {
					auto batch = pool.make_batch(1);
					for (int iter = 0; iter < 64; ++iter) {
						batch.submit([] {});
						batches_flushed.fetch_add(1, std::memory_order_relaxed);
					}
				}
				catch (const std::runtime_error&) {
					// Expected once shutdown() sets stop_ = true.
				}
			});

			std::jthread stopper([&] {
				start_gate.arrive_and_wait();
				int spins = 0;
				while (batches_flushed.load(std::memory_order_relaxed) < 1 && spins < 200000) {
					++spins;
					std::this_thread::yield();
				}
				pool.shutdown();
			});

			producer.join();
			stopper.join();
			TEST_CHECK(pool.is_stopped());
			if (batches_flushed.load(std::memory_order_relaxed) >= 1) {
				++post_flush_shutdown_rounds;
			}
		}

		TEST_CHECK(post_flush_shutdown_rounds >= kRounds / 2);
	}

	struct TestRunner {
		int passed{ 0 };
		int failed{ 0 };

		template <class Fn>
		void run(std::string_view name, Fn&& fn) {
			std::cout << "[RUN ] " << name << std::endl;
			try {
				fn();
				++passed;
				std::cout << "[PASS] " << name << std::endl;
			}
			catch (const std::exception& ex) {
				++failed;
				std::cerr << "[FAIL] " << name << ": " << ex.what() << std::endl;
			}
			catch (...) {
				++failed;
				std::cerr << "[FAIL] " << name << ": unknown exception" << std::endl;
			}
		}
	};

	template <class Pool>
	void register_pool_tests(TestRunner& runner, std::string_view prefix) {
		auto run_named = [&](std::string_view suffix, auto fn) {
			std::string name(prefix);
			name.push_back('.');
			name.append(suffix);
			runner.run(name, fn);
		};

		run_named("zero_threads_throws", [] { test_zero_threads_throws<Pool>(); });
		run_named("enqueue_return_move_only_and_exception", [] { test_enqueue_return_move_only_and_exception<Pool>(); });
		run_named("enqueue_detached_exception_containment", [] { test_enqueue_detached_exception_containment<Pool>(); });
		run_named("wait_all_inflight_barrier", [] { test_wait_all_inflight_barrier<Pool>(); });
		run_named("shutdown_idempotent_and_concurrent", [] { test_shutdown_idempotent_and_concurrent<Pool>(); });
		run_named("shutdown_from_worker_self_detach", [] { test_shutdown_from_worker_self_detach<Pool>(); });
		run_named("multi_producer_correctness", [] { test_multi_producer_correctness<Pool>(); });
		run_named("batch_tail_flush_and_post_stop", [] { test_batch_tail_flush_and_post_stop<Pool>(); });
		run_named("numerical_elementwise_and_gemm", [] { test_numerical_elementwise_and_gemm<Pool>(); });
	}

} // namespace

int main(int argc, char** argv) {
	std::string_view suite = "all";
	for (int i = 1; i < argc; ++i) {
		const std::string_view arg(argv[i]);
		if (arg == "--suite" && i + 1 < argc) {
			suite = argv[++i];
		}
		else {
			std::cerr << "Usage: " << argv[0] << " [--suite core|v25_race|all]\n";
			return 2;
		}
	}

	if (suite != "core" && suite != "v25_race" && suite != "all") {
		std::cerr << "Unknown suite: " << suite << " (expected core, v25_race, or all)\n";
		return 2;
	}

	TestRunner runner;

	if (suite == "core" || suite == "all") {
		register_pool_tests<szz::ThreadPool>(runner, "v24");
		register_pool_tests<szz::batch::ThreadPool>(runner, "v25");
	}

	if (suite == "v25_race" || suite == "all") {
		runner.run("v25.enqueue_bulk_shutdown_tsan_race", test_v25_enqueue_bulk_shutdown_race);
	}

	std::cout << "\nSummary: " << runner.passed << " passed, " << runner.failed << " failed\n";
	return runner.failed == 0 ? 0 : 1;
}
