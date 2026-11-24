#pragma once

#include <concepts>
#include <cstddef>
#include <future>
#include <type_traits>
#include <utility>

namespace szz::contract {

	template <class P>
	concept TaskScheduler = requires(P& p, const P& cp) {
		{ p.wait_all() }           -> std::same_as<void>;
		{ p.shutdown() } noexcept  -> std::same_as<void>;
		{ cp.pending_tasks() } noexcept -> std::same_as<std::size_t>;
		{ cp.is_stopped() } noexcept    -> std::same_as<bool>;
	};

	template <class P, class F, class... Args>
	concept DetachedSubmittable = std::invocable<F, Args...> && requires(P& p, F&& f, Args&&... args) {
		{ p.enqueue_detached(std::forward<F>(f), std::forward<Args>(args)...) } -> std::same_as<void>;
	};

	template <class P, class F, class... Args>
	concept FutureSubmittable = std::invocable<F, Args...> && requires(P& p, F&& f, Args&&... args) {
		{ p.enqueue(std::forward<F>(f), std::forward<Args>(args)...) }
			-> std::same_as<std::future<std::invoke_result_t<F, Args...>>>;
	};

	template <class P>
	concept BatchSubmittable = requires(P& p, void (*fn)()) {
		{ p.make_batch().submit(fn) } -> std::same_as<void>;
		{ p.make_batch().flush() }    -> std::same_as<void>;
	};

	template <class P>
	concept HasStats = requires(const P& p) {
		{ p.park_count() } noexcept      -> std::same_as<std::size_t>;
		{ p.max_queue_depth() } noexcept -> std::same_as<std::size_t>;
	};

	template <class P>
	concept HasBatchStats = requires(const P& p) {
		{ p.consumer_acquisitions() } noexcept -> std::same_as<std::size_t>;
		{ p.avg_take() } noexcept              -> std::same_as<double>;
		{ p.steady_max_depth() } noexcept      -> std::same_as<std::size_t>;
	};

} // namespace szz::contract
