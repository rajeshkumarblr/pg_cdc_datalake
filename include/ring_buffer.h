#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <mutex>
#include <optional>
#include <vector>

/*
 * A bounded, thread-safe Single-Producer Single-Consumer (SPSC) ring buffer.
 *
 * The producer (WAL receiver thread) calls push() to enqueue items.
 * The consumer (Parquet writer thread) calls pop() or pop_batch() to dequeue.
 *
 * Design choices:
 *   - Uses a mutex + condition variables rather than pure atomics for simplicity
 *     and because the bottleneck is I/O (libpq reads / Parquet writes), not the
 *     ring buffer handoff.
 *   - Supports blocking with timeout so the consumer can periodically flush
 *     even when no new data arrives (time-based rotation).
 */
template <typename T>
class RingBuffer {
public:
    explicit RingBuffer(size_t capacity)
        : capacity_(capacity),
          buffer_(capacity),
          head_(0),
          tail_(0),
          count_(0),
          shutdown_(false) {}

    /*
     * Push an item into the ring buffer (producer side).
     * Blocks if the buffer is full, unless shutdown() has been called.
     * Returns false if shutdown was signaled.
     */
    bool push(T&& item) {
        std::unique_lock<std::mutex> lock(mutex_);
        not_full_.wait(lock, [this] {
            return count_ < capacity_ || shutdown_;
        });

        if (shutdown_) {
            return false;
        }

        buffer_[tail_] = std::move(item);
        tail_ = (tail_ + 1) % capacity_;
        ++count_;

        not_empty_.notify_one();
        return true;
    }

    /*
     * Pop a single item from the ring buffer (consumer side).
     * Blocks until an item is available or timeout expires.
     * Returns nullopt on timeout or shutdown.
     */
    std::optional<T> pop(std::chrono::milliseconds timeout) {
        std::unique_lock<std::mutex> lock(mutex_);
        bool available = not_empty_.wait_for(lock, timeout, [this] {
            return count_ > 0 || shutdown_;
        });

        if (!available || (count_ == 0 && shutdown_)) {
            return std::nullopt;
        }

        if (count_ == 0) {
            return std::nullopt;
        }

        T item = std::move(buffer_[head_]);
        head_ = (head_ + 1) % capacity_;
        --count_;

        not_full_.notify_one();
        return item;
    }

    /*
     * Pop a batch of items from the ring buffer (consumer side).
     * Returns up to max_count items. Waits up to timeout for the first item,
     * then drains as many as available without blocking.
     * Returns an empty vector on timeout with no data.
     */
    std::vector<T> pop_batch(size_t max_count, std::chrono::milliseconds timeout) {
        std::vector<T> batch;
        batch.reserve(max_count);

        std::unique_lock<std::mutex> lock(mutex_);

        /* Wait for at least one item or timeout */
        not_empty_.wait_for(lock, timeout, [this] {
            return count_ > 0 || shutdown_;
        });

        /* Drain up to max_count items */
        size_t to_pop = std::min(max_count, count_);
        for (size_t i = 0; i < to_pop; ++i) {
            batch.push_back(std::move(buffer_[head_]));
            head_ = (head_ + 1) % capacity_;
            --count_;
        }

        if (to_pop > 0) {
            not_full_.notify_one();
        }

        return batch;
    }

    /*
     * Signal shutdown. Wakes up all waiting threads so they can exit.
     */
    void shutdown() {
        std::lock_guard<std::mutex> lock(mutex_);
        shutdown_ = true;
        not_empty_.notify_all();
        not_full_.notify_all();
    }

    /*
     * Current number of items in the buffer.
     */
    size_t size() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return count_;
    }

    bool is_shutdown() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return shutdown_;
    }

private:
    const size_t              capacity_;
    std::vector<T>            buffer_;
    size_t                    head_;
    size_t                    tail_;
    size_t                    count_;
    bool                      shutdown_;
    mutable std::mutex        mutex_;
    std::condition_variable   not_empty_;
    std::condition_variable   not_full_;
};
