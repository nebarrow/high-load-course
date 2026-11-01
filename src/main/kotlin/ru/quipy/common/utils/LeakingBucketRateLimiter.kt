package ru.quipy.common.utils

import java.util.concurrent.atomic.AtomicInteger

class LeakingBucketRateLimiter {
    private val level = AtomicInteger(0)

    fun tryAcquire(bucketSize: Double): Boolean {
        while (true) {
            val current = level.get()
            if (current + 1 > bucketSize) return false
            if (level.compareAndSet(current, current + 1)) return true
        }
    }

    fun leak() {
        level.decrementAndGet()
    }
}
