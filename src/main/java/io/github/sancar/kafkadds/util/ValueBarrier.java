package io.github.sancar.kafkadds.util;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A synchronization barrier that blocks until a shared long value
 * is greater than or equal to a target value. The barrier can be
 * used to coordinate multiple coroutines that need to wait for a
 * specific condition to be met before proceeding.
 */
public class ValueBarrier {
    private final Lock mutex = new ReentrantLock();
    private final Condition cond = mutex.newCondition();
    private final AtomicLong sharedLong = new AtomicLong(-1);

    /**
     * Updates the shared long value to be the maximum of its current
     * value and the specified value. If the specified value is less
     * than or equal to the current value, the method does nothing.
     *
     * @param newValue the new value to set, if it is greater than
     *                 the current value
     */
    public void setIfBigger(long newValue) {
        try {
            mutex.lock();
            if (newValue > sharedLong.get()) {
                sharedLong.set(newValue);
                cond.signalAll();
            }
        } finally {
            mutex.unlock();
        }
    }

    /**
     * Waits until the shared long value is greater than or equal to
     * the target value. If the target value is already met, the
     * method returns immediately. Otherwise, the calling coroutine
     * blocks until another coroutine updates the shared long value to
     * be greater than or equal to the target value.
     *
     * @param targetValue the target value to wait for
     * @param timeout     the maximum time to wait
     * @param unit        the time unit of the timeout argument
     * @return true if the shared long value was updated to be greater
     * than or equal to the target value before the timeout,
     * false otherwise
     */
    public boolean await(long targetValue, int timeout, TimeUnit unit) throws InterruptedException {
        if (sharedLong.get() >= targetValue) {
            return true;
        }
        try {
            mutex.lock();
            long deadLineInMillis = System.currentTimeMillis() + unit.toMillis(timeout);
            while (sharedLong.get() < targetValue) {
                long sleepTime = deadLineInMillis - System.currentTimeMillis();
                if (sleepTime <= 0) {
                    return false;
                }
                cond.await(sleepTime, TimeUnit.MILLISECONDS);
            }
        } finally {
            mutex.unlock();
        }
        return true;
    }
}
