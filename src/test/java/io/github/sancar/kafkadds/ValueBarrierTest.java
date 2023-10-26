package io.github.sancar.kafkadds;

import org.junit.Test;
import io.github.sancar.kafkadds.util.ValueBarrier;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class ValueBarrierTest {

    @Test
    public void TestAwaitAlreadyMetCondition() throws InterruptedException {
        ValueBarrier barrier = new ValueBarrier();
        barrier.setIfBigger(1);

        boolean done = barrier.await(1, 1, TimeUnit.SECONDS);
        assertTrue(done);
    }

    @Test
    public void TestTimeout() throws InterruptedException {
        ValueBarrier barrier = new ValueBarrier();
        barrier.setIfBigger(1);

        boolean done = barrier.await(2, 100, TimeUnit.MILLISECONDS);
        assertFalse(done);
    }

    @Test
    public void TestWakeup() throws InterruptedException {
        ValueBarrier barrier = new ValueBarrier();

        new Thread(() -> {
            try {
                Thread.sleep(100);
                barrier.setIfBigger(2);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }).start();

        barrier.await(2, 1, TimeUnit.DAYS);
    }

}
