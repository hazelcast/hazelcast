package com.hazelcast.concurrent.atomiclong;

import com.hazelcast.core.AsyncAtomicLong;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastJUnit4ClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(HazelcastJUnit4ClassRunner.class)
@Category(ParallelTest.class)
public class AsyncAtomicLongTest extends HazelcastTestSupport {

    @Test
    public void asyncGetAndSet() {
        HazelcastInstance hazelcastInstance = createHazelcastInstanceFactory(1).newHazelcastInstance();
        AsyncAtomicLong an = (AsyncAtomicLong) hazelcastInstance.getAtomicLong("asyncGetAndSet");

        final CountDownLatch latch = new CountDownLatch(1);
        an.asyncGetAndSet(10).andThen(new AbstractExecutionCallback<Long>() {
            @Override
            public void onResponse(Long response) {
                assertEquals(new Long(0), response);
                latch.countDown();
            }
        });

        assertCompletes(latch);
        assertEquals(10L, an.get());
    }

    @Test
    public void asyncAddAndGet() {
        HazelcastInstance hazelcastInstance = createHazelcastInstanceFactory(1).newHazelcastInstance();
        AsyncAtomicLong an = (AsyncAtomicLong) hazelcastInstance.getAtomicLong("asyncAddAndGet");

        final CountDownLatch latch = new CountDownLatch(1);
        an.asyncAddAndGet(10).andThen(new AbstractExecutionCallback<Long>() {
            @Override
            public void onResponse(Long response) {
                assertEquals(new Long(10), response);
                latch.countDown();
            }
        });

        assertCompletes(latch);
        assertEquals(10L, an.get());
    }

    @Test
    public void asyncCompareAndSet() {
        HazelcastInstance hazelcastInstance = createHazelcastInstanceFactory(1).newHazelcastInstance();
        AsyncAtomicLong an = (AsyncAtomicLong) hazelcastInstance.getAtomicLong("asyncCompareAndSet");

        final CountDownLatch latch = new CountDownLatch(1);
        an.asyncCompareAndSet(0,10).andThen(new AbstractExecutionCallback<Boolean>() {
            @Override
            public void onResponse(Boolean response) {
                assertEquals(Boolean.TRUE, response);
                latch.countDown();
            }
        });

        assertCompletes(latch);
        assertEquals(10L, an.get());
    }

    @Test
    public void asyncDecrementAndGet() {
        HazelcastInstance hazelcastInstance = createHazelcastInstanceFactory(1).newHazelcastInstance();
        AsyncAtomicLong an = (AsyncAtomicLong) hazelcastInstance.getAtomicLong("asyncDecrementAndGet");

        final CountDownLatch latch = new CountDownLatch(1);
        an.asyncDecrementAndGet().andThen(new AbstractExecutionCallback<Long>() {
            @Override
            public void onResponse(Long response) {
                assertEquals(new Long(-1), response);
                latch.countDown();
            }
        });

        assertCompletes(latch);
        assertEquals(-1, an.get());
    }

    @Test
    public void asyncGet() {
        HazelcastInstance hazelcastInstance = createHazelcastInstanceFactory(1).newHazelcastInstance();
        AsyncAtomicLong an = (AsyncAtomicLong) hazelcastInstance.getAtomicLong("asyncGet");

        final CountDownLatch latch = new CountDownLatch(1);
        an.asyncGet().andThen(new AbstractExecutionCallback<Long>() {
            @Override
            public void onResponse(Long response) {
                assertEquals(new Long(0), response);
                latch.countDown();
            }
        });

        assertCompletes(latch);
        assertEquals(0, an.get());
    }

    @Test
    public void asyncGetAndAdd() {
        HazelcastInstance hazelcastInstance = createHazelcastInstanceFactory(1).newHazelcastInstance();
        AsyncAtomicLong an = (AsyncAtomicLong) hazelcastInstance.getAtomicLong("asyncGetAndAdd");

        final CountDownLatch latch = new CountDownLatch(1);
        an.asyncGetAndAdd(10).andThen(new AbstractExecutionCallback<Long>() {
            @Override
            public void onResponse(Long response) {
                assertEquals(new Long(0), response);
                latch.countDown();
            }
        });

        assertCompletes(latch);
        assertEquals(10, an.get());
    }

    @Test
    public void asyncIncrementAndGet() {
        HazelcastInstance hazelcastInstance = createHazelcastInstanceFactory(1).newHazelcastInstance();
        AsyncAtomicLong an = (AsyncAtomicLong) hazelcastInstance.getAtomicLong("asyncIncrementAndGet");

        final CountDownLatch latch = new CountDownLatch(1);
        an.asyncIncrementAndGet().andThen(new AbstractExecutionCallback<Long>() {
            @Override
            public void onResponse(Long response) {
                assertEquals(new Long(1), response);
                latch.countDown();
            }
        });

        assertCompletes(latch);
        assertEquals(1, an.get());
    }

    @Test
    public void asyncGetAndIncrement() {
        HazelcastInstance hazelcastInstance = createHazelcastInstanceFactory(1).newHazelcastInstance();
        AsyncAtomicLong an = (AsyncAtomicLong) hazelcastInstance.getAtomicLong("asyncGetAndIncrement");

        final CountDownLatch latch = new CountDownLatch(1);
        an.asyncGetAndIncrement().andThen(new AbstractExecutionCallback<Long>() {
            @Override
            public void onResponse(Long response) {
                assertEquals(new Long(0), response);
                latch.countDown();
            }
        });

        assertCompletes(latch);
        assertEquals(1, an.get());
    }

    @Test
    public void asyncSet() {
        HazelcastInstance hazelcastInstance = createHazelcastInstanceFactory(1).newHazelcastInstance();
        AsyncAtomicLong an = (AsyncAtomicLong) hazelcastInstance.getAtomicLong("asyncSet");

        final CountDownLatch latch = new CountDownLatch(1);
        an.asyncSet(10).andThen(new AbstractExecutionCallback<Void>() {
            @Override
            public void onResponse(Void response) {
                latch.countDown();
            }
        });

        assertCompletes(latch);
        assertEquals(10, an.get());
    }

    private static abstract class AbstractExecutionCallback<E> implements ExecutionCallback<E>{
        @Override
        public void onResponse(E response) {
        }

        @Override
        public void onFailure(Throwable t) {
        }
    }

    private void assertCompletes(CountDownLatch latch) {
        try {
            if (!latch.await(1, TimeUnit.MINUTES)) {
                fail("Latch didn't complete within timeout");
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
