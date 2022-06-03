package com.hazelcast.tpc.engine;


import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.tpc.engine.nio.NioEventloop;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static com.hazelcast.test.HazelcastTestSupport.assertOpenEventually;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class})
public class PromiseTest {

    private NioEventloop eventloop;
    private PromiseAllocator promiseAllocator;

    @Before
    public void before() {
        eventloop = new NioEventloop();
        eventloop.start();

        promiseAllocator = new PromiseAllocator(eventloop, 1024);
    }

    @After
    public void after() throws InterruptedException {
        if (eventloop != null) {
            eventloop.shutdown();
            assertTrue(eventloop.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    public void test_pooling(){
        Promise promise = new Promise(eventloop);

        promise.allocator = promiseAllocator;

        assertEquals(1, promise.refCount);
        assertEquals(0, promiseAllocator.size());

        promise.acquire();
        assertEquals(2, promise.refCount);
        assertEquals(0, promiseAllocator.size());

        promise.release();
        assertEquals(1, promise.refCount);
        assertEquals(0, promiseAllocator.size());

        promise.release();
        assertFalse(promise.isDone());
        assertEquals(1, promiseAllocator.size());
    }

    @Test
    public void test_thenOnCompletedFuture(){
        Promise promise = new Promise(eventloop);

        String result = "foobar";
        promise.complete(result);

        CountDownLatch executed = new CountDownLatch(1);
        AtomicReference valueRef = new AtomicReference();
        AtomicReference throwableRef = new AtomicReference();

        promise.then((BiConsumer<Object, Throwable>) (o, throwable) -> {
            valueRef.set(o);
            throwableRef.set(throwable);
            executed.countDown();
        });

        assertOpenEventually(executed);
        assertSame(result, valueRef.get());
        assertNull(throwableRef.get());
    }

    @Test(expected = NullPointerException.class)
    public void test_completeExceptionallyWhenNull(){
        Promise future = new Promise(eventloop);

        future.completeExceptionally(null);
    }

    @Test
    public void test_completeExceptionally(){
        Promise promise = new Promise(eventloop);

        CountDownLatch executed = new CountDownLatch(1);
        AtomicReference valueRef = new AtomicReference();
        AtomicReference throwableRef = new AtomicReference();
        promise.then((BiConsumer<Object, Throwable>) (o, throwable) -> {
            valueRef.set(o);
            throwableRef.set(throwable);
            executed.countDown();
        });

        Exception exception = new Exception();
        promise.completeExceptionally(exception);

        assertOpenEventually(executed);
        assertNull(valueRef.get());
        assertSame(exception, throwableRef.get());
    }


    @Test(expected = IllegalStateException.class)
    public void test_completeExceptionally_whenAlreadyCompleted(){
        Promise promise = new Promise(eventloop);

        promise.completeExceptionally(new Throwable());
        promise.completeExceptionally(new Throwable());
    }

    @Test
    public void test_complete(){
        Promise promise = new Promise(eventloop);

        CountDownLatch executed = new CountDownLatch(1);
        AtomicReference valueRef = new AtomicReference();
        AtomicReference throwableRef = new AtomicReference();
        promise.then((BiConsumer<Object, Throwable>) (o, throwable) -> {
            valueRef.set(o);
            throwableRef.set(throwable);
            executed.countDown();
        });

        String result = "foobar";
        promise.complete(result);

        assertOpenEventually(executed);
        assertSame(result, valueRef.get());
        assertNull(throwableRef.get());
    }

    @Test(expected = IllegalStateException.class)
    public void test_complete_whenAlreadyCompleted(){
        Promise promise = new Promise(eventloop);

        promise.complete("first");
        promise.complete("second");
    }
}
