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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class})
public class FutureTest {

    private NioEventloop eventloop;

    @Before
    public void before() {
        eventloop = new NioEventloop();
        eventloop.start();
    }

    @After
    public void after() throws InterruptedException {
        if (eventloop != null) {
            eventloop.shutdown();
            assertTrue(eventloop.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    public void test_thenOnCompletedFuture(){
        Future future = Future.newFuture();
        future.eventloop = eventloop;

        String result = "foobar";
        future.complete(result);

        CountDownLatch executed = new CountDownLatch(1);
        AtomicReference valueRef = new AtomicReference();
        AtomicReference throwableRef = new AtomicReference();

        future.then((BiConsumer<Object, Throwable>) (o, throwable) -> {
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
        Future future = Future.newFuture();
        future.eventloop = eventloop;

        future.completeExceptionally(null);
    }

    @Test
    public void test_completeExceptionally(){
        Future future = Future.newFuture();
        future.eventloop = eventloop;

        CountDownLatch executed = new CountDownLatch(1);
        AtomicReference valueRef = new AtomicReference();
        AtomicReference throwableRef = new AtomicReference();
        future.then((BiConsumer<Object, Throwable>) (o, throwable) -> {
            valueRef.set(o);
            throwableRef.set(throwable);
            executed.countDown();
        });

        Exception exception = new Exception();
        future.completeExceptionally(exception);

        assertOpenEventually(executed);
        assertNull(valueRef.get());
        assertSame(exception, throwableRef.get());
    }


    @Test(expected = IllegalStateException.class)
    public void test_completeExceptionally_whenAlreadyCompleted(){
        Future future = Future.newFuture();
        future.eventloop = eventloop;

        future.completeExceptionally(new Throwable());
        future.completeExceptionally(new Throwable());
    }

    @Test
    public void test_complete(){
        Future future = Future.newFuture();
        future.eventloop = eventloop;

        CountDownLatch executed = new CountDownLatch(1);
        AtomicReference valueRef = new AtomicReference();
        AtomicReference throwableRef = new AtomicReference();
        future.then((BiConsumer<Object, Throwable>) (o, throwable) -> {
            valueRef.set(o);
            throwableRef.set(throwable);
            executed.countDown();
        });

        String result = "foobar";
        future.complete(result);

        assertOpenEventually(executed);
        assertSame(result, valueRef.get());
        assertNull(throwableRef.get());
    }

    @Test(expected = IllegalStateException.class)
    public void test_complete_whenAlreadyCompleted(){
        Future future = Future.newFuture();
        future.eventloop = eventloop;

        future.complete("first");
        future.complete("second");
    }
}
