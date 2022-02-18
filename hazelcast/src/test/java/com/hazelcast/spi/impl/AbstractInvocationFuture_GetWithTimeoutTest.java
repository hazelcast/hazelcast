/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.spi.impl;

import com.hazelcast.spi.impl.AbstractInvocationFuture.WaitNode;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.spi.impl.AbstractInvocationFuture.UNRESOLVED;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AbstractInvocationFuture_GetWithTimeoutTest extends AbstractInvocationFuture_AbstractTest {

    @Test(expected = NullPointerException.class)
    public void whenNullTimeunit() throws Exception {
        future.get(1, null);
    }

    @Test
    public void whenZeroTimeout_butResponseAvailable() throws Exception {
        future.complete(value);
        Future getFuture = spawn(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                return future.get(0, SECONDS);
            }
        });
        assertCompletesEventually(getFuture);
        assertSame(value, getFuture.get());
    }

    @Test
    public void whenZeroTimeout_andNoResponseAvailable() throws Exception {
        Future getFuture = spawn(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                return future.get(0, SECONDS);
            }
        });

        assertCompletesEventually(getFuture);

        try {
            getFuture.get();
            fail();
        } catch (ExecutionException e) {
            assertInstanceOf(TimeoutException.class, e.getCause());
        }

        // we need to make sure the thread is removed from the waiters.
        assertSame(UNRESOLVED, future.getState());
    }

    @Test
    public void whenResponseAvailable() throws Exception {
        future.complete(value);
        Future getFuture = spawn(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                return future.get(10, SECONDS);
            }
        });
        assertCompletesEventually(getFuture);
        assertSame(value, getFuture.get());
    }

    @Test
    public void whenResultAlreadyAvailable_andInterruptFlagSet() throws Exception {
        future.complete(value);

        final AtomicBoolean interrupted = new AtomicBoolean();
        Future getFuture = spawn(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                // we set the interrupt flag.
                Thread.currentThread().interrupt();
                Object value = future.get(1, SECONDS);
                // and then we check if the interrupt flag is still set
                interrupted.set(Thread.currentThread().isInterrupted());
                return value;
            }
        });

        assertCompletesEventually(getFuture);
        assertSame(value, future.get());
        assertTrue(interrupted.get());
    }

    @Test
    public void whenResponseAvailableAfterSomeDelay() throws Exception {
        Future getFuture = spawn(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                return future.get(60, SECONDS);
            }
        });

        // wait till the thread is registered.
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertNotSame(UNRESOLVED, future.getState());
            }
        });

        future.complete(value);

        assertCompletesEventually(getFuture);
        assertSame(value, getFuture.get());
    }

    @Test
    public void whenTimeout() throws ExecutionException, InterruptedException {
        Future getFuture = spawn(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                return future.get(1, SECONDS);
            }
        });

        assertCompletesEventually(getFuture);

        try {
            getFuture.get();
            fail();
        } catch (ExecutionException e) {
            assertInstanceOf(TimeoutException.class, e.getCause());
        }

        // we need to make sure the thread is removed from the waiters.
        assertSame(UNRESOLVED, future.getState());
    }

    @Test
    public void whenInterruptedWhileWaiting() throws Exception {
        final AtomicReference<Thread> thread = new AtomicReference<Thread>();
        final AtomicBoolean interrupted = new AtomicBoolean();
        Future getFuture = spawn(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                thread.set(Thread.currentThread());
                try {
                    return future.get(1, HOURS);
                } finally {
                    interrupted.set(Thread.currentThread().isInterrupted());
                }
            }
        });

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertNotSame(UNRESOLVED, future.getState());
            }
        });

        sleepSeconds(5);
        thread.get().interrupt();

        assertCompletesEventually(getFuture);
        assertTrue(interrupted.get());

        try {
            future.get();
            fail();
        } catch (InterruptedException e) {
        }
    }

    @Test
    public void whenMultipleGetters() throws Exception {
        List<Future> getFutures = new LinkedList<Future>();
        for (int k = 0; k < 10; k++) {
            getFutures.add(spawn(new Callable<Object>() {
                @Override
                public Object call() throws Exception {
                    return future.get(1, DAYS);
                }
            }));
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertNotSame(UNRESOLVED, future.getState());
            }
        });

        sleepSeconds(5);
        future.complete(value);

        for (Future getFuture : getFutures) {
            assertCompletesEventually(getFuture);
            assertSame(value, future.get());
        }

        assertSame(value, future.getState());
    }

    @Test
    public void unregister() {
        for (int length = 1; length <= 5; length++) {
            for (int position = 0; position < length; position++) {
                unregister(length, position);
            }
        }
    }

    private void unregister(int length, int position) {
        TestFuture future = new TestFuture();
        Thread[] threads = createThread(length);
        for (int k = 0; k < length; k++) {
            if (k == 0) {
                future.compareAndSetState(UNRESOLVED, threads[k]);
            } else {
                WaitNode node = new WaitNode(threads[k], null);
                node.next = future.getState();
                future.compareAndSetState(future.getState(), node);
            }
        }

        future.unregisterWaiter(threads[position]);

        for (int k = 0; k < length; k++) {
            assertContains(future.getState(), threads[k], k != position);
        }
    }

    private void assertContains(Object state, Thread waiter, boolean contains) {
        boolean found = false;

        Object current = state;
        while (current != null) {
            Object currentWaiter = current.getClass() == WaitNode.class ? ((WaitNode) current).waiter : current;
            Object next = current.getClass() == WaitNode.class ? ((WaitNode) current).next : null;

            if (currentWaiter == waiter) {
                found = true;
                current = null;
            } else {
                current = next;
            }
        }

        assertEquals(contains, found);
    }

    private Thread[] createThread(int length) {
        Thread[] threads = new Thread[length];
        for (int k = 0; k < threads.length; k++) {
            threads[k] = new Thread();
        }
        return threads;
    }
}
