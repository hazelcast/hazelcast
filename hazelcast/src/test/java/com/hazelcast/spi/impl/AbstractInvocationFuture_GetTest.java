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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.spi.impl.AbstractInvocationFuture.UNRESOLVED;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AbstractInvocationFuture_GetTest extends AbstractInvocationFuture_AbstractTest {

    @Test
    public void whenResultAlreadyAvailable() throws Exception {
        future.complete(value);

        Future getFuture = spawn(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                return future.get();
            }
        });

        assertCompletesEventually(getFuture);
        assertSame(value, future.get());
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
                Object value = future.get();
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
    public void whenSomeWaitingNeeded() throws ExecutionException, InterruptedException {
        future.complete(value);

        Future getFuture = spawn(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                return future.get();
            }
        });

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertNotSame(UNRESOLVED, future.getState());
            }
        });

        sleepSeconds(5);

        assertCompletesEventually(getFuture);
        assertSame(value, future.get());

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
                    return future.get();
                } finally {
                    interrupted.set(Thread.currentThread().isInterrupted());
                }
            }
        });

        assertTrueEventually(() -> assertNotSame(UNRESOLVED, future.getState()));

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
    public void whenMultipleGetters() throws ExecutionException, InterruptedException {
        List<Future> getFutures = new LinkedList<Future>();
        for (int k = 0; k < 10; k++) {
            getFutures.add(spawn(new Callable<Object>() {
                @Override
                public Object call() throws Exception {
                    return future.get();
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
}
