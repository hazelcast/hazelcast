/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.HazelcastException;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.ExpectedRuntimeException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.spi.impl.AbstractInvocationFuture.VOID;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * The Join forwards to {@link AbstractInvocationFuture#get()}. So most of the testing will be
 * in the {@link AbstractInvocationFuture_GetTest}. This test contains mostly the exception handling.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class AbstractInvocationFuture_JoinTest extends AbstractInvocationFuture_AbstractTest {

    @Test
    public void whenNormalResponse() throws ExecutionException, InterruptedException {
        future.complete(value);

        Future joinFuture = spawn(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                return future.join();
            }
        });

        assertCompletesEventually(joinFuture);
        assertSame(value, joinFuture.get());
    }

    @Test
    public void whenRuntimeException() throws Exception {
        ExpectedRuntimeException ex = new ExpectedRuntimeException();
        future.complete(ex);

        Future joinFuture = spawn(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                return future.join();
            }
        });

        assertCompletesEventually(joinFuture);
        try {
            joinFuture.get();
            fail();
        } catch (ExecutionException e) {
            assertSame(ex, e.getCause());
        }
    }

    @Test
    public void whenRegularException() throws Exception {
        Exception ex = new Exception();
        future.complete(ex);

        Future joinFuture = spawn(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                return future.join();
            }
        });

        assertCompletesEventually(joinFuture);
        try {
            joinFuture.get();
            fail();
        } catch (ExecutionException e) {
            // The 'ex' is wrapped in an unchecked HazelcastException
            HazelcastException hzEx = assertInstanceOf(HazelcastException.class, e.getCause());
            assertSame(ex, hzEx.getCause());
        }
    }

    @Test
    public void whenInterrupted() throws Exception {
        final AtomicReference<Thread> thread = new AtomicReference<Thread>();
        final AtomicBoolean interrupted = new AtomicBoolean();
        Future getFuture = spawn(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                thread.set(Thread.currentThread());
                try {
                    return future.join();
                } finally {
                    interrupted.set(Thread.currentThread().isInterrupted());
                }
            }
        });

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertNotSame(VOID, future.getState());
            }
        });

        sleepSeconds(5);
        thread.get().interrupt();

        assertCompletesEventually(getFuture);
        assertTrue(interrupted.get());

        try {
            future.join();
            fail();
        } catch (HazelcastException e) {
            assertInstanceOf(InterruptedException.class, e.getCause());
        }
    }
}
