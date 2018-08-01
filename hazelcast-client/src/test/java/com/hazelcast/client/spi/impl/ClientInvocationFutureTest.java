/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.spi.impl;

import com.hazelcast.client.test.ClientTestSupport;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientInvocationFutureTest
        extends ClientTestSupport {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @After
    public void cleanup() {
        hazelcastFactory.terminateAll();
    }

    @Test
    public void testChainedExecution_onResponse() {
        hazelcastFactory.newHazelcastInstance();
        HazelcastInstance client = hazelcastFactory.newHazelcastClient();

        InternalCompletableFuture<Double> future = (InternalCompletableFuture<Double>)
                client.getExecutorService("MyTest")
                      .submit(new ValidCallable());
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Double> value = new AtomicReference<Double>();

        future.andThen(new ExecutionCallback<Double>() {
            @Override
            public void onResponse(Double response) {
                value.set(response);
                latch.countDown();
            }

            @Override
            public void onFailure(Throwable t) {
            }
        });

        assertOpenEventually(latch);
        assertEquals(value.get(), 2d, 0);
    }

    @Test
    public void testChainedExecution_onFailure() {
        hazelcastFactory.newHazelcastInstance();
        HazelcastInstance client = hazelcastFactory.newHazelcastClient();

        InternalCompletableFuture<Double> future = (InternalCompletableFuture<Double>)
                client.getExecutorService("MyTest")
                      .submit(new FailingCallable());
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Throwable> error = new AtomicReference<Throwable>();

        future.andThen(new ExecutionCallback<Double>() {
            @Override
            public void onResponse(Double response) {
            }

            @Override
            public void onFailure(Throwable t) {
                error.set(t);
                latch.countDown();
            }
        });

        assertOpenEventually(latch);
        assertNotNull(error.get());
        assertEquals(error.get().getClass(), ExecutionException.class);
        assertEquals(error.get().getCause().getClass(), IllegalStateException.class);
        assertEquals(error.get().getCause().getMessage(), "Failed");
    }

    private static class FailingCallable
            implements Callable<Double>, Serializable {

        @Override
        public Double call()
                throws Exception {
            throw new IllegalStateException("Failed");
        }
    }

    private static class ValidCallable
            implements Callable<Double>, Serializable {

        @Override
        public Double call()
                throws Exception {
            return 2d;
        }
    }

}
