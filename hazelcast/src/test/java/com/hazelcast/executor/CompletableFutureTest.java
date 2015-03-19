/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.executor;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.instance.HazelcastInstanceProxy;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.reflect.Field;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class CompletableFutureTest extends HazelcastTestSupport {

    private static final RuntimeException TEST_EXCEPTION = new RuntimeException("Test exception");

    private ExecutionService executionService;
    private CountDownLatch startLatch, doneLatch;
    private ExecutorService executorService;
    private AtomicReference<Object> ref1, ref2;

    @Before
    public void setUp() throws Exception {
        NodeEngine nodeEngine = getNode(createHazelcastInstance()).getNodeEngine();
        executionService = nodeEngine.getExecutionService();
        startLatch = new CountDownLatch(1);
        executorService = Executors.newSingleThreadExecutor();
        ref1 = new AtomicReference<Object>();
        ref2 = new AtomicReference<Object>();
    }

    @After
    public void tearDown() {
        executorService.shutdown();
    }

    @Test
    public void preregisterCallback() throws Exception {
        doneLatch = new CountDownLatch(1);
        final ICompletableFuture<String> f = submit(awaitStartLatch());
        f.andThen(setRefAndBumpDoneLatch(ref1));
        startLatch.countDown();
        assertOpenEventually(doneLatch);
        assertEquals("success", ref1.get());
    }

    @Test
    public void preregisterTwoCallbacks() throws Exception {
        doneLatch = new CountDownLatch(2);
        final ICompletableFuture<String> f = submit(awaitStartLatch());
        f.andThen(setRefAndBumpDoneLatch(ref1));
        f.andThen(setRefAndBumpDoneLatch(ref2));
        startLatch.countDown();
        assertOpenEventually(doneLatch);
        assertEquals("success", ref1.get());
        assertEquals("success", ref2.get());
    }

    @Test
    public void preregisterTwoCallbacks_withFailure() throws Exception {
        doneLatch = new CountDownLatch(2);
        final ICompletableFuture<String> f = submit(awaitStartLatch(), throwException());
        f.andThen(setRefAndBumpDoneLatch(ref1));
        f.andThen(setRefAndBumpDoneLatch(ref2));
        startLatch.countDown();
        assertOpenEventually(doneLatch);
        assertTestException(ref1, ref2);
    }

    @Test
    public void postregisterCallback() throws Exception {
        doneLatch = new CountDownLatch(1);
        final ICompletableFuture<String> f = submit(openStartLatch());
        assertOpenEventually(startLatch);
        f.andThen(setRefAndBumpDoneLatch(ref1));
        assertOpenEventually(doneLatch);
        assertEquals("success", ref1.get());
    }

    @Test
    public void postregisterTwoCallbacks() throws Exception {
        doneLatch = new CountDownLatch(2);
        final ICompletableFuture<String> f = submit(openStartLatch());
        assertOpenEventually(startLatch);
        f.andThen(setRefAndBumpDoneLatch(ref1));
        f.andThen(setRefAndBumpDoneLatch(ref2));
        assertOpenEventually(doneLatch);
        assertEquals("success", ref1.get());
        assertEquals("success", ref2.get());
    }

    @Test
    public void postregisterTwoCallbacks_withFailure() throws Exception {
        doneLatch = new CountDownLatch(2);
        final ICompletableFuture<String> f = submit(openStartLatch(), throwException());
        assertOpenEventually(startLatch);
        f.andThen(setRefAndBumpDoneLatch(ref1));
        f.andThen(setRefAndBumpDoneLatch(ref2));
        assertOpenEventually(doneLatch);
        assertTestException(ref1, ref2);
    }

    private static void assertTestException(AtomicReference<?>... refs) {
        for (AtomicReference<?> ref : refs)
            assertThat("ExecutionException expected", ref.get(), instanceOf(ExecutionException.class));
        for (AtomicReference<?> ref : refs)
            assertThat("TEST_EXCEPTION expected as cause", ((Throwable) ref.get()).getCause(),
                Matchers.<Throwable>sameInstance(TEST_EXCEPTION));
    }

    private ICompletableFuture<String> submit(final Runnable... rs) {
        return executionService.asCompletableFuture(executionService.submit("default", new Callable<String>() {
            @Override
            public String call() {
                for (Runnable r : rs) r.run();
                return "success";
            }
        }));
    }

    private Runnable awaitStartLatch() {
        return new Runnable() { @Override public void run() {
            assertOpenEventually(startLatch);
        }};
    }

    private Runnable openStartLatch() {
        return new Runnable() { @Override public void run() {
            startLatch.countDown();
        }};
    }

    private static Runnable throwException() {
        return new Runnable() { @Override public void run() {
            throw TEST_EXCEPTION;
        }};
    }

    private ExecutionCallback<String> setRefAndBumpDoneLatch(final AtomicReference<Object> ref) {
        return new ExecutionCallback<String>() {
            @Override public void onResponse(String response) {
                doit(response);
            }
            @Override public void onFailure(Throwable t) {
                doit(t);
            }
            private void doit(Object response) {
                ref.set(response);
                doneLatch.countDown();
            }
        };
    }
}
