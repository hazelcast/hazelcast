/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.executor;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.executor.tasks.CancellationAwareTask;
import com.hazelcast.client.executor.tasks.FailingCallable;
import com.hazelcast.client.executor.tasks.MapPutRunnable;
import com.hazelcast.client.executor.tasks.SelectNoMembers;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.MemberSelector;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ClientExecutorServiceTest {

    static final int CLUSTER_SIZE = 3;
    static HazelcastInstance instance1;
    static HazelcastInstance instance2;
    static HazelcastInstance instance3;

    static HazelcastInstance client;

    @BeforeClass
    public static void init() {
        instance1 = Hazelcast.newHazelcastInstance();
        instance2 = Hazelcast.newHazelcastInstance();
        instance3 = Hazelcast.newHazelcastInstance();
        client = HazelcastClient.newHazelcastClient();
    }

    @AfterClass
    public static void destroy() {
        client.shutdown();
        Hazelcast.shutdownAll();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetLocalExecutorStats() throws Throwable, InterruptedException {
        IExecutorService service = client.getExecutorService(randomString());
        service.getLocalExecutorStats();
    }

    @Test
    public void testIsTerminated() throws InterruptedException, ExecutionException, TimeoutException {
        final IExecutorService service = client.getExecutorService(randomString());
        assertFalse(service.isTerminated());
    }

    @Test
    public void testIsShutdown() throws InterruptedException, ExecutionException, TimeoutException {
        final IExecutorService service = client.getExecutorService(randomString());
        assertFalse(service.isShutdown());
    }

    @Test
    public void testShutdownNow() throws InterruptedException, ExecutionException, TimeoutException {
        final IExecutorService service = client.getExecutorService(randomString());
        service.shutdownNow();

        assertTrueEventually(new AssertTask() {
            public void run() throws Exception {
                assertTrue(service.isShutdown());
            }
        });
    }

    @Test(expected = TimeoutException.class)
    public void testCancellationAwareTask_whenTimeOut() throws InterruptedException, ExecutionException, TimeoutException {
        IExecutorService service = client.getExecutorService(randomString());
        CancellationAwareTask task = new CancellationAwareTask(5000);

        Future future = service.submit(task);

        future.get(1, TimeUnit.SECONDS);
    }

    @Test
    public void testFutureAfterCancellationAwareTaskTimeOut() throws InterruptedException, ExecutionException, TimeoutException {
        IExecutorService service = client.getExecutorService(randomString());
        CancellationAwareTask task = new CancellationAwareTask(5000);

        Future future = service.submit(task);

        try {
            future.get(1, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
        }

        assertFalse(future.isDone());
        assertFalse(future.isCancelled());
    }

    @Test
    public void testCancelFutureAfterCancellationAwareTaskTimeOut() throws InterruptedException, ExecutionException, TimeoutException {
        IExecutorService service = client.getExecutorService(randomString());
        CancellationAwareTask task = new CancellationAwareTask(5000);

        Future future = service.submit(task);

        try {
            future.get(1, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
        }

        assertTrue(future.cancel(true));
        assertTrue(future.isCancelled());
        assertTrue(future.isDone());
    }

    @Test(expected = CancellationException.class)
    public void testGetFutureAfterCancel() throws InterruptedException, ExecutionException, TimeoutException {
        IExecutorService service = client.getExecutorService(randomString());
        CancellationAwareTask task = new CancellationAwareTask(5000);

        Future future = service.submit(task);
        try {
            future.get(1, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
        }
        future.cancel(true);

        future.get();
    }

    @Test(expected = ExecutionException.class)
    public void testSubmitFailingCallableException() throws ExecutionException, InterruptedException {
        IExecutorService service = client.getExecutorService(randomString());
        final Future<String> f = service.submit(new FailingCallable());

        f.get();
    }

    @Test(expected = IllegalStateException.class)
    public void testSubmitFailingCallableReasonExceptionCause() throws Throwable {
        IExecutorService service = client.getExecutorService(randomString());
        final Future<String> f = service.submit(new FailingCallable());

        try {
            f.get();
        } catch (ExecutionException e) {
            throw e.getCause();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testExecute_withNoMemberSelected() {
        final IExecutorService service = client.getExecutorService(randomString());
        final String mapName = randomString();
        final MemberSelector selector = new SelectNoMembers();

        service.execute(new MapPutRunnable(mapName), selector);
    }
}