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
import com.hazelcast.client.executor.tasks.Sim;
import com.hazelcast.client.executor.tasks.CallableTask;
import com.hazelcast.client.executor.tasks.CancellationAwareTask;
import com.hazelcast.client.executor.tasks.FailingTask;
import com.hazelcast.client.executor.tasks.RunnableTask;
import com.hazelcast.core.*;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ProblematicTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.*;

import static com.hazelcast.test.HazelcastTestSupport.*;
import static org.junit.Assert.*;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

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

    @Test
    public void testIsTerminated() throws InterruptedException, ExecutionException, TimeoutException {
        final IExecutorService service = client.getExecutorService(randomString());
        assertFalse( service.isTerminated() );
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

    @Category(ProblematicTest.class)
    @Test
    public void testAwaitTermination() throws InterruptedException, ExecutionException, TimeoutException {
        final IExecutorService service = client.getExecutorService(randomString());
        service.awaitTermination(1, TimeUnit.MILLISECONDS);
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
        }catch (TimeoutException e) {}

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
        }catch (TimeoutException e) {}

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
        }catch (TimeoutException e) {}
        future.cancel(true);

        future.get();
    }

    @Test
    public void testSubmitWithResult() throws ExecutionException, InterruptedException {
        IExecutorService service = client.getExecutorService(randomString());

        final Integer res = 5;
        final Future<Integer> future = service.submit( new RunnableTask("task"), res);
        final Integer integer = future.get();
        assertEquals(res, integer);
    }

    @Test
    public void submitCallableToKeyOwner() throws Exception {
        IExecutorService service = client.getExecutorService(randomString());
        final String msg = randomString();

        final Future<String> future = service.submitToKeyOwner(new CallableTask(msg), "key");
        String result = future.get();
        assertEquals(msg + CallableTask.resultPostFix, result);
    }

    @Test
    public void submitCallableToKeyOwner_withExecutionCallback() throws Exception {
        IExecutorService service = client.getExecutorService(randomString());
        final String msg = randomString();
        final CountDownLatch responseLatch = new CountDownLatch(1);
        service.submitToKeyOwner(new CallableTask(msg), "key", new ExecutionCallback<String>() {
            public void onResponse(String response) {
                if (response.equals(msg + CallableTask.resultPostFix)) {
                    responseLatch.countDown();
                }
            }

            public void onFailure(Throwable t) {
            }
        });

        assertOpenEventually(responseLatch, 5);
    }

    @Test
    public void submitCallableToAllMembers() throws Exception {
        final IExecutorService service = client.getExecutorService(randomString());
        final String msg = randomString();
        final Map<Member, Future<String>> map = service.submitToAllMembers(new CallableTask(msg));
        for (Member member : map.keySet()) {
            final Future<String> future = map.get(member);
            String result = future.get();

            assertEquals(msg+CallableTask.resultPostFix, result);
        }
    }

    @Test
    public void submitCallableToAllMembers_withMultiExecutionCallback() throws Exception {
        final IExecutorService service = client.getExecutorService(randomString());
        final CountDownLatch responseLatch = new CountDownLatch(CLUSTER_SIZE);
        final CountDownLatch completeLatch = new CountDownLatch(1);
        final String msg = randomString();

        service.submitToAllMembers(new CallableTask(msg), new MultiExecutionCallback() {
            public void onResponse(Member member, Object value) {
                if (value.equals(msg + CallableTask.resultPostFix)) {
                    responseLatch.countDown();
                }
            }

            public void onComplete(Map<Member, Object> values) {
                for (Member member : values.keySet()) {
                    Object value = values.get(member);
                    if (value.equals(msg + CallableTask.resultPostFix)) {
                        completeLatch.countDown();
                    }
                }
            }
        });
        assertOpenEventually(responseLatch, 5);
        assertOpenEventually(completeLatch, 5);
    }

    @Test(expected = ExecutionException.class)
    public void testSubmitFailingCallableException() throws ExecutionException, InterruptedException {
        IExecutorService service = client.getExecutorService(randomString());
        final Future<String> f = service.submit(new FailingTask());

        f.get();
    }

    @Test(expected = IllegalStateException.class)
    public void testSubmitFailingCallableReasonExceptionCause() throws Throwable, InterruptedException {
        IExecutorService service = client.getExecutorService(randomString());
        final Future<String> f = service.submit(new FailingTask());

        try{
        f.get();
        }catch(ExecutionException e){
            throw e.getCause();
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testInvokeAll() throws Throwable, InterruptedException {
        IExecutorService service = client.getExecutorService(randomString());
        Collection c = new ArrayList();
        c.add(new CallableTask());
        c.add(new CallableTask());
        service.invokeAll(c, 1, TimeUnit.MINUTES);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testInvokeAny() throws Throwable, InterruptedException {
        IExecutorService service = client.getExecutorService(randomString());
        Collection c = new ArrayList();
        c.add(new CallableTask());
        c.add(new CallableTask());
        service.invokeAny(c);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testInvokeAnyTimeOut() throws Throwable, InterruptedException {
        IExecutorService service = client.getExecutorService(randomString());
        Collection c = new ArrayList();
        c.add(new CallableTask());
        c.add(new CallableTask());
        service.invokeAny(c, 1, TimeUnit.MINUTES);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetLocalExecutorStats() throws Throwable, InterruptedException {
        IExecutorService service = client.getExecutorService(randomString());
        service.getLocalExecutorStats();
    }

    @Test
    public void testExecute() {
        IExecutorService service = client.getExecutorService(randomString());

        service.execute( new RunnableTask("task"));
    }

    @Test
    public void testExecute_whenTaskNull() {
        IExecutorService service = client.getExecutorService(randomString());

        service.execute( null );
    }

    @Test
    public void testExecuteOnKeyOwner() {
        IExecutorService service = client.getExecutorService(randomString());

        service.executeOnKeyOwner(new RunnableTask("task"), "key");
    }

    @Test
    public void testExecuteOnKeyOwner_whenKeyNull() {
        IExecutorService service = client.getExecutorService(randomString());

        service.executeOnKeyOwner(new RunnableTask("task"), null);
    }

    @Test
    public void testExecuteOnMember(){
        IExecutorService service = client.getExecutorService(randomString());

        service.executeOnMember(new RunnableTask("task"), instance1.getCluster().getLocalMember() );
    }

    @Test
    public void testExecuteOnMember_WhenMemberNull() {
        IExecutorService service = client.getExecutorService(randomString());

        service.executeOnMember(new RunnableTask("task"), null);
    }


    @Test
    public void testExecuteOnMembers() {
        IExecutorService service = client.getExecutorService(randomString());
        Collection collection = instance1.getCluster().getMembers();

        service.executeOnMembers(new RunnableTask("task"), collection);
    }

    @Test
    public void testExecuteOnMembers_WhenCollectionNull() {
        IExecutorService service = client.getExecutorService(randomString());
        Collection collection = null;

        service.executeOnMembers(new RunnableTask("task"), collection);
    }

    @Test
    public void testExecuteOnMembers_withSelector() {
        IExecutorService service = client.getExecutorService(randomString());

        MemberSelector selector = null;//new SimpleMem();
        service.executeOnMembers(new RunnableTask("task"), selector);
    }

    @Test
    public void testExecuteOnMembers_whenSelectorNull() {
        IExecutorService service = client.getExecutorService(randomString());

        MemberSelector selector = null;
        service.executeOnMembers(new RunnableTask("task"), selector);
    }

    @Test
    public void testExecuteOnAllMembers() {
        IExecutorService service = client.getExecutorService(randomString());
        Collection collection = null;

        service.executeOnAllMembers(new RunnableTask("task"));
    }



}