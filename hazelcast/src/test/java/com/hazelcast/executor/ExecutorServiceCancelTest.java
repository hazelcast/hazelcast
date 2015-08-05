package com.hazelcast.executor;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.Member;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
@Ignore
/*
 * This test is failing because of order problem between actual invoke and cancel.
 * For random and submit to member, it is because we do not have order guarantee in the first place.
 */
public class ExecutorServiceCancelTest extends ExecutorServiceTestSupport {

    private HazelcastInstance server1;
    private HazelcastInstance server2;

    @Before
    public void setup() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        server1 = factory.newHazelcastInstance();
        server2 = factory.newHazelcastInstance();
    }

    @Test
    public void testCancel_submitRandom() throws ExecutionException, InterruptedException {
        IExecutorService executorService = server1.getExecutorService(randomString());
        Future<Boolean> future = executorService.submit(new SleepingTask(Integer.MAX_VALUE));
        assertTrue(future.cancel(true));
    }

    @Test(expected = CancellationException.class)
    public void testGetValueAfterCancel_submitRandom()
            throws ExecutionException, InterruptedException, TimeoutException {
        IExecutorService executorService = server1.getExecutorService(randomString());
        Future<Boolean> future = executorService.submit(new SleepingTask(Integer.MAX_VALUE));
        future.cancel(true);
        future.get(10, TimeUnit.SECONDS);
    }

    @Test
    public void testCancel_submitToLocalMember() throws ExecutionException, InterruptedException {
        testCancel_submitToMember(server1, server1.getCluster().getLocalMember());
    }

    @Test
    public void testCancel_submitToRemoteMember() throws ExecutionException, InterruptedException {
        testCancel_submitToMember(server1, server2.getCluster().getLocalMember());
    }

    @Test(expected = CancellationException.class)
    public void testGetValueAfterCancel_submitToLocalMember()
            throws ExecutionException, InterruptedException, TimeoutException {
        testGetValueAfterCancel_submitToMember(server1, server1.getCluster().getLocalMember());
    }

    @Test(expected = CancellationException.class)
    public void testGetValueAfterCancel_submitToRemoteMember()
            throws ExecutionException, InterruptedException, TimeoutException {
        testGetValueAfterCancel_submitToMember(server1, server2.getCluster().getLocalMember());
    }

    private void testCancel_submitToMember(HazelcastInstance instance, Member member)
            throws ExecutionException, InterruptedException {
        IExecutorService executorService = instance.getExecutorService(randomString());
        Future<Boolean> future = executorService.submitToMember(new SleepingTask(Integer.MAX_VALUE), member);
        assertTrue(future.cancel(true));
    }

    private void testGetValueAfterCancel_submitToMember(HazelcastInstance instance, Member member)
            throws ExecutionException, InterruptedException, TimeoutException {
        IExecutorService executorService = instance.getExecutorService(randomString());
        Future<Boolean> future = executorService.submitToMember(new SleepingTask(Integer.MAX_VALUE), member);
        future.cancel(true);
        future.get(10, TimeUnit.SECONDS);
    }

    @Test
    public void testCancel_submitToKeyOwner() throws ExecutionException, InterruptedException {
        IExecutorService executorService = server1.getExecutorService(randomString());
        Future<Boolean> future = executorService.submitToKeyOwner(new SleepingTask(Integer.MAX_VALUE), randomString());
        boolean cancelled = future.cancel(true);
        assertTrue(cancelled);
    }

    @Test(expected = CancellationException.class)
    public void testGetValueAfterCancel_submitToKeyOwner()
            throws ExecutionException, InterruptedException, TimeoutException {
        IExecutorService executorService = server1.getExecutorService(randomString());
        Future<Boolean> future = executorService.submitToKeyOwner(new SleepingTask(Integer.MAX_VALUE), randomString());
        future.cancel(true);
        future.get(10, TimeUnit.SECONDS);
    }

}
