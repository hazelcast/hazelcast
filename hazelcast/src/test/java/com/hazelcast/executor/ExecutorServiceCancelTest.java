package com.hazelcast.executor;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.Member;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
@Ignore
/*
 * This test is failing because of order problem between actual invoke and cancel.
 * For random and submit to member, it is because we do not have order guarantee in the first place.
 */
public class ExecutorServiceCancelTest extends ExecutorServiceTestSupport {

    public static final int SLEEP_TIME_SECONDS = 1000;

    private HazelcastInstance server1;
    private HazelcastInstance server2;

    @BeforeClass
    @AfterClass
    public static void cleanup() {
        Hazelcast.shutdownAll();
    }

    @Before
    public void setup() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        server1 = factory.newHazelcastInstance();
        server2 = factory.newHazelcastInstance();
    }


    @Test(expected = CancellationException.class)
    public void testCancel_submitRandom() throws ExecutionException, InterruptedException {
        IExecutorService executorService = server1.getExecutorService(randomString());
        Future<Boolean> future = executorService.submit(new SleepingTask(SLEEP_TIME_SECONDS));
        boolean cancelled = future.cancel(true);
        assertTrue(cancelled);
        future.get();
    }

    @Test(expected = CancellationException.class)
    public void testCancel_submitToLocalMember() throws ExecutionException, InterruptedException {
        testCancel_submitToMember(server1, server1.getCluster().getLocalMember());
    }

    @Test(expected = CancellationException.class)
    public void testCancel_submitToRemoteMember() throws ExecutionException, InterruptedException {
        testCancel_submitToMember(server1, server2.getCluster().getLocalMember());
    }


    private void testCancel_submitToMember(HazelcastInstance instance, Member member)
            throws ExecutionException, InterruptedException {
        IExecutorService executorService = instance.getExecutorService(randomString());
        Future<Boolean> future = executorService.submitToMember(new SleepingTask(SLEEP_TIME_SECONDS), member);
        boolean cancelled = future.cancel(true);
        assertTrue(cancelled);
        future.get();
    }

    @Test(expected = CancellationException.class)
    public void testCancel_submitToKeyOwner() throws ExecutionException, InterruptedException {
        IExecutorService executorService = server1.getExecutorService(randomString());
        Future<Boolean> future = executorService.submitToKeyOwner(new SleepingTask(SLEEP_TIME_SECONDS), randomString());
        boolean cancelled = future.cancel(true);
        assertTrue(cancelled);
        future.get();
    }

}
