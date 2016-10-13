package com.hazelcast.client.spi.impl;

import com.hazelcast.client.impl.ClientLoggingService;
import com.hazelcast.config.Config;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.executor.ExecutorType;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientExecutionServiceImplTest {

    private static ClientExecutionServiceImpl executionService;

    @BeforeClass
    public static void setUp() {
        String name = "ClientExecutionServiceImplTest";
        ThreadGroup threadGroup = new ThreadGroup(name);
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        HazelcastProperties properties = new HazelcastProperties(new Config());
        ClientLoggingService loggingService = new ClientLoggingService(name, "jdk", BuildInfoProvider.getBuildInfo(), name);

        executionService = new ClientExecutionServiceImpl(name, threadGroup, classLoader, properties, 1, loggingService);
    }

    @AfterClass
    public static void tearDown() {
        executionService.shutdown();
    }

    @Test
    public void testExecuteInternal() {
        TestRunnable runnable = new TestRunnable();

        executionService.executeInternal(runnable);

        runnable.await();
    }

    @Test
    public void testSubmitInternal() throws Exception {
        TestRunnable runnable = new TestRunnable();

        Future<?> future = executionService.submitInternal(runnable);
        future.get();

        assertTrue(runnable.isExecuted());
    }

    @Test
    public void testExecute() {
        TestRunnable runnable = new TestRunnable();

        executionService.execute(runnable);

        runnable.await();
    }

    @Test
    public void testSubmit_withRunnable() throws Exception {
        TestRunnable runnable = new TestRunnable();

        Future<?> future = executionService.submit(runnable);
        future.get();

        assertTrue(runnable.isExecuted());
    }

    @Test
    public void testSubmit_withCallable() throws Exception {
        TestCallable callable = new TestCallable();

        Future<Integer> future = executionService.submit(callable);
        int result = future.get();

        assertTrue(callable.isExecuted());
        assertEquals(42, result);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testRegister() {
        executionService.register("myExecutor", 3, 5, ExecutorType.CACHED);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetExecutor() {
        executionService.getExecutor("myExecutor");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testShutdownExecutor() {
        executionService.shutdownExecutor("myExecutor");
    }

    @Test
    public void testExecute_withName() {
        TestRunnable runnable = new TestRunnable();

        executionService.execute("myExecution", runnable);

        runnable.await();
    }

    @Test
    public void testSubmit_withRunnable_withName() throws Exception {
        TestRunnable runnable = new TestRunnable();

        Future<?> future = executionService.submit("mySubmit", runnable);
        future.get();

        assertTrue(runnable.isExecuted());
    }

    @Test
    public void testSubmit_withCallable_withName() throws Exception {
        TestCallable callable = new TestCallable();

        Future<Integer> future = executionService.submit("mySubmit", callable);
        int result = future.get();

        assertTrue(callable.isExecuted());
        assertEquals(42, result);
    }

    @Test
    public void testSchedule() throws Exception {
        TestRunnable runnable = new TestRunnable();

        ScheduledFuture<?> future = executionService.schedule(runnable, 0, SECONDS);
        Object result = future.get();

        assertTrue(runnable.isExecuted());
        assertNull(result);
    }

    @Test
    public void testSchedule_withName() throws Exception {
        TestRunnable runnable = new TestRunnable();

        ScheduledFuture<?> future = executionService.schedule("mySchedule", runnable, 0, SECONDS);
        Object result = future.get();

        assertTrue(runnable.isExecuted());
        assertNull(result);
    }

    @Test
    public void testScheduleWithRepetition() throws Exception {
        TestRunnable runnable = new TestRunnable(5);

        ScheduledFuture<?> future = executionService.scheduleWithRepetition(runnable, 0, 100, MILLISECONDS);
        runnable.await();

        boolean result = future.cancel(true);
        assertTrue(result);
    }

    @Test
    public void testScheduleWithRepetition_withName() throws Exception {
        TestRunnable runnable = new TestRunnable(5);

        ScheduledFuture<?> future = executionService.scheduleWithRepetition("mySchedule", runnable, 0, 100, MILLISECONDS);
        runnable.await();

        boolean result = future.cancel(true);
        assertTrue(result);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetGlobalTaskScheduler() {
        executionService.getGlobalTaskScheduler();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetTaskScheduler() {
        executionService.getTaskScheduler("myScheduler");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testAsCompletableFuture() {
        executionService.asCompletableFuture(null);
    }

    @Test
    public void testGetAsyncExecutor() {
        assertNotNull(executionService.getAsyncExecutor());
    }

    @Test
    public void testGetInternalExecutor() {
        assertNotNull(executionService.getInternalExecutor());
    }

    private static class TestRunnable implements Runnable {

        private final CountDownLatch isExecuted;

        TestRunnable() {
            this(1);
        }

        TestRunnable(int executions) {
            this.isExecuted = new CountDownLatch(executions);
        }

        @Override
        public void run() {
            isExecuted.countDown();
        }

        private boolean isExecuted() {
            return isExecuted.getCount() == 0;
        }

        private void await() {
            try {
                isExecuted.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private class TestCallable implements Callable<Integer> {

        private CountDownLatch isExecuted = new CountDownLatch(1);

        @Override
        public Integer call() throws Exception {
            isExecuted.countDown();
            return 42;
        }

        private boolean isExecuted() {
            return isExecuted.getCount() == 0;
        }
    }
}
