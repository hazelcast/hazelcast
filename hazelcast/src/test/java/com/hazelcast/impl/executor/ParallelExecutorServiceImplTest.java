package com.hazelcast.impl.executor;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.StandardLoggerFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class ParallelExecutorServiceImplTest {

    private ThreadPoolExecutor executorService;
    private ParallelExecutorService parallelExecutorService;

    @Before
    public void setUp() {
        ILogger logger = new StandardLoggerFactory().getLogger(ParallelExecutorServiceImplStressTest.class.getName());
        executorService = new ThreadPoolExecutor(10, 10, 0, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
        parallelExecutorService = new ParallelExecutorService(logger, executorService);
    }

    @After
    public void tearDown() throws InterruptedException {
        executorService.shutdownNow();
        assertTrue("ExecutorService failed to terminate within timeout window", executorService.awaitTermination(10, TimeUnit.SECONDS));
    }

    @Test
    public void testExecuteWithIllegalArguments() {
        ParallelExecutor executor = parallelExecutorService.newParallelExecutor(2);
        try {
            executor.execute(null);
            fail();
        } catch (NullPointerException expected) {
        }
        try {
            executor.execute(null, 1);
            fail();
        } catch (NullPointerException expected) {
        }
    }
}
