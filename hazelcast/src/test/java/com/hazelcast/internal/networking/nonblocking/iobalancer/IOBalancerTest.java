package com.hazelcast.internal.networking.nonblocking.iobalancer;

import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.internal.networking.nonblocking.MigratableHandler;
import com.hazelcast.internal.networking.nonblocking.NonBlockingIOThread;
import com.hazelcast.internal.networking.nonblocking.iobalancer.IOBalancer;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.logging.LoggingServiceImpl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class IOBalancerTest {
    private final LoggingService loggingService = new LoggingServiceImpl("somegroup", "log4j2", BuildInfoProvider.getBuildInfo());

    // https://github.com/hazelcast/hazelcast/issues/11501
    @Test
    public void whenChannelAdded_andDisabled_thenSkipTaskCreation() {
        IOBalancer ioBalancer = new IOBalancer(new NonBlockingIOThread[1], new NonBlockingIOThread[1], "foo", 1, loggingService);
        MigratableHandler readHandler = mock(MigratableHandler.class);
        MigratableHandler writeHandler = mock(MigratableHandler.class);

        ioBalancer.connectionAdded(readHandler, writeHandler);

        assertTrue(ioBalancer.getInLoadTracker().tasks.isEmpty());
        assertTrue(ioBalancer.getOutLoadTracker().tasks.isEmpty());
    }

    // https://github.com/hazelcast/hazelcast/issues/11501
    @Test
    public void whenChannelRemoved_andDisabled_thenSkipTaskCreation() {
        IOBalancer ioBalancer = new IOBalancer(new NonBlockingIOThread[1], new NonBlockingIOThread[1], "foo", 1, loggingService);
        MigratableHandler readHandler = mock(MigratableHandler.class);
        MigratableHandler writeHandler = mock(MigratableHandler.class);

        ioBalancer.connectionRemoved(readHandler, writeHandler);

        assertTrue(ioBalancer.getInLoadTracker().tasks.isEmpty());
        assertTrue(ioBalancer.getOutLoadTracker().tasks.isEmpty());
    }
}
