package com.hazelcast.ringbuffer.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.partition.MigrationEndpoint.DESTINATION;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class RingbufferServiceTest extends HazelcastTestSupport {

    private RingbufferService service;
    private HazelcastInstance hz;

    @Before
    public void setup() {
        hz = createHazelcastInstance();
        service = getNodeEngineImpl(hz).getService(RingbufferService.SERVICE_NAME);
    }

    @Test
    public void rollbackMigration() {
        Ringbuffer ringbuffer = hz.getRingbuffer("foo");
        int partitionId = getPartitionId(hz, ringbuffer.getName());
        PartitionMigrationEvent partitionEvent = new PartitionMigrationEvent(DESTINATION, partitionId);

        service.rollbackMigration(partitionEvent);

        assertEquals(0, service.getContainers().size());
    }

    @Test
    public void reset() {
        service.getContainer("foo");
        service.getContainer("bar");

        service.reset();

        assertEquals(0, service.getContainers().size());
    }
}
