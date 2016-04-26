package com.hazelcast.ringbuffer.impl.operations;

import com.hazelcast.config.Config;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.ringbuffer.StaleSequenceException;
import com.hazelcast.ringbuffer.impl.RingbufferContainer;
import com.hazelcast.ringbuffer.impl.RingbufferService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ReadOneOperationTest extends HazelcastTestSupport {

    private HazelcastInstance hz;
    private NodeEngineImpl nodeEngine;
    private SerializationService serializationService;
    private Ringbuffer<Object> ringbuffer;
    private RingbufferContainer ringbufferContainer;

    @Before
    public void setup() {
        RingbufferConfig rbConfig = new RingbufferConfig("foo").setCapacity(10).setTimeToLiveSeconds(10);

        Config config = new Config().addRingBufferConfig(rbConfig);

        hz = createHazelcastInstance(config);
        nodeEngine = getNodeEngineImpl(hz);
        serializationService = getSerializationService(hz);
        ringbuffer = hz.getRingbuffer(rbConfig.getName());

        RingbufferService ringbufferService = getNodeEngineImpl(hz).getService(RingbufferService.SERVICE_NAME);
        ringbufferContainer = ringbufferService.getContainer(rbConfig.getName());
    }

    private Data toData(Object item) {
        return serializationService.toData(item);
    }

    @Test
    public void whenAtTail() throws Exception {
        ringbuffer.add("tail");

        ReadOneOperation op = new ReadOneOperation(ringbuffer.getName(), ringbuffer.tailSequence());
        op.setNodeEngine(nodeEngine);

        // since there is an item, we don't need to wait
        boolean shouldWait = op.shouldWait();
        assertFalse(shouldWait);

        op.run();

        assertEquals(toData("tail"), op.getResponse());
    }

    @Test
    public void whenOneAfterTail() throws Exception {
        ringbuffer.add("tail");

        ReadOneOperation op = new ReadOneOperation(ringbuffer.getName(), ringbuffer.tailSequence() + 1);
        op.setNodeEngine(nodeEngine);

        // since there is an item, we don't need to wait
        boolean shouldWait = op.shouldWait();
        assertTrue(shouldWait);
    }

    @Test(expected = IllegalArgumentException.class)
    public void whenTooFarAfterTail() throws Exception {
        ringbuffer.add("tail");

        ReadOneOperation op = new ReadOneOperation(ringbuffer.getName(), ringbuffer.tailSequence() + 2);
        op.setNodeEngine(nodeEngine);

        // since there is an item, we don't need to wait
        op.shouldWait();
    }

    @Test
    public void whenOneAfterTailAndBufferEmpty() throws Exception {
        ReadOneOperation op = new ReadOneOperation(ringbuffer.getName(), ringbuffer.tailSequence() + 1);
        op.setNodeEngine(nodeEngine);

        // since there is an item, we don't need to wait
        boolean shouldWait = op.shouldWait();
        assertTrue(shouldWait);
    }

    @Test(expected = StaleSequenceException.class)
    public void whenOnTailAndBufferEmpty() throws Exception {
        ReadOneOperation op = new ReadOneOperation(ringbuffer.getName(), ringbuffer.tailSequence());
        op.setNodeEngine(nodeEngine);

        // since there is an item, we don't need to wait
        op.shouldWait();
    }

    @Test
    public void whenBeforeTail() throws Exception {
        ringbuffer.add("item1");
        ringbuffer.add("item2");
        ringbuffer.add("item3");

        ReadOneOperation op = new ReadOneOperation(ringbuffer.getName(), ringbuffer.tailSequence() - 1);
        op.setNodeEngine(nodeEngine);

        // since there is an item, we don't need to wait
        boolean shouldWait = op.shouldWait();
        assertFalse(shouldWait);

        op.run();

        assertEquals(toData("item2"), op.getResponse());
    }

    @Test
    public void whenAtHead() throws Exception {
        ringbuffer.add("item1");
        ringbuffer.add("item2");
        ringbuffer.add("item3");

        ReadOneOperation op = new ReadOneOperation(ringbuffer.getName(), ringbuffer.headSequence());
        op.setNodeEngine(nodeEngine);

        // since there is an item, we don't need to wait
        boolean shouldWait = op.shouldWait();
        assertFalse(shouldWait);

        op.run();

        assertEquals(toData("item1"), op.getResponse());
    }

    @Test(expected = StaleSequenceException.class)
    public void whenBeforeHead() throws Exception {
        ringbuffer.add("item1");
        ringbuffer.add("item2");
        ringbuffer.add("item3");

        long oldhead = ringbuffer.headSequence();
        ringbufferContainer.setHeadSequence(ringbufferContainer.tailSequence());

        ReadOneOperation op = new ReadOneOperation(ringbuffer.getName(), oldhead);
        op.setNodeEngine(nodeEngine);

        op.shouldWait();
    }
}
