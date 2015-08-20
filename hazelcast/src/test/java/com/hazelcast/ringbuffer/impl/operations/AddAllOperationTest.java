package com.hazelcast.ringbuffer.impl.operations;

import com.hazelcast.config.Config;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.ringbuffer.OverflowPolicy.FAIL;
import static com.hazelcast.ringbuffer.OverflowPolicy.OVERWRITE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class AddAllOperationTest extends HazelcastTestSupport {

    private HazelcastInstance hz;
    private NodeEngineImpl nodeEngine;
    private SerializationService serializationService;
    private Ringbuffer<Object> ringbuffer;

    @Before
    public void setup() {
        RingbufferConfig rbConfig = new RingbufferConfig("foo").setCapacity(10).setTimeToLiveSeconds(10);

        Config config = new Config().addRingBufferConfig(rbConfig);

        hz = createHazelcastInstance(config);
        nodeEngine = getNodeEngineImpl(hz);
        serializationService = getSerializationService(hz);
        ringbuffer = hz.getRingbuffer(rbConfig.getName());
    }

    @Test
    public void whenFailOverflowPolicy_andNoRemainingCapacity() throws Exception {
        for (int k = 0; k < ringbuffer.capacity(); k++) {
            ringbuffer.add("item");
        }

        Data item = serializationService.toData("newItem");

        AddAllOperation addOperation = new AddAllOperation(ringbuffer.getName(), new Data[]{item}, FAIL);
        addOperation.setNodeEngine(nodeEngine);
        addOperation.run();

        assertFalse(addOperation.shouldNotify());
        assertFalse(addOperation.shouldBackup());
        assertEquals(-1l, addOperation.getResponse());
    }

    @Test
    public void whenFailOverflowPolicy_andRemainingCapacity() throws Exception {
        for (int k = 0; k < ringbuffer.capacity() - 1; k++) {
            ringbuffer.add("item");
        }

        Data item = serializationService.toData("newItem");

        AddAllOperation addOperation = new AddAllOperation(ringbuffer.getName(), new Data[]{item}, FAIL);
        addOperation.setNodeEngine(nodeEngine);
        addOperation.run();

        assertTrue(addOperation.shouldNotify());
        assertTrue(addOperation.shouldBackup());
        assertEquals(ringbuffer.tailSequence(), addOperation.getResponse());
    }

    @Test
    public void whenOverwritePolicy_andNoRemainingCapacity() throws Exception {
        for (int k = 0; k < ringbuffer.capacity(); k++) {
            ringbuffer.add("item");
        }

        Data item = serializationService.toData("newItem");

        AddAllOperation addOperation = new AddAllOperation(ringbuffer.getName(), new Data[]{item}, OVERWRITE);
        addOperation.setNodeEngine(nodeEngine);
        addOperation.run();

        assertTrue(addOperation.shouldNotify());
        assertTrue(addOperation.shouldBackup());
        assertEquals(ringbuffer.tailSequence(), addOperation.getResponse());
    }

    @Test
    public void whenOverwritePolicy_andRemainingCapacity() throws Exception {
        for (int k = 0; k < ringbuffer.capacity() - 1; k++) {
            ringbuffer.add("item");
        }

        Data item = serializationService.toData("newItem");

        AddAllOperation addOperation = new AddAllOperation(ringbuffer.getName(), new Data[]{item}, OVERWRITE);
        addOperation.setNodeEngine(nodeEngine);
        addOperation.run();

        assertTrue(addOperation.shouldNotify());
        assertTrue(addOperation.shouldBackup());
        assertEquals(ringbuffer.tailSequence(), addOperation.getResponse());
    }
}
