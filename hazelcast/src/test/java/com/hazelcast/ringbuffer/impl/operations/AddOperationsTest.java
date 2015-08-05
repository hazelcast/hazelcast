package com.hazelcast.ringbuffer.impl.operations;

import com.hazelcast.config.Config;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
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

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class AddOperationsTest extends HazelcastTestSupport {

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
    public void whenFailOverflowPolicy_andNoRemainingCapacity_thenNoBackup() throws Exception {
        for (int k = 0; k < ringbuffer.capacity(); k++) {
            ringbuffer.add("item");
        }

        Data item = serializationService.toData("newItem");

        AddOperation addOperation = new AddOperation(ringbuffer.getName(), item, FAIL);
        addOperation.setNodeEngine(nodeEngine);
        addOperation.run();

        assertFalse(addOperation.shouldBackup());
        assertFalse(addOperation.shouldNotify());
        assertEquals(new Long(-1l), addOperation.getResponse());
    }

    @Test
    public void whenFailOverflowPolicy_andRemainingCapacity_thenBackup() throws Exception {
        for (int k = 0; k < ringbuffer.capacity() - 1; k++) {
            ringbuffer.add("item");
        }

        Data item = serializationService.toData("newItem");

        AddOperation addOperation = new AddOperation(ringbuffer.getName(), item, FAIL);
        addOperation.setNodeEngine(nodeEngine);
        addOperation.run();

        assertTrue(addOperation.shouldBackup());
        assertTrue(addOperation.shouldNotify());
        assertEquals(new Long(ringbuffer.tailSequence()), addOperation.getResponse());
    }

    @Test
    public void whenOverwritePolicy_andNoRemainingCapacity_thenBackup() throws Exception {
        for (int k = 0; k < ringbuffer.capacity(); k++) {
            ringbuffer.add("item");
        }

        Data item = serializationService.toData("newItem");

        AddOperation addOperation = new AddOperation(ringbuffer.getName(), item, OVERWRITE);
        addOperation.setNodeEngine(nodeEngine);
        addOperation.run();

        assertTrue(addOperation.shouldBackup());
        assertTrue(addOperation.shouldNotify());
        assertEquals(new Long(ringbuffer.tailSequence()), addOperation.getResponse());
    }

    @Test
    public void whenOverwritePolicy_andRemainingCapacity_thenBackup() throws Exception {
        for (int k = 0; k < ringbuffer.capacity() - 1; k++) {
            ringbuffer.add("item");
        }

        Data item = serializationService.toData("newItem");

        AddOperation addOperation = new AddOperation(ringbuffer.getName(), item, OVERWRITE);
        addOperation.setNodeEngine(nodeEngine);
        addOperation.run();

        assertTrue(addOperation.shouldNotify());
        assertTrue(addOperation.shouldBackup());
        assertEquals(new Long(ringbuffer.tailSequence()), addOperation.getResponse());
    }
}
