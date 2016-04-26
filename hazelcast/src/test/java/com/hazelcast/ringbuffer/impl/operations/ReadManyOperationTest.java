package com.hazelcast.ringbuffer.impl.operations;

import com.hazelcast.config.Config;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IFunction;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.ringbuffer.StaleSequenceException;
import com.hazelcast.ringbuffer.impl.ReadResultSetImpl;
import com.hazelcast.ringbuffer.impl.RingbufferContainer;
import com.hazelcast.ringbuffer.impl.RingbufferService;
import com.hazelcast.ringbuffer.impl.client.PortableReadResultSet;
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

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ReadManyOperationTest extends HazelcastTestSupport {
    private HazelcastInstance hz;
    private NodeEngineImpl nodeEngine;
    private Ringbuffer<Object> ringbuffer;
    private RingbufferContainer ringbufferContainer;
    private SerializationService serializationService;

    @Before
    public void setup() {
        RingbufferConfig rbConfig = new RingbufferConfig("foo").setCapacity(10).setTimeToLiveSeconds(10);

        Config config = new Config().addRingBufferConfig(rbConfig);

        hz = createHazelcastInstance(config);
        nodeEngine = getNodeEngineImpl(hz);
        serializationService = nodeEngine.getSerializationService();
        ringbuffer = hz.getRingbuffer(rbConfig.getName());

        RingbufferService ringbufferService = getNodeEngineImpl(hz).getService(RingbufferService.SERVICE_NAME);
        ringbufferContainer = ringbufferService.getContainer(rbConfig.getName());
    }

    @Test
    public void whenAtTail() throws Exception {
        ringbuffer.add("tail");

        ReadManyOperation op = new ReadManyOperation(ringbuffer.getName(), ringbuffer.tailSequence(), 1, 1, null);
        op.setNodeEngine(nodeEngine);

        // since there is an item, we don't need to wait
        boolean shouldWait = op.shouldWait();
        assertFalse(shouldWait);

        op.run();

        ReadResultSetImpl response = getReadResultSet(op);
        assertEquals(asList("tail"), response);
        assertEquals(1, response.readCount());
    }

    @Test
    public void whenOneAfterTail() throws Exception {
        ringbuffer.add("tail");

        ReadManyOperation op = new ReadManyOperation(ringbuffer.getName(), ringbuffer.tailSequence() + 1, 1, 1, null);
        op.setNodeEngine(nodeEngine);

        // since there is an item, we don't need to wait
        boolean shouldWait = op.shouldWait();
        assertTrue(shouldWait);

        ReadResultSetImpl response = getReadResultSet(op);
        assertEquals(0, response.readCount());
    }

    @Test(expected = IllegalArgumentException.class)
    public void whenTooFarAfterTail() throws Exception {
        ringbuffer.add("tail");

        ReadManyOperation op = new ReadManyOperation(ringbuffer.getName(), ringbuffer.tailSequence() + 2, 1, 1, null);
        op.setNodeEngine(nodeEngine);

        // since there is an item, we don't need to wait
        op.shouldWait();
    }

    @Test
    public void whenOneAfterTailAndBufferEmpty() throws Exception {
        ReadManyOperation op = new ReadManyOperation(ringbuffer.getName(), ringbuffer.tailSequence() + 1, 1, 1, null);
        op.setNodeEngine(nodeEngine);

        // since there is an item, we don't need to wait
        boolean shouldWait = op.shouldWait();
        assertTrue(shouldWait);

        ReadResultSetImpl response = getReadResultSet(op);
        assertEquals(0, response.readCount());
        assertEquals(0, response.size());
    }

    @Test(expected = StaleSequenceException.class)
    public void whenOnTailAndBufferEmpty() throws Exception {
        ReadManyOperation op = new ReadManyOperation(ringbuffer.getName(), ringbuffer.tailSequence(), 1, 1, null);
        op.setNodeEngine(nodeEngine);

        // since there is an item, we don't need to wait
        op.shouldWait();
    }

    @Test
    public void whenBeforeTail() throws Exception {
        ringbuffer.add("item1");
        ringbuffer.add("item2");
        ringbuffer.add("item3");

        ReadManyOperation op = new ReadManyOperation(ringbuffer.getName(), ringbuffer.tailSequence() - 1, 1, 1, null);
        op.setNodeEngine(nodeEngine);

        // since there is an item, we don't need to wait
        boolean shouldWait = op.shouldWait();
        assertFalse(shouldWait);

        op.run();

        ReadResultSetImpl response = getReadResultSet(op);
        assertEquals(asList("item2"), response);
        assertEquals(1, response.readCount());
        assertEquals(1, response.size());
    }

    @Test
    public void whenAtHead() throws Exception {
        ringbuffer.add("item1");
        ringbuffer.add("item2");
        ringbuffer.add("item3");

        ReadManyOperation op = new ReadManyOperation(ringbuffer.getName(), ringbuffer.headSequence(), 1, 1, null);
        op.setNodeEngine(nodeEngine);

        // since there is an item, we don't need to wait
        boolean shouldWait = op.shouldWait();
        assertFalse(shouldWait);

        op.run();

        ReadResultSetImpl response = getReadResultSet(op);
        assertEquals(asList("item1"), response);
        assertEquals(1, response.readCount());
        assertEquals(1, response.size());
    }

    @Test(expected = StaleSequenceException.class)
    public void whenBeforeHead() throws Exception {
        ringbuffer.add("item1");
        ringbuffer.add("item2");
        ringbuffer.add("item3");

        long oldhead = ringbuffer.headSequence();
        ringbufferContainer.setHeadSequence(ringbufferContainer.tailSequence());

        ReadManyOperation op = new ReadManyOperation(ringbuffer.getName(), oldhead, 1, 1, null);
        op.setNodeEngine(nodeEngine);

        op.shouldWait();
    }

    @Test
    public void whenMinimumNumberOfItemsNotAvailable() throws Exception {
        long startSequence = ringbuffer.tailSequence() + 1;
        ReadManyOperation op = new ReadManyOperation(ringbuffer.getName(), startSequence, 3, 3, null);
        op.setNodeEngine(nodeEngine);

        assertTrue(op.shouldWait());
        assertEquals(startSequence, op.sequence);
        assertTrue(getReadResultSet(op).isEmpty());

        ringbuffer.add("item1");
        assertTrue(op.shouldWait());
        assertEquals(startSequence + 1, op.sequence);
        assertEquals(asList("item1"), op.getResponse());

        ringbuffer.add("item2");
        assertTrue(op.shouldWait());
        assertEquals(startSequence + 2, op.sequence);
        assertEquals(asList("item1", "item2"), op.getResponse());

        ringbuffer.add("item3");
        assertFalse(op.shouldWait());
        assertEquals(startSequence + 3, op.sequence);
        assertEquals(asList("item1", "item2", "item3"), op.getResponse());
    }

    @Test
    public void whenBelowMinimumAvailable() throws Exception {
        long startSequence = ringbuffer.tailSequence() + 1;
        ReadManyOperation op = new ReadManyOperation(ringbuffer.getName(), startSequence, 3, 3, null);
        op.setNodeEngine(nodeEngine);

        ringbuffer.add("item1");
        ringbuffer.add("item2");

        assertTrue(op.shouldWait());
        assertEquals(startSequence + 2, op.sequence);
        assertEquals(asList("item1", "item2"), op.getResponse());

        ringbuffer.add("item3");
        assertFalse(op.shouldWait());
        assertEquals(startSequence + 3, op.sequence);
        assertEquals(asList("item1", "item2", "item3"), op.getResponse());
    }

    @Test
    public void whenMinimumNumberOfItemsAvailable() throws Exception {
        long startSequence = ringbuffer.tailSequence() + 1;
        ReadManyOperation op = new ReadManyOperation(ringbuffer.getName(), startSequence, 3, 3, null);
        op.setNodeEngine(nodeEngine);

        ringbuffer.add("item1");
        ringbuffer.add("item2");
        ringbuffer.add("item3");

        assertFalse(op.shouldWait());
        assertEquals(startSequence + 3, op.sequence);
        assertEquals(asList("item1", "item2", "item3"), op.getResponse());
    }

    @Test
    public void whenEnoughItemsAvailable() throws Exception {
        long startSequence = ringbuffer.tailSequence() + 1;
        ReadManyOperation op = new ReadManyOperation(ringbuffer.getName(), startSequence, 1, 3, null);
        op.setNodeEngine(nodeEngine);

        ringbuffer.add("item1");
        ringbuffer.add("item2");
        ringbuffer.add("item3");
        ringbuffer.add("item4");
        ringbuffer.add("item5");


        assertFalse(op.shouldWait());
        ReadResultSetImpl response = getReadResultSet(op);
        assertEquals(startSequence + 3, op.sequence);
        assertEquals(asList("item1", "item2", "item3"), response);
        assertEquals(3, response.readCount());
    }

    @Test
    public void whenEnoughItemsAvailableAndReturnPortable() throws Exception {
        long startSequence = ringbuffer.tailSequence() + 1;
        ReadManyOperation op = new ReadManyOperation(ringbuffer.getName(), startSequence, 1, 3, null, true);
        op.setNodeEngine(nodeEngine);

        ringbuffer.add("item1");
        ringbuffer.add("item2");
        ringbuffer.add("item3");
        ringbuffer.add("item4");
        ringbuffer.add("item5");

        assertFalse(op.shouldWait());
        HeapData response = assertInstanceOf(HeapData.class, op.getResponse());
        PortableReadResultSet readResultSet = serializationService.toObject(response);
        assertEquals(startSequence + 3, op.sequence);
        assertEquals(3, readResultSet.readCount());
        assertEquals(3, readResultSet.getDataItems().size());
        readResultSet.setSerializationService(serializationService);
        assertIterableEquals(readResultSet, "item1", "item2", "item3");
    }

    private ReadResultSetImpl getReadResultSet(ReadManyOperation op) {
        return (ReadResultSetImpl) op.getResponse();
    }

    @Test
    public void whenFilterProvidedAndNoItemsAvailable() throws Exception {
        long startSequence = ringbuffer.tailSequence() + 1;

        IFunction<String, Boolean> filter = new IFunction<String, Boolean>() {
            @Override
            public Boolean apply(String input) {
                return input.startsWith("good");
            }
        };

        ReadManyOperation op = new ReadManyOperation(ringbuffer.getName(), startSequence, 3, 3, filter);
        op.setNodeEngine(nodeEngine);

        assertTrue(op.shouldWait());
        ReadResultSetImpl response = getReadResultSet(op);
        assertEquals(startSequence, op.sequence);
        assertTrue(getReadResultSet(op).isEmpty());

        ringbuffer.add("bad1");
        assertTrue(op.shouldWait());
        assertEquals(startSequence + 1, op.sequence);
        assertEquals(1, response.readCount());
        assertEquals(0, response.size());


        ringbuffer.add("good1");
        assertTrue(op.shouldWait());
        assertEquals(startSequence + 2, op.sequence);
        assertEquals(asList("good1"), response);
        assertEquals(2, response.readCount());

        ringbuffer.add("bad2");
        assertTrue(op.shouldWait());
        assertEquals(startSequence + 3, op.sequence);
        assertEquals(asList("good1"), response);
        assertEquals(3, response.readCount());

        ringbuffer.add("good2");
        assertTrue(op.shouldWait());
        assertEquals(startSequence + 4, op.sequence);
        assertEquals(asList("good1", "good2"), response);
        assertEquals(4, response.readCount());

        ringbuffer.add("bad3");
        assertTrue(op.shouldWait());
        assertEquals(startSequence + 5, op.sequence);
        assertEquals(asList("good1", "good2"), response);
        assertEquals(5, response.readCount());

        ringbuffer.add("good3");
        assertFalse(op.shouldWait());
        assertEquals(startSequence + 6, op.sequence);
        assertEquals(asList("good1", "good2", "good3"), response);
        assertEquals(6, response.readCount());
    }

    @Test
    public void whenFilterProvidedAndAllItemsAvailable() throws Exception {
        long startSequence = ringbuffer.tailSequence() + 1;

        IFunction<String, Boolean> filter = new IFunction<String, Boolean>() {
            @Override
            public Boolean apply(String input) {
                return input.startsWith("good");
            }
        };

        ReadManyOperation op = new ReadManyOperation(ringbuffer.getName(), startSequence, 3, 3, filter);
        op.setNodeEngine(nodeEngine);

        ringbuffer.add("bad1");
        ringbuffer.add("good1");
        ringbuffer.add("bad2");
        ringbuffer.add("good2");
        ringbuffer.add("bad3");
        ringbuffer.add("good3");

        assertFalse(op.shouldWait());
        assertEquals(startSequence + 6, op.sequence);
        assertEquals(asList("good1", "good2", "good3"), op.getResponse());
    }
}
