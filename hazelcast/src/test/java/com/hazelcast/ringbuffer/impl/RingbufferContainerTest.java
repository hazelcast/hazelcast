package com.hazelcast.ringbuffer.impl;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.ringbuffer.StaleSequenceException;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class RingbufferContainerTest extends HazelcastTestSupport {

    private SerializationService serializationService;
    private NodeEngineImpl nodeEngine;

    @Before
    public void setup() {
        HazelcastInstance hz = createHazelcastInstance();
        nodeEngine = getNodeEngineImpl(hz);
        serializationService = getSerializationService(hz);
    }

    private Data toData(Object item) {
        return serializationService.toData(item);
    }

    private <E> E toObject(Data item) {
        return serializationService.toObject(item);
    }

    // =============== construction =======================

    @Test
    public void constructionNoTTL() {
        final RingbufferConfig config = new RingbufferConfig("foo").setCapacity(100).setTimeToLiveSeconds(0);
        final RingbufferContainer rbContainer = new RingbufferContainer(config.getName(), config,
                nodeEngine.getSerializationService(), nodeEngine.getConfigClassLoader());

        assertEquals(config.getCapacity(), rbContainer.getCapacity());
        final ArrayRingbuffer ringbuffer = (ArrayRingbuffer) rbContainer.getRingbuffer();
        assertNotNull(ringbuffer.ringItems);
        assertEquals(config.getCapacity(), ringbuffer.ringItems.length);
        assertNull(rbContainer.getExpirationPolicy());
        assertSame(config, rbContainer.getConfig());
        assertEquals(-1, rbContainer.tailSequence());
        assertEquals(0, rbContainer.headSequence());
    }

    @Test
    public void constructionWithTTL() {
        RingbufferConfig config = new RingbufferConfig("foo").setCapacity(100).setTimeToLiveSeconds(30);
        RingbufferContainer ringbuffer = new RingbufferContainer(config.getName(), config,
                nodeEngine.getSerializationService(), nodeEngine.getConfigClassLoader());

        assertEquals(config.getCapacity(), ringbuffer.getCapacity());
        assertNotNull(ringbuffer.getExpirationPolicy());
        assertEquals(config.getCapacity(), ringbuffer.getExpirationPolicy().ringExpirationMs.length);
        assertSame(config, ringbuffer.getConfig());
        assertEquals(-1, ringbuffer.tailSequence());
        assertEquals(0, ringbuffer.headSequence());
    }

    @Test
    public void remainingCapacity_whenTTLDisabled() {
        RingbufferConfig config = new RingbufferConfig("foo").setCapacity(100).setTimeToLiveSeconds(0);
        RingbufferContainer ringbuffer = new RingbufferContainer(config.getName(), config,
                nodeEngine.getSerializationService(), nodeEngine.getConfigClassLoader());

        assertEquals(config.getCapacity(), ringbuffer.remainingCapacity());

        ringbuffer.add(toData("1"));
        ringbuffer.add(toData("2"));
        assertEquals(config.getCapacity(), ringbuffer.remainingCapacity());
    }

    @Test
    public void remainingCapacity_whenTTLEnabled() {
        RingbufferConfig config = new RingbufferConfig("foo").setCapacity(100).setTimeToLiveSeconds(1);
        RingbufferContainer ringbuffer = new RingbufferContainer(config.getName(), config,
                nodeEngine.getSerializationService(), nodeEngine.getConfigClassLoader());

        assertEquals(config.getCapacity(), ringbuffer.remainingCapacity());

        ringbuffer.add(toData("1"));
        assertEquals(config.getCapacity() - 1, ringbuffer.remainingCapacity());

        ringbuffer.add(toData("2"));
        assertEquals(config.getCapacity() - 2, ringbuffer.remainingCapacity());
    }

    // ===================================================

    @Test
    public void size_whenEmpty() {
        RingbufferConfig config = new RingbufferConfig("foo").setCapacity(100);
        RingbufferContainer ringbuffer = new RingbufferContainer(config.getName(), config,
                nodeEngine.getSerializationService(), nodeEngine.getConfigClassLoader());

        assertEquals(0, ringbuffer.size());
    }

    @Test
    public void size_whenAddingManyItems() {
        RingbufferConfig config = new RingbufferConfig("foo").setCapacity(100);
        RingbufferContainer ringbuffer = new RingbufferContainer(config.getName(), config,
                nodeEngine.getSerializationService(), nodeEngine.getConfigClassLoader());

        for (int k = 0; k < config.getCapacity(); k++) {
            ringbuffer.add(toData(""));
            assertEquals(k + 1, ringbuffer.size());
        }

        // at this point the ringbuffer is full. So if we add more items, the oldest item is overwritte
        // and therefor the size remains the same
        for (int k = 0; k < config.getCapacity(); k++) {
            ringbuffer.add(toData(""));
            assertEquals(config.getCapacity(), ringbuffer.size());
        }
    }

    // ===================================================

    @Test
    public void add() {
        RingbufferConfig config = new RingbufferConfig("foo").setCapacity(10);
        RingbufferContainer ringbuffer = new RingbufferContainer(config.getName(), config,
                nodeEngine.getSerializationService(), nodeEngine.getConfigClassLoader());
        ringbuffer.add(toData("foo"));
        ringbuffer.add(toData("bar"));

        assertEquals(1, ringbuffer.tailSequence());
        assertEquals(0, ringbuffer.headSequence());
    }

    @Test
    public void add_whenWrapped() {
        RingbufferConfig config = new RingbufferConfig("foo").setInMemoryFormat(InMemoryFormat.OBJECT).setCapacity(3);
        RingbufferContainer ringbuffer = new RingbufferContainer(config.getName(), config,
                nodeEngine.getSerializationService(), nodeEngine.getConfigClassLoader());

        ringbuffer.add(toData("1"));
        assertEquals(0, ringbuffer.headSequence());
        assertEquals(0, ringbuffer.tailSequence());
        assertEquals(toData("1"), ringbuffer.read(0));

        ringbuffer.add(toData("2"));
        assertEquals(1, ringbuffer.tailSequence());
        assertEquals(0, ringbuffer.headSequence());
        assertEquals(toData("1"), ringbuffer.read(0));
        assertEquals(toData("2"), ringbuffer.read(1));

        ringbuffer.add(toData("3"));
        assertEquals(2, ringbuffer.tailSequence());
        assertEquals(0, ringbuffer.headSequence());
        assertEquals(toData("1"), ringbuffer.read(0));
        assertEquals(toData("2"), ringbuffer.read(1));
        assertEquals(toData("3"), ringbuffer.read(2));

        ringbuffer.add(toData("4"));
        assertEquals(3, ringbuffer.tailSequence());
        assertEquals(1, ringbuffer.headSequence());
        assertEquals(toData("2"), ringbuffer.read(1));
        assertEquals(toData("3"), ringbuffer.read(2));
        assertEquals(toData("4"), ringbuffer.read(3));

        ringbuffer.add(toData("5"));
        assertEquals(4, ringbuffer.tailSequence());
        assertEquals(2, ringbuffer.headSequence());
        assertEquals(toData("3"), ringbuffer.read(2));
        assertEquals(toData("4"), ringbuffer.read(3));
        assertEquals(toData("5"), ringbuffer.read(4));
    }

    @Test(expected = StaleSequenceException.class)
    public void read_whenStaleSequence() {
        RingbufferConfig config = new RingbufferConfig("foo").setCapacity(3);
        RingbufferContainer ringbuffer = new RingbufferContainer(config.getName(), config,
                nodeEngine.getSerializationService(), nodeEngine.getConfigClassLoader());

        ringbuffer.add(toData("1"));
        ringbuffer.add(toData("2"));
        ringbuffer.add(toData("3"));
        // this one will overwrite the first item.
        ringbuffer.add(toData("4"));

        ringbuffer.read(0);
    }

    @Test
    public void add_whenBinaryInMemoryFormat() {
        final RingbufferConfig config = new RingbufferConfig("foo").setInMemoryFormat(InMemoryFormat.BINARY);
        final RingbufferContainer rbContainer = new RingbufferContainer(config.getName(), config,
                nodeEngine.getSerializationService(), nodeEngine.getConfigClassLoader());
        final ArrayRingbuffer ringbuffer = (ArrayRingbuffer) rbContainer.getRingbuffer();

        rbContainer.add(toData("foo"));
        assertInstanceOf(Data.class, ringbuffer.ringItems[0]);
    }

    @Test
    public void add_inObjectInMemoryFormat() {
        final RingbufferConfig config = new RingbufferConfig("foo").setInMemoryFormat(InMemoryFormat.OBJECT);
        final RingbufferContainer rbContainer = new RingbufferContainer(config.getName(), config,
                nodeEngine.getSerializationService(), nodeEngine.getConfigClassLoader());
        final ArrayRingbuffer ringbuffer = (ArrayRingbuffer) rbContainer.getRingbuffer();

        rbContainer.add(toData("foo"));
        assertInstanceOf(String.class, ringbuffer.ringItems[0]);
    }

    // ===================================================

    @Test
    public void readMany() {

    }
}
