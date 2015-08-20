
package com.hazelcast.ringbuffer.impl;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.ringbuffer.StaleSequenceException;
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

    @Before
    public void setup() {
        serializationService = new DefaultSerializationServiceBuilder().build();
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
        RingbufferConfig config = new RingbufferConfig("foo").setCapacity(100).setTimeToLiveSeconds(0);
        RingbufferContainer ringbuffer = new RingbufferContainer(config, serializationService);

        assertEquals(config.getCapacity(), ringbuffer.getCapacity());
        assertNotNull(ringbuffer.ringItems);
        assertEquals(config.getCapacity(), ringbuffer.ringItems.length);
        assertNull(ringbuffer.ringExpirationMs);
        assertSame(config, ringbuffer.getConfig());
        assertEquals(-1, ringbuffer.tailSequence());
        assertEquals(0, ringbuffer.headSequence());
    }

    @Test
    public void constructionWithTTL() {
        RingbufferConfig config = new RingbufferConfig("foo").setCapacity(100).setTimeToLiveSeconds(30);
        RingbufferContainer ringbuffer = new RingbufferContainer(config, serializationService);

        assertEquals(config.getCapacity(), ringbuffer.getCapacity());
        assertNotNull(ringbuffer.ringExpirationMs);
        assertEquals(config.getCapacity(), ringbuffer.ringExpirationMs.length);
        assertSame(config, ringbuffer.getConfig());
        assertEquals(-1, ringbuffer.tailSequence());
        assertEquals(0, ringbuffer.headSequence());
    }

    @Test
    public void remainingCapacity_whenTTLDisabled() {
        RingbufferConfig config = new RingbufferConfig("foo").setCapacity(100).setTimeToLiveSeconds(0);
        RingbufferContainer ringbuffer = new RingbufferContainer(config, serializationService);

        assertEquals(config.getCapacity(), ringbuffer.remainingCapacity());

        ringbuffer.add(toData("1"));
        ringbuffer.add(toData("2"));
        assertEquals(config.getCapacity(), ringbuffer.remainingCapacity());
    }

    @Test
    public void remainingCapacity_whenTTLEnabled() {
        RingbufferConfig config = new RingbufferConfig("foo").setCapacity(100).setTimeToLiveSeconds(1);
        RingbufferContainer ringbuffer = new RingbufferContainer(config, serializationService);

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
        RingbufferContainer ringbuffer = new RingbufferContainer(config, serializationService);

        assertEquals(0, ringbuffer.size());
    }

    @Test
    public void size_whenAddingManyItems() {
        RingbufferConfig config = new RingbufferConfig("foo").setCapacity(100);
        RingbufferContainer ringbuffer = new RingbufferContainer(config, serializationService);

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
        RingbufferContainer ringbuffer = new RingbufferContainer(config, serializationService);
        ringbuffer.add(toData("foo"));
        ringbuffer.add(toData("bar"));

        assertEquals(1, ringbuffer.tailSequence());
        assertEquals(0, ringbuffer.headSequence());
    }

    @Test
    public void add_whenWrapped() {
        RingbufferConfig config = new RingbufferConfig("foo").setInMemoryFormat(InMemoryFormat.OBJECT).setCapacity(3);
        RingbufferContainer ringbuffer = new RingbufferContainer(config, serializationService);

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
        RingbufferContainer ringbuffer = new RingbufferContainer(config, serializationService);

        ringbuffer.add(toData("1"));
        ringbuffer.add(toData("2"));
        ringbuffer.add(toData("3"));
        // this one will overwrite the first item.
        ringbuffer.add(toData("4"));

        ringbuffer.read(0);
    }

    @Test
    public void add_whenBinaryInMemoryFormat() {
        RingbufferConfig config = new RingbufferConfig("foo").setInMemoryFormat(InMemoryFormat.BINARY);
        RingbufferContainer ringbuffer = new RingbufferContainer(config, serializationService);

        ringbuffer.add(toData("foo"));
        assertInstanceOf(Data.class, ringbuffer.ringItems[0]);
    }

    @Test
    public void add_inObjectInMemoryFormat() {
        RingbufferConfig config = new RingbufferConfig("foo").setInMemoryFormat(InMemoryFormat.OBJECT);
        RingbufferContainer ringbuffer = new RingbufferContainer(config, serializationService);

        ringbuffer.add(toData("foo"));
        assertInstanceOf(String.class, ringbuffer.ringItems[0]);
    }

    // ===================================================

    @Test
    public void readMany() {

    }
}
