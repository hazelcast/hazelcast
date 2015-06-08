package com.hazelcast.ringbuffer.impl;


import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.nio.BufferObjectDataInput;
import com.hazelcast.nio.BufferObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.nio.serialization.SerializationServiceBuilder;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;

import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static com.hazelcast.nio.IOUtil.closeResource;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * This test verifies that the RingbufferContainer can serialize itself
 * correctly using different in memory formats and using enabling/disabling
 * TTL.
 *
 * This test also forces a delay between the serialization and deserialization. If a ringbuffer is configured
 * with a ttl, we don't want to send over the actual expiration time, because on a different member in the
 * cluster, there could be a big time difference which can lead to the ringbuffer immediately cleaning or cleaning
 * very very late.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class RingbufferContainerSerializationTest extends HazelcastTestSupport {

    private static final int CLOCK_DIFFERENCE_MS = 2000;

    private SerializationService serializationService;

    @Before
    public void setup() {
        SerializationServiceBuilder serializationServiceBuilder = new DefaultSerializationServiceBuilder();
        serializationService = serializationServiceBuilder.build();
    }

    private Data toData(Object item) {
        return serializationService.toData(item);
    }

    @Test
    public void whenObjectInMemoryFormat_andTTLEnabled() {
        test(OBJECT, 100);
    }

    @Test
    public void whenObjectInMemoryFormat_andTTLDisabled() {
        test(OBJECT, 0);
    }

    @Test
    public void whenBinaryInMemoryFormat_andTTLEnabled() {
        test(BINARY, 100);
    }

    @Test
    public void whenBinaryInMemoryFormat_andTTLDisabled() {
        test(BINARY, 0);
    }

    public void test(InMemoryFormat inMemoryFormat, int ttlSeconds) {
        RingbufferConfig config = new RingbufferConfig("foobar")
                .setCapacity(5)
                .setAsyncBackupCount(2)
                .setBackupCount(2)
                .setInMemoryFormat(inMemoryFormat)
                .setTimeToLiveSeconds(ttlSeconds);

        RingbufferContainer ringbuffer = new RingbufferContainer(config, serializationService);
        testSerialization(ringbuffer);

        for (int k = 0; k < config.getCapacity() * 2; k++) {
            ringbuffer.add(toData("old"));
            testSerialization(ringbuffer);
        }

        // now we are going to force the head to move
        for (int k = 0; k < config.getCapacity() / 2; k++) {
            ringbuffer.ringItems[k] = null;
            if (ttlSeconds != 0) {
                // we need to set the expiration slot to 0, because it won't be serialized (optimization)
                // serialization will only dump what is between head and tail
                ringbuffer.ringExpirationMs[k] = 0;
            }
            ringbuffer.headSequence++;
            testSerialization(ringbuffer);
        }
    }

    private void testSerialization(RingbufferContainer original) {
        RingbufferContainer clone = clone(original);

        assertEquals(original.headSequence, clone.headSequence);
        assertEquals(original.tailSequence, clone.tailSequence);
        assertEquals(original.capacity, clone.capacity);
        assertEquals(original.ttlMs, clone.ttlMs);
        assertArrayEquals(original.ringItems, clone.ringItems);


        // the most complicated part is the expiration.
        if (original.getConfig().getTimeToLiveSeconds() == 0) {
            assertNull(clone.ringExpirationMs);
            return;
        }

        assertNotNull(clone.ringExpirationMs);
        assertEquals(original.ringExpirationMs.length, clone.ringExpirationMs.length);

        for (long seq = original.headSequence; seq <= original.tailSequence; seq++) {
            int index = original.toIndex(seq);
            long originalExpiration = original.ringExpirationMs[index];
            long actualExpiration = clone.ringExpirationMs[index];
            double difference = actualExpiration - originalExpiration;
            assertTrue(difference > 0.75 * CLOCK_DIFFERENCE_MS);
            assertTrue(difference < 1.25 * CLOCK_DIFFERENCE_MS);
        }
   }

    private RingbufferContainer clone(RingbufferContainer original) {
        BufferObjectDataOutput out = serializationService.createObjectDataOutput(100000);
        BufferObjectDataInput in = null;
        try {
            original.writeData(out);
            byte[] bytes = out.toByteArray();
            sleepMillis(CLOCK_DIFFERENCE_MS);
            RingbufferContainer clone = new RingbufferContainer(original.name);
            in = serializationService.createObjectDataInput(bytes);
            clone.readData(in);
            return clone;
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            closeResource(out);
            closeResource(in);
        }
    }
}
