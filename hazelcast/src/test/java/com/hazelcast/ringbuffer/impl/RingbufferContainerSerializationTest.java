/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.ringbuffer.impl;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.nio.BufferObjectDataInput;
import com.hazelcast.internal.nio.BufferObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;

import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static com.hazelcast.internal.nio.IOUtil.closeResource;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static com.hazelcast.test.Accessors.getSerializationService;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * This test verifies that the RingbufferContainer can serialize itself
 * correctly using different in memory formats and using enabling/disabling
 * TTL.
 * <p>
 * This test also forces a delay between the serialization and deserialization. If a ringbuffer is configured
 * with a ttl, we don't want to send over the actual expiration time, because on a different member in the
 * cluster, there could be a big time difference which can lead to the ringbuffer immediately cleaning or cleaning
 * very very late.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class RingbufferContainerSerializationTest extends HazelcastTestSupport {

    private static final int CLOCK_DIFFERENCE_MS = 2000;

    private InternalSerializationService serializationService;
    private NodeEngineImpl nodeEngine;

    @Before
    public void setup() {
        HazelcastInstance hz = createHazelcastInstance();
        this.nodeEngine = getNodeEngineImpl(hz);
        this.serializationService = getSerializationService(hz);
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
        final RingbufferConfig config = new RingbufferConfig("foobar")
                .setCapacity(3)
                .setAsyncBackupCount(2)
                .setBackupCount(2)
                .setInMemoryFormat(inMemoryFormat)
                .setTimeToLiveSeconds(ttlSeconds);

        final RingbufferContainer rbContainer = getRingbufferContainer(config);
        testSerialization(rbContainer);

        for (int k = 0; k < config.getCapacity() * 2; k++) {
            rbContainer.add(toData("old"));
            testSerialization(rbContainer);
        }

        // now we are going to force the head to move
        final ArrayRingbuffer ringbuffer = (ArrayRingbuffer) rbContainer.getRingbuffer();
        for (int k = 0; k < config.getCapacity() / 2; k++) {
            ringbuffer.getItems()[k] = null;
            if (ttlSeconds != 0) {
                // we need to set the expiration slot to 0, because it won't be serialized (optimization)
                // serialization will only dump what is between head and tail
                rbContainer.getExpirationPolicy().ringExpirationMs[k] = 0;
            }
            ringbuffer.setHeadSequence(ringbuffer.headSequence() + 1);
            testSerialization(rbContainer);
        }
    }

    private RingbufferContainer getRingbufferContainer(RingbufferConfig config) {
        // partitionId is irrelevant for this test
        return new RingbufferContainer(RingbufferService.getRingbufferNamespace(config.getName()), config, nodeEngine, 0);
    }

    private void testSerialization(RingbufferContainer original) {
        final RingbufferContainer clone = clone(original);

        final RingbufferExpirationPolicy originalExpirationPolicy = original.getExpirationPolicy();
        final RingbufferExpirationPolicy cloneExpirationPolicy = clone.getExpirationPolicy();

        assertEquals(original.headSequence(), clone.headSequence());
        assertEquals(original.tailSequence(), clone.tailSequence());
        assertEquals(original.getCapacity(), clone.getCapacity());
        if (originalExpirationPolicy != null) {
            assertNotNull(cloneExpirationPolicy);
            assertEquals(originalExpirationPolicy.getTtlMs(), cloneExpirationPolicy.getTtlMs());
        }
        final ArrayRingbuffer originalRingbuffer = (ArrayRingbuffer) original.getRingbuffer();
        final ArrayRingbuffer cloneRingbuffer = (ArrayRingbuffer) original.getRingbuffer();
        assertArrayEquals(originalRingbuffer.getItems(), cloneRingbuffer.getItems());

        // the most complicated part is the expiration
        if (original.getConfig().getTimeToLiveSeconds() == 0) {
            assertNull(cloneExpirationPolicy);
            return;
        }
        assertNotNull(originalExpirationPolicy);
        assertNotNull(cloneExpirationPolicy);

        assertEquals(originalExpirationPolicy.ringExpirationMs.length, cloneExpirationPolicy.ringExpirationMs.length);

        for (long seq = original.headSequence(); seq <= original.tailSequence(); seq++) {
            int index = originalExpirationPolicy.toIndex(seq);
            long originalExpiration = originalExpirationPolicy.ringExpirationMs[index];
            long actualExpiration = cloneExpirationPolicy.ringExpirationMs[index];
            double difference = actualExpiration - originalExpiration;
            assertTrue("difference was: " + difference, difference > 0.50 * CLOCK_DIFFERENCE_MS);
            assertTrue("difference was: " + difference, difference < 1.50 * CLOCK_DIFFERENCE_MS);
        }
    }

    private RingbufferContainer clone(RingbufferContainer original) {
        BufferObjectDataOutput out = serializationService.createObjectDataOutput(100000);
        try {
            out.writeObject(original);
            byte[] bytes = out.toByteArray();
            sleepMillis(CLOCK_DIFFERENCE_MS);
            BufferObjectDataInput in = serializationService.createObjectDataInput(bytes);
            RingbufferContainer clone = in.readObject();
            return clone;
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            closeResource(out);
        }
    }

    @SuppressWarnings("SameParameterValue")
    private Data toData(Object item) {
        return serializationService.toData(item);
    }
}
