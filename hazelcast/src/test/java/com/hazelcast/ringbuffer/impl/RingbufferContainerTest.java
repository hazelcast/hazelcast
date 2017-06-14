/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.ringbuffer.StaleSequenceException;
import com.hazelcast.spi.DistributedObjectNamespace;
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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

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

    // ======================= construction =======================

    @Test
    public void constructionNoTTL() {
        final RingbufferConfig config = new RingbufferConfig("foo").setCapacity(100).setTimeToLiveSeconds(0);
        final RingbufferContainer rbContainer = getRingbufferContainer(config);

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
        RingbufferContainer ringbuffer = getRingbufferContainer(config);

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
        RingbufferContainer ringbuffer = getRingbufferContainer(config);

        assertEquals(config.getCapacity(), ringbuffer.remainingCapacity());

        ringbuffer.add(toData("1"));
        ringbuffer.add(toData("2"));
        assertEquals(config.getCapacity(), ringbuffer.remainingCapacity());
    }

    private RingbufferContainer getRingbufferContainer(RingbufferConfig config) {
        return new RingbufferContainer(
                RingbufferService.getRingbufferNamespace(config.getName()), config,
                nodeEngine.getSerializationService(), nodeEngine.getConfigClassLoader());
    }

    @Test
    public void remainingCapacity_whenTTLEnabled() {
        RingbufferConfig config = new RingbufferConfig("foo").setCapacity(100).setTimeToLiveSeconds(1);
        RingbufferContainer ringbuffer = getRingbufferContainer(config);

        assertEquals(config.getCapacity(), ringbuffer.remainingCapacity());

        ringbuffer.add(toData("1"));
        assertEquals(config.getCapacity() - 1, ringbuffer.remainingCapacity());

        ringbuffer.add(toData("2"));
        assertEquals(config.getCapacity() - 2, ringbuffer.remainingCapacity());
    }

    // ======================= size =======================

    @Test
    public void size_whenEmpty() {
        RingbufferConfig config = new RingbufferConfig("foo").setCapacity(100);
        RingbufferContainer ringbuffer = getRingbufferContainer(config);

        assertEquals(0, ringbuffer.size());
        assertTrue(ringbuffer.isEmpty());
    }

    @Test
    public void size_whenAddingManyItems() {
        RingbufferConfig config = new RingbufferConfig("foo").setCapacity(100);
        RingbufferContainer ringbuffer = getRingbufferContainer(config);

        for (int k = 0; k < config.getCapacity(); k++) {
            ringbuffer.add(toData(""));
            assertEquals(k + 1, ringbuffer.size());
        }
        assertFalse(ringbuffer.isEmpty());

        // at this point the ringbuffer is full. So if we add more items, the oldest item is overwritten
        // and therefor the size remains the same
        for (int k = 0; k < config.getCapacity(); k++) {
            ringbuffer.add(toData(""));
            assertEquals(config.getCapacity(), ringbuffer.size());
        }
    }

    // ======================= add =======================

    @Test
    public void add() {
        RingbufferConfig config = new RingbufferConfig("foo").setCapacity(10);
        RingbufferContainer ringbuffer = getRingbufferContainer(config);
        ringbuffer.add(toData("foo"));
        ringbuffer.add(toData("bar"));

        assertEquals(1, ringbuffer.tailSequence());
        assertEquals(0, ringbuffer.headSequence());
    }

    @Test
    public void add_whenWrapped() {
        RingbufferConfig config = new RingbufferConfig("foo").setInMemoryFormat(InMemoryFormat.OBJECT).setCapacity(3);
        RingbufferContainer ringbuffer = getRingbufferContainer(config);

        ringbuffer.add(toData("1"));
        assertEquals(0, ringbuffer.headSequence());
        assertEquals(0, ringbuffer.tailSequence());
        assertEquals(toData("1"), ringbuffer.readAsData(0));

        ringbuffer.add(toData("2"));
        assertEquals(1, ringbuffer.tailSequence());
        assertEquals(0, ringbuffer.headSequence());
        assertEquals(toData("1"), ringbuffer.readAsData(0));
        assertEquals(toData("2"), ringbuffer.readAsData(1));

        ringbuffer.add(toData("3"));
        assertEquals(2, ringbuffer.tailSequence());
        assertEquals(0, ringbuffer.headSequence());
        assertEquals(toData("1"), ringbuffer.readAsData(0));
        assertEquals(toData("2"), ringbuffer.readAsData(1));
        assertEquals(toData("3"), ringbuffer.readAsData(2));

        ringbuffer.add(toData("4"));
        assertEquals(3, ringbuffer.tailSequence());
        assertEquals(1, ringbuffer.headSequence());
        assertEquals(toData("2"), ringbuffer.readAsData(1));
        assertEquals(toData("3"), ringbuffer.readAsData(2));
        assertEquals(toData("4"), ringbuffer.readAsData(3));

        ringbuffer.add(toData("5"));
        assertEquals(4, ringbuffer.tailSequence());
        assertEquals(2, ringbuffer.headSequence());
        assertEquals(toData("3"), ringbuffer.readAsData(2));
        assertEquals(toData("4"), ringbuffer.readAsData(3));
        assertEquals(toData("5"), ringbuffer.readAsData(4));
    }

    @Test(expected = StaleSequenceException.class)
    public void read_whenStaleSequence() {
        RingbufferConfig config = new RingbufferConfig("foo").setCapacity(3);
        RingbufferContainer<Data> ringbuffer = getRingbufferContainer(config);

        ringbuffer.add(toData("1"));
        ringbuffer.add(toData("2"));
        ringbuffer.add(toData("3"));
        // this one will overwrite the first item
        ringbuffer.add(toData("4"));

        ringbuffer.readAsData(0);
    }

    @Test
    public void add_whenBinaryInMemoryFormat() {
        final RingbufferConfig config = new RingbufferConfig("foo").setInMemoryFormat(InMemoryFormat.BINARY);
        final RingbufferContainer rbContainer = getRingbufferContainer(config);
        final ArrayRingbuffer ringbuffer = (ArrayRingbuffer) rbContainer.getRingbuffer();

        rbContainer.add(toData("foo"));
        assertInstanceOf(Data.class, ringbuffer.ringItems[0]);
    }

    @Test
    public void add_inObjectInMemoryFormat() {
        final RingbufferConfig config = new RingbufferConfig("foo").setInMemoryFormat(InMemoryFormat.OBJECT);
        final RingbufferContainer rbContainer = getRingbufferContainer(config);
        final ArrayRingbuffer ringbuffer = (ArrayRingbuffer) rbContainer.getRingbuffer();

        rbContainer.add(toData("foo"));
        assertInstanceOf(String.class, ringbuffer.ringItems[0]);
    }
}
