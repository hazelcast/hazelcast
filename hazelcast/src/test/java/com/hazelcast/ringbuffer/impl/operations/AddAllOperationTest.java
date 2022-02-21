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

package com.hazelcast.ringbuffer.impl.operations;

import com.hazelcast.config.Config;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.ringbuffer.OverflowPolicy;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.ringbuffer.impl.RingbufferService;
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
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static com.hazelcast.test.Accessors.getSerializationService;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class AddAllOperationTest extends HazelcastTestSupport {

    private NodeEngineImpl nodeEngine;
    private SerializationService serializationService;
    private Ringbuffer<Object> ringbuffer;
    private RingbufferService ringbufferService;

    @Before
    public void setup() {
        RingbufferConfig rbConfig = new RingbufferConfig("foo").setCapacity(10).setTimeToLiveSeconds(10);

        Config config = new Config().addRingBufferConfig(rbConfig);

        HazelcastInstance hz = createHazelcastInstance(config);
        nodeEngine = getNodeEngineImpl(hz);
        serializationService = getSerializationService(hz);
        ringbuffer = hz.getRingbuffer(rbConfig.getName());
        ringbufferService = nodeEngine.getService(RingbufferService.SERVICE_NAME);
    }

    @Test
    public void whenFailOverflowPolicy_andNoRemainingCapacity() throws Exception {
        for (int k = 0; k < ringbuffer.capacity(); k++) {
            ringbuffer.add("item");
        }

        Data item = serializationService.toData("newItem");

        AddAllOperation addOperation = getAddAllOperation(new Data[]{item}, FAIL);
        addOperation.run();

        assertFalse(addOperation.shouldNotify());
        assertFalse(addOperation.shouldBackup());
        assertEquals(-1L, addOperation.getResponse());
    }

    @Test
    public void whenFailOverflowPolicy_andRemainingCapacity() throws Exception {
        for (int k = 0; k < ringbuffer.capacity() - 1; k++) {
            ringbuffer.add("item");
        }

        Data item = serializationService.toData("newItem");

        AddAllOperation addOperation = getAddAllOperation(new Data[]{item}, FAIL);
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

        AddAllOperation addOperation = getAddAllOperation(new Data[]{item}, OVERWRITE);
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

        AddAllOperation addOperation = getAddAllOperation(new Data[]{item}, OVERWRITE);
        addOperation.run();

        assertTrue(addOperation.shouldNotify());
        assertTrue(addOperation.shouldBackup());
        assertEquals(ringbuffer.tailSequence(), addOperation.getResponse());
    }

    private AddAllOperation getAddAllOperation(Data[] items, OverflowPolicy policy) {
        AddAllOperation op = new AddAllOperation(ringbuffer.getName(), items, policy);
        op.setPartitionId(ringbufferService.getRingbufferPartitionId(ringbuffer.getName()));
        op.setNodeEngine(nodeEngine);
        return op;
    }
}
