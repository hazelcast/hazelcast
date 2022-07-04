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

package com.hazelcast.internal.serialization.impl.bufferpool;

import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.internal.nio.BufferObjectDataInput;
import com.hazelcast.internal.nio.BufferObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.Version;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class BufferPoolTest extends HazelcastTestSupport {

    private InternalSerializationService serializationService;
    private BufferPoolImpl bufferPool;

    @Before
    public void setup() {
        serializationService = new DefaultSerializationServiceBuilder().build();
        bufferPool = new BufferPoolImpl(serializationService);
    }

    // ======================= out ==========================================

    @Test
    public void takeOutputBuffer_whenPooledInstance() {
        BufferObjectDataOutput found1 = bufferPool.takeOutputBuffer();
        bufferPool.returnOutputBuffer(found1);
        BufferObjectDataOutput found2 = bufferPool.takeOutputBuffer();

        assertSame(found1, found2);
    }

    @Test
    public void takeOutputBuffer_whenNestedInstance() {
        BufferObjectDataOutput found1 = bufferPool.takeOutputBuffer();
        BufferObjectDataOutput found2 = bufferPool.takeOutputBuffer();

        assertNotSame(found1, found2);
    }

    @Test
    public void returnOutputBuffer_whenNull() {
        bufferPool.returnOutputBuffer(null);
        assertEquals(0, bufferPool.outputQueue.size());
    }

    @Test
    public void returnOutputBuffer() {
        BufferObjectDataOutput out = mock(BufferObjectDataOutput.class);

        bufferPool.returnOutputBuffer(out);

        // lets see if the item was pushed on the queue
        assertEquals(1, bufferPool.outputQueue.size());
        // we need to make sure clear was called
        verify(out, times(1)).clear();
    }

    @Test
    public void returnOutputBuffer_whenOverflowing() {
        for (int k = 0; k < BufferPoolImpl.MAX_POOLED_ITEMS; k++) {
            bufferPool.returnOutputBuffer(mock(BufferObjectDataOutput.class));
        }
        BufferObjectDataOutput out = mock(BufferObjectDataOutput.class);

        bufferPool.returnOutputBuffer(out);

        assertEquals(BufferPoolImpl.MAX_POOLED_ITEMS, bufferPool.outputQueue.size());
    }

    @Test
    public void takeOutputBuffer_whenPooledInstanceWithVersionSetIsReturned() {
        BufferObjectDataOutput found1 = bufferPool.takeOutputBuffer();
        assertEquals(Version.UNKNOWN, found1.getVersion());
        found1.setVersion(Versions.CURRENT_CLUSTER_VERSION);
        bufferPool.returnOutputBuffer(found1);
        BufferObjectDataOutput found2 = bufferPool.takeOutputBuffer();
        assertEquals(Version.UNKNOWN, found2.getVersion());
    }

    // ======================= in ==========================================

    @Test
    public void takeInputBuffer_whenPooledInstance() {
        Data data = new HeapData(new byte[]{});
        BufferObjectDataInput found1 = bufferPool.takeInputBuffer(data);
        bufferPool.returnInputBuffer(found1);
        BufferObjectDataInput found2 = bufferPool.takeInputBuffer(data);

        assertSame(found1, found2);
    }

    @Test
    public void takeInputBuffer_whenNestedInstance() {
        Data data = new HeapData(new byte[]{});
        BufferObjectDataInput found1 = bufferPool.takeInputBuffer(data);
        BufferObjectDataInput found2 = bufferPool.takeInputBuffer(data);

        assertNotSame(found1, found2);
    }

    @Test
    public void returnInputBuffer() {
        BufferObjectDataInput in = mock(BufferObjectDataInput.class);

        bufferPool.returnInputBuffer(in);

        // lets see if the item was pushed on the queue
        assertEquals(1, bufferPool.inputQueue.size());
        // we need to make sure clear was called
        verify(in, times(1)).clear();
    }

    @Test
    public void returnInputBuffer_whenOverflowing() {
        for (int k = 0; k < BufferPoolImpl.MAX_POOLED_ITEMS; k++) {
            bufferPool.returnInputBuffer(mock(BufferObjectDataInput.class));
        }
        BufferObjectDataInput in = mock(BufferObjectDataInput.class);

        bufferPool.returnInputBuffer(in);

        assertEquals(BufferPoolImpl.MAX_POOLED_ITEMS, bufferPool.inputQueue.size());
    }

    @Test
    public void returnInputBuffer_whenNull() {
        bufferPool.returnInputBuffer(null);
        assertEquals(0, bufferPool.inputQueue.size());
    }

    @Test
    public void takeInputBuffer_whenPooledInstanceWithVersionSetIsReturned() {
        Data data = new HeapData(new byte[]{});
        BufferObjectDataInput found1 = bufferPool.takeInputBuffer(data);
        assertEquals(Version.UNKNOWN, found1.getVersion());
        found1.setVersion(Versions.CURRENT_CLUSTER_VERSION);

        bufferPool.returnInputBuffer(found1);
        BufferObjectDataInput found2 = bufferPool.takeInputBuffer(data);
        assertEquals(Version.UNKNOWN, found2.getVersion());
    }
}
