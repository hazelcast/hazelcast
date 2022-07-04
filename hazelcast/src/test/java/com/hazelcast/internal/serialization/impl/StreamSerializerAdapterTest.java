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

package com.hazelcast.internal.serialization.impl;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.defaultserializers.ConstantSerializers;
import com.hazelcast.nio.serialization.Serializer;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.nio.ByteOrder;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class StreamSerializerAdapterTest {

    private StreamSerializerAdapter adapter;
    private ConstantSerializers.IntegerArraySerializer serializer;

    private InternalSerializationService mockSerializationService;

    @Before
    public void setUp() {
        mockSerializationService = mock(InternalSerializationService.class);
        serializer = new ConstantSerializers.IntegerArraySerializer();
        adapter = new StreamSerializerAdapter(serializer);
    }

    @After
    public void tearDown() throws Exception {
        adapter.destroy();
    }

    @Test
    public void testAdaptor() throws Exception {
        int[] testIn = new int[]{1, 2, 3};

        ByteArrayObjectDataOutput out = new ByteArrayObjectDataOutput(100, mockSerializationService, ByteOrder.BIG_ENDIAN);
        ByteArrayObjectDataInput in = new ByteArrayObjectDataInput(out.buffer, mockSerializationService, ByteOrder.BIG_ENDIAN);
        adapter.write(out, testIn);
        int[] read = (int[]) adapter.read(in);

        Serializer impl = adapter.getImpl();

        assertArrayEquals(testIn, read);
        assertEquals(serializer, impl);
    }

    @Test
    public void testAdaptorEqualAndHashCode() {
        StreamSerializerAdapter theOther = new StreamSerializerAdapter(serializer);
        StreamSerializerAdapter theEmptyOne = new StreamSerializerAdapter(null);

        assertEquals(adapter, adapter);
        assertEquals(adapter, theOther);
        assertNotEquals(adapter, null);
        assertNotEquals(adapter, "Not An Adaptor");
        assertNotEquals(adapter, theEmptyOne);

        assertEquals(adapter.hashCode(), serializer.hashCode());

        assertEquals(0, theEmptyOne.hashCode());
    }

    @Test
    public void testString() {
        assertNotNull(adapter.toString());
    }
}
