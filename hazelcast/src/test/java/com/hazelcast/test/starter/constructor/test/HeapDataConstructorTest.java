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

package com.hazelcast.test.starter.constructor.test;

import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.starter.constructor.HeapDataConstructor;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HeapDataConstructorTest {

    @Test
    public void testConstructor() {
        SerializationService serializationService = new DefaultSerializationServiceBuilder().build();
        int original = 42;
        Data data = serializationService.toData(original);

        HeapDataConstructor constructor = new HeapDataConstructor(HeapData.class);
        Data clonedData = (Data) constructor.createNew(data);

        assertEquals(data.toByteArray(), clonedData.toByteArray());
        assertEquals(data.getType(), clonedData.getType());
        assertEquals(data.totalSize(), clonedData.totalSize());
        assertEquals(data.dataSize(), clonedData.dataSize());
        assertEquals(data.getHeapCost(), clonedData.getHeapCost());
        assertEquals(data.getPartitionHash(), clonedData.getPartitionHash());
        assertEquals(data.hasPartitionHash(), clonedData.hasPartitionHash());
        assertEquals(data.hash64(), clonedData.hash64());
        assertEquals(data.isPortable(), clonedData.isPortable());
    }
}
