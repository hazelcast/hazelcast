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

package com.hazelcast.map.impl;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestJavaSerializationUtils;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.io.Serializable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LazyMapEntryTest extends HazelcastTestSupport {

    private LazyMapEntry entry = new LazyMapEntry();
    private InternalSerializationService serializationService = new DefaultSerializationServiceBuilder().build();

    @Test
    public void testJavaSerialization() throws IOException, ClassNotFoundException {
        Data keyData = serializationService.toData("keyData");
        Data valueData = serializationService.toData("valueData");
        entry.init(serializationService, keyData, valueData, null);
        LazyMapEntry copy = TestJavaSerializationUtils.serializeAndDeserialize(entry);

        assertEquals(entry, copy);
    }

    @Test
    public void testIdentifiedDataSerializableSerialization() throws IOException, ClassNotFoundException {
        Data keyData = serializationService.toData("keyData");
        Data valueData = serializationService.toData("valueData");
        entry.init(serializationService, keyData, valueData, null);
        LazyMapEntry copy = serializationService.toObject(serializationService.toData(entry));

        assertEquals(entry, copy);
    }

    @Test
    public void test_init() {
        Data keyData = serializationService.toData("keyData");
        Data valueData = serializationService.toData("valueData");
        entry.init(serializationService, keyData, valueData, null);

        Object valueObject = entry.getValue();

        entry.init(serializationService, keyData, valueObject, null);

        assertTrue("Old valueData should not be here", valueData != entry.getValueData());
    }

    @Test
    public void test_init_doesNotSerializeObject() {
        Data key = serializationService.toData("keyData");
        MyObject value = new MyObject();

        entry.init(serializationService, key, value, null);

        assertEquals(0, value.serializedCount);
    }

    @Test
    public void test_init_doesNotDeserializeObject() {
        MyObject keyObject = new MyObject();
        MyObject valueObject = new MyObject();

        Data keyData = serializationService.toData(keyObject);
        Data valueData = serializationService.toData(valueObject);

        entry.init(serializationService, keyData, valueData, null);

        assertEquals(1, keyObject.serializedCount);
        assertEquals(1, valueObject.serializedCount);
        assertEquals(0, keyObject.deserializedCount);
        assertEquals(0, valueObject.deserializedCount);
    }

    @Test
    public void testLazyDeserializationWorks() {
        MyObject keyObject = new MyObject();
        MyObject valueObject = new MyObject();

        Data keyData = serializationService.toData(keyObject);
        Data valueData = serializationService.toData(valueObject);

        entry.init(serializationService, keyData, valueData, null);

        Object key = entry.getKey();
        Object value = entry.getValue();

        assertInstanceOf(MyObject.class, key);
        assertInstanceOf(MyObject.class, value);
        assertEquals(1, ((MyObject) key).deserializedCount);
        assertEquals(1, ((MyObject) value).deserializedCount);
    }

    private static class MyObject implements DataSerializable, Serializable {

        int serializedCount = 0;
        int deserializedCount = 0;

        MyObject() {
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeInt(++serializedCount);
            out.writeInt(deserializedCount);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            serializedCount = in.readInt();
            deserializedCount = in.readInt() + 1;
        }

        @Override
        public String toString() {
            return "MyObject{"
                    + "deserializedCount=" + deserializedCount
                    + ", serializedCount=" + serializedCount
                    + '}';
        }
    }
}
