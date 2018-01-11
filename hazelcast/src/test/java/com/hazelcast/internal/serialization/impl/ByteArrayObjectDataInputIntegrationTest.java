/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ByteArrayObjectDataInputIntegrationTest {
    private final InternalSerializationService serializationService = new DefaultSerializationServiceBuilder().build();

    @Test
    public void testNotNull() throws Exception {
        readDataAsObject("foo");
    }

    @Test
    public void testNull() throws Exception {
        readDataAsObject(null);
    }

    public void readDataAsObject(Object value) throws Exception {
        Data data = serializationService.toData(value);
        MyObject myObject = new MyObject(data);
        Data myObjectData = serializationService.toData(myObject);
        MyObject myObjectDeserialized = serializationService.toObject(myObjectData);

        assertEquals(value, myObjectDeserialized.o);
    }

    private static class MyObject implements DataSerializable {
        private Data data;
        private Object o;

        public MyObject() {
        }

        public MyObject(Data data) {
            this.data = data;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeData(data);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            o = in.readDataAsObject();
        }
    }
}
