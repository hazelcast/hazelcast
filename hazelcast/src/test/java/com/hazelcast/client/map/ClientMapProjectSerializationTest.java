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

package com.hazelcast.client.map;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.projection.Projection;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientMapProjectSerializationTest extends HazelcastTestSupport {
    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @Before
    public void setup() {

    }

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }


    public static class ValuesProjection
            implements Projection<Map.Entry<Integer, OnlyDeserializedTwiceObject>, OnlyDeserializedTwiceObject>, Serializable {

        @Override
        public OnlyDeserializedTwiceObject transform(Map.Entry<Integer, OnlyDeserializedTwiceObject> input) {
            return input.getValue();
        }
    }

    @Test
    public void testProjectObjectShouldDeserializedOnlyTwice() {
        // One deserialization on server when object is accessed from transform
        // Second deserialization on client side when result passed to user
        hazelcastFactory.newHazelcastInstance();
        HazelcastInstance client = hazelcastFactory.newHazelcastClient();

        IMap<Integer, OnlyDeserializedTwiceObject> map = client.getMap("test");
        OnlyDeserializedTwiceObject value = new OnlyDeserializedTwiceObject("test");
        map.put(1, value);
        Collection<OnlyDeserializedTwiceObject> result = map.project(new ValuesProjection());

        assertEquals(Collections.singletonList(value), result);
    }

    private static class OnlyDeserializedTwiceObject implements DataSerializable {

        private String value;

        private static AtomicInteger readCalled = new AtomicInteger(0);

        OnlyDeserializedTwiceObject() {
        }

        OnlyDeserializedTwiceObject(String value) {
            this.value = value;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeString(value);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            if (readCalled.incrementAndGet() > 2) {
                throw new AssertionError("Read called more than twice!!!");
            }
            value = in.readString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof OnlyDeserializedTwiceObject)) {
                return false;
            }

            OnlyDeserializedTwiceObject that = (OnlyDeserializedTwiceObject) o;

            return value.equals(that.value);
        }

        @Override
        public int hashCode() {
            return value.hashCode();
        }
    }
}
