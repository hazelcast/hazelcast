/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.instance.HazelcastInstanceProxy;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.TestObject;
import com.hazelcast.nio.serialization.Data;

import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;

public final class TestSerialization {

    private TestSerialization() {
    }

    public static void main(String[] args) throws Exception {

        Map<Integer, String> map = new HashMap<>();
        map.put(42, "hello world");
        TestObject testObject = new TestObject(
                true, (byte) 42, 'd', (short) 42, 42, 42, 4.2f, 4.2, "hello world",
                newArrayList("hello world"),
                newHashSet("hello world"),
                map
        );

        HazelcastInstanceProxy proxy = (HazelcastInstanceProxy) Hazelcast.newHazelcastInstance();
        InternalSerializationService service = proxy.getSerializationService();

        Data data = service.toData(testObject);
        byte[] serialized = data.toByteArray();

        try (OutputStream out = new FileOutputStream("./testObject")) {
            out.write(serialized);
        }

        proxy.shutdown();
    }
}
