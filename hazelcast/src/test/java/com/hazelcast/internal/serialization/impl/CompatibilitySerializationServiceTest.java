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

import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.commons.io.IOUtils;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CompatibilitySerializationServiceTest {

    private final SerializationService serializationService = new DefaultSerializationServiceBuilder()
            .setVersion(InternalSerializationService.VERSION_1)
            .setNotActiveExceptionSupplier(HazelcastInstanceNotActiveException::new)
            .isCompatibility(true)
            .build();

    @Test
    public void testDeserializeHz3DataToObject() throws Exception {

        HeapData data = readObjectData();

        TestObject object = serializationService.toObject(data);

        Map<Integer, String> map = new HashMap<>();
        map.put(42, "hello world");

        assertThat(object.bool).isEqualTo(true);
        assertThat(object.b).isEqualTo((byte) 42);
        assertThat(object.ch).isEqualTo('d');
        assertThat(object.s1).isEqualTo((short) 42);
        assertThat(object.i).isEqualTo(42);
        assertThat(object.l).isEqualTo(42);
        assertThat(object.f).isEqualTo(4.2f);
        assertThat(object.d).isEqualTo(4.2);
        assertThat(object.s).isEqualTo("hello world");
        assertThat(object.list).isEqualTo(newArrayList("hello world"));
        assertThat(object.set).isEqualTo(newHashSet("hello world"));
        assertThat(object.map).isEqualTo(map);
    }

    @Test
    public void testSerializeObjectAsHz3Data() throws Exception {
        Map<Integer, String> map = new HashMap<>();
        map.put(42, "hello world");
        TestObject testObject = new TestObject(
                true, (byte) 42, 'd', (short) 42, 42, 42, 4.2f, 4.2, "hello world",
                newArrayList("hello world"),
                newHashSet("hello world"),
                map
        );

        byte[] expectedPayload = readObjectData().toByteArray();

        byte[] payload = serializationService.toData(testObject).toByteArray();
        assertThat(payload).isEqualTo(expectedPayload);
    }

    @Nonnull
    private HeapData readObjectData() throws IOException {
        try (InputStream is = new FileInputStream("src/test/resources/testHz3Object")) {
            byte[] payload = IOUtils.toByteArray(is);
            return  new HeapData(payload);
        }
    }
}
