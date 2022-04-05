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

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category(QuickTest.class)
public class MapSerializationTest {

    @Parameterized.Parameters(name = "index: {index}")
    public static Collection<Map> parameters() {
        return asList(
                new HashMap(2),
                new ConcurrentSkipListMap<>(new SerializationConcurrencyTest.PortableIntegerComparator()),
                new ConcurrentHashMap(2),
                new LinkedHashMap(2),
                new TreeMap<>(new SerializationConcurrencyTest.PortableIntegerComparator()));
    }

    @Parameterized.Parameter
    public Map map;

    private InternalSerializationService serializationService;

    @Before
    public void setup() {
        PortableFactory portableFactory = classId -> {
            switch (classId) {
                case 3:
                    return new SerializationConcurrencyTest.PortableIntegerComparator();
                default:
                    throw new IllegalArgumentException();
            }
        };

        serializationService = new DefaultSerializationServiceBuilder()
                .addPortableFactory(SerializationConcurrencyTest.FACTORY_ID, portableFactory).build();
    }

    @After
    public void tearDown() {
        serializationService.dispose();
    }

    @Test
    public void testMapSerialization() {
        map.put(35, new SerializationConcurrencyTest.Person(35, 180, 100, "Orhan", null));
        map.put(12, new SerializationConcurrencyTest.Person(12, 120, 60, "Osman", null));
        Data data = serializationService.toData(map);
        Map deserialized = serializationService.toObject(data);
        assertEquals("Object classes are not identical!", map.getClass(), deserialized.getClass());
        assertEquals("Objects are not identical!", map, deserialized);
    }

}
