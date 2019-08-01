/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.nio.serialization;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
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

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category(QuickTest.class)
public class MapSerializationTest {

    @Parameterized.Parameters(name = "index: {index}")
    public static Collection<Map> parameters() {
        return asList(
                new HashMap(2),
                new ConcurrentSkipListMap<>(new SerializationConcurrencyTest.PersonComparator()),
                new ConcurrentHashMap(2),
                new LinkedHashMap(2),
                new TreeMap<>(new SerializationConcurrencyTest.PersonComparator()));
    }

    @Parameterized.Parameter
    public Map map;

    private InternalSerializationService serializationService;

    @Before
    public void setup() {
        serializationService = new DefaultSerializationServiceBuilder().build();
    }

    @After
    public void tearDown() {
        serializationService.dispose();
    }

    @Test
    public void testMapSerialization() {
        SerializationService ss = new DefaultSerializationServiceBuilder().build();
        map.put(35, new SerializationConcurrencyTest.Person(35, 180, 100, "Orhan", null));
        map.put(12, new SerializationConcurrencyTest.Person(12, 120, 60, "Osman", null));
        Data data = ss.toData(map);
        Map deserialized = ss.toObject(data);
        assertEquals("Object classes are not identical!", map.getClass(), deserialized.getClass());
        assertEquals("Objects are not identical!", map, deserialized);
    }

}
