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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.PriorityQueue;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.PriorityBlockingQueue;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category(QuickTest.class)
public class CollectionSerializationTest {

    @Parameterized.Parameters(name = "index: {index}")
    public static Collection<Collection> parameters() {
        return asList(
                new ArrayList(),
                new LinkedList(),
                new CopyOnWriteArrayList(),
                new HashSet(),
                new TreeSet<>(new SerializationConcurrencyTest.PortablePersonComparator()),
                new LinkedHashSet(),
                new CopyOnWriteArraySet(),
                new ConcurrentSkipListSet<>(new SerializationConcurrencyTest.PortablePersonComparator()),
                new ArrayDeque(),
                new LinkedBlockingQueue(),
                new ArrayBlockingQueue(2),
                new PriorityBlockingQueue<>(5, new SerializationConcurrencyTest.PortablePersonComparator()),
                new PriorityQueue<>(),
                new DelayQueue(),
                new LinkedTransferQueue());
    }

    @Parameterized.Parameter
    public Collection collection;

    private InternalSerializationService serializationService;

    @Before
    public void setup() {
        PortableFactory portableFactory = classId -> {
            switch (classId) {
                case 1:
                    return new SerializationConcurrencyTest.PortablePerson();
                case 2:
                    return new SerializationConcurrencyTest.PortableAddress();
                case 4:
                    return new SerializationConcurrencyTest.PortablePersonComparator();
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
    public void testCollectionSerialization() {
        collection.add(new SerializationConcurrencyTest.PortablePerson(12, 120, "Osman",
                new SerializationConcurrencyTest.PortableAddress("Main street", 35)));
        collection.add(new SerializationConcurrencyTest.PortablePerson(35, 120, "Orhan",
                new SerializationConcurrencyTest.PortableAddress("2nd street", 40)));
        Data data = serializationService.toData(collection);
        Collection deserialized = serializationService.toObject(data);
        assertTrue("Collections are not identical!", collection.containsAll(deserialized));
        assertTrue("Collections are not identical!", deserialized.containsAll(collection));
        assertEquals("Collection classes are not identical!", collection.getClass(), deserialized.getClass());
    }

}
