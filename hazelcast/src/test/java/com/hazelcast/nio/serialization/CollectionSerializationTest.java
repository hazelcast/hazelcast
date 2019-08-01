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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
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

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category(QuickTest.class)
public class CollectionSerializationTest {

    @Parameterized.Parameters(name = "index: {index}")
    public static Collection<Collection> parameters() {
        return asList(
                new ArrayList(),
                new LinkedList(),
                new CopyOnWriteArrayList(),
                new HashSet(),
                new TreeSet<>(new SerializationConcurrencyTest.PersonComparator()),
                new LinkedHashSet(),
                new CopyOnWriteArraySet(),
                new ConcurrentSkipListSet<>(new SerializationConcurrencyTest.PersonComparator()),
                new ArrayDeque(),
                new LinkedBlockingQueue(),
                new ArrayBlockingQueue(2),
                new PriorityBlockingQueue<>(5, new SerializationConcurrencyTest.PersonComparator()),
                new DelayQueue(),
                new LinkedTransferQueue());
    }

    @Parameterized.Parameter
    public Collection collection;

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
    public void testCollectionSerialization() {
        SerializationService ss = new DefaultSerializationServiceBuilder().build();
        collection.add(new SerializationConcurrencyTest.Person(12, 120, 60, "Osman", null));
        collection.add(new SerializationConcurrencyTest.Person(35, 180, 100, "Orhan", null));
        Data data = ss.toData(collection);
        Collection deserialized = ss.toObject(data);
        assertTrue("Collections are not identical!", collection.containsAll(deserialized));
        assertTrue("Collections are not identical!", deserialized.containsAll(collection));
        assertEquals("Collection classes are not identical!", collection.getClass(), deserialized.getClass());
    }

}
