/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.exec.scan.index;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.query.impl.InternalIndex;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.junit.After;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

@SuppressWarnings({"rawtypes", "unchecked"})
public class IndexFilterIteratorTestSupport extends IndexFilterTestSupport {

    protected static final String MAP_NAME = "map";
    protected static final String INDEX_NAME = "index";

    protected final TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory(1);

    @After
    public void after() {
        factory.shutdownAll();
    }

    protected static InternalIndex getIndex(HazelcastInstance instance) {
        MapService mapService = nodeEngine(instance).getService(MapService.SERVICE_NAME);

        MapContainer mapContainer = mapService.getMapServiceContext().getMapContainer(MAP_NAME);

        return mapContainer.getIndexes().getIndex(INDEX_NAME);
    }

    protected static <T> void checkIterator(Iterator<QueryableEntry> iterator, T... expectedKeys) {
        Set<T> expected;

        if (expectedKeys != null) {
            expected = new HashSet<>(Arrays.asList(expectedKeys));

            assertEquals("Key passed to this method must be unique!", expected.size(), expectedKeys.length);
        } else {
            expected = Collections.emptySet();
        }

        Set<T> actual = new HashSet<>();

        while (iterator.hasNext()) {
            Object key = iterator.next().getKey();

            if (key instanceof Data) {
                key = getSerializationService().toObject(key);
            }

            assertTrue("Duplicate key: " + key, actual.add((T) key));
        }

        assertEquals("Expected: " + expected + ", actual: " + actual, expected.size(), actual.size());
        assertTrue("Expected: " + expected + ", actual: " + actual, expected.containsAll(actual));
    }

    public static class Value implements Serializable {
        public Integer value1;
        public Integer value2;

        public Value(Integer value) {
            this(value, value);
        }

        public Value(Integer value1, Integer value2) {
            this.value1 = value1;
            this.value2 = value2;
        }
    }
}
