/*
 * Copyright 2025 Hazelcast Inc.
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

package com.hazelcast.jet.sql.impl.index;

import com.hazelcast.config.IndexType;
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

import static com.hazelcast.config.IndexType.SORTED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@SuppressWarnings({"rawtypes", "unchecked"})
public abstract class IndexFilterIteratorTestSupport extends IndexFilterTestSupport {

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

        return mapContainer.getGlobalIndexRegistry().getIndex(INDEX_NAME);
    }

    protected static <T> void checkIterator(IndexType indexType, boolean expectedDescending, Iterator<QueryableEntry> iterator, T... expectedKeys) {
        Set<T> expected;

        if (expectedKeys != null) {
            expected = new HashSet<>(Arrays.asList(expectedKeys));

            assertEquals("Key passed to this method must be unique!", expected.size(), expectedKeys.length);
        } else {
            expected = Collections.emptySet();
        }

        Set<T> actual = new HashSet<>();

        Object prevValue = null;
        while (iterator.hasNext()) {
            QueryableEntry entry = iterator.next();
            Object key = entry.getKey();
            Object value = entry.getValue();

            if (key instanceof Data) {
                key = serializationService().toObject(key);
            }

            if (value instanceof Data) {
                value = serializationService().toObject(value);
            }

            if (indexType == SORTED && prevValue != null) {
                int cmp = ((Value) prevValue).compareTo((Value) value, expectedDescending);
                assertTrue("Wrong collation, prevValue " + prevValue + ", value " + value + ", expectedDescending " + expectedDescending,
                    cmp <= 0);
            }
            prevValue = value;

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

        public int compareTo(Value o, boolean descending) {
            // NULLs are less than any other value
            if (value1 == null) {
                return o.value1 == null ? 0 : (descending ? 1 : -1);
            }
            if (o.value1 == null) {
                return descending ? -1 : 1;
            }
            int cmp1 = descending ? Integer.compare(o.value1, value1) : Integer.compare(value1, o.value1);
            if (cmp1 == 0) {

                if (value2 == null) {
                    return o.value2 == null ? 0 : (descending ? 1 : -1);
                }

                if (o.value2 == null) {
                    return descending ? -1 : 1;
                }

                return descending ? Integer.compare(o.value2, value2) : Integer.compare(value2, o.value2);
            }
            return cmp1;
        }

        @Override
        public String toString() {
            return "[" + value1 + ", " + value2 + "]";
        }
    }
}
