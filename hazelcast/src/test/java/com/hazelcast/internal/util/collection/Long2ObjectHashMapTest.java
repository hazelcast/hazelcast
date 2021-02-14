/*
 * Original work Copyright 2015 Real Logic Ltd.
 * Modified work Copyright (c) 2015-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.util.collection;


import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import static java.lang.Long.valueOf;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.number.IsCloseTo.closeTo;
import static org.hamcrest.number.OrderingComparison.lessThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class Long2ObjectHashMapTest {
    private final Long2ObjectHashMap<String> longToObjectMap = new Long2ObjectHashMap<String>();

    @Test
    public void shouldDoPutAndThenGet() {
        final String value = "Seven";
        longToObjectMap.put(7, value);

        assertThat(longToObjectMap.get(7), is(value));
    }

    @Test
    public void shouldReplaceExistingValueForTheSameKey() {
        final long key = 7;
        final String value = "Seven";
        longToObjectMap.put(key, value);

        final String newValue = "New Seven";
        final String oldValue = longToObjectMap.put(key, newValue);

        assertThat(longToObjectMap.get(key), is(newValue));
        assertThat(oldValue, is(value));
        assertThat(valueOf(longToObjectMap.size()), is(valueOf(1)));
    }

    @Test
    public void shouldGrowWhenThresholdExceeded() {
        final double loadFactor = 0.5d;
        final Long2ObjectHashMap<String> map = new Long2ObjectHashMap<String>(32, loadFactor);
        for (int i = 0; i < 16; i++) {
            map.put(i, Long.toString(i));
        }

        assertThat(valueOf(map.resizeThreshold()), is(valueOf(16)));
        assertThat(valueOf(map.capacity()), is(valueOf(32)));
        assertThat(valueOf(map.size()), is(valueOf(16)));

        map.put(16, "16");

        assertThat(valueOf(map.resizeThreshold()), is(valueOf(32)));
        assertThat(valueOf(map.capacity()), is(valueOf(64)));
        assertThat(valueOf(map.size()), is(valueOf(17)));

        assertThat(map.get(16), equalTo("16"));
        assertThat(loadFactor, closeTo(map.loadFactor(), 0.0));

    }

    @Test
    public void shouldHandleCollisionAndThenLinearProbe() {
        final double loadFactor = 0.5d;
        final Long2ObjectHashMap<String> map = new Long2ObjectHashMap<String>(32, loadFactor);
        final long key = 7;
        final String value = "Seven";
        map.put(key, value);

        final long collisionKey = key + map.capacity();
        final String collisionValue = Long.toString(collisionKey);
        map.put(collisionKey, collisionValue);

        assertThat(map.get(key), is(value));
        assertThat(map.get(collisionKey), is(collisionValue));
        assertThat(loadFactor, closeTo(map.loadFactor(), 0.0));
    }

    @Test
    public void shouldClearCollection() {
        for (int i = 0; i < 15; i++) {
            longToObjectMap.put(i, Long.toString(i));
        }

        assertThat(valueOf(longToObjectMap.size()), is(valueOf(15)));
        assertThat(longToObjectMap.get(1), is("1"));

        longToObjectMap.clear();

        assertThat(valueOf(longToObjectMap.size()), is(valueOf(0)));
        Assert.assertNull(longToObjectMap.get(1));
    }

    @Test
    public void shouldCompactCollection() {
        final int totalItems = 50;
        for (int i = 0; i < totalItems; i++) {
            longToObjectMap.put(i, Long.toString(i));
        }

        for (int i = 0, limit = totalItems - 4; i < limit; i++) {
            longToObjectMap.remove(i);
        }

        final int capacityBeforeCompaction = longToObjectMap.capacity();
        longToObjectMap.compact();

        assertThat(valueOf(longToObjectMap.capacity()), lessThan(valueOf(capacityBeforeCompaction)));
    }

    @Test
    public void shouldContainValue() {
        final long key = 7;
        final String value = "Seven";

        longToObjectMap.put(key, value);

        Assert.assertTrue(longToObjectMap.containsValue(value));
        Assert.assertFalse(longToObjectMap.containsValue("NoKey"));
    }

    @Test
    public void shouldContainKey() {
        final long key = 7;
        final String value = "Seven";

        longToObjectMap.put(key, value);

        Assert.assertTrue(longToObjectMap.containsKey(key));
        Assert.assertFalse(longToObjectMap.containsKey(0));
    }

    @Test
    public void shouldRemoveEntry() {
        final long key = 7;
        final String value = "Seven";

        longToObjectMap.put(key, value);

        Assert.assertTrue(longToObjectMap.containsKey(key));

        longToObjectMap.remove(key);

        Assert.assertFalse(longToObjectMap.containsKey(key));
    }

    @Test
    public void shouldRemoveEntryAndCompactCollisionChain() {
        final long key = 12;
        final String value = "12";

        longToObjectMap.put(key, value);
        longToObjectMap.put(13, "13");

        final long collisionKey = key + longToObjectMap.capacity();
        final String collisionValue = Long.toString(collisionKey);

        longToObjectMap.put(collisionKey, collisionValue);
        longToObjectMap.put(14, "14");

        assertThat(longToObjectMap.remove(key), is(value));
    }

    @Test
    public void shouldIterateValues() {
        final Collection<String> initialSet = new HashSet<String>();

        for (int i = 0; i < 11; i++) {
            final String value = Long.toString(i);
            longToObjectMap.put(i, value);
            initialSet.add(value);
        }

        final Collection<String> copyToSet = new HashSet<String>(longToObjectMap.values());

        assertThat(copyToSet, is(initialSet));
    }

    @Test
    public void shouldIterateKeysGettingLongAsPrimitive() {
        final Collection<Long> initialSet = new HashSet<Long>();

        for (int i = 0; i < 11; i++) {
            final String value = Long.toString(i);
            longToObjectMap.put(i, value);
            initialSet.add(valueOf(i));
        }

        final Collection<Long> copyToSet = new HashSet<Long>();

        for (final Long2ObjectHashMap.KeyIterator iter = longToObjectMap.keySet().iterator(); iter.hasNext(); ) {
            copyToSet.add(valueOf(iter.nextLong()));
        }

        assertThat(copyToSet, is(initialSet));
    }

    @Test
    public void shouldIterateKeys() {
        final Collection<Long> initialSet = new HashSet<Long>();

        for (int i = 0; i < 11; i++) {
            final String value = Long.toString(i);
            longToObjectMap.put(i, value);
            initialSet.add(valueOf(i));
        }

        final Collection<Long> copyToSet = new HashSet<Long>(longToObjectMap.keySet());

        assertThat(copyToSet, is(initialSet));
    }

    @Test
    public void shouldIterateAndHandleRemove() {
        final Collection<Long> initialSet = new HashSet<Long>();

        final int count = 11;
        for (int i = 0; i < count; i++) {
            final String value = Long.toString(i);
            longToObjectMap.put(i, value);
            initialSet.add(valueOf(i));
        }

        final Collection<Long> copyOfSet = new HashSet<Long>();

        int i = 0;
        for (final Iterator<Long> iter = longToObjectMap.keySet().iterator(); iter.hasNext(); ) {
            final Long item = iter.next();
            if (i++ == 7) {
                iter.remove();
            } else {
                copyOfSet.add(item);
            }
        }

        assertThat(valueOf(initialSet.size()), is(valueOf(count)));
        final int reducedSetSize = count - 1;
        assertThat(valueOf(longToObjectMap.size()), is(valueOf(reducedSetSize)));
        assertThat(valueOf(copyOfSet.size()), is(valueOf(reducedSetSize)));
    }

    @Test
    public void shouldIterateEntries() {
        final int count = 11;
        for (int i = 0; i < count; i++) {
            final String value = Long.toString(i);
            longToObjectMap.put(i, value);
        }

        final String testValue = "Wibble";
        for (final Map.Entry<Long, String> entry : longToObjectMap.entrySet()) {
            assertThat(entry.getKey(), equalTo(valueOf(entry.getValue())));

            if (entry.getKey().intValue() == 7) {
                entry.setValue(testValue);
            }
        }

        assertThat(longToObjectMap.get(7), equalTo(testValue));
    }

    @Test
    public void shouldGenerateStringRepresentation() {
        final int[] testEntries = {3, 1, 19, 7, 11, 12, 7};

        for (final int testEntry : testEntries) {
            longToObjectMap.put(testEntry, String.valueOf(testEntry));
        }

        final String mapAsAString = "{11=11, 7=7, 3=3, 12=12, 19=19, 1=1}";
        assertThat(longToObjectMap.toString(), equalTo(mapAsAString));
    }

    @Test
    public void sizeShouldReturnNumberOfEntries() {
        final int count = 100;
        for (int key = 0; key < count; key++) {
            longToObjectMap.put(key, "value");
        }

        assertEquals(count, longToObjectMap.size());
    }
}
