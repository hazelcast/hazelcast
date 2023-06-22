/*
 * Original work Copyright 2015 Real Logic Ltd.
 * Modified work Copyright (c) 2015-2023, Hazelcast, Inc. All Rights Reserved.
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
import org.assertj.core.data.Offset;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import static java.lang.Long.valueOf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class Long2ObjectHashMapTest {
    private final Long2ObjectHashMap<String> longToObjectMap = new Long2ObjectHashMap<>();

    @Test
    public void shouldDoPutAndThenGet() {
        final String value = "Seven";
        longToObjectMap.put(7, value);

        assertThat(longToObjectMap.get(7)).isEqualTo(value);
    }

    @Test
    public void shouldReplaceExistingValueForTheSameKey() {
        final long key = 7;
        final String value = "Seven";
        longToObjectMap.put(key, value);

        final String newValue = "New Seven";
        final String oldValue = longToObjectMap.put(key, newValue);

        assertThat(longToObjectMap.get(key)).isEqualTo(newValue);
        assertThat(oldValue).isEqualTo(value);
        assertThat(valueOf(longToObjectMap.size())).isEqualTo(valueOf(1));
    }

    @Test
    public void shouldGrowWhenThresholdExceeded() {
        final double loadFactor = 0.5d;
        final Long2ObjectHashMap<String> map = new Long2ObjectHashMap<>(32, loadFactor);
        for (int i = 0; i < 16; i++) {
            map.put(i, Long.toString(i));
        }

        assertThat(valueOf(map.resizeThreshold())).isEqualTo(valueOf(16));
        assertThat(valueOf(map.capacity())).isEqualTo(valueOf(32));
        assertThat(valueOf(map.size())).isEqualTo(valueOf(16));

        map.put(16, "16");

        assertThat(valueOf(map.resizeThreshold())).isEqualTo(valueOf(32));
        assertThat(valueOf(map.capacity())).isEqualTo(valueOf(64));
        assertThat(valueOf(map.size())).isEqualTo(valueOf(17));

        assertThat(map.get(16)).isEqualTo("16");
        assertThat(loadFactor).isCloseTo(map.loadFactor(), Offset.offset(0.0));

    }

    @Test
    public void shouldHandleCollisionAndThenLinearProbe() {
        final double loadFactor = 0.5d;
        final Long2ObjectHashMap<String> map = new Long2ObjectHashMap<>(32, loadFactor);
        final long key = 7;
        final String value = "Seven";
        map.put(key, value);

        final long collisionKey = key + map.capacity();
        final String collisionValue = Long.toString(collisionKey);
        map.put(collisionKey, collisionValue);

        assertThat(map.get(key)).isEqualTo(value);
        assertThat(map.get(collisionKey)).isEqualTo(collisionValue);
        assertThat(loadFactor).isCloseTo(map.loadFactor(), Offset.offset(0.0));
    }

    @Test
    public void shouldClearCollection() {
        for (int i = 0; i < 15; i++) {
            longToObjectMap.put(i, Long.toString(i));
        }

        assertThat(valueOf(longToObjectMap.size())).isEqualTo(valueOf(15));
        assertThat(longToObjectMap.get(1)).isEqualTo("1");

        longToObjectMap.clear();

        assertThat(valueOf(longToObjectMap.size())).isEqualTo(valueOf(0));
        assertNull(longToObjectMap.get(1));
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

        assertThat(valueOf(longToObjectMap.capacity())).isLessThan(valueOf(capacityBeforeCompaction));
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

        assertThat(longToObjectMap.remove(key)).isEqualTo(value);
    }

    @Test
    public void shouldIterateValues() {
        final Collection<String> initialSet = new HashSet<>();

        for (int i = 0; i < 11; i++) {
            final String value = Long.toString(i);
            longToObjectMap.put(i, value);
            initialSet.add(value);
        }

        final Collection<String> copyToSet = new HashSet<>(longToObjectMap.values());

        assertThat(copyToSet).isEqualTo(initialSet);
    }

    @Test
    public void shouldIterateKeysGettingLongAsPrimitive() {
        final Collection<Long> initialSet = new HashSet<>();

        for (int i = 0; i < 11; i++) {
            final String value = Long.toString(i);
            longToObjectMap.put(i, value);
            initialSet.add((long) i);
        }

        final Collection<Long> copyToSet = new HashSet<>();

        for (final var iter = longToObjectMap.keySet().iterator(); iter.hasNext(); ) {
            copyToSet.add(iter.nextLong());
        }
        assertThat(copyToSet).isEqualTo(initialSet);
    }

    @Test
    public void shouldIterateKeys() {
        final Collection<Long> initialSet = new HashSet<>();

        for (int i = 0; i < 11; i++) {
            final String value = Long.toString(i);
            longToObjectMap.put(i, value);
            initialSet.add((long) i);
        }

        final Collection<Long> copyToSet = new HashSet<>(longToObjectMap.keySet());

        assertThat(copyToSet).isEqualTo(initialSet);
    }

    @Test
    public void shouldIterateAndHandleRemove() {
        final Collection<Long> initialSet = new HashSet<>();

        final int count = 11;
        for (int i = 0; i < count; i++) {
            final String value = Long.toString(i);
            longToObjectMap.put(i, value);
            initialSet.add((long) i);
        }

        final Collection<Long> copyOfSet = new HashSet<>();

        int i = 0;
        for (final Iterator<Long> iter = longToObjectMap.keySet().iterator(); iter.hasNext(); ) {
            final Long item = iter.next();
            if (i++ == 7) {
                iter.remove();
            } else {
                copyOfSet.add(item);
            }
        }

        assertThat(valueOf(initialSet.size())).isEqualTo(valueOf(count));
        final int reducedSetSize = count - 1;
        assertThat(valueOf(longToObjectMap.size())).isEqualTo(valueOf(reducedSetSize));
        assertThat(valueOf(copyOfSet.size())).isEqualTo(valueOf(reducedSetSize));
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
            assertThat(entry.getKey()).isEqualTo(valueOf(entry.getValue()));

            if (entry.getKey().intValue() == 7) {
                entry.setValue(testValue);
            }
        }

        assertThat(longToObjectMap.get(7)).isEqualTo(testValue);
    }

    @Test
    public void shouldGenerateStringRepresentation() {
        final int[] testEntries = {3, 1, 19, 7, 11, 12, 7};

        for (final int testEntry : testEntries) {
            longToObjectMap.put(testEntry, String.valueOf(testEntry));
        }

        final String mapAsAString = "{11=11, 7=7, 3=3, 12=12, 19=19, 1=1}";
        assertThat(longToObjectMap.toString()).isEqualTo(mapAsAString);
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
