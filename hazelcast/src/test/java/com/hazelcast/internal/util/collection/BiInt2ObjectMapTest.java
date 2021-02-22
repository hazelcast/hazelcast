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
import com.hazelcast.internal.util.collection.BiInt2ObjectMap.EntryConsumer;
import org.hamcrest.core.CombinableMatcher;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.either;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class BiInt2ObjectMapTest {
    private final BiInt2ObjectMap<String> map = new BiInt2ObjectMap<String>();

    @Test
    public void shouldInitialiseUnderlyingImplementation() {
        final int initialCapacity = 10;
        final double loadFactor = 0.6;
        final BiInt2ObjectMap<String> map = new BiInt2ObjectMap<String>(initialCapacity, loadFactor);

        CombinableMatcher.CombinableEitherMatcher<Integer> either = either(is(initialCapacity));
        assertThat(map.capacity(), either.or(greaterThan(initialCapacity)));
        assertThat(map.loadFactor(), is(loadFactor));
    }

    @Test
    public void shouldReportEmpty() {
        assertThat(map.isEmpty(), is(true));
    }

    @Test
    public void shouldPutItem() {
        final String testValue = "Test";
        final int keyPartA = 3;
        final int keyPartB = 7;

        assertNull(map.put(keyPartA, keyPartB, testValue));
        assertThat(map.size(), is(1));
    }

    @Test
    public void shouldPutAndGetItem() {
        final String testValue = "Test";
        final int keyPartA = 3;
        final int keyPartB = 7;

        assertNull(map.put(keyPartA, keyPartB, testValue));
        assertThat(map.get(keyPartA, keyPartB), is(testValue));
    }

    @Test
    public void shouldReturnNullWhenNotFoundItem() {
        final int keyPartA = 3;
        final int keyPartB = 7;

        assertNull(map.get(keyPartA, keyPartB));
    }

    @Test
    public void shouldRemoveItem() {
        final String testValue = "Test";
        final int keyPartA = 3;
        final int keyPartB = 7;

        map.put(keyPartA, keyPartB, testValue);
        assertThat(map.remove(keyPartA, keyPartB), is(testValue));
        assertNull(map.get(keyPartA, keyPartB));
    }

    @Test
    public void shouldIterateValues() {
        final Set<String> expectedSet = new HashSet<String>();
        final int count = 7;

        for (int i = 0; i < count; i++) {
            final String value = String.valueOf(i);
            expectedSet.add(value);
            map.put(i, i + 97, value);
        }

        final Set<String> actualSet = new HashSet<String>();

        map.forEach(new Consumer<String>() {
            @Override
            public void accept(String s) {
                actualSet.add(s);
            }
        });

        assertThat(actualSet, equalTo(expectedSet));
    }

    @Test
    public void shouldIterateEntries() {
        final Set<EntryCapture<String>> expectedSet = new HashSet<EntryCapture<String>>();
        final int count = 7;

        for (int i = 0; i < count; i++) {
            final String value = String.valueOf(i);
            expectedSet.add(new EntryCapture<String>(i, i + 97, value));
            map.put(i, i + 97, value);
        }

        final Set<EntryCapture<String>> actualSet = new HashSet<EntryCapture<String>>();

        map.forEach(new EntryConsumer<String>() {
            @Override
            public void accept(int keyPartA, int keyPartB, String value) {
                actualSet.add(new EntryCapture<String>(keyPartA, keyPartB, value));
            }
        });

        assertThat(actualSet, equalTo(expectedSet));
    }

    public static class EntryCapture<V> {
        public final int keyPartA;
        public final int keyPartB;
        public final V value;

        public EntryCapture(final int keyPartA, final int keyPartB, final V value) {
            this.keyPartA = keyPartA;
            this.keyPartB = keyPartB;
            this.value = value;
        }

        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            final EntryCapture that = (EntryCapture) o;

            return keyPartA == that.keyPartA && keyPartB == that.keyPartB && value.equals(that.value);

        }

        public int hashCode() {
            int result = keyPartA;
            result = 31 * result + keyPartB;
            result = 31 * result + value.hashCode();

            return result;
        }
    }
}
