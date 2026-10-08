/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.datastructures.cpmap.store;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.jspecify.annotations.NonNull;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.cp.internal.datastructures.cpmap.store.HeapCPMapStore.LIMIT_MAX_CAPACITY_MB;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HeapCPMapStoreTest {

    protected static final int CAPACITY_MB = 1;
    protected static final int BULK_KEY_COUNT = 500;

    protected HeapCPMapStore store;

    @Before
    public final void before() {
        store = createStore(CAPACITY_MB);
    }

    protected @NonNull HeapCPMapStore createStore(int capacityInMb) {
        return new HeapCPMapStore(capacityInMb);
    }

    @Test
    public final void testPutAndGet() {
        Data k = getData();
        Data v1 = getData();

        assertNull(store.get(k));

        Data previousValue = store.put(k, v1, getTimestamp());
        assertNull(previousValue);

        assertEquals(v1, store.get(k));
        Data v2 = getData();
        previousValue = store.put(k, v2, getTimestamp());
        assertEquals(v1, previousValue);
        assertEquals(v2, store.get(k));
    }

    @Test
    public final void testRemove() {
        assertNull(store.remove(getData()));
        Data k = getData();
        Data v = getData();
        assertNull(store.put(k, v, getTimestamp()));
        assertEquals(v, store.remove(k));
    }

    @Test
    public final void testCas() {
        Data k = getData();
        Data v1 = getData();
        assertNull(store.put(k, v1, getTimestamp()));
        assertFalse(store.compareAndSet(k, getData(), getData(), getTimestamp()));
        assertEquals(v1, store.get(k));
        Data v2 = getData();
        assertTrue(store.compareAndSet(k, v1, v2, getTimestamp()));
        assertEquals(v2, store.get(k));
    }

    @Test
    public final void testTotalSize_put() {
        assertEquals(0, store.getUsedDataBytes());

        int keySize = 16;
        Data key = getData(keySize);

        int value1Size = 32;
        Data value1 = getData(value1Size);

        assertNull(store.put(key, value1, getTimestamp()));
        assertEquals(keySize + value1Size
                + metadataCost(), store.getUsedDataBytes());

        int value2Size = 10;
        Data value2 = getData(value2Size);

        Data previousValue = store.put(key, value2, getTimestamp());
        assertNotNull(previousValue);
        assertEquals(value1Size, previousValue.dataSize());

        assertEquals(keySize + value2Size
                + metadataCost(), store.getUsedDataBytes());
    }

    @Test
    public final void testTotalSize_remove() {
        assertEquals(0, store.getUsedDataBytes());
        int k1Size = 32;
        Data k1 = getData(k1Size);
        store.remove(k1);
        assertEquals(0, store.getUsedDataBytes());
        int v1Size = 128;
        Data v1 = getData(v1Size);
        store.put(k1, v1, getTimestamp());
        assertEquals(k1Size + v1Size + metadataCost(),
                store.getUsedDataBytes());
        assertNotNull(store.remove(k1));
        assertEquals(0, store.getUsedDataBytes());

        List<Data> keys = new ArrayList<>();
        int currentStoreBytes = store.getUsedDataBytes();
        for (int i = 1; i < 1_00; i++) {
            Data key = getData(i);
            Data value = getData(i + 1);
            assertNull(store.put(key, value, getTimestamp()));
            assertEquals(currentStoreBytes + (key.dataSize() + value.dataSize()
                    + metadataCost()), store.getUsedDataBytes());
            currentStoreBytes = store.getUsedDataBytes();
            keys.add(key);
        }

        keys.forEach(store::remove);
        assertEquals(0, store.getUsedDataBytes());
    }

    @Test
    public final void testTotalSize_cas() {
        assertEquals(0, store.getUsedDataBytes());
        Data k1 = getData(64);
        Data v1 = getData(23);
        assertNull(store.put(k1, v1, getTimestamp()));
        int expected = k1.dataSize() + v1.dataSize() + metadataCost();
        assertEquals(expected, store.getUsedDataBytes());
        Data v2 = getData(192);
        assertTrue(store.compareAndSet(k1, v1, v2, getTimestamp()));
        assertEquals(k1.dataSize() + v2.dataSize()
                + metadataCost(), store.getUsedDataBytes());
        assertFalse(store.compareAndSet(k1, v1, v2, getTimestamp()));
        assertEquals(k1.dataSize() + v2.dataSize()
                + metadataCost(), store.getUsedDataBytes());
    }

    @Test
    public final void testMbSizeTooLarge() {
        String expectedMessage = "'capacityInMb' must be <= " + LIMIT_MAX_CAPACITY_MB
                + " MB, got: " + (LIMIT_MAX_CAPACITY_MB + 1) + " MB";
        Throwable t =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> createStore(LIMIT_MAX_CAPACITY_MB + 1));
        assertEquals(expectedMessage, t.getMessage());
    }

    @Test
    public final void testMbSizeTooSmall() {
        String expectedMessage = "'capacityInMb' must be >= 1 MB, got: 0 MB";
        Throwable t =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> createStore(0));
        assertEquals(expectedMessage, t.getMessage());
    }

    @Test
    public final void testExceedsCapacity() {
        int kb = 1_000;
        // key is 1kb, value is 1kb; key-pair == 2kb
        int keysToCreate = BULK_KEY_COUNT;
        // keys[i] |-> values[i]
        Data[] keys = new Data[keysToCreate];
        Data[] values = new Data[keysToCreate];
        for (int i = 0; i < keysToCreate; i++) {
            Data key = getData(kb);
            Data value = getData(kb);
            keys[i] = key;
            values[i] = value;
            store.put(key, value, getTimestamp());
        }

        String expectedMessage = "Write not permitted as it would exceed the user defined capacity limit of "
                + CAPACITY_MB + "MB";

        // we're at 1MB limit; next one will push it over
        Data key = getData(kb);
        Data value = getData(kb);

        Throwable t = assertThrows(IllegalStateException.class, () -> store.put(key, value, getTimestamp()));
        assertEquals(expectedMessage, t.getMessage());
        assertNull(store.get(key));

        // remove one key-value so we have space for new 2kb key-value pair
        store.remove(keys[keys.length - 1]);
        assertNull(store.put(key, value, getTimestamp()));
        assertEquals(value, store.remove(key));

        Data newValue = getData(kb);
        assertTrue(store.compareAndSet(keys[keys.length - 2], values[values.length - 2], newValue, getTimestamp()));

        t = assertThrows(IllegalStateException.class, () -> store.compareAndSet(keys[keys.length - 2],
                newValue, getData(3 * kb), getTimestamp()));
        assertEquals(expectedMessage, t.getMessage());
    }

    @Test
    public final void testPutIfAbsent() {
        Data key = getData(32);
        Data value1 = getData(64);
        assertNull(store.putIfAbsent(key, value1, getTimestamp()));
        int expectedUsedBytes = key.dataSize() + value1.dataSize()
                + metadataCost();
        assertEquals(expectedUsedBytes, store.getUsedDataBytes());
        Data value2 = getData(128);
        assertEquals(value1, store.putIfAbsent(key, value2, getTimestamp()));
        assertEquals(expectedUsedBytes, store.getUsedDataBytes());
    }

    @Test
    public final void testPutIfAbsent_ExceedsCapacity() {
        int dataSizeBytes = 1000;
        for (int i = 0; i < BULK_KEY_COUNT; i++) {
            assertNull(store.putIfAbsent(getData(dataSizeBytes), getData(dataSizeBytes), getTimestamp()));
        }
        // push it over limit
        Throwable t = assertThrows(IllegalStateException.class,
                () -> store.putIfAbsent(getData(dataSizeBytes), getData(dataSizeBytes), getTimestamp()));
        String expectedMessage = "Write not permitted as it would exceed the user defined capacity limit of "
                + CAPACITY_MB + "MB";
        assertEquals(t.getMessage(), expectedMessage);
    }

    @Test
    public final void testPutIfAbsent_NotExceedsCapacityWhenKeyAlreadyPresent() {
        int dataSizeBytes = 1000;
        Data lastKeyPut = null;
        Data lastValuePut = null;
        for (int i = 0; i < BULK_KEY_COUNT; i++) {
            lastKeyPut = getData(dataSizeBytes);
            lastValuePut = getData(dataSizeBytes);
            assertNull(store.putIfAbsent(lastKeyPut, lastValuePut, getTimestamp()));
        }
        assertNotNull(lastKeyPut);
        assertNotNull(lastValuePut);
        // doesn't throw as the key is already present so we won't do anything that exceeds the capacity, by contrast to
        // testPutIfAbsent_ExceedsCapacity which puts a distinct key
        assertEquals(lastValuePut, store.putIfAbsent(lastKeyPut, getData(dataSizeBytes), getTimestamp()));
    }

    protected static Data getData(int dataSize) {
        Data data = mock(Data.class);
        when(data.dataSize()).thenReturn(dataSize);
        return data;
    }

    protected static Data getData() {
        return getData(4);
    }

    protected long getTimestamp() {
        return CPMapStore.NO_TIMESTAMP;
    }

    protected int metadataCost() {
        return 0;
    }
}
