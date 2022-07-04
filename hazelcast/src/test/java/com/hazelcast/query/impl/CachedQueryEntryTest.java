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

package com.hazelcast.query.impl;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.map.impl.LazyMapEntry;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.query.SampleTestObjects.PortableEmployee;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CachedQueryEntryTest extends QueryEntryTest {

    @Test
    public void getKey_caching() {
        QueryableEntry entry = createEntry("key", "value");

        assertSame(entry.getKey(), entry.getKey());
    }

    @Test
    public void getValue_caching() {
        QueryableEntry entry = createEntry("key", "value");

        assertSame(entry.getValue(), entry.getValue());
    }

    @Test
    public void getKeyData_caching() {
        QueryableEntry entry = createEntry("key", "value");

        assertSame(entry.getKeyData(), entry.getKeyData());
    }

    @Test
    public void getValueData_caching() {
        QueryableEntry entry = createEntry("key", "value");

        assertSame(entry.getValueData(), entry.getValueData());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInit_whenKeyIsNull_thenThrowIllegalArgumentException() {
        createEntry(null, new Object(), newExtractor());
    }

    @Test
    public void testGetKey() {
        String keyObject = "key";
        Data keyData = serializationService.toData(keyObject);
        QueryableEntry entry = createEntry(keyData, new Object(), newExtractor());

        Object key = entry.getKey();
        assertEquals(keyObject, key);
    }

    @Test
    public void testGetTargetObject_givenKeyDataIsPortable_whenKeyFlagIsTrue_thenReturnKeyData() {
        Data keyData = mockPortableData();
        QueryableEntry entry = createEntry(keyData, new Object(), newExtractor());

        Object targetObject = entry.getTargetObject(true);

        assertSame(keyData, targetObject);
    }

    @Test
    public void testGetTargetObject_givenKeyDataIsNotPortable_whenKeyFlagIsTrue_thenReturnKeyObject() {
        Object keyObject = "key";
        Data keyData = serializationService.toData(keyObject);
        QueryableEntry entry = createEntry(keyData, new Object(), newExtractor());

        Object targetObject = entry.getTargetObject(true);

        assertEquals(keyObject, targetObject);
    }

    @Test
    public void testGetTargetObject_givenValueIsDataAndPortable_whenKeyFlagIsFalse_thenReturnValueData() {
        Data key = serializationService.toData("indexedKey");
        Data value = serializationService.toData(new PortableEmployee(30, "peter"));
        QueryableEntry entry = createEntry(key, value, newExtractor());

        Object targetObject = entry.getTargetObject(false);

        assertEquals(value, targetObject);
    }

    @Test
    public void testGetTargetObject_givenValueIsData_whenKeyFlagIsFalse_thenReturnValueObject() {
        Data key = serializationService.toData("key");
        Data value = serializationService.toData("value");
        QueryableEntry entry = createEntry(key, value, newExtractor());

        Object targetObject = entry.getTargetObject(false);

        assertEquals("value", targetObject);
    }

    @Test
    public void testGetTargetObject_givenValueIsPortable_whenKeyFlagIsFalse_thenReturnValueData() {
        Data key = serializationService.toData("indexedKey");
        Portable value = new PortableEmployee(30, "peter");
        QueryableEntry entry = createEntry(key, value, newExtractor());

        Object targetObject = entry.getTargetObject(false);

        assertEquals(serializationService.toData(value), targetObject);
    }

    @Test
    public void testGetTargetObject_givenValueIsNotPortable_whenKeyFlagIsFalse_thenReturnValueObject() {
        Data key = serializationService.toData("key");
        String value = "value";
        QueryableEntry entry = createEntry(key, value, newExtractor());

        Object targetObject = entry.getTargetObject(false);

        assertSame(value, targetObject);
    }

    @Test(expected = NullPointerException.class)
    public void testGetTargetObject_givenInstanceIsNotInitialized_whenKeyFlagIsTrue_thenThrowNPE() {
        QueryableEntry entry = createEntry();

        entry.getTargetObject(true);
    }

    @Test
    public void testGetTargetObject_givenInstanceIsNotInitialized_whenKeyFlagIsFalse_thenReturnNull() {
        QueryableEntry entry = createEntry();

        assertNull(entry.getTargetObject(false));
    }

    @Test
    @SuppressWarnings("EqualsWithItself")
    public void testEquals_givenSameInstance_thenReturnTrue() {
        CachedQueryEntry entry1 = createEntry("key");

        assertTrue(entry1.equals(entry1));
    }

    @Test
    @SuppressWarnings("ConstantConditions")
    public void testEquals_givenOtherIsNull_thenReturnFalse() {
        CachedQueryEntry entry1 = createEntry("key");
        CachedQueryEntry entry2 = null;

        assertFalse(entry1.equals(entry2));
    }

    @Test
    public void testEquals_givenOtherIsDifferentClass_thenReturnFalse() {
        CachedQueryEntry entry1 = createEntry("key");
        Object entry2 = new Object();

        assertFalse(entry1.equals(entry2));
    }

    @Test
    public void testEquals_givenOtherHasDifferentKey_thenReturnFalse() {
        CachedQueryEntry entry1 = createEntry("key1");
        CachedQueryEntry entry2 = createEntry("key2");

        assertFalse(entry1.equals(entry2));
    }

    @Test
    public void testEquals_givenOtherHasEqualKey_thenReturnTrue() {
        CachedQueryEntry entry1 = createEntry("key");
        CachedQueryEntry entry2 = createEntry("key");

        assertTrue(entry1.equals(entry2));
    }

    @Test
    public void testHashCode() {
        CachedQueryEntry entry = createEntry("key");

        assertEquals(entry.hashCode(), entry.hashCode());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void givenNewEntry_whenSetValue_thenThrowUnsupportedOperationException() {
        CachedQueryEntry<Object, Object> entry = createEntry("key");

        entry.setValue(new Object());
    }

    @Test
    public void testDeserialization() {
        QueryableEntry entry = createEntry("key", "value");
        int hashCode = entry.hashCode();
        Data data = serializationService.toData(entry);
        LazyMapEntry lazyMapEntry = serializationService.toObject(data);
        assertEquals("key", lazyMapEntry.getKey());
        assertEquals("value", lazyMapEntry.getValue());
    }

    private CachedQueryEntry<Object, Object> createEntry(Object key) {
        Data keyData = serializationService.toData(key);
        return new CachedQueryEntry<Object, Object>(serializationService, keyData, new Object(),
                newExtractor());
    }

    private Extractors newExtractor() {
        return Extractors.newBuilder(serializationService).build();
    }

    private Data mockPortableData() {
        Data keyData = mock(Data.class);
        when(keyData.isPortable()).thenReturn(true);
        return keyData;
    }

    @Override
    protected QueryableEntry createEntry() {
        return new CachedQueryEntry();
    }

    @Override
    protected QueryableEntry createEntry(Data key, Object value, Extractors extractors) {
        return new CachedQueryEntry(serializationService, key, value, extractors);
    }

    @Override
    protected void initEntry(Object entry, InternalSerializationService serializationService, Data key, Object value,
                             Extractors extractors) {
        ((CachedQueryEntry) entry).init(serializationService, key, value, extractors);
    }
}
