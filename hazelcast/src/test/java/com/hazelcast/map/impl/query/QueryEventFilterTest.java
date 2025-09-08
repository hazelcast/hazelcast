/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.query;

import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueryEventFilterTest {

    private SerializationService serializationService;

    @Before
    public void setUp() {
        serializationService = new DefaultSerializationServiceBuilder().build();
    }

    @Test
    public void testEval_givenFilterContainsKey_whenKeyOfEntryIsNotEqual_thenReturnFalse() {
        //given
        Data key1 = serializationService.toData("key1");
        Predicate predicate = Predicates.alwaysTrue();
        QueryEventFilter filter = new QueryEventFilter(key1, predicate, true);

        //when
        Data key2 = serializationService.toData("key2");
        QueryableEntry entry = mockEntryWithKeyData(key2);

        //then
        boolean result = filter.eval(entry);
        assertFalse(result);
    }

    @Test
    public void testEval_givenFilterContainsKey_whenKeyOfEntryIsEqualAndPredicacteIsMatching_thenReturnTrue() {
        //given
        Data key1 = serializationService.toData("key1");
        Predicate predicate = Predicates.alwaysTrue();
        QueryEventFilter filter = new QueryEventFilter(key1, predicate, true);

        //when
        Data key2 = serializationService.toData("key1");
        QueryableEntry entry = mockEntryWithKeyData(key2);

        //then
        boolean result = filter.eval(entry);
        assertTrue(result);
    }

    @Test
    public void testEval_givenFilterDoesNotContainKey_whenPredicateIsMatching_thenReturnTrue() {
        //given
        Predicate predicate = Predicates.alwaysTrue();
        QueryEventFilter filter = new QueryEventFilter(null, predicate, true);

        //when
        Data key2 = serializationService.toData("key");
        QueryableEntry entry = mockEntryWithKeyData(key2);

        //then
        boolean result = filter.eval(entry);
        assertTrue(result);
    }

    @Test
    public void testEval_givenFilterDoesNotContainKey_whenPredicateIsNotMatching_thenReturnFalse() {
        //given
        Predicate predicate = Predicates.alwaysFalse();
        QueryEventFilter filter = new QueryEventFilter(null, predicate, true);

        //when
        Data key2 = serializationService.toData("key");
        QueryableEntry entry = mockEntryWithKeyData(key2);

        //then
        boolean result = filter.eval(entry);
        assertFalse(result);
    }

    @Test
    public void testEquals_givenSameInstance_thenReturnTrue() {
        Data key = serializationService.toData("key");
        QueryEventFilter filter1 = new QueryEventFilter(key, Predicates.alwaysTrue(), true);
        QueryEventFilter filter2 = filter1;

        assertTrue(filter1.equals(filter2));
    }

    @Test
    public void testEquals_givenOtherIsNull_thenReturnFalse() {
        Data key = serializationService.toData("key");
        QueryEventFilter filter1 = new QueryEventFilter(key, Predicates.alwaysTrue(), true);
        QueryEventFilter filter2 = null;

        assertFalse(filter1.equals(filter2));
    }

    @Test
    public void testEquals_givenOtherIsDifferentClass_thenReturnFalse() {
        Data key = serializationService.toData("key");
        QueryEventFilter filter1 = new QueryEventFilter(key, Predicates.alwaysTrue(), true);
        Object filter2 = new Object();

        assertFalse(filter1.equals(filter2));
    }

    @Test
    public void testEquals_givenIncludeValueIsTrue_whenOtherHasIncludeValueFalse_thenReturnFalse() {
        Data key = serializationService.toData("key");
        QueryEventFilter filter1 = new QueryEventFilter(key, Predicates.alwaysTrue(), true);

        QueryEventFilter filter2 = new QueryEventFilter(key, Predicates.alwaysTrue(), false);

        assertFalse(filter1.equals(filter2));
    }

    @Test
    public void testEquals_givenIncludeValueIsFalse_whenOtherHasIncludeValueTrue_thenReturnFalse() {
        Data key = serializationService.toData("key");
        QueryEventFilter filter1 = new QueryEventFilter(key, Predicates.alwaysTrue(), false);

        QueryEventFilter filter2 = new QueryEventFilter(key, Predicates.alwaysTrue(), true);

        assertFalse(filter1.equals(filter2));
    }

    @Test
    public void testEquals_givenKeyIsNull_whenOtherHasKeyNonNull_thenReturnFalse() {
        QueryEventFilter filter1 = new QueryEventFilter(null, Predicates.alwaysTrue(), true);

        Data key = serializationService.toData("key");
        QueryEventFilter filter2 = new QueryEventFilter(key, Predicates.alwaysTrue(), true);

        assertFalse(filter1.equals(filter2));
    }

    @Test
    public void testEquals_givenKeyIsNonNull_whenOtherHasNonEqualsKey_thenReturnFalse() {
        Data key1 = serializationService.toData("key1");
        QueryEventFilter filter1 = new QueryEventFilter(key1, Predicates.alwaysTrue(), true);

        Data key2 = serializationService.toData("key2");
        QueryEventFilter filter2 = new QueryEventFilter(key2, Predicates.alwaysTrue(), true);

        assertFalse(filter1.equals(filter2));
    }

    @Test
    public void testEquals_givenKeyIsNull_whenOtherHasKeyNull_thenReturnTrue() {
        QueryEventFilter filter1 = new QueryEventFilter(null, Predicates.alwaysTrue(), true);
        QueryEventFilter filter2 = new QueryEventFilter(null, Predicates.alwaysTrue(), true);

        assertTrue(filter1.equals(filter2));
    }

    @Test
    public void testEquals_whenPredicatesAreNotEquals_thenReturnFalse() {
        QueryEventFilter filter1 = new QueryEventFilter(null, Predicates.alwaysTrue(), true);
        QueryEventFilter filter2 = new QueryEventFilter(null, Predicates.alwaysFalse(), true);

        assertFalse(filter1.equals(filter2));
    }

    @Test
    public void testEquals_whenPredicatesAreEquals_thenReturnTrue() {
        QueryEventFilter filter1 = new QueryEventFilter(null, Predicates.alwaysTrue(), true);
        QueryEventFilter filter2 = new QueryEventFilter(null, Predicates.alwaysFalse(), true);

        assertFalse(filter1.equals(filter2));
    }

    private QueryableEntry mockEntryWithKeyData(Data key) {
        QueryableEntry entry = mock(QueryableEntry.class);
        when(entry.getKeyData()).thenReturn(key);
        return entry;
    }
}
