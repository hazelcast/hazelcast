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

package com.hazelcast.multimap.impl;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.HazelcastTestSupport.assumeDifferentHashCodes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MultiMapEventFilterTest {

    private MultiMapEventFilter multiMapEventFilter;
    private MultiMapEventFilter multiMapEventFilterSameAttributes;

    private MultiMapEventFilter multiMapEventFilterOtherIncludeValue;
    private MultiMapEventFilter multiMapEventFilterOtherKey;
    private MultiMapEventFilter multiMapEventFilterDefaultParameters;

    @Before
    public void setUp() {
        Data key = mock(Data.class);
        Data otherKey = mock(Data.class);

        multiMapEventFilter = new MultiMapEventFilter(true, key);
        multiMapEventFilterSameAttributes = new MultiMapEventFilter(true, key);

        multiMapEventFilterOtherIncludeValue = new MultiMapEventFilter(false, key);
        multiMapEventFilterOtherKey = new MultiMapEventFilter(true, otherKey);
        multiMapEventFilterDefaultParameters = new MultiMapEventFilter();
    }

    @Test
    public void testEval() {
        assertFalse(multiMapEventFilter.eval(null));
    }

    @Test
    public void testEquals() {
        assertEquals(multiMapEventFilter, multiMapEventFilter);
        assertEquals(multiMapEventFilter, multiMapEventFilterSameAttributes);

        assertNotEquals(multiMapEventFilter, null);
        assertNotEquals(multiMapEventFilter, new Object());

        assertNotEquals(multiMapEventFilter, multiMapEventFilterOtherIncludeValue);
        assertNotEquals(multiMapEventFilter, multiMapEventFilterOtherKey);
        assertNotEquals(multiMapEventFilter, multiMapEventFilterDefaultParameters);
    }

    @Test
    public void testHashCode() {
        assertEquals(multiMapEventFilter.hashCode(), multiMapEventFilter.hashCode());
        assertEquals(multiMapEventFilter.hashCode(), multiMapEventFilterSameAttributes.hashCode());

        assumeDifferentHashCodes();
        assertNotEquals(multiMapEventFilter.hashCode(), multiMapEventFilterOtherIncludeValue.hashCode());
        assertNotEquals(multiMapEventFilter.hashCode(), multiMapEventFilterOtherKey.hashCode());
        assertNotEquals(multiMapEventFilter.hashCode(), multiMapEventFilterDefaultParameters.hashCode());
    }
}
