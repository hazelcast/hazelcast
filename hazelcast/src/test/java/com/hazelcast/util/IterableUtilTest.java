/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.util;

import com.hazelcast.core.IFunction;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Iterator;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class IterableUtilTest {

    private List<Integer> numbers = asList(1, 2, 3);

    @Test
    public void testElementsConverted_whenIterableMapped() throws Exception {

        Iterable<String> strings = IterableUtil.map(numbers, new IFunction<Integer, String>() {
            @Override
            public String apply(Integer integer) {
                return integer.toString();
            }
        });

        Iterator<String> iter = strings.iterator();
        for (Integer i : numbers) {
            assertEquals(i.toString(), iter.next());
        }
    }

    @Test
    public void testUpToNElement_whenIteratorLimited() throws Exception {
        Iterator<Integer> limitedIterator = IterableUtil.limit(numbers.iterator(), 2);

        assertEquals(Integer.valueOf(1), limitedIterator.next());
        assertEquals(Integer.valueOf(2), limitedIterator.next());
        assertFalse(limitedIterator.hasNext());
    }

    @Test
    public void testIterableIsEmpty_whenNullUsed() throws Exception {
        assertEquals(emptyList(), IterableUtil.nullToEmpty(null));
        assertEquals(numbers, IterableUtil.nullToEmpty(numbers));
    }
}
