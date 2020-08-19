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

import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.query.impl.InternalIndex;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.expression.SimpleExpressionEvalContext;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class IndexRangeFilterIteratorTest extends IndexFilterIteratorTestSupport {
    @Test
    public void testIterator_simple_from() {
        HazelcastInstance instance = factory.newHazelcastInstance(getConfig());

        IMap<Integer, Value> map = instance.getMap(MAP_NAME);
        map.addIndex(new IndexConfig().setName(INDEX_NAME).setType(IndexType.SORTED).addAttribute("value1"));

        InternalIndex index = getIndex(instance);

        ExpressionEvalContext evalContext = SimpleExpressionEvalContext.create();

        // Check missing value.
        map.put(0, new Value(0));

        checkIterator(new IndexRangeFilter(intValue(1), true, null, false).getEntries(index, evalContext));
        checkIterator(new IndexRangeFilter(intValue(1), false, null, false).getEntries(index, evalContext));

        // Check single value.
        map.put(1, new Value(1));

        checkIterator(new IndexRangeFilter(intValue(1), true, null, false).getEntries(index, evalContext), 1);
        checkIterator(new IndexRangeFilter(intValue(1), false, null, false).getEntries(index, evalContext));

        // Check multiple values.
        map.put(2, new Value(1));
        map.put(3, new Value(2));
        map.put(4, new Value(2));

        checkIterator(new IndexRangeFilter(intValue(1), true, null, false).getEntries(index, evalContext), 1, 2, 3, 4);
        checkIterator(new IndexRangeFilter(intValue(1), false, null, false).getEntries(index, evalContext), 3, 4);

        // Check null value.
        checkIterator(new IndexRangeFilter(intValue(null, false), true, null, false).getEntries(index, evalContext));
        checkIterator(new IndexRangeFilter(intValue(null, true), true, null, false).getEntries(index, evalContext));
        checkIterator(new IndexRangeFilter(intValue(null, false), false, null, false).getEntries(index, evalContext));
        checkIterator(new IndexRangeFilter(intValue(null, true), false, null, false).getEntries(index, evalContext));
    }

    @Test
    public void testIterator_simple_to() {
        HazelcastInstance instance = factory.newHazelcastInstance(getConfig());

        IMap<Integer, Value> map = instance.getMap(MAP_NAME);
        map.addIndex(new IndexConfig().setName(INDEX_NAME).setType(IndexType.SORTED).addAttribute("value1"));

        InternalIndex index = getIndex(instance);

        ExpressionEvalContext evalContext = SimpleExpressionEvalContext.create();

        // Check missing value.
        map.put(0, new Value(10));

        checkIterator(new IndexRangeFilter(null, false, intValue(2), false).getEntries(index, evalContext));
        checkIterator(new IndexRangeFilter(null, false, intValue(2), true).getEntries(index, evalContext));

        // Check single value.
        map.put(1, new Value(2));

        checkIterator(new IndexRangeFilter(null, false, intValue(2), false).getEntries(index, evalContext));
        checkIterator(new IndexRangeFilter(null, false, intValue(2), true).getEntries(index, evalContext), 1);

        // Check multiple values.
        map.put(2, new Value(2));
        map.put(3, new Value(1));
        map.put(4, new Value(1));

        checkIterator(new IndexRangeFilter(null, false, intValue(2), false).getEntries(index, evalContext), 3, 4);
        checkIterator(new IndexRangeFilter(null, false, intValue(2), true).getEntries(index, evalContext), 1, 2, 3, 4);

        // Check null value.
        checkIterator(new IndexRangeFilter(null, false, intValue(null, false), false).getEntries(index, evalContext));
        checkIterator(new IndexRangeFilter(null, false, intValue(null, true), false).getEntries(index, evalContext));
        checkIterator(new IndexRangeFilter(null, false, intValue(null, false), true).getEntries(index, evalContext));
        checkIterator(new IndexRangeFilter(null, false, intValue(null, true), true).getEntries(index, evalContext));
    }

    @Test
    public void testIterator_simple_between() {
        HazelcastInstance instance = factory.newHazelcastInstance(getConfig());

        IMap<Integer, Value> map = instance.getMap(MAP_NAME);
        map.addIndex(new IndexConfig().setName(INDEX_NAME).setType(IndexType.SORTED).addAttribute("value1"));

        InternalIndex index = getIndex(instance);

        ExpressionEvalContext evalContext = SimpleExpressionEvalContext.create();

        // Check missing value.
        map.put(0, new Value(0));
        map.put(1, new Value(10));

        checkIterator(new IndexRangeFilter(intValue(1), false, intValue(5), false).getEntries(index, evalContext));
        checkIterator(new IndexRangeFilter(intValue(1), true, intValue(5), false).getEntries(index, evalContext));
        checkIterator(new IndexRangeFilter(intValue(1), false, intValue(5), true).getEntries(index, evalContext));
        checkIterator(new IndexRangeFilter(intValue(1), true, intValue(5), true).getEntries(index, evalContext));

        // Check left bound
        map.put(2, new Value(1));

        checkIterator(new IndexRangeFilter(intValue(1), false, intValue(5), false).getEntries(index, evalContext));
        checkIterator(new IndexRangeFilter(intValue(1), true, intValue(5), false).getEntries(index, evalContext), 2);
        checkIterator(new IndexRangeFilter(intValue(1), false, intValue(5), true).getEntries(index, evalContext));
        checkIterator(new IndexRangeFilter(intValue(1), true, intValue(5), true).getEntries(index, evalContext), 2);

        map.put(3, new Value(1));

        checkIterator(new IndexRangeFilter(intValue(1), false, intValue(5), false).getEntries(index, evalContext));
        checkIterator(new IndexRangeFilter(intValue(1), true, intValue(5), false).getEntries(index, evalContext), 2, 3);
        checkIterator(new IndexRangeFilter(intValue(1), false, intValue(5), true).getEntries(index, evalContext));
        checkIterator(new IndexRangeFilter(intValue(1), true, intValue(5), true).getEntries(index, evalContext), 2, 3);

        map.remove(2);
        map.remove(3);

        // Check right bound
        map.put(2, new Value(5));

        checkIterator(new IndexRangeFilter(intValue(1), false, intValue(5), false).getEntries(index, evalContext));
        checkIterator(new IndexRangeFilter(intValue(1), true, intValue(5), false).getEntries(index, evalContext));
        checkIterator(new IndexRangeFilter(intValue(1), false, intValue(5), true).getEntries(index, evalContext), 2);
        checkIterator(new IndexRangeFilter(intValue(1), true, intValue(5), true).getEntries(index, evalContext), 2);

        map.put(3, new Value(5));

        checkIterator(new IndexRangeFilter(intValue(1), false, intValue(5), false).getEntries(index, evalContext));
        checkIterator(new IndexRangeFilter(intValue(1), true, intValue(5), false).getEntries(index, evalContext));
        checkIterator(new IndexRangeFilter(intValue(1), false, intValue(5), true).getEntries(index, evalContext), 2, 3);
        checkIterator(new IndexRangeFilter(intValue(1), true, intValue(5), true).getEntries(index, evalContext), 2, 3);

        map.remove(2);
        map.remove(3);

        // Check middle
        map.put(2, new Value(3));

        checkIterator(new IndexRangeFilter(intValue(1), false, intValue(5), false).getEntries(index, evalContext), 2);
        checkIterator(new IndexRangeFilter(intValue(1), true, intValue(5), false).getEntries(index, evalContext), 2);
        checkIterator(new IndexRangeFilter(intValue(1), false, intValue(5), true).getEntries(index, evalContext), 2);
        checkIterator(new IndexRangeFilter(intValue(1), true, intValue(5), true).getEntries(index, evalContext), 2);

        map.put(3, new Value(3));

        checkIterator(new IndexRangeFilter(intValue(1), false, intValue(5), false).getEntries(index, evalContext), 2, 3);
        checkIterator(new IndexRangeFilter(intValue(1), true, intValue(5), false).getEntries(index, evalContext), 2, 3);
        checkIterator(new IndexRangeFilter(intValue(1), false, intValue(5), true).getEntries(index, evalContext), 2, 3);
        checkIterator(new IndexRangeFilter(intValue(1), true, intValue(5), true).getEntries(index, evalContext), 2, 3);

        map.remove(2);
        map.remove(3);

        // Check combined
        map.put(2, new Value(1));
        map.put(3, new Value(1));
        map.put(4, new Value(3));
        map.put(5, new Value(3));
        map.put(6, new Value(5));
        map.put(7, new Value(5));

        checkIterator(new IndexRangeFilter(intValue(1), false, intValue(5), false).getEntries(index, evalContext), 4, 5);
        checkIterator(new IndexRangeFilter(intValue(1), true, intValue(5), false).getEntries(index, evalContext), 2, 3, 4, 5);
        checkIterator(new IndexRangeFilter(intValue(1), false, intValue(5), true).getEntries(index, evalContext), 4, 5, 6, 7);
        checkIterator(new IndexRangeFilter(intValue(1), true, intValue(5), true).getEntries(index, evalContext), 2, 3, 4, 5, 6, 7);

        // Check null value.
        checkIterator(new IndexRangeFilter(intValue(null, false), false, intValue(5), false).getEntries(index, evalContext));
        checkIterator(new IndexRangeFilter(intValue(null, true), false, intValue(5), false).getEntries(index, evalContext));

        checkIterator(new IndexRangeFilter(intValue(1), false, intValue(null, false), false).getEntries(index, evalContext));
        checkIterator(new IndexRangeFilter(intValue(1), false, intValue(null, true), false).getEntries(index, evalContext));

        checkIterator(new IndexRangeFilter(intValue(null, false), false, intValue(null, false), false).getEntries(index, evalContext));
        checkIterator(new IndexRangeFilter(intValue(null, true), false, intValue(null, true), false).getEntries(index, evalContext));
    }

    /**
     * Test composite iterator. Note that parent components are always the same, therefore we do not test open bounds.
     */
    @Test
    public void testIterator_composite() {
        HazelcastInstance instance = factory.newHazelcastInstance(getConfig());

        IMap<Integer, Value> map = instance.getMap(MAP_NAME);
        map.addIndex(new IndexConfig().setName(INDEX_NAME).setType(IndexType.SORTED).addAttribute("value1").addAttribute("value2"));

        InternalIndex index = getIndex(instance);

        ExpressionEvalContext evalContext = SimpleExpressionEvalContext.create();

        map.put(0, new Value(0, 0));
        map.put(1, new Value(0, 1));
        map.put(3, new Value(0, 3));
        map.put(4, new Value(0, 5));
        map.put(5, new Value(0, 6));

        map.put(6, new Value(2, 0));
        map.put(7, new Value(2, 1));
        map.put(8, new Value(2, 3));
        map.put(9, new Value(2, 5));
        map.put(10, new Value(2, 6));

        checkIterator(new IndexRangeFilter(intValues(1, 1), false, intValues(1, 5), false).getEntries(index, evalContext));
        checkIterator(new IndexRangeFilter(intValues(1, 1), false, intValues(1, 5), true).getEntries(index, evalContext));
        checkIterator(new IndexRangeFilter(intValues(1, 1), true, intValues(1, 5), false).getEntries(index, evalContext));
        checkIterator(new IndexRangeFilter(intValues(1, 1), true, intValues(1, 5), true).getEntries(index, evalContext));

        map.put(11, new Value(1, 1));
        map.put(12, new Value(1, 1));
        map.put(13, new Value(1, 3));
        map.put(14, new Value(1, 3));
        map.put(15, new Value(1, 5));
        map.put(16, new Value(1, 5));

        checkIterator(new IndexRangeFilter(intValues(1, 1), false, intValues(1, 5), false).getEntries(index, evalContext), 13, 14);
        checkIterator(new IndexRangeFilter(intValues(1, 1), false, intValues(1, 5), true).getEntries(index, evalContext), 13, 14, 15, 16);
        checkIterator(new IndexRangeFilter(intValues(1, 1), true, intValues(1, 5), false).getEntries(index, evalContext), 11, 12, 13, 14);
        checkIterator(new IndexRangeFilter(intValues(1, 1), true, intValues(1, 5), true).getEntries(index, evalContext), 11, 12, 13, 14, 15, 16);
    }
}
