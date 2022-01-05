/*
 * Copyright 2021 Hazelcast Inc.
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

import com.hazelcast.config.IndexConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.query.impl.InternalIndex;
import com.hazelcast.sql.impl.exec.scan.index.IndexRangeFilter;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;

import static com.hazelcast.config.IndexType.SORTED;
import static java.util.Arrays.asList;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class IndexRangeFilterIteratorTest extends IndexFilterIteratorTestSupport {

    @Parameterized.Parameters(name = "descendingDirection:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{{true}, {false}});
    }

    @Parameterized.Parameter
    public boolean descendingDirection;

    @Test
    public void testIterator_simple_from() {
        HazelcastInstance instance = factory.newHazelcastInstance(getConfig());

        IMap<Integer, Value> map = instance.getMap(MAP_NAME);
        map.addIndex(new IndexConfig().setName(INDEX_NAME).setType(SORTED).addAttribute("value1"));

        InternalIndex index = getIndex(instance);

        ExpressionEvalContext evalContext = createExpressionEvalContext();

        // Check missing value.
        map.put(0, new Value(0));

        checkIterator(SORTED, descendingDirection, new IndexRangeFilter(intValue(1), true, null, false).getEntries(index, descendingDirection, evalContext));
        checkIterator(SORTED, descendingDirection, new IndexRangeFilter(intValue(1), false, null, false).getEntries(index, descendingDirection, evalContext));

        // Check single value.
        map.put(1, new Value(1));

        checkIterator(SORTED, descendingDirection, new IndexRangeFilter(intValue(1), true, null, false).getEntries(index, descendingDirection, evalContext), 1);
        checkIterator(SORTED, descendingDirection, new IndexRangeFilter(intValue(1), false, null, false).getEntries(index, descendingDirection, evalContext));

        // Check multiple values.
        map.put(2, new Value(1));
        map.put(3, new Value(2));
        map.put(4, new Value(2));

        checkIterator(SORTED, descendingDirection, new IndexRangeFilter(intValue(1), true, null, false).getEntries(index, descendingDirection, evalContext), 1, 2, 3, 4);
        checkIterator(SORTED, descendingDirection, new IndexRangeFilter(intValue(1), false, null, false).getEntries(index, descendingDirection, evalContext), 3, 4);

        // Check null value.
        checkIterator(SORTED, descendingDirection, new IndexRangeFilter(intValue(null, false), true, null, false).getEntries(index, descendingDirection, evalContext));
        checkIterator(SORTED, descendingDirection, new IndexRangeFilter(intValue(null, false), false, null, false).getEntries(index, descendingDirection, evalContext));
    }

    @Test
    public void testIterator_simple_to() {
        HazelcastInstance instance = factory.newHazelcastInstance(getConfig());

        IMap<Integer, Value> map = instance.getMap(MAP_NAME);
        map.addIndex(new IndexConfig().setName(INDEX_NAME).setType(SORTED).addAttribute("value1"));

        InternalIndex index = getIndex(instance);

        ExpressionEvalContext evalContext = createExpressionEvalContext();

        // Check missing value.
        map.put(0, new Value(10));

        checkIterator(SORTED, descendingDirection, new IndexRangeFilter(null, false, intValue(2), false).getEntries(index, descendingDirection, evalContext));
        checkIterator(SORTED, descendingDirection, new IndexRangeFilter(null, false, intValue(2), true).getEntries(index, descendingDirection, evalContext));

        // Check single value.
        map.put(1, new Value(2));

        checkIterator(SORTED, descendingDirection, new IndexRangeFilter(null, false, intValue(2), false).getEntries(index, descendingDirection, evalContext));
        checkIterator(SORTED, descendingDirection, new IndexRangeFilter(null, false, intValue(2), true).getEntries(index, descendingDirection, evalContext), 1);

        // Check multiple values.
        map.put(2, new Value(2));
        map.put(3, new Value(1));
        map.put(4, new Value(1));

        checkIterator(SORTED, descendingDirection, new IndexRangeFilter(null, false, intValue(2), false).getEntries(index, descendingDirection, evalContext), 3, 4);
        checkIterator(SORTED, descendingDirection, new IndexRangeFilter(null, false, intValue(2), true).getEntries(index, descendingDirection, evalContext), 1, 2, 3, 4);

        // Check null value.
        checkIterator(SORTED, descendingDirection, new IndexRangeFilter(null, false, intValue(null, false), false).getEntries(index, descendingDirection, evalContext));
        checkIterator(SORTED, descendingDirection, new IndexRangeFilter(null, false, intValue(null, false), true).getEntries(index, descendingDirection, evalContext));
    }

    @Test
    public void testIterator_simple_between() {
        HazelcastInstance instance = factory.newHazelcastInstance(getConfig());

        IMap<Integer, Value> map = instance.getMap(MAP_NAME);
        map.addIndex(new IndexConfig().setName(INDEX_NAME).setType(SORTED).addAttribute("value1"));

        InternalIndex index = getIndex(instance);

        ExpressionEvalContext evalContext = createExpressionEvalContext();

        // Check missing value.
        map.put(0, new Value(0));
        map.put(1, new Value(10));

        checkIterator(SORTED, descendingDirection, new IndexRangeFilter(intValue(1), false, intValue(5), false).getEntries(index, descendingDirection, evalContext));
        checkIterator(SORTED, descendingDirection, new IndexRangeFilter(intValue(1), true, intValue(5), false).getEntries(index, descendingDirection, evalContext));
        checkIterator(SORTED, descendingDirection, new IndexRangeFilter(intValue(1), false, intValue(5), true).getEntries(index, descendingDirection, evalContext));
        checkIterator(SORTED, descendingDirection, new IndexRangeFilter(intValue(1), true, intValue(5), true).getEntries(index, descendingDirection, evalContext));

        // Check left bound
        map.put(2, new Value(1));

        checkIterator(SORTED, descendingDirection, new IndexRangeFilter(intValue(1), false, intValue(5), false).getEntries(index, descendingDirection, evalContext));
        checkIterator(SORTED, descendingDirection, new IndexRangeFilter(intValue(1), true, intValue(5), false).getEntries(index, descendingDirection, evalContext), 2);
        checkIterator(SORTED, descendingDirection, new IndexRangeFilter(intValue(1), false, intValue(5), true).getEntries(index, descendingDirection, evalContext));
        checkIterator(SORTED, descendingDirection, new IndexRangeFilter(intValue(1), true, intValue(5), true).getEntries(index, descendingDirection, evalContext), 2);

        map.put(3, new Value(1));

        checkIterator(SORTED, descendingDirection, new IndexRangeFilter(intValue(1), false, intValue(5), false).getEntries(index, descendingDirection, evalContext));
        checkIterator(SORTED, descendingDirection, new IndexRangeFilter(intValue(1), true, intValue(5), false).getEntries(index, descendingDirection, evalContext), 2, 3);
        checkIterator(SORTED, descendingDirection, new IndexRangeFilter(intValue(1), false, intValue(5), true).getEntries(index, descendingDirection, evalContext));
        checkIterator(SORTED, descendingDirection, new IndexRangeFilter(intValue(1), true, intValue(5), true).getEntries(index, descendingDirection, evalContext), 2, 3);

        map.remove(2);
        map.remove(3);

        // Check right bound
        map.put(2, new Value(5));

        checkIterator(SORTED, descendingDirection, new IndexRangeFilter(intValue(1), false, intValue(5), false).getEntries(index, descendingDirection, evalContext));
        checkIterator(SORTED, descendingDirection, new IndexRangeFilter(intValue(1), true, intValue(5), false).getEntries(index, descendingDirection, evalContext));
        checkIterator(SORTED, descendingDirection, new IndexRangeFilter(intValue(1), false, intValue(5), true).getEntries(index, descendingDirection, evalContext), 2);
        checkIterator(SORTED, descendingDirection, new IndexRangeFilter(intValue(1), true, intValue(5), true).getEntries(index, descendingDirection, evalContext), 2);

        map.put(3, new Value(5));

        checkIterator(SORTED, descendingDirection, new IndexRangeFilter(intValue(1), false, intValue(5), false).getEntries(index, descendingDirection, evalContext));
        checkIterator(SORTED, descendingDirection, new IndexRangeFilter(intValue(1), true, intValue(5), false).getEntries(index, descendingDirection, evalContext));
        checkIterator(SORTED, descendingDirection, new IndexRangeFilter(intValue(1), false, intValue(5), true).getEntries(index, descendingDirection, evalContext), 2, 3);
        checkIterator(SORTED, descendingDirection, new IndexRangeFilter(intValue(1), true, intValue(5), true).getEntries(index, descendingDirection, evalContext), 2, 3);

        map.remove(2);
        map.remove(3);

        // Check middle
        map.put(2, new Value(3));

        checkIterator(SORTED, descendingDirection, new IndexRangeFilter(intValue(1), false, intValue(5), false).getEntries(index, descendingDirection, evalContext), 2);
        checkIterator(SORTED, descendingDirection, new IndexRangeFilter(intValue(1), true, intValue(5), false).getEntries(index, descendingDirection, evalContext), 2);
        checkIterator(SORTED, descendingDirection, new IndexRangeFilter(intValue(1), false, intValue(5), true).getEntries(index, descendingDirection, evalContext), 2);
        checkIterator(SORTED, descendingDirection, new IndexRangeFilter(intValue(1), true, intValue(5), true).getEntries(index, descendingDirection, evalContext), 2);

        map.put(3, new Value(3));

        checkIterator(SORTED, descendingDirection, new IndexRangeFilter(intValue(1), false, intValue(5), false).getEntries(index, descendingDirection, evalContext), 2, 3);
        checkIterator(SORTED, descendingDirection, new IndexRangeFilter(intValue(1), true, intValue(5), false).getEntries(index, descendingDirection, evalContext), 2, 3);
        checkIterator(SORTED, descendingDirection, new IndexRangeFilter(intValue(1), false, intValue(5), true).getEntries(index, descendingDirection, evalContext), 2, 3);
        checkIterator(SORTED, descendingDirection, new IndexRangeFilter(intValue(1), true, intValue(5), true).getEntries(index, descendingDirection, evalContext), 2, 3);

        map.remove(2);
        map.remove(3);

        // Check combined
        map.put(2, new Value(1));
        map.put(3, new Value(1));
        map.put(4, new Value(3));
        map.put(5, new Value(3));
        map.put(6, new Value(5));
        map.put(7, new Value(5));

        checkIterator(SORTED, descendingDirection, new IndexRangeFilter(intValue(1), false, intValue(5), false).getEntries(index, descendingDirection, evalContext), 4, 5);
        checkIterator(SORTED, descendingDirection, new IndexRangeFilter(intValue(1), true, intValue(5), false).getEntries(index, descendingDirection, evalContext), 2, 3, 4, 5);
        checkIterator(SORTED, descendingDirection, new IndexRangeFilter(intValue(1), false, intValue(5), true).getEntries(index, descendingDirection, evalContext), 4, 5, 6, 7);
        checkIterator(SORTED, descendingDirection, new IndexRangeFilter(intValue(1), true, intValue(5), true).getEntries(index, descendingDirection, evalContext), 2, 3, 4, 5, 6, 7);

        // Check null value.
        checkIterator(SORTED, descendingDirection, new IndexRangeFilter(intValue(null, false), false, intValue(5), false).getEntries(index, descendingDirection, evalContext));
        checkIterator(SORTED, descendingDirection, new IndexRangeFilter(intValue(1), false, intValue(null, false), false).getEntries(index, descendingDirection, evalContext));
        checkIterator(SORTED, descendingDirection, new IndexRangeFilter(intValue(null, false), false, intValue(null, false), false).getEntries(index, descendingDirection, evalContext));
    }

    /**
     * Test composite iterator. Note that parent components are always the same, therefore we do not test open bounds.
     */
    @Test
    public void testIterator_composite() {
        HazelcastInstance instance = factory.newHazelcastInstance(getConfig());

        IMap<Integer, Value> map = instance.getMap(MAP_NAME);
        map.addIndex(new IndexConfig().setName(INDEX_NAME).setType(SORTED).addAttribute("value1").addAttribute("value2"));

        InternalIndex index = getIndex(instance);

        ExpressionEvalContext evalContext = createExpressionEvalContext();

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

        checkIterator(SORTED, descendingDirection, new IndexRangeFilter(intValues(1, 1), false, intValues(1, 5), false).getEntries(index, descendingDirection, evalContext));
        checkIterator(SORTED, descendingDirection, new IndexRangeFilter(intValues(1, 1), false, intValues(1, 5), true).getEntries(index, descendingDirection, evalContext));
        checkIterator(SORTED, descendingDirection, new IndexRangeFilter(intValues(1, 1), true, intValues(1, 5), false).getEntries(index, descendingDirection, evalContext));
        checkIterator(SORTED, descendingDirection, new IndexRangeFilter(intValues(1, 1), true, intValues(1, 5), true).getEntries(index, descendingDirection, evalContext));

        map.put(11, new Value(1, 1));
        map.put(12, new Value(1, 1));
        map.put(13, new Value(1, 3));
        map.put(14, new Value(1, 3));
        map.put(15, new Value(1, 5));
        map.put(16, new Value(1, 5));

        checkIterator(SORTED, descendingDirection, new IndexRangeFilter(intValues(1, 1), false, intValues(1, 5), false).getEntries(index, descendingDirection, evalContext), 13, 14);
        checkIterator(SORTED, descendingDirection, new IndexRangeFilter(intValues(1, 1), false, intValues(1, 5), true).getEntries(index, descendingDirection, evalContext), 13, 14, 15, 16);
        checkIterator(SORTED, descendingDirection, new IndexRangeFilter(intValues(1, 1), true, intValues(1, 5), false).getEntries(index, descendingDirection, evalContext), 11, 12, 13, 14);
        checkIterator(SORTED, descendingDirection, new IndexRangeFilter(intValues(1, 1), true, intValues(1, 5), true).getEntries(index, descendingDirection, evalContext), 11, 12, 13, 14, 15, 16);
    }
}
