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
import com.hazelcast.config.IndexType;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.query.impl.InternalIndex;
import com.hazelcast.sql.impl.exec.scan.index.IndexEqualsFilter;
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

import static java.util.Arrays.asList;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class IndexEqualsFilterIterationTest extends IndexFilterIteratorTestSupport {

    @Parameterized.Parameters(name = "descendingDirection:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{{true}, {false}});
    }

    @Parameterized.Parameter
    public boolean descendingDirection;

    @Test
    public void testIterator_simple_sorted() {
        checkIteratorSimple(IndexType.SORTED);
    }

    @Test
    public void testIterator_simple_hash() {
        checkIteratorSimple(IndexType.HASH);
    }

    private void checkIteratorSimple(IndexType indexType) {
        HazelcastInstance instance = factory.newHazelcastInstance(getConfig());

        IMap<Integer, Value> map = instance.getMap(MAP_NAME);
        map.addIndex(new IndexConfig().setName(INDEX_NAME).setType(indexType).addAttribute("value1"));

        InternalIndex index = getIndex(instance);

        ExpressionEvalContext evalContext = createExpressionEvalContext();

        // Check missing value.
        checkIterator(indexType, descendingDirection, new IndexEqualsFilter(intValue(1)).getEntries(index, descendingDirection, evalContext));

        // Check single value.
        map.put(1, new Value(1));
        checkIterator(indexType, descendingDirection, new IndexEqualsFilter(intValue(1)).getEntries(index, descendingDirection, evalContext), 1);

        // Check multiple values.
        map.put(2, new Value(1));
        checkIterator(indexType, descendingDirection, new IndexEqualsFilter(intValue(1)).getEntries(index, descendingDirection, evalContext), 1, 2);

        // Check null value.
        checkIterator(indexType, descendingDirection, new IndexEqualsFilter(intValue(null, false)).getEntries(index, descendingDirection, evalContext));
        checkIterator(indexType, descendingDirection, new IndexEqualsFilter(intValue(null, true)).getEntries(index, descendingDirection, evalContext));

        map.put(3, new Value(null));
        checkIterator(indexType, descendingDirection, new IndexEqualsFilter(intValue(null, false)).getEntries(index, descendingDirection, evalContext));
        checkIterator(indexType, descendingDirection, new IndexEqualsFilter(intValue(null, true)).getEntries(index, descendingDirection, evalContext), 3);

        map.put(4, new Value(null));
        checkIterator(indexType, descendingDirection, new IndexEqualsFilter(intValue(null, false)).getEntries(index, descendingDirection, evalContext));
        checkIterator(indexType, descendingDirection, new IndexEqualsFilter(intValue(null, true)).getEntries(index, descendingDirection, evalContext), 3, 4);
    }

    @Test
    public void testIterator_composite_sorted() {
        checkIteratorComposite(IndexType.SORTED);
    }

    @Test
    public void testIterator_composite_hash() {
        checkIteratorComposite(IndexType.HASH);
    }

    private void checkIteratorComposite(IndexType indexType) {
        HazelcastInstance instance = factory.newHazelcastInstance();

        IMap<Integer, Value> map = instance.getMap(MAP_NAME);
        map.addIndex(new IndexConfig().setName(INDEX_NAME).setType(indexType).addAttribute("value1").addAttribute("value2"));

        InternalIndex index = getIndex(instance);

        ExpressionEvalContext evalContext = createExpressionEvalContext();

        // Check missing value.
        checkIterator(indexType, descendingDirection, new IndexEqualsFilter(intValues(1, 2)).getEntries(index, descendingDirection, evalContext));

        map.put(1, new Value(1, 1));
        checkIterator(indexType, descendingDirection, new IndexEqualsFilter(intValues(1, 2)).getEntries(index, descendingDirection, evalContext));

        map.put(2, new Value(2, 1));
        checkIterator(indexType, descendingDirection, new IndexEqualsFilter(intValues(1, 2)).getEntries(index, descendingDirection, evalContext));

        // Check single value.
        map.put(3, new Value(1, 2));
        checkIterator(indexType, descendingDirection, new IndexEqualsFilter(intValues(1, 2)).getEntries(index, descendingDirection, evalContext), 3);

        // Check multiple values.
        map.put(4, new Value(1, 2));
        checkIterator(indexType, descendingDirection, new IndexEqualsFilter(intValues(1, 2)).getEntries(index, descendingDirection, evalContext), 3, 4);

        // Check null values (first).
        checkIterator(indexType, descendingDirection, new IndexEqualsFilter(intValues(null, false, 2, false)).getEntries(index, descendingDirection, evalContext));
        checkIterator(indexType, descendingDirection, new IndexEqualsFilter(intValues(null, true, 2, false)).getEntries(index, descendingDirection, evalContext));

        map.put(5, new Value(null, 2));
        checkIterator(indexType, descendingDirection, new IndexEqualsFilter(intValues(null, false, 2, false)).getEntries(index, descendingDirection, evalContext));
        checkIterator(indexType, descendingDirection, new IndexEqualsFilter(intValues(null, true, 2, false)).getEntries(index, descendingDirection, evalContext), 5);

        map.put(6, new Value(null, 2));
        checkIterator(indexType, descendingDirection, new IndexEqualsFilter(intValues(null, false, 2, false)).getEntries(index, descendingDirection, evalContext));
        checkIterator(indexType, descendingDirection, new IndexEqualsFilter(intValues(null, true, 2, false)).getEntries(index, descendingDirection, evalContext), 5, 6);

        // Check null values (last).
        checkIterator(indexType, descendingDirection, new IndexEqualsFilter(intValues(1, false, null, false)).getEntries(index, descendingDirection, evalContext));
        checkIterator(indexType, descendingDirection, new IndexEqualsFilter(intValues(1, false, null, true)).getEntries(index, descendingDirection, evalContext));

        map.put(7, new Value(1, null));
        checkIterator(indexType, descendingDirection, new IndexEqualsFilter(intValues(1, false, null, false)).getEntries(index, descendingDirection, evalContext));
        checkIterator(indexType, descendingDirection, new IndexEqualsFilter(intValues(1, false, null, true)).getEntries(index, descendingDirection, evalContext), 7);

        map.put(8, new Value(1, null));
        checkIterator(indexType, descendingDirection, new IndexEqualsFilter(intValues(1, false, null, false)).getEntries(index, descendingDirection, evalContext));
        checkIterator(indexType, descendingDirection, new IndexEqualsFilter(intValues(1, false, null, true)).getEntries(index, descendingDirection, evalContext), 7, 8);

        // Check null values (both).
        checkIterator(indexType, descendingDirection, new IndexEqualsFilter(intValues(null, false, null, false)).getEntries(index, descendingDirection, evalContext));
        checkIterator(indexType, descendingDirection, new IndexEqualsFilter(intValues(null, false, null, true)).getEntries(index, descendingDirection, evalContext));
        checkIterator(indexType, descendingDirection, new IndexEqualsFilter(intValues(null, true, null, false)).getEntries(index, descendingDirection, evalContext));
        checkIterator(indexType, descendingDirection, new IndexEqualsFilter(intValues(null, true, null, true)).getEntries(index, descendingDirection, evalContext));

        map.put(9, new Value(null, null));
        checkIterator(indexType, descendingDirection, new IndexEqualsFilter(intValues(null, false, null, false)).getEntries(index, descendingDirection, evalContext));
        checkIterator(indexType, descendingDirection, new IndexEqualsFilter(intValues(null, false, null, true)).getEntries(index, descendingDirection, evalContext));
        checkIterator(indexType, descendingDirection, new IndexEqualsFilter(intValues(null, true, null, false)).getEntries(index, descendingDirection, evalContext));
        checkIterator(indexType, descendingDirection, new IndexEqualsFilter(intValues(null, true, null, true)).getEntries(index, descendingDirection, evalContext), 9);

        map.put(10, new Value(null, null));
        checkIterator(indexType, descendingDirection, new IndexEqualsFilter(intValues(null, false, null, false)).getEntries(index, descendingDirection, evalContext));
        checkIterator(indexType, descendingDirection, new IndexEqualsFilter(intValues(null, false, null, true)).getEntries(index, descendingDirection, evalContext));
        checkIterator(indexType, descendingDirection, new IndexEqualsFilter(intValues(null, true, null, false)).getEntries(index, descendingDirection, evalContext));
        checkIterator(indexType, descendingDirection, new IndexEqualsFilter(intValues(null, true, null, true)).getEntries(index, descendingDirection, evalContext), 9, 10);
    }
}
