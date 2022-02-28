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
import com.hazelcast.sql.impl.exec.scan.index.IndexCompositeFilter;
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

import java.util.Arrays;
import java.util.Collection;

import static java.util.Arrays.asList;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class IndexCompositeFilterIterationTest extends IndexFilterIteratorTestSupport {

    @Parameterized.Parameters(name = "descendingDirection:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{{true}, {false}});
    }

    @Parameterized.Parameter
    public boolean descendingDirection;

    @Test
    public void test_sorted() {
        check(IndexType.SORTED);
    }

    @Test
    public void test_hash() {
        check(IndexType.HASH);
    }

    private void check(IndexType indexType) {
        HazelcastInstance instance = factory.newHazelcastInstance(getConfig());

        IMap<Integer, Value> map = instance.getMap(MAP_NAME);
        map.addIndex(new IndexConfig().setName(INDEX_NAME).setType(indexType).addAttribute("value1"));

        InternalIndex index = getIndex(instance);

        ExpressionEvalContext evalContext = createExpressionEvalContext();

        map.put(1, new Value(null));
        map.put(2, new Value(0));
        map.put(3, new Value(1));

        // No values from both filters
        checkIterator(indexType, descendingDirection, in(equals(2), equals(3)).getEntries(index, descendingDirection, evalContext));
        checkIterator(indexType, descendingDirection, in(equals(3), equals(2)).getEntries(index, descendingDirection, evalContext));

        // No values from one filter
        checkIterator(indexType, descendingDirection, in(equals(1), equals(2)).getEntries(index, descendingDirection, evalContext), 3);
        checkIterator(indexType, descendingDirection, in(equals(2), equals(1)).getEntries(index, descendingDirection, evalContext), 3);

        checkIterator(indexType, descendingDirection, in(equals(null, true), equals(2)).getEntries(index, descendingDirection, evalContext), 1);
        checkIterator(indexType, descendingDirection, in(equals(2), equals(null, true)).getEntries(index, descendingDirection, evalContext), 1);

        // Values from both filters
        checkIterator(indexType, descendingDirection, in(equals(0), equals(1)).getEntries(index, descendingDirection, evalContext), 2, 3);
        checkIterator(indexType, descendingDirection, in(equals(1), equals(0)).getEntries(index, descendingDirection, evalContext), 2, 3);

        checkIterator(indexType, descendingDirection, in(equals(null, true), equals(0)).getEntries(index, descendingDirection, evalContext), 1, 2);
        checkIterator(indexType, descendingDirection, in(equals(0), equals(null, true)).getEntries(index, descendingDirection, evalContext), 1, 2);

        // One distinct value
        checkIterator(indexType, descendingDirection, in(equals(0), equals(0)).getEntries(index, descendingDirection, evalContext), 2);
        checkIterator(indexType, descendingDirection, in(equals(null, true), equals(null, true)).getEntries(index, descendingDirection, evalContext), 1);

        // One null value
        checkIterator(indexType, descendingDirection, in(equals(0), equals(null, false)).getEntries(index, descendingDirection, evalContext), 2);
        checkIterator(indexType, descendingDirection, in(equals(null, false), equals(0)).getEntries(index, descendingDirection, evalContext), 2);

        checkIterator(indexType, descendingDirection, in(equals(null, false), equals(null, true)).getEntries(index, descendingDirection, evalContext), 1);
        checkIterator(indexType, descendingDirection, in(equals(null, true), equals(null, false)).getEntries(index, descendingDirection, evalContext), 1);

        // Two null values
        checkIterator(indexType, descendingDirection, in(equals(null, false), equals(null, false)).getEntries(index, descendingDirection, evalContext));
    }

    private static IndexCompositeFilter in(IndexEqualsFilter... filters) {
        assert filters != null;

        return new IndexCompositeFilter(Arrays.asList(filters));
    }

    private static IndexEqualsFilter equals(Integer value) {
        return equals(value, false);
    }

    private static IndexEqualsFilter equals(Integer value, boolean allowNulls) {
        return new IndexEqualsFilter(intValue(value, allowNulls));
    }
}
