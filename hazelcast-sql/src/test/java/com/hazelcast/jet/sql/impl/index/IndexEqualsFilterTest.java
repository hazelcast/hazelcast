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

import com.hazelcast.query.impl.AbstractIndex;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.exec.scan.index.IndexEqualsFilter;
import com.hazelcast.sql.impl.exec.scan.index.IndexFilterValue;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class IndexEqualsFilterTest extends IndexFilterTestSupport {
    @Test
    public void testContent() {
        IndexFilterValue value = intValue(1, true);

        IndexEqualsFilter filter = new IndexEqualsFilter(value);

        assertSame(value, filter.getValue());
    }

    @Test
    public void testEquals() {
        IndexEqualsFilter filter = new IndexEqualsFilter(intValue(1, true));

        checkEquals(filter, new IndexEqualsFilter(intValue(1, true)), true);
        checkEquals(filter, new IndexEqualsFilter(intValue(2, true)), false);
    }

    @Test
    public void testSerialization() {
        IndexEqualsFilter original = new IndexEqualsFilter(intValue(1, true));
        IndexEqualsFilter restored = serializeAndCheck(original, SqlDataSerializerHook.INDEX_FILTER_EQUALS);

        checkEquals(original, restored, true);
    }

    @Test
    public void testComparable() {
        ExpressionEvalContext evalContext = createExpressionEvalContext();

        // Simple, not null
        assertEquals(1, new IndexEqualsFilter(intValue(1, false)).getComparable(evalContext));

        // Simple, null
        assertEquals(AbstractIndex.NULL, new IndexEqualsFilter(intValue(null, true)).getComparable(evalContext));
        assertNull(new IndexEqualsFilter(intValue(null, false)).getComparable(evalContext));

        // Composite, not null
        assertEquals(
                composite(1, 2),
                new IndexEqualsFilter(intValues(1, true, 2, true)).getComparable(evalContext)
        );

        // Composite, null
        assertEquals(
                composite(1, AbstractIndex.NULL),
                new IndexEqualsFilter(intValues(1, true, null, true)).getComparable(evalContext)
        );

        assertEquals(
                composite(AbstractIndex.NULL, 2),
                new IndexEqualsFilter(intValues(null, true, 2, true)).getComparable(evalContext)
        );

        assertEquals(
                composite(AbstractIndex.NULL, AbstractIndex.NULL),
                new IndexEqualsFilter(intValues(null, true, null, true)).getComparable(evalContext)
        );

        assertNull(new IndexEqualsFilter(intValues(1, true, null, false)).getComparable(evalContext));
        assertNull(new IndexEqualsFilter(intValues(null, false, 2, true)).getComparable(evalContext));
        assertNull(new IndexEqualsFilter(intValues(null, false, null, false)).getComparable(evalContext));
    }
}
