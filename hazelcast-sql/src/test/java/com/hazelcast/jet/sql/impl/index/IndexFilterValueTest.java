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
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.sql.impl.exec.scan.index.IndexFilterValue;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@SuppressWarnings("rawtypes")
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class IndexFilterValueTest extends IndexFilterTestSupport {
    @Test
    public void testContent() {
        List<Expression> components = singletonList(constant(1, QueryDataType.INT));
        List<Boolean> allowNulls = singletonList(true);

        IndexFilterValue value = new IndexFilterValue(components, allowNulls);

        assertSame(components, value.getComponents());
        assertSame(allowNulls, value.getAllowNulls());
    }

    @Test
    public void testEquals() {
        IndexFilterValue value = new IndexFilterValue(
                singletonList(constant(1, QueryDataType.INT)),
                singletonList(true)
        );

        checkEquals(
                value,
                new IndexFilterValue(
                        singletonList(constant(1, QueryDataType.INT)),
                        singletonList(true)
                ),
                true
        );

        checkEquals(
                value,
                new IndexFilterValue(
                        singletonList(constant(1, QueryDataType.BIGINT)),
                        singletonList(true)
                ),
                false
        );

        checkEquals(
                value,
                new IndexFilterValue(
                        singletonList(constant(1, QueryDataType.INT)),
                        singletonList(false)
                ),
                false
        );
    }

    @Test
    public void testSerialization() {
        IndexFilterValue original = new IndexFilterValue(
                singletonList(constant(1, QueryDataType.INT)),
                singletonList(true)
        );

        IndexFilterValue restored = serializeAndCheck(original, SqlDataSerializerHook.INDEX_FILTER_VALUE);

        checkEquals(original, restored, true);
    }

    @Test
    public void testValueSimple() {
        ExpressionEvalContext evalContext = createExpressionEvalContext();

        IndexFilterValue value = new IndexFilterValue(
                singletonList(constant(1, QueryDataType.BIGINT)),
                singletonList(true)
        );
        assertEquals(1L, value.getValue(evalContext));

        value = new IndexFilterValue(
                singletonList(constant(null, QueryDataType.BIGINT)),
                singletonList(true)
        );
        assertEquals(AbstractIndex.NULL, value.getValue(evalContext));

        value = new IndexFilterValue(
                singletonList(constant(null, QueryDataType.BIGINT)),
                singletonList(false)
        );
        assertNull(value.getValue(evalContext));
    }

    @Test
    public void testValueComposite() {
        ExpressionEvalContext evalContext = createExpressionEvalContext();

        IndexFilterValue value = new IndexFilterValue(
                asList(constant(1, QueryDataType.BIGINT), constant("2", QueryDataType.VARCHAR)),
                asList(true, true)
        );
        assertEquals(composite(1L, "2"), value.getValue(evalContext));

        value = new IndexFilterValue(
                asList(constant(null, QueryDataType.BIGINT), constant("2", QueryDataType.VARCHAR)),
                asList(true, true)
        );
        assertEquals(composite(AbstractIndex.NULL, "2"), value.getValue(evalContext));

        value = new IndexFilterValue(
                asList(constant(null, QueryDataType.BIGINT), constant("2", QueryDataType.VARCHAR)),
                asList(false, true)
        );
        assertNull(value.getValue(evalContext));

        value = new IndexFilterValue(
                asList(constant(1L, QueryDataType.BIGINT), constant(null, QueryDataType.VARCHAR)),
                asList(true, false)
        );
        assertNull(value.getValue(evalContext));
    }

    @Test
    public void testNonComparable() {
        ExpressionEvalContext evalContext = createExpressionEvalContext();

        IndexFilterValue value = new IndexFilterValue(
                singletonList(constant(new Object(), QueryDataType.OBJECT)),
                singletonList(true)
        );

        try {
            value.getValue(evalContext);

            fail("Must fail");
        } catch (QueryException e) {
            assertEquals(SqlErrorCode.DATA_EXCEPTION, e.getCode());
            assertTrue(e.getMessage().contains("Values used in index lookups must be Comparable"));
        }
    }
}
