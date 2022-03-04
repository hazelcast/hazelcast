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

package com.hazelcast.sql.impl.row;

import com.hazelcast.sql.impl.CoreSqlTestSupport;
import com.hazelcast.sql.impl.SqlCustomClass;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HeapRowTest extends CoreSqlTestSupport {
    @Test
    public void testHeapRow() {
        Object[] values = new Object[2];

        values[0] = new Object();
        values[1] = new Object();

        HeapRow row = new HeapRow(values);

        assertEquals(2, row.getColumnCount());
        assertSame(values[0], row.get(0));
        assertSame(values[1], row.get(1));

        row = new HeapRow(2);

        row.set(0, values[0]);
        row.set(1, values[1]);

        assertEquals(2, row.getColumnCount());
        assertSame(values[0], row.get(0));
        assertSame(values[1], row.get(1));
    }

    @Test
    public void testEquals() {
        checkEquals(new HeapRow(2), new HeapRow(2), true);
        checkEquals(new HeapRow(2), new HeapRow(3), false);

        HeapRow row1 = new HeapRow(2);
        HeapRow row2 = new HeapRow(2);
        HeapRow row3 = new HeapRow(2);

        Object value1 = new Object();
        Object value2 = new Object();
        Object value3 = new Object();

        row1.set(0, value1);
        row2.set(0, value1);
        row3.set(0, value1);

        row1.set(1, value2);
        row2.set(1, value2);
        row3.set(1, value3);

        checkEquals(row1, row2, true);
        checkEquals(row1, row3, false);
    }

    @Test
    public void testSerialization() {
        HeapRow original = new HeapRow(2);
        original.set(0, 1);
        original.set(1, new SqlCustomClass(1));

        HeapRow restored = serializeAndCheck(original, SqlDataSerializerHook.ROW_HEAP);

        checkEquals(original, restored, true);
    }
}
