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

package com.hazelcast.sql.impl.row;

import com.hazelcast.sql.impl.SqlCustomClass;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.SqlTestSupport;
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
public class JoinRowTest extends SqlTestSupport {
    @Test
    public void testJoinRow() {
        HeapRow row1 = new HeapRow(2);
        HeapRow row2 = new HeapRow(3);

        row1.set(0, new Object());
        row1.set(1, new Object());
        row2.set(0, new Object());
        row2.set(1, new Object());
        row2.set(2, new Object());

        JoinRow joinRow = new JoinRow(row1, row2);

        assertEquals(5, joinRow.getColumnCount());
        assertSame(row1.get(0), joinRow.get(0));
        assertSame(row1.get(1), joinRow.get(1));
        assertSame(row2.get(0), joinRow.get(2));
        assertSame(row2.get(1), joinRow.get(3));
        assertSame(row2.get(2), joinRow.get(4));
    }

    @Test
    public void testEquals() {
        HeapRow row1 = new HeapRow(1);
        HeapRow row2 = new HeapRow(2);

        checkEquals(new JoinRow(row1, row2), new JoinRow(row1, row2), true);
        checkEquals(new JoinRow(row1, row2), new JoinRow(row2, row1), false);
    }

    @Test
    public void testSerialization() {
        HeapRow row1 = new HeapRow(2);
        HeapRow row2 = new HeapRow(2);

        row1.set(0, 1);
        row1.set(1, new SqlCustomClass(1));
        row2.set(0, 2);
        row2.set(1, new SqlCustomClass(2));

        JoinRow original = new JoinRow(row1, row2);
        JoinRow restored = serializeAndCheck(original, SqlDataSerializerHook.ROW_JOIN);

        checkEquals(original, restored, true);
    }
}
