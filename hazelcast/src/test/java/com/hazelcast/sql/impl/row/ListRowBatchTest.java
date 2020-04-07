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

import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.SqlTestSupport;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ListRowBatchTest extends SqlTestSupport {
    @Test
    public void testListRowBatch() {
        List<Row> rows = new ArrayList<>(2);

        rows.add(new HeapRow(1));
        rows.add(new HeapRow(2));

        ListRowBatch batch = new ListRowBatch(rows);

        assertEquals(2, batch.getRowCount());
        assertEquals(rows.get(0), batch.getRow(0));
        assertEquals(rows.get(1), batch.getRow(1));
    }

    @Test
    public void testSerialization() {
        List<Row> rows = new ArrayList<>(2);

        rows.add(new HeapRow(1));
        rows.add(new HeapRow(2));

        ListRowBatch original = new ListRowBatch(rows);
        ListRowBatch restored = serializeAndCheck(original, SqlDataSerializerHook.ROW_BATCH_LIST);

        assertEquals(original.getRowCount(), restored.getRowCount());
        assertEquals(original.getRow(0), restored.getRow(0));
        assertEquals(original.getRow(1), restored.getRow(1));
    }
}
