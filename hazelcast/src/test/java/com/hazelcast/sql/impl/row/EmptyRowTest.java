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
public class EmptyRowTest extends CoreSqlTestSupport {
    @Test
    public void testEmptyRow() {
        EmptyRow row = EmptyRow.INSTANCE;

        assertEquals(0, row.getColumnCount());
    }

    @Test
    public void testSerialization() {
        EmptyRow original = EmptyRow.INSTANCE;
        Object restored = serializeAndCheck(original, SqlDataSerializerHook.ROW_EMPTY);

        assertSame(EmptyRow.class, restored.getClass());
    }
}
