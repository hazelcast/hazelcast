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

package com.hazelcast.sql.impl.schema.map;

import com.hazelcast.sql.impl.CoreSqlTestSupport;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapTableFieldTest extends CoreSqlTestSupport {
    @Test
    public void testContent() {
        MapTableField field = new MapTableField("name", QueryDataType.INT, false, QueryPath.KEY_PATH);

        assertEquals("name", field.getName());
        assertEquals(QueryDataType.INT, field.getType());
        assertFalse(field.isHidden());
        assertEquals(QueryPath.KEY_PATH, field.getPath());
    }

    @Test
    public void testEquals() {
        MapTableField field = new MapTableField("name1", QueryDataType.INT, false, QueryPath.KEY_PATH);

        checkEquals(field, new MapTableField("name1", QueryDataType.INT, false, QueryPath.KEY_PATH), true);

        checkEquals(field, new MapTableField("name2", QueryDataType.INT, false, QueryPath.KEY_PATH), false);
        checkEquals(field, new MapTableField("name1", QueryDataType.BIGINT, false, QueryPath.KEY_PATH), false);
        checkEquals(field, new MapTableField("name1", QueryDataType.INT, true, QueryPath.KEY_PATH), false);
        checkEquals(field, new MapTableField("name1", QueryDataType.INT, false, QueryPath.VALUE_PATH), false);
    }
}
