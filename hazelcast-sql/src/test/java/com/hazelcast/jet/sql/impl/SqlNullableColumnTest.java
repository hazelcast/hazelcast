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

package com.hazelcast.jet.sql.impl;

import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.sql.SqlColumnMetadata;
import com.hazelcast.sql.SqlResult;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SqlNullableColumnTest extends SqlTestSupport {

    @BeforeClass
    public static void beforeClass() {
        initialize(1, null);
    }

    @Test
    public void testSelectWithNonNullSupport() {
        createMapping("map", String.class, int.class);
        instance().getMap("map").put("key", 1);

        SqlResult result = instance().getSql().execute("SELECT __key, 1 FROM map");
        List<SqlColumnMetadata> columns = result.getRowMetadata().getColumns();

        assertEquals(columns.size(), 2);
        assertTrue(columns.get(0).isNullable());
        assertFalse(columns.get(1).isNullable());
    }
}
