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

package com.hazelcast.sql;

import com.hazelcast.sql.impl.SqlRowImpl;
import com.hazelcast.sql.impl.SqlTestSupport;
import com.hazelcast.sql.impl.row.HeapRow;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collections;

import static com.hazelcast.sql.impl.QueryUtils.getColumnMetadata;
import static org.junit.Assert.assertEquals;

/**
 * Tests for row and column metadata.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlMetadataTest extends SqlTestSupport {
    @Test
    public void testColumnMetadata() {
        SqlColumnMetadata column = new SqlColumnMetadata("a", SqlColumnType.INT);

        assertEquals("a", column.getName());
        assertEquals(SqlColumnType.INT, column.getType());
        assertEquals("a INT", column.toString());

        checkEquals(column, new SqlColumnMetadata("a", SqlColumnType.INT), true);
        checkEquals(column, new SqlColumnMetadata("b", SqlColumnType.INT), false);
        checkEquals(column, new SqlColumnMetadata("a", SqlColumnType.BIGINT), false);
    }

    @Test
    public void testRow() {
        SqlColumnMetadata column0Metadata = new SqlColumnMetadata("a", SqlColumnType.INT);
        SqlColumnMetadata column1Metadata = new SqlColumnMetadata("b", SqlColumnType.VARCHAR);

        SqlRow row = new SqlRowImpl(
            new SqlRowMetadata(Arrays.asList(column0Metadata, column1Metadata)),
            HeapRow.of(1, "2")
        );

        assertEquals("[a INT=1, b VARCHAR=2]", row.toString());
    }

    @Test
    public void testRowMetadata() {
        SqlColumnMetadata column0 = new SqlColumnMetadata("a", SqlColumnType.INT);
        SqlColumnMetadata column1 = new SqlColumnMetadata("b", SqlColumnType.BIGINT);
        SqlColumnMetadata column2 = new SqlColumnMetadata("c", SqlColumnType.VARCHAR);

        SqlRowMetadata row = new SqlRowMetadata(Arrays.asList(column0, column1));

        assertEquals("[a INT, b BIGINT]", row.toString());

        assertEquals(2, row.getColumnCount());
        assertEquals(column0, row.getColumn(0));
        assertEquals(column1, row.getColumn(1));

        assertEquals(0, row.findColumn("a"));
        assertEquals(1, row.findColumn("b"));
        assertEquals(SqlRowMetadata.COLUMN_NOT_FOUND, row.findColumn("c"));

        assertThrows(IndexOutOfBoundsException.class, () -> row.getColumn(-1));
        assertThrows(IndexOutOfBoundsException.class, () -> row.getColumn(2));

        checkEquals(row, new SqlRowMetadata(Arrays.asList(column0, column1)), true);
        checkEquals(row, new SqlRowMetadata(Collections.singletonList(column0)), false);
        checkEquals(row, new SqlRowMetadata(Arrays.asList(column0, column2)), false);
        checkEquals(row, new SqlRowMetadata(Arrays.asList(column0, column1, column2)), false);
    }

    @Test
    public void testConversions() {
        String name = "a";

        assertEquals(column(name, SqlColumnType.BOOLEAN), getColumnMetadata(name, QueryDataType.BOOLEAN));
        assertEquals(column(name, SqlColumnType.TINYINT), getColumnMetadata(name, QueryDataType.TINYINT));
        assertEquals(column(name, SqlColumnType.SMALLINT), getColumnMetadata(name, QueryDataType.SMALLINT));
        assertEquals(column(name, SqlColumnType.INT), getColumnMetadata(name, QueryDataType.INT));
        assertEquals(column(name, SqlColumnType.BIGINT), getColumnMetadata(name, QueryDataType.BIGINT));
        assertEquals(column(name, SqlColumnType.DECIMAL), getColumnMetadata(name, QueryDataType.DECIMAL));
        assertEquals(column(name, SqlColumnType.DECIMAL), getColumnMetadata(name, QueryDataType.DECIMAL_BIG_INTEGER));
        assertEquals(column(name, SqlColumnType.REAL), getColumnMetadata(name, QueryDataType.REAL));
        assertEquals(column(name, SqlColumnType.DOUBLE), getColumnMetadata(name, QueryDataType.DOUBLE));

        assertEquals(column(name, SqlColumnType.VARCHAR), getColumnMetadata(name, QueryDataType.VARCHAR));
        assertEquals(column(name, SqlColumnType.VARCHAR), getColumnMetadata(name, QueryDataType.VARCHAR_CHARACTER));

        assertEquals(column(name, SqlColumnType.DATE), getColumnMetadata(name, QueryDataType.DATE));
        assertEquals(column(name, SqlColumnType.TIME), getColumnMetadata(name, QueryDataType.TIME));
        assertEquals(column(name, SqlColumnType.TIMESTAMP), getColumnMetadata(name, QueryDataType.TIMESTAMP));

        assertEquals(
            column(name, SqlColumnType.TIMESTAMP_WITH_TIME_ZONE),
            getColumnMetadata(name, QueryDataType.TIMESTAMP_WITH_TZ_DATE)
        );

        assertEquals(
            column(name, SqlColumnType.TIMESTAMP_WITH_TIME_ZONE),
            getColumnMetadata(name, QueryDataType.TIMESTAMP_WITH_TZ_CALENDAR)
        );

        assertEquals(
            column(name, SqlColumnType.TIMESTAMP_WITH_TIME_ZONE),
            getColumnMetadata(name, QueryDataType.TIMESTAMP_WITH_TZ_INSTANT)
        );

        assertEquals(
            column(name, SqlColumnType.TIMESTAMP_WITH_TIME_ZONE),
            getColumnMetadata(name, QueryDataType.TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME)
        );

        assertEquals(
            column(name, SqlColumnType.TIMESTAMP_WITH_TIME_ZONE),
            getColumnMetadata(name, QueryDataType.TIMESTAMP_WITH_TZ_ZONED_DATE_TIME)
        );
    }

    private static SqlColumnMetadata column(String name, SqlColumnType type) {
        return new SqlColumnMetadata(name, type);
    }
}
