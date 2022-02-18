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

package com.hazelcast.sql;

import com.hazelcast.sql.impl.CoreSqlTestSupport;
import com.hazelcast.sql.impl.SqlRowImpl;
import com.hazelcast.sql.impl.row.JetSqlRow;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collections;

import static com.hazelcast.jet.core.JetTestSupport.TEST_SS;
import static com.hazelcast.sql.impl.QueryUtils.getColumnMetadata;
import static org.junit.Assert.assertEquals;

/**
 * Tests for row and column metadata.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlMetadataTest extends CoreSqlTestSupport {
    @Test
    public void testColumnMetadata() {
        SqlColumnMetadata column = new SqlColumnMetadata("a", SqlColumnType.INTEGER, true);

        assertEquals("a", column.getName());
        assertEquals(SqlColumnType.INTEGER, column.getType());
        assertEquals("a INTEGER", column.toString());

        checkEquals(column, new SqlColumnMetadata("a", SqlColumnType.INTEGER, true), true);
        checkEquals(column, new SqlColumnMetadata("b", SqlColumnType.INTEGER, true), false);
        checkEquals(column, new SqlColumnMetadata("a", SqlColumnType.BIGINT, true), false);
    }

    @Test
    public void testRow() {
        SqlColumnMetadata column0Metadata = new SqlColumnMetadata("a", SqlColumnType.INTEGER, true);
        SqlColumnMetadata column1Metadata = new SqlColumnMetadata("b", SqlColumnType.VARCHAR, true);

        SqlRow row = new SqlRowImpl(
            new SqlRowMetadata(Arrays.asList(column0Metadata, column1Metadata)),
            new JetSqlRow(TEST_SS, new Object[]{1, "2"})
        );

        assertEquals("[a INTEGER=1, b VARCHAR=2]", row.toString());
    }

    @Test
    public void testRowMetadata() {
        SqlColumnMetadata column0 = new SqlColumnMetadata("a", SqlColumnType.INTEGER, true);
        SqlColumnMetadata column1 = new SqlColumnMetadata("b", SqlColumnType.BIGINT, true);
        SqlColumnMetadata column2 = new SqlColumnMetadata("c", SqlColumnType.VARCHAR, true);

        SqlRowMetadata row = new SqlRowMetadata(Arrays.asList(column0, column1));

        assertEquals("[a INTEGER, b BIGINT]", row.toString());

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

        assertEquals(column(name, SqlColumnType.BOOLEAN, true), getColumnMetadata(name, QueryDataType.BOOLEAN, true));
        assertEquals(column(name, SqlColumnType.TINYINT, true), getColumnMetadata(name, QueryDataType.TINYINT, true));
        assertEquals(column(name, SqlColumnType.SMALLINT, true), getColumnMetadata(name, QueryDataType.SMALLINT, true));
        assertEquals(column(name, SqlColumnType.INTEGER, true), getColumnMetadata(name, QueryDataType.INT, true));
        assertEquals(column(name, SqlColumnType.BIGINT, true), getColumnMetadata(name, QueryDataType.BIGINT, true));
        assertEquals(column(name, SqlColumnType.DECIMAL, true), getColumnMetadata(name, QueryDataType.DECIMAL, true));
        assertEquals(column(name, SqlColumnType.DECIMAL, true), getColumnMetadata(name, QueryDataType.DECIMAL_BIG_INTEGER, true));
        assertEquals(column(name, SqlColumnType.REAL, true), getColumnMetadata(name, QueryDataType.REAL, true));
        assertEquals(column(name, SqlColumnType.DOUBLE, true), getColumnMetadata(name, QueryDataType.DOUBLE, true));

        assertEquals(column(name, SqlColumnType.VARCHAR, true), getColumnMetadata(name, QueryDataType.VARCHAR, true));
        assertEquals(column(name, SqlColumnType.VARCHAR, true), getColumnMetadata(name, QueryDataType.VARCHAR_CHARACTER, true));

        assertEquals(column(name, SqlColumnType.DATE, false), getColumnMetadata(name, QueryDataType.DATE, false));
        assertEquals(column(name, SqlColumnType.TIME, false), getColumnMetadata(name, QueryDataType.TIME, false));
        assertEquals(column(name, SqlColumnType.TIMESTAMP, false), getColumnMetadata(name, QueryDataType.TIMESTAMP, false));

        assertEquals(
            column(name, SqlColumnType.TIMESTAMP_WITH_TIME_ZONE, true),
            getColumnMetadata(name, QueryDataType.TIMESTAMP_WITH_TZ_DATE, true)
        );

        assertEquals(
            column(name, SqlColumnType.TIMESTAMP_WITH_TIME_ZONE, true),
            getColumnMetadata(name, QueryDataType.TIMESTAMP_WITH_TZ_CALENDAR, true)
        );

        assertEquals(
            column(name, SqlColumnType.TIMESTAMP_WITH_TIME_ZONE, true),
            getColumnMetadata(name, QueryDataType.TIMESTAMP_WITH_TZ_INSTANT, true)
        );

        assertEquals(
            column(name, SqlColumnType.TIMESTAMP_WITH_TIME_ZONE, true),
            getColumnMetadata(name, QueryDataType.TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME, true)
        );

        assertEquals(
            column(name, SqlColumnType.TIMESTAMP_WITH_TIME_ZONE, true),
            getColumnMetadata(name, QueryDataType.TIMESTAMP_WITH_TZ_ZONED_DATE_TIME, true)
        );
    }

    private static SqlColumnMetadata column(String name, SqlColumnType type, boolean nullable) {
        return new SqlColumnMetadata(name, type, nullable);
    }
}
