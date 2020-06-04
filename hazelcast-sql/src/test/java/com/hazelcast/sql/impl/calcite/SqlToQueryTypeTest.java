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

package com.hazelcast.sql.impl.calcite;

import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertSame;

/**
 * Tests for type mapping between Hazelcast and Apache Calcite.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlToQueryTypeTest {
    @Test
    public void testHazelcastToCalcite() {
        assertSame(SqlTypeName.VARCHAR, SqlToQueryType.map(QueryDataTypeFamily.VARCHAR));

        assertSame(SqlTypeName.BOOLEAN, SqlToQueryType.map(QueryDataTypeFamily.BOOLEAN));

        assertSame(SqlTypeName.TINYINT, SqlToQueryType.map(QueryDataTypeFamily.TINYINT));
        assertSame(SqlTypeName.SMALLINT, SqlToQueryType.map(QueryDataTypeFamily.SMALLINT));
        assertSame(SqlTypeName.INTEGER, SqlToQueryType.map(QueryDataTypeFamily.INT));
        assertSame(SqlTypeName.BIGINT, SqlToQueryType.map(QueryDataTypeFamily.BIGINT));

        assertSame(SqlTypeName.DECIMAL, SqlToQueryType.map(QueryDataTypeFamily.DECIMAL));

        assertSame(SqlTypeName.REAL, SqlToQueryType.map(QueryDataTypeFamily.REAL));
        assertSame(SqlTypeName.DOUBLE, SqlToQueryType.map(QueryDataTypeFamily.DOUBLE));

        assertSame(SqlTypeName.DATE, SqlToQueryType.map(QueryDataTypeFamily.DATE));
        assertSame(SqlTypeName.TIME, SqlToQueryType.map(QueryDataTypeFamily.TIME));
        assertSame(SqlTypeName.TIMESTAMP, SqlToQueryType.map(QueryDataTypeFamily.TIMESTAMP));
        assertSame(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, SqlToQueryType.map(QueryDataTypeFamily.TIMESTAMP_WITH_TIME_ZONE));
    }

    @Test
    public void testCalciteToHazelcast() {
        assertSame(QueryDataType.VARCHAR, SqlToQueryType.map(SqlTypeName.VARCHAR));

        assertSame(QueryDataType.BOOLEAN, SqlToQueryType.map(SqlTypeName.BOOLEAN));

        assertSame(QueryDataType.TINYINT, SqlToQueryType.map(SqlTypeName.TINYINT));
        assertSame(QueryDataType.SMALLINT, SqlToQueryType.map(SqlTypeName.SMALLINT));
        assertSame(QueryDataType.INT, SqlToQueryType.map(SqlTypeName.INTEGER));
        assertSame(QueryDataType.BIGINT, SqlToQueryType.map(SqlTypeName.BIGINT));

        assertSame(QueryDataType.DECIMAL, SqlToQueryType.map(SqlTypeName.DECIMAL));

        assertSame(QueryDataType.REAL, SqlToQueryType.map(SqlTypeName.REAL));
        assertSame(QueryDataType.DOUBLE, SqlToQueryType.map(SqlTypeName.DOUBLE));

        assertSame(QueryDataType.DATE, SqlToQueryType.map(SqlTypeName.DATE));
        assertSame(QueryDataType.TIME, SqlToQueryType.map(SqlTypeName.TIME));
        assertSame(QueryDataType.TIMESTAMP, SqlToQueryType.map(SqlTypeName.TIMESTAMP));
        assertSame(QueryDataType.TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME, SqlToQueryType.map(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE));
    }
}
