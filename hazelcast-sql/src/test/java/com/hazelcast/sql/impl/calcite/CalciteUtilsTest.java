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
public class CalciteUtilsTest {
    @Test
    public void testHazelcastToCalcite() {
        assertSame(SqlTypeName.VARCHAR, CalciteUtils.map(QueryDataTypeFamily.VARCHAR));

        assertSame(SqlTypeName.BOOLEAN, CalciteUtils.map(QueryDataTypeFamily.BOOLEAN));

        assertSame(SqlTypeName.TINYINT, CalciteUtils.map(QueryDataTypeFamily.TINYINT));
        assertSame(SqlTypeName.SMALLINT, CalciteUtils.map(QueryDataTypeFamily.SMALLINT));
        assertSame(SqlTypeName.INTEGER, CalciteUtils.map(QueryDataTypeFamily.INTEGER));
        assertSame(SqlTypeName.BIGINT, CalciteUtils.map(QueryDataTypeFamily.BIGINT));

        assertSame(SqlTypeName.DECIMAL, CalciteUtils.map(QueryDataTypeFamily.DECIMAL));

        assertSame(SqlTypeName.REAL, CalciteUtils.map(QueryDataTypeFamily.REAL));
        assertSame(SqlTypeName.DOUBLE, CalciteUtils.map(QueryDataTypeFamily.DOUBLE));

        assertSame(SqlTypeName.DATE, CalciteUtils.map(QueryDataTypeFamily.DATE));
        assertSame(SqlTypeName.TIME, CalciteUtils.map(QueryDataTypeFamily.TIME));
        assertSame(SqlTypeName.TIMESTAMP, CalciteUtils.map(QueryDataTypeFamily.TIMESTAMP));
        assertSame(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, CalciteUtils.map(QueryDataTypeFamily.TIMESTAMP_WITH_TIME_ZONE));
    }

    @Test
    public void testCalciteToHazelcast() {
        assertSame(QueryDataType.VARCHAR, CalciteUtils.map(SqlTypeName.VARCHAR));

        assertSame(QueryDataType.BOOLEAN, CalciteUtils.map(SqlTypeName.BOOLEAN));

        assertSame(QueryDataType.TINYINT, CalciteUtils.map(SqlTypeName.TINYINT));
        assertSame(QueryDataType.SMALLINT, CalciteUtils.map(SqlTypeName.SMALLINT));
        assertSame(QueryDataType.INT, CalciteUtils.map(SqlTypeName.INTEGER));
        assertSame(QueryDataType.BIGINT, CalciteUtils.map(SqlTypeName.BIGINT));

        assertSame(QueryDataType.DECIMAL, CalciteUtils.map(SqlTypeName.DECIMAL));

        assertSame(QueryDataType.REAL, CalciteUtils.map(SqlTypeName.REAL));
        assertSame(QueryDataType.DOUBLE, CalciteUtils.map(SqlTypeName.DOUBLE));

        assertSame(QueryDataType.DATE, CalciteUtils.map(SqlTypeName.DATE));
        assertSame(QueryDataType.TIME, CalciteUtils.map(SqlTypeName.TIME));
        assertSame(QueryDataType.TIMESTAMP, CalciteUtils.map(SqlTypeName.TIMESTAMP));
        assertSame(QueryDataType.TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME, CalciteUtils.map(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE));
    }
}
