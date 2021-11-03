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

package com.hazelcast.jet.sql.impl.validate.types;

import com.hazelcast.sql.impl.type.QueryDataType;
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
public class HazelcastTypeUtilsTest {
    @Test
    public void testHazelcastToCalcite() {
        assertSame(SqlTypeName.VARCHAR, HazelcastTypeUtils.toCalciteType(QueryDataType.VARCHAR));

        assertSame(SqlTypeName.BOOLEAN, HazelcastTypeUtils.toCalciteType(QueryDataType.BOOLEAN));

        assertSame(SqlTypeName.TINYINT, HazelcastTypeUtils.toCalciteType(QueryDataType.TINYINT));
        assertSame(SqlTypeName.SMALLINT, HazelcastTypeUtils.toCalciteType(QueryDataType.SMALLINT));
        assertSame(SqlTypeName.INTEGER, HazelcastTypeUtils.toCalciteType(QueryDataType.INT));
        assertSame(SqlTypeName.BIGINT, HazelcastTypeUtils.toCalciteType(QueryDataType.BIGINT));

        assertSame(SqlTypeName.DECIMAL, HazelcastTypeUtils.toCalciteType(QueryDataType.DECIMAL));

        assertSame(SqlTypeName.REAL, HazelcastTypeUtils.toCalciteType(QueryDataType.REAL));
        assertSame(SqlTypeName.DOUBLE, HazelcastTypeUtils.toCalciteType(QueryDataType.DOUBLE));

        assertSame(SqlTypeName.DATE, HazelcastTypeUtils.toCalciteType(QueryDataType.DATE));
        assertSame(SqlTypeName.TIME, HazelcastTypeUtils.toCalciteType(QueryDataType.TIME));
        assertSame(SqlTypeName.TIMESTAMP, HazelcastTypeUtils.toCalciteType(QueryDataType.TIMESTAMP));
        assertSame(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, HazelcastTypeUtils.toCalciteType(QueryDataType.TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME));
    }

    // TODO add/refactor into RelDataType -> QueryDataType test
    @Test
    public void testCalciteToHazelcast() {
        assertSame(QueryDataType.VARCHAR, HazelcastTypeUtils.toHazelcastTypeFromSqlTypeName(SqlTypeName.VARCHAR));

        assertSame(QueryDataType.BOOLEAN, HazelcastTypeUtils.toHazelcastTypeFromSqlTypeName(SqlTypeName.BOOLEAN));

        assertSame(QueryDataType.TINYINT, HazelcastTypeUtils.toHazelcastTypeFromSqlTypeName(SqlTypeName.TINYINT));
        assertSame(QueryDataType.SMALLINT, HazelcastTypeUtils.toHazelcastTypeFromSqlTypeName(SqlTypeName.SMALLINT));
        assertSame(QueryDataType.INT, HazelcastTypeUtils.toHazelcastTypeFromSqlTypeName(SqlTypeName.INTEGER));
        assertSame(QueryDataType.BIGINT, HazelcastTypeUtils.toHazelcastTypeFromSqlTypeName(SqlTypeName.BIGINT));

        assertSame(QueryDataType.DECIMAL, HazelcastTypeUtils.toHazelcastTypeFromSqlTypeName(SqlTypeName.DECIMAL));

        assertSame(QueryDataType.REAL, HazelcastTypeUtils.toHazelcastTypeFromSqlTypeName(SqlTypeName.REAL));
        assertSame(QueryDataType.DOUBLE, HazelcastTypeUtils.toHazelcastTypeFromSqlTypeName(SqlTypeName.DOUBLE));

        assertSame(QueryDataType.DATE, HazelcastTypeUtils.toHazelcastTypeFromSqlTypeName(SqlTypeName.DATE));
        assertSame(QueryDataType.TIME, HazelcastTypeUtils.toHazelcastTypeFromSqlTypeName(SqlTypeName.TIME));
        assertSame(QueryDataType.TIMESTAMP, HazelcastTypeUtils.toHazelcastTypeFromSqlTypeName(SqlTypeName.TIMESTAMP));
        assertSame(QueryDataType.TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME, HazelcastTypeUtils.toHazelcastTypeFromSqlTypeName(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE));
    }
}
