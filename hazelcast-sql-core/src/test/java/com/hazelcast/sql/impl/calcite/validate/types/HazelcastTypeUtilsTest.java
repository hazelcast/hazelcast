/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.calcite.validate.types;

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

    @Test
    public void testCalciteToHazelcast() {
        assertSame(QueryDataType.VARCHAR, HazelcastTypeUtils.toHazelcastType(SqlTypeName.VARCHAR));

        assertSame(QueryDataType.BOOLEAN, HazelcastTypeUtils.toHazelcastType(SqlTypeName.BOOLEAN));

        assertSame(QueryDataType.TINYINT, HazelcastTypeUtils.toHazelcastType(SqlTypeName.TINYINT));
        assertSame(QueryDataType.SMALLINT, HazelcastTypeUtils.toHazelcastType(SqlTypeName.SMALLINT));
        assertSame(QueryDataType.INT, HazelcastTypeUtils.toHazelcastType(SqlTypeName.INTEGER));
        assertSame(QueryDataType.BIGINT, HazelcastTypeUtils.toHazelcastType(SqlTypeName.BIGINT));

        assertSame(QueryDataType.DECIMAL, HazelcastTypeUtils.toHazelcastType(SqlTypeName.DECIMAL));

        assertSame(QueryDataType.REAL, HazelcastTypeUtils.toHazelcastType(SqlTypeName.REAL));
        assertSame(QueryDataType.DOUBLE, HazelcastTypeUtils.toHazelcastType(SqlTypeName.DOUBLE));

        assertSame(QueryDataType.DATE, HazelcastTypeUtils.toHazelcastType(SqlTypeName.DATE));
        assertSame(QueryDataType.TIME, HazelcastTypeUtils.toHazelcastType(SqlTypeName.TIME));
        assertSame(QueryDataType.TIMESTAMP, HazelcastTypeUtils.toHazelcastType(SqlTypeName.TIMESTAMP));
        assertSame(QueryDataType.TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME, HazelcastTypeUtils.toHazelcastType(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE));
    }
}
