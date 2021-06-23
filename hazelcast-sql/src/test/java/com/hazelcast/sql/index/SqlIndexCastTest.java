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

package com.hazelcast.sql.index;

import com.hazelcast.config.Config;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlStatement;
import com.hazelcast.sql.impl.plan.node.MapIndexScanPlanNode;
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;
import com.hazelcast.sql.support.expressions.ExpressionType;
import com.hazelcast.sql.support.expressions.ExpressionTypes;
import com.hazelcast.sql.support.expressions.ExpressionValue;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * Make sure that CAST expressions are unwrapped properly.
 */
@SuppressWarnings({"unchecked", "rawtypes"})
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlIndexCastTest extends SqlIndexTestSupport {

    private static final String MAP_NAME = "map";

    private final TestHazelcastInstanceFactory FACTORY = new TestHazelcastInstanceFactory(1);
    private HazelcastInstance member;

    @Before
    public void before() {
        IndexConfig indexConfig = new IndexConfig()
                .setName("index")
                .setType(IndexType.SORTED)
                .addAttribute("field1");

        MapConfig mapConfig = new MapConfig().setName(MAP_NAME).addIndexConfig(indexConfig);

        Config config = new Config().addMapConfig(mapConfig);

        member = FACTORY.newHazelcastInstance(config);
    }

    @After
    public void after() {
        member = null;

        FACTORY.shutdownAll();
    }

    @Test
    public void test_tinyint() {
        check(ExpressionTypes.BYTE, QueryDataTypeFamily.TINYINT, true);
        check(ExpressionTypes.BYTE, QueryDataTypeFamily.SMALLINT, true);
        check(ExpressionTypes.BYTE, QueryDataTypeFamily.INTEGER, true);
        check(ExpressionTypes.BYTE, QueryDataTypeFamily.BIGINT, true);
        check(ExpressionTypes.BYTE, QueryDataTypeFamily.DECIMAL, true);
        check(ExpressionTypes.BYTE, QueryDataTypeFamily.REAL, true);
        check(ExpressionTypes.BYTE, QueryDataTypeFamily.DOUBLE, true);
    }

    @Test
    public void test_smallint() {
        check(ExpressionTypes.SHORT, QueryDataTypeFamily.TINYINT, false);
        check(ExpressionTypes.SHORT, QueryDataTypeFamily.SMALLINT, true);
        check(ExpressionTypes.SHORT, QueryDataTypeFamily.INTEGER, true);
        check(ExpressionTypes.SHORT, QueryDataTypeFamily.BIGINT, true);
        check(ExpressionTypes.SHORT, QueryDataTypeFamily.DECIMAL, true);
        check(ExpressionTypes.SHORT, QueryDataTypeFamily.REAL, true);
        check(ExpressionTypes.SHORT, QueryDataTypeFamily.DOUBLE, true);
    }

    @Test
    public void test_integer() {
        check(ExpressionTypes.INTEGER, QueryDataTypeFamily.TINYINT, false);
        check(ExpressionTypes.INTEGER, QueryDataTypeFamily.SMALLINT, false);
        check(ExpressionTypes.INTEGER, QueryDataTypeFamily.INTEGER, true);
        check(ExpressionTypes.INTEGER, QueryDataTypeFamily.BIGINT, true);
        check(ExpressionTypes.INTEGER, QueryDataTypeFamily.DECIMAL, true);
        check(ExpressionTypes.INTEGER, QueryDataTypeFamily.REAL, true);
        check(ExpressionTypes.INTEGER, QueryDataTypeFamily.DOUBLE, true);
    }

    @Test
    public void test_bigint() {
        check(ExpressionTypes.LONG, QueryDataTypeFamily.TINYINT, false);
        check(ExpressionTypes.LONG, QueryDataTypeFamily.SMALLINT, false);
        check(ExpressionTypes.LONG, QueryDataTypeFamily.INTEGER, false);
        check(ExpressionTypes.LONG, QueryDataTypeFamily.BIGINT, true);
        check(ExpressionTypes.LONG, QueryDataTypeFamily.DECIMAL, true);
        check(ExpressionTypes.LONG, QueryDataTypeFamily.REAL, true);
        check(ExpressionTypes.LONG, QueryDataTypeFamily.DOUBLE, true);
    }

    @Test
    public void test_decimal() {
        check(ExpressionTypes.BIG_DECIMAL, QueryDataTypeFamily.TINYINT, false);
        check(ExpressionTypes.BIG_DECIMAL, QueryDataTypeFamily.SMALLINT, false);
        check(ExpressionTypes.BIG_DECIMAL, QueryDataTypeFamily.INTEGER, false);
        check(ExpressionTypes.BIG_DECIMAL, QueryDataTypeFamily.BIGINT, false);
        check(ExpressionTypes.BIG_DECIMAL, QueryDataTypeFamily.DECIMAL, true);
        check(ExpressionTypes.BIG_DECIMAL, QueryDataTypeFamily.REAL, true);
        check(ExpressionTypes.BIG_DECIMAL, QueryDataTypeFamily.DOUBLE, true);
    }

    @Test
    public void test_real() {
        check(ExpressionTypes.FLOAT, QueryDataTypeFamily.TINYINT, false);
        check(ExpressionTypes.FLOAT, QueryDataTypeFamily.SMALLINT, false);
        check(ExpressionTypes.FLOAT, QueryDataTypeFamily.INTEGER, false);
        check(ExpressionTypes.FLOAT, QueryDataTypeFamily.BIGINT, false);
        check(ExpressionTypes.FLOAT, QueryDataTypeFamily.DECIMAL, false);
        check(ExpressionTypes.FLOAT, QueryDataTypeFamily.REAL, true);
        check(ExpressionTypes.FLOAT, QueryDataTypeFamily.DOUBLE, true);
    }

    @Test
    public void test_double() {
        check(ExpressionTypes.DOUBLE, QueryDataTypeFamily.TINYINT, false);
        check(ExpressionTypes.DOUBLE, QueryDataTypeFamily.SMALLINT, false);
        check(ExpressionTypes.DOUBLE, QueryDataTypeFamily.INTEGER, false);
        check(ExpressionTypes.DOUBLE, QueryDataTypeFamily.BIGINT, false);
        check(ExpressionTypes.DOUBLE, QueryDataTypeFamily.DECIMAL, false);
        check(ExpressionTypes.DOUBLE, QueryDataTypeFamily.REAL, false);
        check(ExpressionTypes.DOUBLE, QueryDataTypeFamily.DOUBLE, true);
    }

    private void check(ExpressionType<?> typeFrom, QueryDataTypeFamily typeTo, boolean expectedIndexUsage) {
        // Clear plan cache
        clearPlanCache(member);

        // Put value into map.
        Map map = member.getMap(MAP_NAME);

        Class<? extends ExpressionValue> valueClass = ExpressionValue.createClass(typeFrom);
        ExpressionValue value = ExpressionValue.create(valueClass).field1(typeFrom.valueFrom());

        map.put(0, value);

        checkComparison(typeTo, expectedIndexUsage);
        checkIsNull(typeTo, expectedIndexUsage);
    }

    private void checkComparison(QueryDataTypeFamily typeTo, boolean expectedIndexUsage) {
        String sql = "SELECT field1 FROM " + MAP_NAME + " WHERE CAST(field1 AS " + typeTo + ") = CAST(? as " + typeTo + ")";

        SqlStatement statement = new SqlStatement(sql).addParameter(null);

        checkIndexUsage(statement, expectedIndexUsage);
    }

    private void checkIsNull(QueryDataTypeFamily typeTo, boolean expectedIndexUsage) {
        String sql = "SELECT field1 FROM " + MAP_NAME + " WHERE CAST(field1 AS " + typeTo + ") IS NULL";

        SqlStatement statement = new SqlStatement(sql);

        checkIndexUsage(statement, expectedIndexUsage);
    }

    private void checkIndexUsage(SqlStatement statement, boolean expectedIndexUsage) {
        try (SqlResult result = member.getSql().execute(statement)) {
            MapIndexScanPlanNode indexNode = findFirstIndexNode(result);

            if (expectedIndexUsage) {
                assertNotNull(indexNode);
            } else {
                assertNull(indexNode);
            }
        }
    }
}
