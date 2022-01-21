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

package com.hazelcast.jet.sql.impl.connector.map.index;

import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.jet.sql.impl.opt.OptimizerTestSupport;
import com.hazelcast.jet.sql.impl.opt.logical.FullScanLogicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.FullScanPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.IndexScanMapPhysicalRel;
import com.hazelcast.jet.sql.impl.schema.HazelcastTable;
import com.hazelcast.jet.sql.impl.support.expressions.ExpressionType;
import com.hazelcast.jet.sql.impl.support.expressions.ExpressionTypes;
import com.hazelcast.jet.sql.impl.support.expressions.ExpressionValue;
import com.hazelcast.sql.SqlStatement;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.map.MapTableField;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static com.hazelcast.sql.impl.schema.map.MapTableUtils.getPartitionedMapIndexes;
import static java.util.Arrays.asList;

/**
 * Make sure that CAST expressions are unwrapped properly.
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class SqlIndexCastTest extends OptimizerTestSupport {

    private static final String MAP_NAME = "map";

    @BeforeClass
    public static void beforeClass() {
        initialize(1, smallInstanceConfig());
    }

    @Before
    public void before() {
        IndexConfig indexConfig = new IndexConfig()
                .setName("index")
                .setType(IndexType.HASH)
                .addAttribute("field1");

        instance().getMap(MAP_NAME).addIndex(indexConfig);
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
        // Put value into map.
        Map map = instance().getMap(MAP_NAME);

        Class<? extends ExpressionValue> valueClass = ExpressionValue.createClass(typeFrom);
        ExpressionValue value = ExpressionValue.create(valueClass).field1(typeFrom.valueFrom());
        map.put(0, value);

        checkComparison(typeFrom, typeTo, expectedIndexUsage);
        checkIsNull(typeFrom, typeTo, expectedIndexUsage);
    }

    private void checkComparison(ExpressionType<?> typeFrom, QueryDataTypeFamily typeTo, boolean expectedIndexUsage) {
        String sql = "SELECT field1 FROM " + MAP_NAME + " WHERE CAST(field1 AS " + typeTo + ") = CAST(? as " + typeTo + ")";
        SqlStatement statement = new SqlStatement(sql).addParameter(null);
        checkIndexUsage(statement, typeFrom, expectedIndexUsage);
    }

    private void checkIsNull(ExpressionType<?> typeFrom, QueryDataTypeFamily typeTo, boolean expectedIndexUsage) {
        String sql = "SELECT field1 FROM " + MAP_NAME + " WHERE CAST(field1 AS " + typeTo + ") IS NULL";
        checkIndexUsage(new SqlStatement(sql), typeFrom, expectedIndexUsage);
    }

    private void checkIndexUsage(SqlStatement statement, ExpressionType<?> field, boolean expectedIndexUsage) {
        List<QueryDataType> parameterTypes = asList(QueryDataType.INT, field.getFieldConverterType());
        List<TableField> mapTableFields = asList(
                new MapTableField("__key", QueryDataType.INT, false, QueryPath.KEY_PATH),
                new MapTableField("field1", field.getFieldConverterType(), false, new QueryPath("field1", false))
        );
        HazelcastTable table = partitionedTable(
                MAP_NAME,
                mapTableFields,
                getPartitionedMapIndexes(mapContainer(instance().getMap(MAP_NAME)), mapTableFields),
                1
        );
        Result optimizationResult = optimizePhysical(statement.getSql(), parameterTypes, table);
        assertPlan(
                optimizationResult.getLogical(),
                plan(planRow(0, FullScanLogicalRel.class))
        );
        assertPlan(
                optimizationResult.getPhysical(),
                plan(planRow(0, expectedIndexUsage ? IndexScanMapPhysicalRel.class : FullScanPhysicalRel.class))
        );
    }
}
