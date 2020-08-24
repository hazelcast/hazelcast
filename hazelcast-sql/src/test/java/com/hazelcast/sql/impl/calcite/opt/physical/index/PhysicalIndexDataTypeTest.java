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

package com.hazelcast.sql.impl.calcite.opt.physical.index;

import com.hazelcast.config.IndexType;
import com.hazelcast.sql.impl.calcite.opt.OptimizerTestSupport;
import com.hazelcast.sql.impl.calcite.opt.PlanRows;
import com.hazelcast.sql.impl.calcite.opt.physical.MapIndexScanPhysicalRel;
import com.hazelcast.sql.impl.calcite.schema.HazelcastSchema;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTable;
import com.hazelcast.sql.impl.schema.map.MapTableIndex;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.Table;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.hazelcast.sql.impl.type.QueryDataType.BIGINT;
import static com.hazelcast.sql.impl.type.QueryDataType.BOOLEAN;
import static com.hazelcast.sql.impl.type.QueryDataType.DECIMAL;
import static com.hazelcast.sql.impl.type.QueryDataType.DECIMAL_BIG_INTEGER;
import static com.hazelcast.sql.impl.type.QueryDataType.DOUBLE;
import static com.hazelcast.sql.impl.type.QueryDataType.INT;
import static com.hazelcast.sql.impl.type.QueryDataType.OBJECT;
import static com.hazelcast.sql.impl.type.QueryDataType.REAL;
import static com.hazelcast.sql.impl.type.QueryDataType.SMALLINT;
import static com.hazelcast.sql.impl.type.QueryDataType.TINYINT;
import static com.hazelcast.sql.impl.type.QueryDataType.VARCHAR;
import static com.hazelcast.sql.impl.type.QueryDataType.VARCHAR_CHARACTER;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Tests for different column types
 */
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PhysicalIndexDataTypeTest extends IndexOptimizerTestSupport {

    private static final String FIRST_INDEX = "index";

    @Parameterized.Parameter
    public IndexType indexType;

    @Parameterized.Parameter(1)
    public boolean hd;

    @Parameterized.Parameters(name = "indexType:{0}, hd:{1}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
            {IndexType.SORTED, true},
            {IndexType.SORTED, false},
            {IndexType.HASH, true},
            {IndexType.HASH, false}
        });
    }

    @Override
    protected HazelcastSchema createDefaultSchema() {
        Map<String, Table> tableMap = new HashMap<>();

        HazelcastTable pTable = OptimizerTestSupport.partitionedTable(
            "p",
            OptimizerTestSupport.fields(
                "ret", INT,
                "f_boolean", BOOLEAN,
                "f_tinyint", TINYINT,
                "f_smallint", SMALLINT,
                "f_int", INT,
                "f_bigint", BIGINT,
                "f_decimal", DECIMAL,
                "f_decimal_bigint", DECIMAL_BIG_INTEGER,
                "f_real", REAL,
                "f_double", DOUBLE,
                "f_varchar", VARCHAR,
                "f_varchar_char", VARCHAR_CHARACTER,
                "f_object", OBJECT,
                "f_composite", VARCHAR
            ),
            Arrays.asList(
                new MapTableIndex(FIRST_INDEX, indexType, 1, singletonList(0), singletonList(INT)),
                createIndex("index_boolean", 1, BOOLEAN),
                createIndex("index_tinyint", 2, TINYINT),
                createIndex("index_smallint", 3, SMALLINT),
                createIndex("index_int", 4, INT),
                createIndex("index_bigint", 5, BIGINT),
                createIndex("index_decimal", 6, DECIMAL),
                createIndex("index_decimal_bigint", 7, DECIMAL_BIG_INTEGER),
                createIndex("index_real", 8, REAL),
                createIndex("index_double", 9, DOUBLE),
                createIndex("index_varchar", 10, VARCHAR),
                createIndex("index_varchar_char", 11, VARCHAR_CHARACTER),
                createIndex("index_object", 12, OBJECT)
            ),
            100,
            hd
        );

        tableMap.put("p", pTable);

        return new HazelcastSchema(tableMap);
    }

    private MapTableIndex createIndex(String indexName, int columnIndex, QueryDataType columnType) {
        List<Integer> fieldOrdinals = Collections.singletonList(columnIndex);
        List<QueryDataType> fieldConverterTypes = Collections.singletonList(columnType);

        return new MapTableIndex(indexName, indexType, 1, fieldOrdinals, fieldConverterTypes);
    }

    @Test
    public void test_boolean() {
        checkIndexForCondition("f_boolean", "index_boolean", "$1");
        checkIndexForCondition("f_boolean=TRUE", "index_boolean", "$1");

        checkIndexForCondition("f_boolean IS TRUE", "index_boolean", "$1");
        checkIndexForCondition("f_boolean IS FALSE", "index_boolean", "NOT($1)");
        checkIndexForCondition("f_boolean IS NOT TRUE", "index_boolean", "IS NOT TRUE($1)");
        checkIndexForCondition("f_boolean IS NOT FALSE", "index_boolean", "IS NOT FALSE($1)");

        checkIndexForCondition("f_boolean IS NULL", "index_boolean", "IS NULL($1)");
    }

    @Test
    public void test_tinyint() {
        checkNumericBasic("f_tinyint", 2, "index_tinyint", TINYINT);

        checkIndexForCondition("CAST(f_tinyint AS SMALLINT)=1", "index_tinyint", "=(CAST($2):SMALLINT(7), 1)");
        checkIndexForCondition("CAST(f_tinyint AS INT)=1", "index_tinyint", "=(CAST($2):INTEGER(7), 1)");
        checkIndexForCondition("CAST(f_tinyint AS BIGINT)=1", "index_tinyint", "=(CAST($2):BIGINT(7), 1)");
        checkIndexForCondition("CAST(f_tinyint AS DECIMAL)=1", "index_tinyint", "=(CAST($2):DECIMAL(38, 38), 1)");
        checkIndexForCondition("CAST(f_tinyint AS REAL)=1", "index_tinyint", "=(CAST($2):REAL, 1E0)");
        checkIndexForCondition("CAST(f_tinyint AS DOUBLE)=1", "index_tinyint", "=(CAST($2):DOUBLE, 1E0)");

        checkNoIndexForCondition("CAST(f_tinyint AS VARCHAR) IS NULL");
        checkNoIndexForCondition("CAST(f_tinyint AS VARCHAR)='1'");
    }

    @Test
    public void test_smallint() {
        checkNumericBasic("f_smallint", 3, "index_smallint", SMALLINT);

        checkNoIndexForCondition("CAST(f_smallint AS TINYINT)=1");
        checkIndexForCondition("CAST(f_smallint AS INT)=1", "index_smallint", "=(CAST($3):INTEGER(15), 1)");
        checkIndexForCondition("CAST(f_smallint AS BIGINT)=1", "index_smallint", "=(CAST($3):BIGINT(15), 1)");
        checkIndexForCondition("CAST(f_smallint AS DECIMAL)=1", "index_smallint", "=(CAST($3):DECIMAL(38, 38), 1)");
        checkIndexForCondition("CAST(f_smallint AS REAL)=1", "index_smallint", "=(CAST($3):REAL, 1E0)");
        checkIndexForCondition("CAST(f_smallint AS DOUBLE)=1", "index_smallint", "=(CAST($3):DOUBLE, 1E0)");

        checkNoIndexForCondition("CAST(f_smallint AS VARCHAR) IS NULL");
        checkNoIndexForCondition("CAST(f_smallint AS VARCHAR)='1'");
    }

    @Test
    public void test_int() {
        checkNumericBasic("f_int", 4, "index_int", INT);

        checkNoIndexForCondition("CAST(f_int AS TINYINT)=1");
        checkNoIndexForCondition("CAST(f_int AS SMALLINT)=1");
        checkIndexForCondition("CAST(f_int AS BIGINT)=1", "index_int", "=(CAST($4):BIGINT(31), 1)");
        checkIndexForCondition("CAST(f_int AS DECIMAL)=1", "index_int", "=(CAST($4):DECIMAL(38, 38), 1)");
        checkIndexForCondition("CAST(f_int AS REAL)=1", "index_int", "=(CAST($4):REAL, 1E0)");
        checkIndexForCondition("CAST(f_int AS DOUBLE)=1", "index_int", "=(CAST($4):DOUBLE, 1E0)");

        checkNoIndexForCondition("CAST(f_int AS VARCHAR) IS NULL");
        checkNoIndexForCondition("CAST(f_int AS VARCHAR)='1'");
    }

    @Test
    public void test_bigint() {
        checkNumericBasic("f_bigint", 5, "index_bigint", BIGINT);

        checkNoIndexForCondition("CAST(f_bigint AS TINYINT)=1");
        checkNoIndexForCondition("CAST(f_bigint AS SMALLINT)=1");
        checkNoIndexForCondition("CAST(f_bigint AS INT)=1");
        checkIndexForCondition("CAST(f_bigint AS DECIMAL)=1", "index_bigint", "=(CAST($5):DECIMAL(38, 38), 1)");
        checkIndexForCondition("CAST(f_bigint AS REAL)=1", "index_bigint", "=(CAST($5):REAL, 1E0)");
        checkIndexForCondition("CAST(f_bigint AS DOUBLE)=1", "index_bigint", "=(CAST($5):DOUBLE, 1E0)");

        checkNoIndexForCondition("CAST(f_bigint AS VARCHAR) IS NULL");
        checkNoIndexForCondition("CAST(f_bigint AS VARCHAR)='1'");
    }

    @Test
    public void test_decimal() {
        checkNumericBasic("f_decimal", 6, "index_decimal", DECIMAL);
        checkNumericBasic("f_decimal_bigint", 7, "index_decimal_bigint", DECIMAL_BIG_INTEGER);

        checkNoIndexForCondition("CAST(f_decimal AS TINYINT)=1");
        checkNoIndexForCondition("CAST(f_decimal AS SMALLINT)=1");
        checkNoIndexForCondition("CAST(f_decimal AS INT)=1");
        checkNoIndexForCondition("CAST(f_decimal AS BIGINT)=1");
        checkIndexForCondition("CAST(f_decimal AS REAL)=1", "index_decimal", "=(CAST($6):REAL, 1E0)");
        checkIndexForCondition("CAST(f_decimal AS DOUBLE)=1", "index_decimal", "=(CAST($6):DOUBLE, 1E0)");

        checkNoIndexForCondition("CAST(f_decimal AS VARCHAR) IS NULL");
        checkNoIndexForCondition("CAST(f_decimal AS VARCHAR)='1'");
    }

    @Test
    public void test_real() {
        checkNumericBasic("f_real", 8, "index_real", REAL);

        if (indexType == IndexType.SORTED) {
            checkIndexForCondition("f_real>1 AND f_real<3", "index_real", "AND(<($8, 3E0), >($8, 1E0))");
        }

        checkNoIndexForCondition("CAST(f_real AS TINYINT)=1");
        checkNoIndexForCondition("CAST(f_real AS SMALLINT)=1");
        checkNoIndexForCondition("CAST(f_real AS INT)=1");
        checkNoIndexForCondition("CAST(f_real AS BIGINT)=1");
        checkNoIndexForCondition("CAST(f_real AS DECIMAL)=1");
        checkIndexForCondition("CAST(f_real AS DOUBLE)=1", "index_real", "=(CAST($8):DOUBLE, 1E0)");

        checkNoIndexForCondition("CAST(f_real AS VARCHAR) IS NULL");
        checkNoIndexForCondition("CAST(f_real AS VARCHAR)='1'");
    }

    @Test
    public void test_double() {
        checkNumericBasic("f_double", 9, "index_double", DOUBLE);

        if (indexType == IndexType.SORTED) {
            checkIndexForCondition("f_double>1 AND f_double<3", "index_double", "AND(<($9, 3E0), >($9, 1E0))");
        }

        checkNoIndexForCondition("CAST(f_double AS TINYINT)=1");
        checkNoIndexForCondition("CAST(f_double AS SMALLINT)=1");
        checkNoIndexForCondition("CAST(f_double AS INT)=1");
        checkNoIndexForCondition("CAST(f_double AS BIGINT)=1");
        checkNoIndexForCondition("CAST(f_double AS DECIMAL)=1");
        checkNoIndexForCondition("CAST(f_double AS REAL)=1");

        checkNoIndexForCondition("CAST(f_double AS VARCHAR) IS NULL");
        checkNoIndexForCondition("CAST(f_double AS VARCHAR)='1'");
    }

    private void checkNumericBasic(String columnName, int columnIndex, String indexName, QueryDataType type) {
        String literal1 = type == REAL || type == DOUBLE ? "1E0" : "1";
        String literal2 = type == REAL || type == DOUBLE ? "3E0" : "3";

        // Equality
        checkIndexForConditionNumeric("{col}=1", columnName, "=(${col}, " + literal1 + ")", Integer.toString(columnIndex), indexName);

        // IN
        checkIndexForConditionNumeric("{col}=1 OR {col}=3", columnName, "OR(=(${col}, " + literal1 + "), =(${col}, " + literal2 + "))", Integer.toString(columnIndex), indexName);

        // Open range
        if (indexType == IndexType.SORTED) {
            checkIndexForConditionNumeric("{col}>1", columnName, ">(${col}, " + literal1 + ")", Integer.toString(columnIndex), indexName);
        }

        // Closed range
        if (indexType == IndexType.SORTED && type != REAL && type != DOUBLE) {
            // Operands order is changed for REAL in the Calcite, so check it separately
            checkIndexForConditionNumeric("{col}>1 AND {col}<3", columnName, "AND(>(${col}, " + literal1 + "), <(${col}, " + literal2 + "))", Integer.toString(columnIndex), indexName);
        }

        // IS NULL
        checkIndexForConditionNumeric("{col} IS NULL", columnName, "IS NULL(${col})", Integer.toString(columnIndex), indexName);
    }

    private void checkIndexForConditionNumeric(
        String conditionTemplate,
        String columnName,
        String expectedFilterTemplate,
        String expectedFilterColumn,
        String expectedIndexName,
        QueryDataType... parameterTypes
    ) {
        String condition = conditionTemplate.replace("{col}", columnName);
        String expectedFilter = expectedFilterTemplate.replace("{col}", expectedFilterColumn);

        checkIndexForCondition(condition, expectedIndexName, expectedFilter, parameterTypes);
    }

    @Test
    public void test_varchar() {
        checkIndexForCondition("f_varchar='1'", "index_varchar", "=($10, _UTF-16LE'1')");
        checkIndexForCondition("f_varchar='1' OR f_varchar='3'", "index_varchar", "OR(=($10, _UTF-16LE'1'), =($10, _UTF-16LE'3'))");
        checkIndexForCondition("f_varchar IS NULL", "index_varchar", "IS NULL($10)");

        checkIndexForCondition("f_varchar_char='1'", "index_varchar_char", "=($11, _UTF-16LE'1')");
        checkIndexForCondition("f_varchar_char='1' OR f_varchar_char='3'", "index_varchar_char", "OR(=($11, _UTF-16LE'1'), =($11, _UTF-16LE'3'))");
        checkIndexForCondition("f_varchar_char IS NULL", "index_varchar_char", "IS NULL($11)");

        if (indexType == IndexType.SORTED) {
            checkIndexForCondition("f_varchar>'1'", "index_varchar", ">($10, _UTF-16LE'1')");
            checkIndexForCondition("f_varchar>'1' AND f_varchar<'3'", "index_varchar", "AND(>($10, _UTF-16LE'1'), <($10, _UTF-16LE'3'))");

            checkIndexForCondition("f_varchar_char>'1'", "index_varchar_char", ">($11, _UTF-16LE'1')");
            checkIndexForCondition("f_varchar_char>'1' AND f_varchar_char<'3'", "index_varchar_char", "AND(>($11, _UTF-16LE'1'), <($11, _UTF-16LE'3'))");
        }

        checkNoIndexForCondition("CAST(f_varchar AS TINYINT)=1");
        checkNoIndexForCondition("CAST(f_varchar AS SMALLINT)=1");
        checkNoIndexForCondition("CAST(f_varchar AS INT)=1");
        checkNoIndexForCondition("CAST(f_varchar AS BIGINT)=1");
        checkNoIndexForCondition("CAST(f_varchar AS DECIMAL)=1");
        checkNoIndexForCondition("CAST(f_varchar AS REAL)=1");
        checkNoIndexForCondition("CAST(f_varchar AS DOUBLE)=1");
    }

    @Test
    public void test_object() {
        checkIndexForCondition("f_object IS NULL", "index_object", "IS NULL($12)");
    }

    private void checkIndexForCondition(
        String condition,
        String indexName,
        String expectedIndexFilter,
        QueryDataType... parameterTypes
    ) {
        String sql = "SELECT ret FROM p WHERE " + condition;

        checkIndex(sql, indexName, expectedIndexFilter, "null", parameterTypes);
    }

    private void checkNoIndexForCondition(String condition, QueryDataType... parameterTypes) {
        String sql = "SELECT ret FROM p WHERE " + condition;

        if (hd) {
            RelNode rel = optimizePhysical(sql, parameterTypes);
            PlanRows plan = plan(rel);

            assertEquals(2, plan.getRowCount());
            assertEquals(MapIndexScanPhysicalRel.class.getSimpleName(), plan.getRow(1).getNode());

            MapIndexScanPhysicalRel indexScan = (MapIndexScanPhysicalRel) rel.getInput(0);
            assertEquals(FIRST_INDEX, indexScan.getIndex().getName());
            assertNull(indexScan.getIndexFilter());
        } else {
            checkNoIndex(sql, parameterTypes);
        }
    }
}
