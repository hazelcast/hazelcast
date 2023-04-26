/*
 * Copyright 2023 Hazelcast Inc.
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

package com.hazelcast.jet.sql.impl.opt.logical;

import com.hazelcast.jet.sql.impl.opt.OptimizerTestSupport;
import com.hazelcast.jet.sql.impl.schema.HazelcastTable;
import com.hazelcast.sql.impl.type.QueryDataType;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexVisitorImpl;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;

import static com.hazelcast.jet.sql.impl.opt.OptUtils.extractHazelcastTable;
import static com.hazelcast.sql.impl.extract.QueryPath.KEY;
import static com.hazelcast.sql.impl.extract.QueryPath.VALUE;
import static com.hazelcast.sql.impl.type.QueryDataType.INT;
import static com.hazelcast.sql.impl.type.QueryDataType.OBJECT;
import static com.hazelcast.sql.impl.type.QueryDataType.VARCHAR;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(JUnitParamsRunner.class)
public class LogicalUpdateTest extends OptimizerTestSupport {

    @BeforeClass
    public static void setUpClass() {
        initialize(1, null);
    }

    @Test
    public void test_requiresJob() {
        HazelcastTable table = partitionedTable("m", asList(field(KEY, INT), field(VALUE, VARCHAR)), 0);
        assertImapUpdateWithScanPlan(
                optimizeLogical("UPDATE m SET this = '2' WHERE __key = 1", true, table),
                plan(
                        planRow(0, UpdateLogicalRel.class),
                        planRow(1, FullScanLogicalRel.class)
                )
        );
    }

    @Test
    public void test_updateWithoutWhere() {
        HazelcastTable table = partitionedTable("m", asList(field(KEY, INT), field(VALUE, VARCHAR)), 10);
        LogicalRel logicalRel = optimizeLogical("UPDATE m SET this = '2'", table);
        assertImapUpdateWithScanPlan(
                logicalRel,
                plan(
                        planRow(0, UpdateLogicalRel.class),
                        planRow(1, FullScanLogicalRel.class)
                )
        );
    }

    @Test
    public void test_updateComplexKeyWithoutWhere() {
        HazelcastTable table = complexKeyTable();
        LogicalRel logicalRel = optimizeLogical("UPDATE m SET this = '2'", table);
        assertImapUpdateWithScanPlan(
                logicalRel,
                plan(
                        planRow(0, UpdateLogicalRel.class),
                        planRow(1, FullScanLogicalRel.class)
                ),
                2
        );
    }

    @Test
    public void test_updateComplexKeyWithoutWhereWithExpression() {
        HazelcastTable table = complexKeyTable();
        LogicalRel logicalRel = optimizeLogical("UPDATE m SET this = TO_CHAR(k_field2, '9')", table);
        assertImapUpdateWithScanPlan(
                logicalRel,
                plan(
                        planRow(0, UpdateLogicalRel.class),
                        planRow(1, FullScanLogicalRel.class)
                ),
                2
        );

        boolean[] inputRefFound = new boolean[1];
        ((UpdateLogicalRel) logicalRel).getSourceExpressionList().get(0).accept(
                new RexVisitorImpl<Object>(true) {
                    @Override
                    public Object visitInputRef(RexInputRef inputRef) {
                        // indexes are determined by IMap predefined row structure
                        assertThat(inputRef.getIndex()).isEqualTo(1);
                        // we found input ref
                        inputRefFound[0] = true;
                        return null;
                    }
                });
        assertThat(inputRefFound[0]).as("Should reference input parameter").isTrue();
    }

    @Test
    public void test_updateComplexKeyByKeyField() {
        HazelcastTable table = partitionedTable("m",
                asList(field("k_field1", INT), field("k_field2", VARCHAR), field(KEY, OBJECT), field(VALUE, VARCHAR)),
                10);
        LogicalRel logicalRel = optimizeLogical("UPDATE m SET k_field2 = '2'", table);
        assertImapUpdateWithScanPlan(
                logicalRel,
                plan(
                        planRow(0, UpdateLogicalRel.class),
                        planRow(1, FullScanLogicalRel.class)
                ),
                2
        );
    }

    @Test
    public void test_updateByValue() {
        HazelcastTable table = partitionedTable("m", asList(field(KEY, INT), field(VALUE, VARCHAR)), 10);
        assertImapUpdateWithScanPlan(
                optimizeLogical("UPDATE m SET this = '2' WHERE this = '1'", table),
                plan(
                        planRow(0, UpdateLogicalRel.class),
                        planRow(1, FullScanLogicalRel.class)
                )
        );
    }

    @Test
    public void test_updateByKeyAndValue() {
        HazelcastTable table = partitionedTable("m", asList(field(KEY, INT), field(VALUE, VARCHAR)), 10);
        assertImapUpdateWithScanPlan(
                optimizeLogical("UPDATE m SET this = '2' WHERE __key = 1 AND this = '1'", table),
                plan(
                        planRow(0, UpdateLogicalRel.class),
                        planRow(1, FullScanLogicalRel.class)
                )
        );
    }

    @Test
    public void test_updateByKeyAndKey() {
        HazelcastTable table = partitionedTable("m", asList(field(KEY, INT), field(VALUE, VARCHAR)), 10);
        assertPlan(
                optimizeLogical("UPDATE m SET this = '2' WHERE __key = 1 AND __key = 2", table),
                plan(
                        planRow(0, UpdateLogicalRel.class),
                        planRow(1, ValuesLogicalRel.class)
                )
        );
    }

    @Test
    public void test_updateByKeyOrKey() {
        HazelcastTable table = partitionedTable("m", asList(field(KEY, INT), field(VALUE, VARCHAR)), 10);
        assertImapUpdateWithScanPlan(
                optimizeLogical("UPDATE m SET this = '2' WHERE __key = 1 OR __key = 2", table),
                plan(
                        planRow(0, UpdateLogicalRel.class),
                        planRow(1, FullScanLogicalRel.class)
                )
        );
    }

    @Test
    public void test_updateWithConstantCondition() {
        HazelcastTable table = partitionedTable("m", asList(field(KEY, INT), field(VALUE, VARCHAR)), 10);
        assertImapUpdateWithScanPlan(
                optimizeLogical("UPDATE m SET this = '2' WHERE 1 = 1", table),
                plan(
                        planRow(0, UpdateLogicalRel.class),
                        planRow(1, FullScanLogicalRel.class)
                )
        );
    }

    @SuppressWarnings("unused")
    private Object[] literals() {
        return new Object[]{
                new Object[]{QueryDataType.BOOLEAN, "true"},
                new Object[]{QueryDataType.BOOLEAN, "false"},
                new Object[]{QueryDataType.TINYINT, '1'},
                new Object[]{QueryDataType.SMALLINT, '1'},
                new Object[]{QueryDataType.INT, '1'},
                new Object[]{QueryDataType.BIGINT, '1'},
                new Object[]{QueryDataType.DECIMAL, '1'},
                new Object[]{QueryDataType.REAL, '1'},
                new Object[]{QueryDataType.DOUBLE, '1'},
                new Object[]{QueryDataType.VARCHAR, "'string'"},
                new Object[]{QueryDataType.TIME, "'12:23:34'"},
                new Object[]{QueryDataType.DATE, "'2021-07-01'"},
                new Object[]{QueryDataType.TIMESTAMP, "'2021-07-01T12:23:34'"},
                new Object[]{QueryDataType.TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME, "'2021-07-01T12:23:34Z'"},
                new Object[]{QueryDataType.OBJECT, "CAST(1 AS OBJECT)"},
        };
    }

    @Test
    @Parameters(method = "literals")
    public void test_updateByKeyWithLiteral(QueryDataType type, String literalValue) {
        HazelcastTable table = partitionedTable("m", asList(field(KEY, type), field(VALUE, VARCHAR)), 1);
        assertPlan(
                optimizeLogical("UPDATE m SET this = '2' WHERE __key = " + literalValue, table),
                plan(
                        planRow(0, UpdateByKeyMapLogicalRel.class)
                )
        );
        assertPlan(
                optimizeLogical("UPDATE m SET this = '2' WHERE " + literalValue + " = __key", table),
                plan(
                        planRow(0, UpdateByKeyMapLogicalRel.class)
                )
        );
    }

    @Test
    public void test_updateByKeyWithLiteralExpression() {
        HazelcastTable table = partitionedTable("m", asList(field(KEY, INT), field(VALUE, VARCHAR)), 1);
        assertPlan(
                optimizeLogical("UPDATE m SET this = '2' WHERE __key = 1 + 1", table),
                plan(
                        planRow(0, UpdateByKeyMapLogicalRel.class)
                )
        );
    }

    @SuppressWarnings("unused")
    private Object[] types() {
        return new Object[]{
                new Object[]{QueryDataType.BOOLEAN},
                new Object[]{QueryDataType.TINYINT},
                new Object[]{QueryDataType.SMALLINT},
                new Object[]{QueryDataType.INT},
                new Object[]{QueryDataType.BIGINT},
                new Object[]{QueryDataType.DECIMAL},
                new Object[]{QueryDataType.REAL},
                new Object[]{QueryDataType.DOUBLE},
                new Object[]{QueryDataType.VARCHAR},
                new Object[]{QueryDataType.TIME},
                new Object[]{QueryDataType.DATE},
                new Object[]{QueryDataType.TIMESTAMP},
                new Object[]{QueryDataType.TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME},
                new Object[]{QueryDataType.OBJECT},
        };
    }

    @Test
    @Parameters(method = "types")
    public void test_updateByKeyWithDynamicParam(QueryDataType type) {
        HazelcastTable table = partitionedTable("m", asList(field(KEY, type), field(VALUE, VARCHAR)), 1);
        assertPlan(
                optimizeLogical("UPDATE m SET this = '2' WHERE __key = ?", table),
                plan(
                        planRow(0, UpdateByKeyMapLogicalRel.class)
                )
        );
    }

    @Test
    public void test_updateByKeyWithDynamicParamAndImplicitCastOnKey() {
        HazelcastTable table = partitionedTable("m", asList(field(KEY, INT), field(VALUE, VARCHAR)), 1);
        assertImapUpdateWithScanPlan(
                optimizeLogical("UPDATE m SET this = '2' WHERE __key = ? + 1", table),
                plan(
                        planRow(0, UpdateLogicalRel.class),
                        planRow(1, FullScanLogicalRel.class)
                )
        );
    }

    @Test
    public void test_updateByKeyWithDynamicParamExpression() {
        HazelcastTable table = partitionedTable("m", asList(field(KEY, INT), field(VALUE, VARCHAR)), 1);
        assertPlan(
                optimizeLogical("UPDATE m SET this = '2' WHERE __key = CAST(? + 1 AS INT)", table),
                plan(
                        planRow(0, UpdateByKeyMapLogicalRel.class)
                )
        );
    }

    @Nonnull
    static HazelcastTable complexKeyTable() {
        return partitionedTable("m",
                asList(field("k_field1", INT), field("k_field2", INT), field(KEY, OBJECT), field(VALUE, VARCHAR)),
                10);
    }

    static void assertImapUpdateWithScanPlan(RelNode rel, PlanRows expected) {
        assertImapUpdateWithScanPlan(rel, expected, 0);
    }

    static void assertImapUpdateWithScanPlan(RelNode rel, PlanRows expected, int keyFields) {
        assertPlan(rel, expected);

        HazelcastTable updateInputTable = extractHazelcastTable(rel.getInput(0));
        assertThatScanProjectsOnlyKey(updateInputTable, keyFields);
    }

    static void assertThatScanProjectsOnlyKey(HazelcastTable updateInputTable, int keyFields) {
        assertThat(updateInputTable.getProjects())
                .as("IMap should project only key for update")
                .hasSize(1)
                .element(0).isInstanceOfSatisfying(RexInputRef.class,
                        input -> assertThat(input.getIndex())
                                // IMap table first has fields of Key if Key is object and then __key as an object
                                .as("Should project __key")
                                .isEqualTo(keyFields));
    }
}
