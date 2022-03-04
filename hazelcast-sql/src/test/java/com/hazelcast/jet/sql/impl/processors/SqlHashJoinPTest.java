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

package com.hazelcast.jet.sql.impl.processors;

import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.test.TestSupport;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.jet.sql.impl.JetJoinInfo;
import com.hazelcast.sql.impl.row.JetSqlRow;
import com.hazelcast.sql.impl.expression.ColumnExpression;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.predicate.ComparisonMode;
import com.hazelcast.sql.impl.expression.predicate.ComparisonPredicate;
import org.apache.calcite.rel.core.JoinRelType;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static com.hazelcast.jet.TestContextSupport.adaptSupplier;
import static com.hazelcast.sql.impl.expression.ExpressionEvalContext.SQL_ARGUMENTS_KEY_NAME;
import static com.hazelcast.sql.impl.type.QueryDataType.BOOLEAN;
import static com.hazelcast.sql.impl.type.QueryDataType.INT;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.calcite.rel.core.JoinRelType.INNER;
import static org.apache.calcite.rel.core.JoinRelType.LEFT;

public class SqlHashJoinPTest extends SqlTestSupport {
    private static final int LOW_PRIORITY = 10;
    private static final int HIGH_PRIORITY = 1;

    private static final Expression<Boolean> TRUE_PREDICATE =
            (Expression<Boolean>) ConstantExpression.create(true, BOOLEAN);

    private static final Expression<Boolean> LEFT_LT_RIGHT =
            ComparisonPredicate.create(
                    ColumnExpression.create(0, INT),
                    ColumnExpression.create(2, INT),
                    ComparisonMode.LESS_THAN
            );

    private static final Expression<Boolean> LEFT_GT_RIGHT =
            ComparisonPredicate.create(
                    ColumnExpression.create(0, INT),
                    ColumnExpression.create(2, INT),
                    ComparisonMode.GREATER_THAN
            );

    @BeforeClass
    public static void beforeClass() {
        initialize(2, null);
    }

    @Test
    public void test_innerJoin() {
        runTest(INNER, TRUE_PREDICATE, 2, new int[]{0}, new int[]{0},
                asList(
                        jetRow(1, "left-1"),
                        jetRow(2, "left-2")
                ),
                asList(
                        jetRow(2, "right-2"),
                        jetRow(3, "right-3")
                ),
                singletonList(jetRow(2, "left-2", 2, "right-2"))
        );
    }

    @Test
    public void test_innerNonEquiJoin() {
        runTest(INNER, LEFT_LT_RIGHT, 2, new int[]{}, new int[]{},
                asList(
                        jetRow(1, "left-1"),
                        jetRow(2, "left-2")
                ),
                asList(
                        jetRow(2, "right-2"),
                        jetRow(3, "right-3")
                ),
                asList(
                        jetRow(1, "left-1", 2, "right-2"),
                        jetRow(1, "left-1", 3, "right-3"),
                        jetRow(2, "left-2", 3, "right-3")
                )
        );
    }

    @Test
    public void test_leftEquiJoin() {
        runTest(LEFT, TRUE_PREDICATE, 2, new int[]{0}, new int[]{0},
                asList(
                        jetRow(1, "left-1"),
                        jetRow(2, "left-2")
                ),
                asList(
                        jetRow(2, "right-2"),
                        jetRow(3, "right-3")
                ),
                asList(
                        jetRow(1, "left-1", null, null),
                        jetRow(2, "left-2", 2, "right-2")
                )
        );
    }

    @Test
    public void test_leftNonEquiJoin() {
        runTest(LEFT, LEFT_GT_RIGHT, 2, new int[]{}, new int[]{},
                asList(
                        jetRow(1, "left-1"),
                        jetRow(2, "left-2")
                ),
                asList(
                        jetRow(2, "right-2"),
                        jetRow(3, "right-3")
                ),
                asList(
                        jetRow(1, "left-1", null, null),
                        jetRow(2, "left-2", null, null)
                )
        );
    }

    private void runTest(
            JoinRelType joinType,
            Expression<Boolean> nonEquiCondition,
            int rightInputColumnCount,
            int[] leftEquiJoinIndices,
            int[] rightEquiJoinIndices,
            List<JetSqlRow> leftInput,
            List<JetSqlRow> rightInput,
            List<JetSqlRow> output
    ) {

        ProcessorSupplier processor = SqlHashJoinP.supplier(
                new JetJoinInfo(joinType, leftEquiJoinIndices, rightEquiJoinIndices, nonEquiCondition, null),
                rightInputColumnCount
        );

        TestSupport
                .verifyProcessor(adaptSupplier(processor))
                .jobConfig(new JobConfig().setArgument(SQL_ARGUMENTS_KEY_NAME, emptyList()))
                .inputs(asList(leftInput, rightInput), new int[]{LOW_PRIORITY, HIGH_PRIORITY})
                .hazelcastInstance(instance())
                .outputChecker(SqlTestSupport::compareRowLists)
                .disableSnapshots()
                .expectOutput(output);
    }
}
