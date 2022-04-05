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

import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.test.TestSupport;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.sql.impl.expression.ColumnExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.jet.TestContextSupport.adaptSupplier;
import static com.hazelcast.sql.impl.expression.ExpressionEvalContext.SQL_ARGUMENTS_KEY_NAME;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;

@Category({QuickTest.class, ParallelJVMTest.class})
@RunWith(HazelcastSerialClassRunner.class)
public class LateItemsDropPTest extends SqlTestSupport {
    private static final Expression<?> timestampEx = ColumnExpression.create(0, QueryDataType.BIGINT);

    @BeforeClass
    public static void beforeClass() {
        initialize(1, null);
    }

    @Test
    public void when_noEventIsLate_then_successful() {
        SupplierEx<Processor> supplier = () -> new LateItemsDropP(timestampEx);

        TestSupport.verifyProcessor(adaptSupplier(ProcessorSupplier.of(supplier)))
                .hazelcastInstance(instance())
                .jobConfig(new JobConfig().setArgument(SQL_ARGUMENTS_KEY_NAME, emptyList()))
                .outputChecker(SqlTestSupport::compareRowLists)
                .disableSnapshots()
                .input(asList(
                        wm(0L),
                        jetRow(0L, 1L),
                        jetRow(1L, 2L),
                        jetRow(2L, 2L),
                        wm(3L)
                ))
                .expectOutput(asList(
                        wm(0L),
                        jetRow(0L, 1L),
                        jetRow(1L, 2L),
                        jetRow(2L, 2L),
                        wm(3L)
                ));
    }

    @Test
    public void when_oneEventIsLate_then_dropEvent() {
        SupplierEx<Processor> supplier = () -> new LateItemsDropP(timestampEx);

        TestSupport.verifyProcessor(adaptSupplier(ProcessorSupplier.of(supplier)))
                .hazelcastInstance(instance())
                .jobConfig(new JobConfig().setArgument(SQL_ARGUMENTS_KEY_NAME, emptyList()))
                .outputChecker(SqlTestSupport::compareRowLists)
                .disableSnapshots()
                .input(asList(
                        wm(0L),
                        jetRow(0L, 1L),
                        jetRow(1L, 2L),
                        wm(3L),
                        jetRow(1L, 2L)
                ))
                .expectOutput(asList(
                        wm(0L),
                        jetRow(0L, 1L),
                        jetRow(1L, 2L),
                        wm(3L)
                ));
    }

    @Test
    public void when_fewEventsAreLate_then_dropEvents() {
        SupplierEx<Processor> supplier = () -> new LateItemsDropP(timestampEx);

        TestSupport.verifyProcessor(adaptSupplier(ProcessorSupplier.of(supplier)))
                .hazelcastInstance(instance())
                .jobConfig(new JobConfig().setArgument(SQL_ARGUMENTS_KEY_NAME, emptyList()))
                .outputChecker(SqlTestSupport::compareRowLists)
                .disableSnapshots()
                .input(asList(
                        wm(0L),
                        jetRow(0L, 1L),
                        jetRow(1L, 2L),
                        wm(3L),
                        jetRow(2L, 2L),
                        jetRow(1L, 3L),
                        wm(5L)
                ))
                .expectOutput(asList(
                        wm(0L),
                        jetRow(0L, 1L),
                        jetRow(1L, 2L),
                        wm(3L),
                        wm(5L)
                ));
    }
}
