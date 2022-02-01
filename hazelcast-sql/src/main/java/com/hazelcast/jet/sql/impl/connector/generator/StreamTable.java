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

package com.hazelcast.jet.sql.impl.connector.generator;

import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.SourceBuilder.SourceBuffer;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.sql.impl.ExpressionUtil;
import com.hazelcast.jet.sql.impl.connector.SqlConnector;
import com.hazelcast.sql.impl.row.JetSqlRow;
import com.hazelcast.jet.sql.impl.schema.JetTable;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.optimizer.PlanObjectKey;
import com.hazelcast.sql.impl.row.EmptyRow;
import com.hazelcast.sql.impl.schema.ConstantTableStatistics;
import com.hazelcast.sql.impl.schema.TableField;

import java.util.List;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

class StreamTable extends JetTable {

    private final List<Expression<?>> argumentExpressions;

    StreamTable(
            SqlConnector sqlConnector,
            List<TableField> fields,
            String schemaName,
            String name,
            List<Expression<?>> argumentExpressions
    ) {
        super(sqlConnector, fields, schemaName, name, new ConstantTableStatistics(Integer.MAX_VALUE));

        this.argumentExpressions = argumentExpressions;
    }

    StreamSource<JetSqlRow> items(Expression<Boolean> predicate, List<Expression<?>> projections) {
        List<Expression<?>> argumentExpressions = this.argumentExpressions;
        return SourceBuilder
                .stream("stream", ctx -> {
                    ExpressionEvalContext evalContext = ExpressionEvalContext.from(ctx);

                    Integer rate = evaluate(argumentExpressions.get(0), evalContext);
                    if (rate == null) {
                        throw QueryException.error("Invalid argument of a call to function GENERATE_STREAM" +
                                " - rate cannot be null");
                    }
                    if (rate < 0) {
                        throw QueryException.error("Invalid argument of a call to function GENERATE_STREAM" +
                                " - rate cannot be less than zero");
                    }

                    return new DataGenerator(rate, predicate, projections, evalContext);
                })
                .fillBufferFn(DataGenerator::fillBuffer)
                .build();
    }

    private static Integer evaluate(Expression<?> argumentExpression, ExpressionEvalContext evalContext) {
        if (argumentExpression == null) {
            return null;
        }
        return (Integer) argumentExpression.eval(EmptyRow.INSTANCE, evalContext);
    }

    @Override
    public PlanObjectKey getObjectKey() {
        // table is always available and its field list does not change
        return null;
    }

    private static final class DataGenerator {

        private static final int MAX_BATCH_SIZE = 1024;
        private static final long NANOS_PER_MICRO = MICROSECONDS.toNanos(1);
        private static final long MICROS_PER_SECOND = SECONDS.toMicros(1);

        private final long startTime;
        private final int rate;
        private final Expression<Boolean> predicate;
        private final List<Expression<?>> projections;
        private final ExpressionEvalContext evalContext;

        private long sequence;

        private DataGenerator(
                int rate,
                Expression<Boolean> predicate,
                List<Expression<?>> projections,
                ExpressionEvalContext evalContext
        ) {
            this.startTime = System.nanoTime();
            this.rate = rate;
            this.predicate = predicate;
            this.projections = projections;
            this.evalContext = evalContext;
        }

        private void fillBuffer(SourceBuffer<JetSqlRow> buffer) {
            long now = System.nanoTime();
            long emitValuesUpTo = (now - startTime) / NANOS_PER_MICRO * rate / MICROS_PER_SECOND;
            for (int i = 0; i < MAX_BATCH_SIZE && sequence < emitValuesUpTo; i++) {
                JetSqlRow row = ExpressionUtil.evaluate(predicate, projections,
                                new JetSqlRow(evalContext.getSerializationService(), new Object[]{sequence}), evalContext);
                if (row != null) {
                    buffer.add(row);
                }
                sequence++;
            }
        }
    }
}
