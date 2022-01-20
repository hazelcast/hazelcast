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

import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.SourceBuilder.SourceBuffer;
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

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

class SeriesTable extends JetTable {

    private final List<Expression<?>> argumentExpressions;

    SeriesTable(
            SqlConnector sqlConnector,
            List<TableField> fields,
            String schemaName,
            String name,
            List<Expression<?>> argumentExpressions
    ) {
        super(sqlConnector, fields, schemaName, name, new ConstantTableStatistics(0));

        this.argumentExpressions = argumentExpressions;
    }

    BatchSource<JetSqlRow> items(Expression<Boolean> predicate, List<Expression<?>> projections) {
        List<Expression<?>> argumentExpressions = this.argumentExpressions;
        return SourceBuilder
                .batch("series", ctx -> {
                    ExpressionEvalContext evalContext = ExpressionEvalContext.from(ctx);

                    Integer start = evaluate(argumentExpressions.get(0), null, evalContext);
                    Integer stop = evaluate(argumentExpressions.get(1), null, evalContext);
                    Integer step = evaluate(argumentExpressions.get(2), 1, evalContext);
                    if (start == null || stop == null || step == null) {
                        throw QueryException.error("Invalid argument of a call to function GENERATE_SERIES" +
                                " - null argument(s)");
                    }
                    if (step == 0) {
                        throw QueryException.error("Invalid argument of a call to function GENERATE_SERIES" +
                                " - step cannot be equal to zero");
                    }

                    return new DataGenerator(start, stop, step, predicate, projections, evalContext);
                })
                .fillBufferFn(DataGenerator::fillBuffer)
                .build();
    }

    private static Integer evaluate(
            Expression<?> argumentExpression,
            Integer defaultValue,
            ExpressionEvalContext evalContext
    ) {
        if (argumentExpression == null) {
            return defaultValue;
        }
        Integer value = (Integer) argumentExpression.eval(EmptyRow.INSTANCE, evalContext);
        return value == null ? defaultValue : value;
    }

    @Override
    public PlanObjectKey getObjectKey() {
        // table is always available and its field list does not change
        return null;
    }

    private static final class DataGenerator {

        private static final int MAX_BATCH_SIZE = 1024;

        private final Iterator<JetSqlRow> iterator;

        private DataGenerator(
                int start,
                int stop,
                int step,
                Expression<Boolean> predicate,
                List<Expression<?>> projections,
                ExpressionEvalContext evalContext
        ) {
            this.iterator = IntStream.iterate(start, i -> i + step)
                    .limit(numberOfItems(start, stop, step))
                    .mapToObj(i -> ExpressionUtil.evaluate(predicate, projections,
                            new JetSqlRow(evalContext.getSerializationService(), new Object[]{i}), evalContext))
                    .filter(Objects::nonNull)
                    .iterator();
        }

        private void fillBuffer(SourceBuffer<JetSqlRow> buffer) {
            for (int i = 0; i < MAX_BATCH_SIZE; i++) {
                if (iterator.hasNext()) {
                    buffer.add(iterator.next());
                } else {
                    buffer.close();
                }
            }
        }

        private static long numberOfItems(int start, int stop, int step) {
            if (start <= stop) {
                return step < 0 ? 0 : ((long) stop - start) / step + 1;
            } else {
                return step > 0 ? 0 : ((long) start - stop) / (-step) + 1;
            }
        }
    }
}
