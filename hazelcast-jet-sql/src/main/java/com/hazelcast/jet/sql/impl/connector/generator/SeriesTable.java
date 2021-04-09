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

package com.hazelcast.jet.sql.impl.connector.generator;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.jet.impl.execution.init.Contexts.ProcSupplierCtx;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.SourceBuilder.SourceBuffer;
import com.hazelcast.jet.sql.impl.ExpressionUtil;
import com.hazelcast.jet.sql.impl.SimpleExpressionEvalContext;
import com.hazelcast.jet.sql.impl.connector.SqlConnector;
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

    BatchSource<Object[]> items(Expression<Boolean> predicate, List<Expression<?>> projections) {
        List<Expression<?>> argumentExpressions = this.argumentExpressions;
        return SourceBuilder
                .batch("series", ctx -> {
                    InternalSerializationService serializationService = ((ProcSupplierCtx) ctx).serializationService();
                    SimpleExpressionEvalContext context = new SimpleExpressionEvalContext(serializationService);

                    Integer start = evaluate(argumentExpressions.get(0), null, context);
                    Integer stop = evaluate(argumentExpressions.get(1), null, context);
                    Integer step = evaluate(argumentExpressions.get(2), 1, context);
                    if (start == null || stop == null || step == null) {
                        throw QueryException.error("Invalid argument of a call to function GENERATE_SERIES" +
                                " - null argument(s)");
                    }
                    if (step == 0) {
                        throw QueryException.error("Invalid argument of a call to function GENERATE_SERIES" +
                                " - step cannot be equal to zero");
                    }

                    return new DataGenerator(start, stop, step, predicate, projections, context);
                })
                .fillBufferFn(DataGenerator::fillBuffer)
                .build();
    }

    private static Integer evaluate(
            Expression<?> argumentExpression,
            Integer defaultValue,
            ExpressionEvalContext context
    ) {
        if (argumentExpression == null) {
            return defaultValue;
        }
        Integer value = (Integer) argumentExpression.eval(EmptyRow.INSTANCE, context);
        return value == null ? defaultValue : value;
    }

    @Override
    public PlanObjectKey getObjectKey() {
        // table is always available and its field list does not change
        return null;
    }

    private static final class DataGenerator {

        private static final int MAX_BATCH_SIZE = 1024;

        private final Iterator<Object[]> iterator;

        private DataGenerator(
                int start,
                int stop,
                int step,
                Expression<Boolean> predicate,
                List<Expression<?>> projections,
                SimpleExpressionEvalContext context
        ) {
            this.iterator = IntStream.iterate(start, i -> i + step)
                    .limit(numberOfItems(start, stop, step))
                    .mapToObj(i -> ExpressionUtil.evaluate(predicate, projections, new Object[]{i}, context))
                    .filter(Objects::nonNull)
                    .iterator();
        }

        private void fillBuffer(SourceBuffer<Object[]> buffer) {
            for (int i = 0; i < MAX_BATCH_SIZE; i++) {
                if (iterator.hasNext()) {
                    buffer.add(iterator.next());
                } else {
                    buffer.close();
                    return;
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
