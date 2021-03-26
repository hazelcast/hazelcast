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
import com.hazelcast.sql.impl.schema.ConstantTableStatistics;
import com.hazelcast.sql.impl.schema.TableField;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

class SeriesTable extends JetTable {

    private final Integer start;
    private final Integer stop;
    private final Integer step;

    SeriesTable(
            SqlConnector sqlConnector,
            List<TableField> fields,
            String schemaName,
            String name,
            Integer start,
            Integer stop,
            Integer step
    ) {
        super(sqlConnector, fields, schemaName, name, new ConstantTableStatistics(numberOfItems(start, stop, step)));

        this.start = start;
        this.stop = stop;
        this.step = step;
    }

    BatchSource<Object[]> items(Expression<Boolean> predicate, List<Expression<?>> projections) {
        if (start == null || stop == null || step == null) {
            throw QueryException.error("null arguments to GENERATE_SERIES functions");
        }
        if (step == 0) {
            throw QueryException.error("step cannot equal zero");
        }

        int start = this.start;
        int stop = this.stop;
        int step = this.step;
        return SourceBuilder
                .batch("series", ctx -> {
                    InternalSerializationService serializationService = ((ProcSupplierCtx) ctx).serializationService();
                    SimpleExpressionEvalContext context = new SimpleExpressionEvalContext(serializationService);
                    return new DataGenerator(start, stop, step, predicate, projections, context);
                })
                .fillBufferFn(DataGenerator::fillBuffer)
                .build();
    }

    long numberOfItems() {
        return numberOfItems(start, stop, step);
    }

    private static long numberOfItems(Integer start, Integer stop, Integer step) {
        // ignore bad arguments, it will be reported in items()
        if (start == null || stop == null || step == null || step == 0) {
            return 0;
        }

        if (start <= stop) {
            return step < 0 ? 0 : ((long) stop - start) / step + 1;
        } else {
            return step > 0 ? 0 : ((long) start - stop) / (-step) + 1;
        }
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
                                     .mapToObj(i -> ExpressionUtil.evaluate(predicate, projections, new Object[]{i},
                                             context))
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
    }
}
