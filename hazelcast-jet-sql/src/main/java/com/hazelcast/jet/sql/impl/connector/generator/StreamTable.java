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

package com.hazelcast.jet.sql.impl.connector.generator;

import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.SourceBuilder.SourceBuffer;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.sql.impl.ExpressionUtil;
import com.hazelcast.jet.sql.impl.connector.SqlConnector;
import com.hazelcast.jet.sql.impl.schema.JetTable;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.schema.ConstantTableStatistics;
import com.hazelcast.sql.impl.schema.TableField;

import java.util.List;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

class StreamTable extends JetTable {

    private final int rate;

    StreamTable(
            SqlConnector sqlConnector,
            List<TableField> fields,
            String schemaName,
            String name,
            int rate
    ) {
        super(sqlConnector, fields, schemaName, name, new ConstantTableStatistics(Integer.MAX_VALUE));

        if (rate < 0) {
            throw QueryException.error("rate cannot be less than zero");
        }

        this.rate = rate;
    }

    StreamSource<Object[]> items(Expression<Boolean> predicate, List<Expression<?>> projections) {
        int rate = this.rate;
        return SourceBuilder.stream("stream", ctx -> new StreamGenerator(rate, predicate, projections))
                            .fillBufferFn(StreamGenerator::fillBuffer)
                            .build();
    }

    private static final class StreamGenerator {

        private static final int MAX_BATCH_SIZE = 1024;
        private static final long NANOS_PER_MICRO = MICROSECONDS.toNanos(1);
        private static final long MICROS_PER_SECOND = SECONDS.toMicros(1);

        private final long startTime;
        private final int rate;
        private final Expression<Boolean> predicate;
        private final List<Expression<?>> projections;

        private long sequence;

        private StreamGenerator(int rate, Expression<Boolean> predicate, List<Expression<?>> projections) {
            this.startTime = System.nanoTime();
            this.rate = rate;
            this.predicate = predicate;
            this.projections = projections;
        }

        private void fillBuffer(SourceBuffer<Object[]> buffer) {
            long now = System.nanoTime();
            long emitValuesUpTo = (now - startTime) / NANOS_PER_MICRO * rate / MICROS_PER_SECOND;
            for (int i = 0; i < MAX_BATCH_SIZE && sequence < emitValuesUpTo; i++) {
                Object[] row = ExpressionUtil.evaluate(predicate, projections, new Object[]{sequence});
                if (row != null) {
                    buffer.add(row);
                }
                sequence++;
            }
        }
    }
}
