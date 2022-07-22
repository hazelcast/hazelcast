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

import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.util.collection.Object2LongHashMap;
import com.hazelcast.internal.util.counters.Counter;
import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.sql.impl.aggregate.WindowUtils;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.JetSqlRow;
import com.hazelcast.sql.impl.row.Row;

import javax.annotation.Nonnull;
import java.util.Map;

import static com.hazelcast.jet.impl.util.Util.logLateEvent;

/**
 * Implementation of processor which removes late items from stream.
 * While {@link LateItemsDropP#tryProcessWatermark} call captures
 * the most recent watermark, {@link LateItemsDropP#tryProcess}
 * filters each input item by its timestamp.
 * SQL engine-specific private API.
 *
 * @since 5.2
 */
public class LateItemsDropP extends AbstractProcessor {
    @Probe(name = "lateEventsDropped")
    private final Counter lateEventsDropped = SwCounter.newSwCounter();

    private final Map<Byte, Expression<?>> timestampExpression;
    private final Object2LongHashMap<Byte> currentWm = new Object2LongHashMap<>(Long.MAX_VALUE);

    private ExpressionEvalContext evalContext;

    public LateItemsDropP(Map<Byte, Expression<?>> timestampExpression) {
        this.timestampExpression = timestampExpression;
        for (Byte wmKey : timestampExpression.keySet()) {
            currentWm.put(wmKey, Long.MIN_VALUE);
        }
    }

    @Override
    protected void init(@Nonnull Context context) throws Exception {
        this.evalContext = ExpressionEvalContext.from(context);
        super.init(context);
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) {
        Row row = ((JetSqlRow) item).getRow();
        for (Map.Entry<Byte, Expression<?>> entry: timestampExpression.entrySet()) {
            long timestamp = WindowUtils.extractMillis(entry.getValue().eval(row, evalContext));
            long lastWmTimestamp = currentWm.getValue(entry.getKey());
            if (timestamp < lastWmTimestamp) {
                logLateEvent(getLogger(), entry.getKey(), lastWmTimestamp, item);
                lateEventsDropped.inc();
                return true;
            }
        }
        return tryEmit(item);
    }

    @Override
    public boolean tryProcessWatermark(@Nonnull Watermark watermark) {
        currentWm.replace((Byte) watermark.key(), watermark.timestamp());
        return super.tryProcessWatermark(watermark);
    }
}
