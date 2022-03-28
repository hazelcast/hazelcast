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
import com.hazelcast.internal.util.counters.Counter;
import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Watermark;

import javax.annotation.Nonnull;
import java.util.function.ToLongFunction;

import static com.hazelcast.jet.impl.util.Util.logLateEvent;

/**
 * Implementation of processor which removes late items from stream.
 * While {@link LateItemsDropP#tryProcessWatermark} call captures
 * the most recent watermark, {@link LateItemsDropP#tryProcess}
 * filters each input item by its timestamp.
 * SQL engine-specific private API.
 *
 * @param <T> processed item
 * @since 5.2
 */
public class LateItemsDropP<T> extends AbstractProcessor {
    @Probe(name = "lateEventsDropped")
    private final Counter lateEventsDropped = SwCounter.newSwCounter();

    private final ToLongFunction<? super T> timestampFn;
    private final long allowedLag;

    private long currentWm = Long.MIN_VALUE;

    public LateItemsDropP(ToLongFunction<? super T> timestampFn, long allowedLag) {
        this.timestampFn = timestampFn;
        this.allowedLag = allowedLag;
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) {
        @SuppressWarnings("unchecked")
        long timestamp = timestampFn.applyAsLong((T) item);
        if (timestamp + allowedLag < currentWm) {
            logLateEvent(getLogger(), currentWm, item);
            lateEventsDropped.inc();
            return true;
        } else {
            return tryEmit(item);
        }
    }

    @Override
    public boolean tryProcessWatermark(@Nonnull Watermark watermark) {
        currentWm = watermark.timestamp();
        return super.tryProcessWatermark(watermark);
    }
}
