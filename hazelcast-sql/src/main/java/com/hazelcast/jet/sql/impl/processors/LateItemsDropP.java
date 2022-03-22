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
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Watermark;

import javax.annotation.Nonnull;
import java.util.function.ToLongFunction;

import static com.hazelcast.jet.impl.util.Util.logLateEvent;

public class LateItemsDropP<T> extends AbstractProcessor {
    @Probe(name = "lateEventsDropped")
    private final Counter lateEventsDropped = SwCounter.newSwCounter();

    private final ToLongFunction<? super T> timestampFn;
    private final FlatMapper<T, T> flatMapper = flatMapper(this::flatMapEvent);

    private long currentWm = Long.MIN_VALUE;

    public LateItemsDropP(ToLongFunction<? super T> timestampFn) {
        this.timestampFn = timestampFn;
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) {
        return flatMapper.tryProcess((T) item);
    }

    @Override
    public boolean tryProcessWatermark(@Nonnull Watermark watermark) {
        currentWm = watermark.timestamp();
        return super.tryProcessWatermark(watermark);
    }

    @Nonnull
    private Traverser<T> flatMapEvent(T event) {
        long timestamp = timestampFn.applyAsLong(event);
        if (timestamp < currentWm) {
            logLateEvent(getLogger(), currentWm, event);
            lateEventsDropped.inc();
            return Traversers.empty();
        }
        return Traversers.singleton(event);
    }
}
