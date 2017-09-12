/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.processor;

import com.hazelcast.jet.AbstractProcessor;
import com.hazelcast.jet.ResettableSingletonTraverser;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.Watermark;
import com.hazelcast.jet.WatermarkEmissionPolicy;
import com.hazelcast.jet.WatermarkPolicy;
import com.hazelcast.jet.function.DistributedToLongFunction;

import javax.annotation.Nonnull;
import java.util.function.ToLongFunction;

/**
 * A processor that inserts watermark into a data stream. See
 * {@link com.hazelcast.jet.processor.Processors#insertWatermarks(
 *     DistributedToLongFunction,
 *     com.hazelcast.jet.function.DistributedSupplier,
 *     WatermarkEmissionPolicy) Processors.insertWatermarks()}.
 *
 * @param <T> type of the stream item
 */
public class InsertWatermarksP<T> extends AbstractProcessor {

    private final ToLongFunction<T> getTimestampFn;
    private final WatermarkPolicy wmPolicy;
    private final WatermarkEmissionPolicy wmEmitPolicy;
    private final ResettableSingletonTraverser<Object> singletonTraverser;
    private final FlatMapper<Object, Object> flatMapper;

    private long currWm = Long.MIN_VALUE;
    private long lastEmittedWm = Long.MIN_VALUE;

    /**
     * @param getTimestampFn function that extracts the timestamp from the item
     * @param wmPolicy the watermark policy
     */
    public InsertWatermarksP(
            @Nonnull DistributedToLongFunction<T> getTimestampFn,
            @Nonnull WatermarkPolicy wmPolicy,
            @Nonnull WatermarkEmissionPolicy wmEmitPolicy
    ) {
        this.getTimestampFn = getTimestampFn;
        this.wmPolicy = wmPolicy;
        this.wmEmitPolicy = wmEmitPolicy;
        this.flatMapper = flatMapper(this::traverser);
        this.singletonTraverser = new ResettableSingletonTraverser<>();
    }

    @Override
    public boolean tryProcess() {
        currWm = wmPolicy.getCurrentWatermark();
        if (!wmEmitPolicy.shouldEmit(currWm, lastEmittedWm)) {
            return true;
        }
        boolean didEmit = tryEmit(new Watermark(currWm));
        if (didEmit) {
            lastEmittedWm = currWm;
        }
        return didEmit;
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) throws Exception {
        return flatMapper.tryProcess(item);
    }

    private Traverser<Object> traverser(Object item) {
        long timestamp = getTimestampFn.applyAsLong((T) item);
        if (timestamp < currWm) {
            // drop late event
            return Traversers.empty();
        }
        currWm = wmPolicy.reportEvent(timestamp);
        singletonTraverser.accept(item);
        if (wmEmitPolicy.shouldEmit(currWm, lastEmittedWm)) {
            lastEmittedWm = currWm;
            return singletonTraverser.prepend(new Watermark(currWm));
        }
        return singletonTraverser;
    }
}
