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
import com.hazelcast.jet.Watermark;
import com.hazelcast.jet.WatermarkEmissionPolicy;
import com.hazelcast.jet.WatermarkPolicy;
import com.hazelcast.jet.function.DistributedToLongFunction;

import javax.annotation.Nonnull;
import java.util.function.ToLongFunction;

import static com.hazelcast.jet.BroadcastKey.broadcastKey;
import static com.hazelcast.jet.impl.util.LoggingUtil.logFine;

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

    private final ToLongFunction<T> getTimestampF;
    private final WatermarkPolicy wmPolicy;
    private final WatermarkEmissionPolicy wmEmitPolicy;
    private final ResettableSingletonTraverser<Object> singletonTraverser;
    private final FlatMapper<Object, Object> flatMapper;

    private long currWm = Long.MIN_VALUE;
    private long lastEmittedWm = Long.MIN_VALUE;
    private int index;

    // value to be used during temporarily snapshot restore
    private long minRestoredWm = Long.MAX_VALUE;

    /**
     * @param getTimestampF function that extracts the timestamp from the item
     * @param wmPolicy the watermark policy
     */
    public InsertWatermarksP(
            @Nonnull DistributedToLongFunction<T> getTimestampF,
            @Nonnull WatermarkPolicy wmPolicy,
            @Nonnull WatermarkEmissionPolicy wmEmitPolicy
    ) {
        this.getTimestampF = getTimestampF;
        this.wmPolicy = wmPolicy;
        this.wmEmitPolicy = wmEmitPolicy;
        this.flatMapper = flatMapper(this::traverser);
        this.singletonTraverser = new ResettableSingletonTraverser<>();
    }

    @Override
    protected void init(@Nonnull Context context) throws Exception {
        index = context.globalProcessorIndex();
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

    @Override
    public boolean saveToSnapshot() {
        return tryEmitToSnapshot(broadcastKey(index), lastEmittedWm);
    }

    @Override
    public void restoreFromSnapshot(@Nonnull Object key, @Nonnull  Object value) {
        // we restart at the oldest WM any instance was at at the time of snapshot
        minRestoredWm = Math.min(minRestoredWm, (long) value);
    }

    @Override
    public boolean finishSnapshotRestore() {
        lastEmittedWm = minRestoredWm;
        logFine(getLogger(), "restored lastEmittedWm=%s", lastEmittedWm);
        return true;
    }

    private Traverser<Object> traverser(Object item) {
        long timestamp = getTimestampF.applyAsLong((T) item);
        currWm = wmPolicy.reportEvent(timestamp);
        if (timestamp >= currWm) {
            // only emit non-late events
            singletonTraverser.accept(item);
        }
        if (wmEmitPolicy.shouldEmit(currWm, lastEmittedWm)) {
            lastEmittedWm = currWm;
            return singletonTraverser.prepend(new Watermark(currWm));
        }
        return singletonTraverser;
    }
}
