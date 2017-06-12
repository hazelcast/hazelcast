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
import com.hazelcast.jet.Watermark;
import com.hazelcast.jet.WatermarkPolicy;
import com.hazelcast.jet.ResettableSingletonTraverser;
import com.hazelcast.jet.Traverser;

import javax.annotation.Nonnull;
import java.util.function.ToLongFunction;

import static com.hazelcast.jet.Traversers.empty;

/**
 * A processor that inserts watermark into a data stream. See
 * {@link com.hazelcast.jet.processor.Processors#insertWatermarks(
 *      com.hazelcast.jet.function.DistributedToLongFunction,
 *      com.hazelcast.jet.function.DistributedSupplier)
 * Processors.insertWatermark()}.
 *
 * @param <T> type of the stream item
 */
public class InsertWatermarkP<T> extends AbstractProcessor {

    private final ToLongFunction<T> getTimestampF;
    private final WatermarkPolicy watermarkPolicy;
    private final ResettableSingletonTraverser<Object> singletonTraverser;
    private final FlatMapper<Object, Object> flatMapper;

    private long currWm = Long.MIN_VALUE;

    /**
     * @param getTimestampF function that extracts the timestamp from the item
     * @param watermarkPolicy the watermark policy
     */
    public InsertWatermarkP(@Nonnull ToLongFunction<T> getTimestampF,
                       @Nonnull WatermarkPolicy watermarkPolicy
    ) {
        this.getTimestampF = getTimestampF;
        this.watermarkPolicy = watermarkPolicy;
        this.flatMapper = flatMapper(this::traverser);
        this.singletonTraverser = new ResettableSingletonTraverser<>();
    }

    @Override
    public boolean tryProcess() {
        long newWm = watermarkPolicy.getCurrentWatermark();
        if (newWm <= currWm) {
            return true;
        }
        boolean didEmit = tryEmit(new Watermark(newWm));
        if (didEmit) {
            currWm = newWm;
        }
        return didEmit;
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) throws Exception {
        return flatMapper.tryProcess(item);
    }

    private Traverser<Object> traverser(Object item) {
        long timestamp = getTimestampF.applyAsLong((T) item);
        if (timestamp < currWm) {
            // drop late event
            return empty();
        }
        long newWm = watermarkPolicy.reportEvent(timestamp);
        singletonTraverser.accept(item);
        if (newWm > currWm) {
            currWm = newWm;
            return singletonTraverser.prepend(new Watermark(currWm));
        }
        return singletonTraverser;
    }
}
