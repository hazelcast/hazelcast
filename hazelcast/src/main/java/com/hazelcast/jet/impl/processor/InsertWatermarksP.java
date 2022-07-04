/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.BroadcastKey;
import com.hazelcast.jet.core.EventTimeMapper;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.core.processor.Processors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static com.hazelcast.jet.core.BroadcastKey.broadcastKey;
import static com.hazelcast.jet.impl.util.LoggingUtil.logFine;

/**
 * See {@link Processors#insertWatermarksP}.
 *
 * @param <T> type of the stream item
 */
public class InsertWatermarksP<T> extends AbstractProcessor {

    private final EventTimeMapper<? super T> eventTimeMapper;
    private Traverser<Object> traverser;

    // value to be used temporarily during snapshot restore
    private long minRestoredWm = Long.MAX_VALUE;

    public InsertWatermarksP(EventTimePolicy<? super T> eventTimePolicy) {
        eventTimeMapper = new EventTimeMapper<>(eventTimePolicy);
        eventTimeMapper.addPartitions(1);
    }

    @Override
    public boolean tryProcess() {
        return tryProcessInternal(null);
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) {
        return tryProcessInternal(item);
    }

    @SuppressWarnings("unchecked")
    private boolean tryProcessInternal(@Nullable Object item) {
        if (traverser == null) {
            traverser = eventTimeMapper.flatMapEvent((T) item, 0, EventTimeMapper.NO_NATIVE_TIME);
        }
        if (emitFromTraverser(traverser)) {
            traverser = null;
            return true;
        }
        return false;
    }

    @Override
    public boolean saveToSnapshot() {
        return tryEmitToSnapshot(broadcastKey(Keys.LAST_EMITTED_WM), eventTimeMapper.getWatermark(0));
    }

    @Override
    public void restoreFromSnapshot(@Nonnull Object key, @Nonnull Object value) {
        assert ((BroadcastKey) key).key().equals(Keys.LAST_EMITTED_WM) : "Unexpected key: " + key;
        // we restart at the oldest WM any instance was at at the time of snapshot
        minRestoredWm = Math.min(minRestoredWm, (long) value);
    }

    @Override
    public boolean finishSnapshotRestore() {
        eventTimeMapper.restoreWatermark(0, minRestoredWm);
        logFine(getLogger(), "restored lastEmittedWm=%s", minRestoredWm);
        return true;
    }

    private enum Keys {
        LAST_EMITTED_WM
    }
}
