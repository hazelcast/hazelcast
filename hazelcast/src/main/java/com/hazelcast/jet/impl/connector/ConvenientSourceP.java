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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.core.ManagedContext;
import com.hazelcast.function.BiConsumerEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.BroadcastKey;
import com.hazelcast.jet.core.EventTimeMapper;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.core.processor.SourceProcessors;
import com.hazelcast.jet.impl.JetEvent;
import com.hazelcast.security.PermissionsUtil;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.hazelcast.jet.core.BroadcastKey.broadcastKey;

/**
 * Implements a data source the user created using the Source Builder API.
 *
 * @see SourceProcessors#convenientSourceP
 * @see SourceProcessors#convenientTimestampedSourceP
 */
public class ConvenientSourceP<C, T, S> extends AbstractProcessor {

    /**
     * This processor's view of the buffer accessible to the user. Abstracts
     * away the difference between the plain and the timestamped buffer.
     */
    public interface SourceBufferConsumerSide<T> {
        /**
         * Returns a traverser over the contents of the buffer. Traversing the
         * items automatically removes them from the buffer.
         */
        Traverser<T> traverse();
        boolean isEmpty();
        boolean isClosed();
    }

    private final Function<? super Context, ? extends C> createFn;
    private final BiConsumer<? super C, ? super SourceBufferConsumerSide<?>> fillBufferFn;
    private final FunctionEx<? super C, ? extends S> createSnapshotFn;
    private final BiConsumerEx<? super C, ? super List<S>> restoreSnapshotFn;
    private final Consumer<? super C> destroyFn;
    private final SourceBufferConsumerSide<?> buffer;
    private final EventTimeMapper<T> eventTimeMapper;
    private BroadcastKey<Integer> snapshotKey;

    private boolean initialized;
    private C ctx;
    private Traverser<?> traverser;
    private S pendingState;
    private List<S> restoredStates;

    public ConvenientSourceP(
            @Nonnull Function<? super Context, ? extends C> createFn,
            @Nonnull BiConsumer<? super C, ? super SourceBufferConsumerSide<?>> fillBufferFn,
            @Nonnull FunctionEx<? super C, ? extends S> createSnapshotFn,
            @Nonnull BiConsumerEx<? super C, ? super List<S>> restoreSnapshotFn,
            @Nonnull Consumer<? super C> destroyFn,
            @Nonnull SourceBufferConsumerSide<?> buffer,
            @Nullable EventTimePolicy<? super T> eventTimePolicy
    ) {
        this.createFn = createFn;
        this.fillBufferFn = fillBufferFn;
        this.createSnapshotFn = createSnapshotFn;
        this.restoreSnapshotFn = restoreSnapshotFn;
        this.destroyFn = destroyFn;
        this.buffer = buffer;
        if (eventTimePolicy != null) {
            eventTimeMapper = new EventTimeMapper<>(eventTimePolicy);
            eventTimeMapper.addPartitions(1);
        } else {
            eventTimeMapper = null;
        }
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    @Override
    protected void init(@Nonnull Context context) {
        PermissionsUtil.checkPermission(createSnapshotFn, context);
        // createFn is allowed to return null, we'll call `destroyFn` even for null `ctx`
        ManagedContext managedContext = context.managedContext();
        ctx = (C) managedContext.initialize(createFn.apply(context));
        snapshotKey = broadcastKey(context.globalProcessorIndex());
        initialized = true;
    }

    @Override
    public boolean complete() {
        if (traverser == null) {
            fillBufferFn.accept(ctx, buffer);
            traverser =
                    eventTimeMapper == null ? buffer.traverse()
                    : buffer.isEmpty() ? eventTimeMapper.flatMapIdle()
                    : buffer.traverse().flatMap(t -> {
                        // if eventTimeMapper is not null, we know that T is JetEvent<T>
                        @SuppressWarnings("unchecked")
                        JetEvent<T> je = (JetEvent<T>) t;
                        return eventTimeMapper.flatMapEvent(je.payload(), 0, je.timestamp());
                    });
        }
        boolean bufferEmitted = emitFromTraverser(traverser);
        if (bufferEmitted) {
            traverser = null;
        }
        return bufferEmitted && buffer.isClosed();
    }

    @Override
    public boolean saveToSnapshot() {
        // finish current traverser
        if (traverser != null && !emitFromTraverser(traverser)) {
            return false;
        }
        if (buffer.isClosed()) {
            // don't call createSnapshotFn after the buffer is closed
            return true;
        }
        traverser = null;
        if (pendingState == null) {
            pendingState = createSnapshotFn.apply(ctx);
        }
        if (pendingState == null || tryEmitToSnapshot(snapshotKey, pendingState)) {
            pendingState = null;
            return true;
        }
        return false;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void restoreFromSnapshot(@Nonnull Object key, @Nonnull Object value) {
        if (restoredStates == null) {
            restoredStates = new ArrayList<>();
        }
        restoredStates.add((S) value);
    }

    @Override
    public boolean finishSnapshotRestore() {
        if (restoredStates != null) {
            restoreSnapshotFn.accept(ctx, restoredStates);
        }
        restoredStates = null;
        return true;
    }

    @Override
    public void close() {
        if (initialized) {
            destroyFn.accept(ctx);
        }
    }
}
