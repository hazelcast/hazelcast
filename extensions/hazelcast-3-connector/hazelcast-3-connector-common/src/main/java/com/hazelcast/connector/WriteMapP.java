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

package com.hazelcast.connector;

import com.hazelcast.connector.map.AsyncMap;
import com.hazelcast.connector.map.Hz3MapAdapter;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.jet.config.EdgeConfig;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Outbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Watermark;

import javax.annotation.Nonnull;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static java.util.stream.Collectors.toList;

/**
 * Adapted from last Jet 3.x release
 * https://github.com/hazelcast/hazelcast-jet/tree/v3.2.2
 * from
 * hazelcast-jet-core/src/main/java/com/hazelcast/jet/impl/connector/WriteMapP.java
 */
final class WriteMapP<T, K, V> implements Processor {

    // This is a cooperative processor it will use maximum
    // local parallelism by default. We also use an incoming
    // local partitioned edge so each processor deals with a
    // subset of the partitions. We want to limit the number of
    // in flight operations since putAll operation can be slow
    // and bulky, otherwise we may face timeouts.
    private final AtomicBoolean pendingOp = new AtomicBoolean();
    private final AtomicReference<Throwable> firstFailure = new AtomicReference<>();
    private final String mapName;

    @Nonnull
    private final FunctionEx<? super T, ? extends K> toKeyFn;
    @Nonnull
    private final FunctionEx<? super T, ? extends V> toValueFn;

    private final ArrayMap<Object, Object> buffer = new ArrayMap<>(EdgeConfig.DEFAULT_QUEUE_SIZE);
    private final BiConsumer<Object, Throwable> callback = (r, t) -> {
        if (t != null) {
            firstFailure.compareAndSet(null, t);
        }
        buffer.clear();
        pendingOp.set(false);
    };

    private Hz3MapAdapter hz3MapAdapter;
    private AsyncMap<Object, Object> map;
    private Consumer<T> addToBuffer;

    private WriteMapP(@Nonnull Hz3MapAdapter hz3MapAdapter,
                      @Nonnull String mapName,
                      @Nonnull FunctionEx<? super T, ? extends K> toKeyFn,
                      @Nonnull FunctionEx<? super T, ? extends V> toValueFn) {
        this.hz3MapAdapter = hz3MapAdapter;
        this.mapName = mapName;
        this.toKeyFn = toKeyFn;
        this.toValueFn = toValueFn;
    }

    @Override
    public void init(@Nonnull Outbox outbox, @Nonnull Context context) {
        map = hz3MapAdapter.getMap(mapName);

        InternalSerializationService serializationService =
                ((HazelcastInstanceImpl) context.hazelcastInstance()).node.getCompatibilitySerializationService();
        addToBuffer = item -> {
            Data key = serializationService.toData(key(item));
            Data value = serializationService.toData(value(item));

            Object hz3Key = hz3MapAdapter.toHz3Data(key.toByteArray());
            Object hz3Value = hz3MapAdapter.toHz3Data(value.toByteArray());
            buffer.add(new AbstractMap.SimpleEntry<>(hz3Key, hz3Value));
        };
    }

    private K key(T item) {
        return toKeyFn.apply(item);
    }

    private V value(T item) {
        return toValueFn.apply(item);
    }


    @Override
    public boolean tryProcess() {
        checkFailure();
        return true;
    }

    @Override
    public void process(int ordinal, @Nonnull Inbox inbox) {
        checkFailure();
        if (pendingOp.compareAndSet(false, true)) {
            inbox.drain(addToBuffer);
            map.putAllAsync(buffer)
                    .whenComplete(callback);
        }
    }

    @Override
    public boolean tryProcessWatermark(@Nonnull Watermark watermark) {
        return true;
    }

    @Override
    public boolean saveToSnapshot() {
        return ensureAllSuccessfullyWritten();
    }

    @Override
    public boolean complete() {
        return ensureAllSuccessfullyWritten();
    }

    private void checkFailure() {
        Throwable failure = firstFailure.get();
        if (failure != null) {
            throw sneakyThrow(failure);
        }
    }

    private boolean ensureAllSuccessfullyWritten() {
        try {
            return !pendingOp.get();
        } finally {
            checkFailure();
        }
    }

    public static class Supplier<T, K, V> implements ProcessorSupplier {
        private static final long serialVersionUID = 1L;

        // use a conservative max parallelism to prevent overloading
        // the cluster with putAll operations
        private static final int MAX_PARALLELISM = 16;

        private final String clientXml;
        private final String mapName;
        private final FunctionEx<? super T, ? extends K> toKeyFn;
        private final FunctionEx<? super T, ? extends V> toValueFn;

        Supplier(@Nonnull String clientXml, @Nonnull String mapName,
                        @Nonnull FunctionEx<? super T, ? extends K> toKeyFn,
                        @Nonnull FunctionEx<? super T, ? extends V> toValueFn
        ) {
            this.clientXml = clientXml;
            this.mapName = mapName;
            this.toKeyFn = toKeyFn;
            this.toValueFn = toValueFn;
        }

        @Nonnull
        @Override
        public Collection<? extends Processor> get(int count) {
            return Stream.generate(() -> new WriteMapP(
                            Hz3Util.createMapAdapter(clientXml), mapName,
                    toKeyFn,
                    toValueFn))
                    .limit(count)
                    .collect(toList());
        }

    }
}
