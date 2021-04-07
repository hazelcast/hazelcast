/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.impl.proxy.ClientMapProxy;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.jet.config.EdgeConfig;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Outbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.impl.connector.HazelcastWriters.ArrayMap;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.partition.PartitioningStrategy;

import javax.annotation.Nonnull;
import java.util.AbstractMap.SimpleEntry;
import java.util.function.Consumer;

import static java.lang.Integer.max;

public final class WriteMapP<T, K, V> extends AsyncHazelcastWriterP {

    private static final int BUFFER_LIMIT = 1024;

    private final String mapName;
    private final SerializationService serializationService;
    private final ArrayMap<Data, Data> buffer = new ArrayMap<>(EdgeConfig.DEFAULT_QUEUE_SIZE);
    private final FunctionEx<? super T, ? extends K> toKeyFn;
    private final FunctionEx<? super T, ? extends V> toValueFn;

    private IMap<Data, Data> map;
    private Consumer<T> addToBuffer;

    private WriteMapP(
            @Nonnull HazelcastInstance instance,
            int maxParallelAsyncOps,
            String mapName,
            @Nonnull SerializationService serializationService,
            @Nonnull FunctionEx<? super T, ? extends K> toKeyFn,
            @Nonnull FunctionEx<? super T, ? extends V> toValueFn
    ) {
        super(instance, maxParallelAsyncOps);
        this.mapName = mapName;
        this.serializationService = serializationService;
        this.toKeyFn = toKeyFn;
        this.toValueFn = toValueFn;
    }

    @Override
    public void init(@Nonnull Outbox outbox, @Nonnull Context context) {
        map = instance().getMap(mapName);

        if (map instanceof MapProxyImpl) {
            PartitioningStrategy<?> partitionStrategy = ((MapProxyImpl<K, V>) map).getPartitionStrategy();
            addToBuffer = item -> {
                Data key = serializationService.toData(key(item), partitionStrategy);
                Data value = serializationService.toData(value(item));
                buffer.add(new SimpleEntry<>(key, value));
            };
        } else if (map instanceof ClientMapProxy) {
            // TODO: add strategy/unify after https://github.com/hazelcast/hazelcast/issues/13950 is fixed
            addToBuffer = item -> {
                Data key = serializationService.toData(key(item));
                Data value = serializationService.toData(value(item));
                buffer.add(new SimpleEntry<>(key, value));
            };
        } else {
            throw new RuntimeException("Unexpected map class: " + map.getClass().getName());
        }
    }

    private K key(T item) {
        return toKeyFn.apply(item);
    }

    private V value(T item) {
        return toValueFn.apply(item);
    }

    @Override
    protected void processInternal(Inbox inbox) {
        if (buffer.size() < BUFFER_LIMIT) {
            inbox.drain(addToBuffer);
        }
        submitPending();
    }

    @Override
    protected boolean flushInternal() {
        return submitPending();
    }

    private boolean submitPending() {
        if (buffer.isEmpty()) {
            return true;
        }
        if (!tryAcquirePermit()) {
            return false;
        }
        setCallback(map.putAllAsync(buffer));
        buffer.clear();
        return true;
    }

    public static class Supplier<T, K, V> extends AbstractHazelcastConnectorSupplier {

        private static final long serialVersionUID = 1L;

        // use a conservative max parallelism to prevent overloading
        // the cluster with putAll operations
        private static final int MAX_PARALLELISM = 16;

        private final String mapName;
        private final FunctionEx<? super T, ? extends K> toKeyFn;
        private final FunctionEx<? super T, ? extends V> toValueFn;
        private int maxParallelAsyncOps;

        public Supplier(
                String clientXml, String mapName,
                @Nonnull FunctionEx<? super T, ? extends K> toKeyFn,
                @Nonnull FunctionEx<? super T, ? extends V> toValueFn
        ) {
            super(clientXml);
            this.mapName = mapName;
            this.toKeyFn = toKeyFn;
            this.toValueFn = toValueFn;
        }

        @Override
        public void init(@Nonnull Context context) {
            super.init(context);
            maxParallelAsyncOps = max(1, MAX_PARALLELISM / context.localParallelism());
        }

        @Override
        protected Processor createProcessor(HazelcastInstance instance, SerializationService serializationService) {
            return new WriteMapP<>(instance, maxParallelAsyncOps, mapName, serializationService, toKeyFn, toValueFn);
        }
    }
}
