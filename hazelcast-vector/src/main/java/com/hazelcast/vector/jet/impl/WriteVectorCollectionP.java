/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.vector.jet.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.internal.serialization.BinaryInterface;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.jet.config.EdgeConfig;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Outbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.impl.connector.AbstractHazelcastConnectorSupplier;
import com.hazelcast.jet.impl.connector.AsyncHazelcastWriterP;
import com.hazelcast.jet.impl.connector.HazelcastWriters.ArrayMap;
import com.hazelcast.jet.impl.serialization.DelegatingSerializationService;
import com.hazelcast.security.permission.VectorCollectionPermission;
import com.hazelcast.vector.VectorCollection;
import com.hazelcast.vector.VectorDocument;
import com.hazelcast.vector.impl.VectorUtil;

import javax.annotation.Nonnull;
import java.io.Serial;
import java.security.Permission;
import java.util.AbstractMap.SimpleEntry;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static com.hazelcast.security.permission.ActionConstants.ACTION_CREATE;
import static com.hazelcast.security.permission.ActionConstants.ACTION_PUT;
import static java.lang.Integer.max;

public final class WriteVectorCollectionP<T, K, V> extends AsyncHazelcastWriterP {

    private static final int BUFFER_LIMIT = 1024;

    private final String collectionName;
    private final SerializationService serializationService;
    private final FunctionEx<? super T, ? extends K> toKeyFn;
    private final FunctionEx<? super T, VectorDocument<? extends V>> toDocumentFn;

    // key may be K or Data (if custom Jet job serializers are used)
    // value may hold VectorDocument<V> or VectorDocument<Data> (if custom Jet job serializers are used)
    private ArrayMap<Object, VectorDocument<?>> buffer;
    private VectorCollection<K, V> collection;
    private Consumer<T> addToBuffer;

    private WriteVectorCollectionP(
            @Nonnull HazelcastInstance instance,
            int maxParallelAsyncOps,
            @Nonnull String collectionName,
            @Nonnull SerializationService serializationService,
            @Nonnull FunctionEx<? super T, ? extends K> toKeyFn,
            @Nonnull FunctionEx<? super T, VectorDocument<? extends V>> toDocumentFn
    ) {
        super(instance, maxParallelAsyncOps);
        this.collectionName = collectionName;
        this.serializationService = serializationService;
        this.toKeyFn = toKeyFn;
        this.toDocumentFn = toDocumentFn;

        resetBuffer();
    }

    @Override
    public void init(@Nonnull Outbox outbox, @Nonnull Context context) {
        collection = instance().getVectorCollection(collectionName);

        boolean hasCustomSerializers = serializationService instanceof DelegatingSerializationService dss
                && dss.hasAddedSerializers();

        if (!hasCustomSerializers) {
            addToBuffer = item -> buffer.add(new SimpleEntry<>(key(item), document(item)));
        } else {
            addToBuffer = item -> buffer.add(new SimpleEntry<>(
                    serializationService.toData(key(item)),
                    VectorUtil.serialize(document(item), serializationService)));
        }
    }

    private K key(T item) {
        return toKeyFn.apply(item);
    }

    private VectorDocument<? extends V> document(T item) {
        return toDocumentFn.apply(item);
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
        // we rely on the fact that VectorCollection does not care about the types
        // and will not serialize Data again.
        setCallback(collection.putAllAsync((Map) buffer));
        resetBuffer();
        return true;
    }

    private void resetBuffer() {
        buffer = new ArrayMap<>(EdgeConfig.DEFAULT_QUEUE_SIZE);
    }

    /**
     * A ProcessorSupplier that transforms the key, value and vectors of a stream
     * and writes the transformed data to a vector collection sink.
     */
    @BinaryInterface
    public static class Supplier<T, K, V> extends AbstractHazelcastConnectorSupplier {

        @Serial
        private static final long serialVersionUID = 1L;

        // use a conservative max parallelism to prevent overloading
        // the cluster with putAll operations
        private static final int MAX_PARALLELISM = 16;

        private final String collectionName;
        private final FunctionEx<? super T, ? extends K> toKeyFn;
        private final FunctionEx<? super T, VectorDocument<? extends V>> toDocumentFn;
        private int maxParallelAsyncOps;

        public Supplier(String collectionName,
                        @Nonnull FunctionEx<? super T, ? extends K> toKeyFn,
                        @Nonnull FunctionEx<? super T, VectorDocument<? extends V>> toDocumentFn) {
            // no support yet for remote vector collections
            super(null, null);
            this.collectionName = collectionName;
            this.toKeyFn = toKeyFn;
            this.toDocumentFn = toDocumentFn;
        }

        @Override
        public void init(@Nonnull Context context) {
            super.init(context);
            maxParallelAsyncOps = max(1, MAX_PARALLELISM / context.localParallelism());
        }

        @Override
        protected Processor createProcessor(HazelcastInstance instance, SerializationService serializationService) {
            return new WriteVectorCollectionP<>(instance, maxParallelAsyncOps, collectionName,
                    serializationService, toKeyFn, toDocumentFn);
        }

        @Override
        public List<Permission> permissions() {
            return List.of(vectorCollectionPutPermission(collectionName));
        }
    }

    public static Permission vectorCollectionPutPermission(String collectionName) {
        return new VectorCollectionPermission(collectionName, ACTION_CREATE, ACTION_PUT);
    }
}
