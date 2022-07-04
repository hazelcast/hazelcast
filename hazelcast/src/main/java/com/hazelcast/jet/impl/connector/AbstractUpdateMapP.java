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

import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.client.impl.spi.ClientPartitionService;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.partition.IPartitionService;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.JetDataSerializerHook;
import com.hazelcast.jet.core.Outbox;
import com.hazelcast.jet.impl.util.ImdgUtil;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.partition.PartitioningStrategy;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.spi.impl.NodeEngineImpl;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.ToIntFunction;

import static com.hazelcast.jet.impl.util.Util.getNodeEngine;

/**
 * @param <T> type of input items to this processor
 * @param <K> type of keys of the map being written
 * @param <V> type of values of the map being written
 */
public abstract class AbstractUpdateMapP<T, K, V> extends AsyncHazelcastWriterP {

    private static final int PENDING_ITEM_COUNT_LIMIT = 1024;

    protected final FunctionEx<? super T, ? extends K> keyFn;

    protected final String mapName;

    protected IMap<K, V> map;
    protected SerializationContext<K> serializationContext;

    protected Map<Data, Object>[] partitionBuffers;
    protected int[] pendingInPartition;

    protected int pendingItemCount;
    protected int currentPartitionId;

    public AbstractUpdateMapP(
            @Nonnull HazelcastInstance instance,
            int maxParallelAsyncOps,
            @Nonnull String mapName,
            @Nonnull FunctionEx<? super T, ? extends K> keyFn
    ) {
        super(instance, maxParallelAsyncOps);
        this.mapName = Objects.requireNonNull(mapName, "mapName");
        this.keyFn = keyFn;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(@Nonnull Outbox outbox, @Nonnull Context context) {
        map = instance().getMap(mapName);
        serializationContext = new SerializationContext<>(instance(), map);

        int partitionCount = serializationContext.partitionCount();
        partitionBuffers = new Map[partitionCount];
        pendingInPartition = new int[partitionCount];
        for (int i = 0; i < partitionCount; i++) {
            partitionBuffers[i] = new HashMap<>();
        }
    }

    @Override
    protected final void processInternal(Inbox inbox) {
        if (pendingItemCount < PENDING_ITEM_COUNT_LIMIT) {
            pendingItemCount += inbox.size();
            inbox.drain(this::addToBuffer);
        }
        submitPending();
    }

    protected abstract void addToBuffer(T item);

    @Override
    protected final boolean flushInternal() {
        return submitPending();
    }

    // returns true if we were able to submit all pending items
    @SuppressWarnings({"rawtypes", "unchecked"})
    private boolean submitPending() {
        if (pendingItemCount == 0) {
            return true;
        }
        for (int i = 0; i < partitionBuffers.length; i++,
                currentPartitionId = incrCircular(currentPartitionId, partitionBuffers.length)) {
            if (partitionBuffers[currentPartitionId].isEmpty()) {
                continue;
            }
            if (!tryAcquirePermit()) {
                return false;
            }

            Map<Data, Object> buffer = partitionBuffers[currentPartitionId];
            EntryProcessor<K, V, Void> entryProcessor = entryProcessor(buffer);
            // submit Set<Data> here
            IMap map = this.map;
            setCallback(map.submitToKeys(buffer.keySet(), entryProcessor));
            pendingItemCount -= pendingInPartition[currentPartitionId];
            pendingInPartition[currentPartitionId] = 0;
            partitionBuffers[currentPartitionId] = new HashMap<>();
        }
        if (currentPartitionId == partitionBuffers.length) {
            currentPartitionId = 0;
        }
        assert pendingItemCount == 0 : "pending item count should be 0, but was " + pendingItemCount;
        return true;
    }

    protected abstract EntryProcessor<K, V, Void> entryProcessor(Map<Data, Object> buffer);

    /**
     * Returns {@code v+1} or 0, if {@code v+1 == limit}.
     */
    private static int incrCircular(int v, int limit) {
        v++;
        if (v == limit) {
            v = 0;
        }
        return v;
    }

    public static class SerializationContext<K> {

        private final int partitionCount;

        private final ToIntFunction<Data> partitionIdFn;
        private final SerializationService serializationService;
        private final PartitioningStrategy<K> partitioningStrategy;

        @SuppressWarnings("unchecked")
        SerializationContext(HazelcastInstance instance, IMap<K, ?> map) {
            if (ImdgUtil.isMemberInstance(instance)) {
                NodeEngineImpl nodeEngine = getNodeEngine(instance);
                IPartitionService partitionService = nodeEngine.getPartitionService();
                partitionCount = partitionService.getPartitionCount();
                partitionIdFn = partitionService::getPartitionId;
                serializationService = nodeEngine.getSerializationService();
                partitioningStrategy = ((MapProxyImpl<K, ?>) map).getPartitionStrategy();
            } else {
                HazelcastClientProxy clientProxy = (HazelcastClientProxy) instance;
                ClientPartitionService clientPartitionService = clientProxy.client.getClientPartitionService();
                partitionCount = clientPartitionService.getPartitionCount();
                partitionIdFn = clientPartitionService::getPartitionId;
                serializationService = clientProxy.getSerializationService();
                partitioningStrategy = null;
            }
        }

        public int partitionCount() {
            return partitionCount;
        }

        public int partitionId(Data data) {
            return partitionIdFn.applyAsInt(data);
        }

        public Data toKeyData(K key) {
            if (partitioningStrategy != null) {
                // We pre-serialize the key and value to avoid double serialization when partitionId
                // is calculated and when the value for backup operation is re-serialized
                return serializationService.toData(key, partitioningStrategy);
            } else {
                // We ignore partition strategy for remote connection, the client doesn't know it.
                // TODO we might be able to fix this after https://github.com/hazelcast/hazelcast/issues/13950 is fixed
                // The functionality should work, but will be ineffective: the submitOnKey calls will have wrongly
                // partitioned data.
                return serializationService.toData(key);
            }
        }

        public Data toData(Object value) {
            return serializationService.toData(value);
        }
    }

    public static class ApplyValuesEntryProcessor<K, V>
            implements EntryProcessor<K, V, Void>, IdentifiedDataSerializable {

        private Map<Data, Object> keysToUpdate;

        public ApplyValuesEntryProcessor() { //needed for (de)serialization
        }

        public ApplyValuesEntryProcessor(Map<Data, Object> keysToUpdate) {
            this.keysToUpdate = keysToUpdate;
        }

        @Override
        public Void process(Map.Entry<K, V> entry) {
            // avoid re-serialization
            QueryableEntry<Data, Object> e = ((QueryableEntry<Data, Object>) entry);
            e.setValue(keysToUpdate.get(e.getKeyData()));
            return null;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeInt(keysToUpdate.size());
            for (Map.Entry<Data, Object> e : keysToUpdate.entrySet()) {
                IOUtil.writeData(out, e.getKey());
                IOUtil.writeData(out, (Data) e.getValue());
            }
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            int size = in.readInt();
            keysToUpdate = new LinkedHashMap<>(size);
            for (int i = 0; i < size; i++) {
                Data key = IOUtil.readData(in);
                Data value = IOUtil.readData(in);
                keysToUpdate.put(key, value);
            }
        }

        @Override
        public int getFactoryId() {
            return JetDataSerializerHook.FACTORY_ID;
        }

        @Override
        public int getClassId() {
            return JetDataSerializerHook.APPLY_VALUE_ENTRY_PROCESSOR;
        }

    }

}
