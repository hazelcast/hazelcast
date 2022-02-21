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


import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.SerializationServiceAware;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.core.JetDataSerializerHook;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.query.impl.QueryableEntry;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BiFunction;

import static com.hazelcast.internal.util.MapUtil.createHashMap;

public final class UpdateMapP<T, K, V> extends AbstractUpdateMapP<T, K, V> {

    private final BiFunctionEx<? super V, ? super T, ? extends V> updateFn;
    private final BiFunction<Object, Object, Object> remappingFunction =
            (o, n) -> ApplyFnEntryProcessor.append(o, (Data) n);

    public UpdateMapP(HazelcastInstance instance,
               String mapName,
               @Nonnull FunctionEx<? super T, ? extends K> keyFn,
               @Nonnull BiFunctionEx<? super V, ? super T, ? extends V> updateFn) {
        this(instance, MAX_PARALLEL_ASYNC_OPS_DEFAULT, mapName, keyFn, updateFn);
    }

    UpdateMapP(HazelcastInstance instance,
               int maxParallelAsyncOps,
               String mapName,
               @Nonnull FunctionEx<? super T, ? extends K> keyFn,
               @Nonnull BiFunctionEx<? super V, ? super T, ? extends V> updateFn) {
        super(instance, maxParallelAsyncOps, mapName, keyFn);
        this.updateFn = updateFn;
    }

    @Override
    protected EntryProcessor<K, V, Void> entryProcessor(Map<Data, Object> buffer) {
        return new ApplyFnEntryProcessor<>(buffer, updateFn);
    }

    @Override
    protected void addToBuffer(T item) {
        K key = keyFn.apply(item);
        Data keyData = serializationContext.toKeyData(key);
        int partitionId = serializationContext.partitionId(keyData);
        Data itemData = serializationContext.toData(item);
        partitionBuffers[partitionId].merge(keyData, itemData, remappingFunction);
        pendingInPartition[partitionId]++;
    }

    @SuppressFBWarnings(value = {"SE_BAD_FIELD", "SE_NO_SERIALVERSIONID"},
        justification = "the class is never java-serialized")
    public static class ApplyFnEntryProcessor<K, V, T>
            implements EntryProcessor<K, V, Void>, IdentifiedDataSerializable,
            SerializationServiceAware {
        private Map<Data, Object> keysToUpdate;
        private BiFunctionEx<? super V, ? super T, ? extends V> updateFn;
        private SerializationService serializationService;

        public ApplyFnEntryProcessor() {
        }

        ApplyFnEntryProcessor(
            Map<Data, Object> keysToUpdate,
            BiFunctionEx<? super V, ? super T, ? extends V> updateFn
        ) {
            this.keysToUpdate = keysToUpdate;
            this.updateFn = updateFn;
        }

        @Override
        public Void process(Entry<K, V> entry) {
            // it should not matter that we don't take the PartitionStrategy here into account
            Data keyData = ((QueryableEntry<K, V>) entry).getKeyData();
            Object item = keysToUpdate.get(keyData);
            if (item == null && !keysToUpdate.containsKey(keyData)) {
                // Implementing equals/hashCode is not required for IMap keys since serialized version is used
                // instead. After serializing/deserializing the keys they will have different identity. And since they
                // don't implement the methods, they key can't be found in the map.
                throw new JetException("A key not found in the map - is equals/hashCode " +
                        "correctly implemented for the key? Key type: " + entry.getKey().getClass().getName());
            }
            if (item instanceof List) {
                @SuppressWarnings("unchecked")
                List<Data> castList = (List<Data>) item;
                for (Data o : castList) {
                    handle(entry, o);
                }
            } else {
                handle(entry, (Data) item);
            }
            return null;
        }

        private void handle(Entry<K, V> entry, Data itemData) {
            T item = serializationService.toObject(itemData);
            V oldValue = entry.getValue();
            V newValue = updateFn.apply(oldValue, item);
            entry.setValue(newValue);
        }

        @Override
        public void setSerializationService(SerializationService serializationService) {
            this.serializationService = serializationService;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeInt(keysToUpdate.size());
            for (Entry<Data, Object> en : keysToUpdate.entrySet()) {
                IOUtil.writeData(out, en.getKey());
                Object value = en.getValue();
                if (value instanceof Data) {
                    out.writeInt(1);
                    IOUtil.writeData(out, (Data) value);
                } else if (value instanceof List) {
                    @SuppressWarnings("unchecked")
                    List<Data> list = (List<Data>) value;
                    out.writeInt(list.size());
                    for (Data data : list) {
                        IOUtil.writeData(out, data);
                    }
                } else {
                    assert false : "Unknown value type: " + value.getClass();
                }
            }
            out.writeObject(updateFn);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            int keysToUpdateSize = in.readInt();
            keysToUpdate = createHashMap(keysToUpdateSize);
            for (int i = 0; i < keysToUpdateSize; i++) {
                Data key = IOUtil.readData(in);
                int size = in.readInt();
                Object value;
                if (size == 1) {
                    value = IOUtil.readData(in);
                } else {
                    List<Data> list = new ArrayList<>(size);
                    for (int j = 0; j < size; j++) {
                        list.add(IOUtil.readData(in));
                    }
                    value = list;
                }
                keysToUpdate.put(key, value);
            }
            updateFn = in.readObject();
        }

        @Override
        public int getFactoryId() {
            return JetDataSerializerHook.FACTORY_ID;
        }

        @Override
        public int getClassId() {
            return JetDataSerializerHook.APPLY_FN_ENTRY_PROCESSOR;
        }

        // used to group entries when more than one entry exists for the same key
        @SuppressWarnings("unchecked")
        static Object append(Object value, Data item) {
            List<Data> list;
            if (value instanceof List) {
                list = (List<Data>) value;
            } else {
                list = new ArrayList<>();
                list.add((Data) value);
            }
            list.add(item);
            return list;
        }
    }

}
