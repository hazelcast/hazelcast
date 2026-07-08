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

package com.hazelcast.vector.internal.impl.storage;

import com.hazelcast.config.vector.VectorCollectionConfig;
import com.hazelcast.config.vector.VectorIndexConfig;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.impl.SerializationUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.NodeEngineThreadLocalContext;
import com.hazelcast.vector.internal.impl.VectorCollectionSerializerHook;
import com.hazelcast.vector.internal.impl.VectorCollectionService;
import com.hazelcast.vector.internal.impl.service.VectorCollectionServiceImpl;
import io.github.jbellis.jvector.disk.RandomAccessReader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.hazelcast.internal.nio.Bits.FLOAT_SIZE_IN_BYTES;

public class ReplicationStateHolder implements IdentifiedDataSerializable {

    static class CollectionReplicationStateHolder {
        VectorCollectionConfig config;
        Map<Data, Data> entries;
        List<IndexReplicationStateHolder> indexes;
        private int entryCount;

        class IndexReplicationStateHolder {
            VectorIndexConfig indexConfig;
            int idGeneratorState;
            AbstractVectorIndex index;
            UpdatableVectorsSource vectorsSupplier;
            Map<Data, Integer> keyToNodeIdMap;

            IndexReplicationStateHolder(VectorIndexConfig indexConfig) {
                this.indexConfig = indexConfig;
            }

            private void readData(ObjectDataInput in) throws IOException {
                // This uses VectorIndexFactory to simplify creation, but the index is not usable
                // until entire migration is executed. It contains only JVector parts and vectorSupplier.
                var nodeEngine = NodeEngineThreadLocalContext.getNodeEngineThreadLocalContextOrNull();
                if (nodeEngine == null) {
                    throw new IllegalStateException(
                        "NodeEngine is not available for the current thread: " + Thread.currentThread().getName()
                    );
                }

                VectorCollectionServiceImpl vectorServiceInternal = nodeEngine.getService(VectorCollectionService.SERVICE_NAME);
                index = vectorServiceInternal.getVectorIndexFactory().create(indexConfig);

                vectorsSupplier = index.vectorsSupplier;

                idGeneratorState = in.readInt();
                vectorsSupplier.readData(in);

                // create map that will receive key to nodeId mapping
                keyToNodeIdMap = new HashMap<>(entryCount);

                index.indexBuilder.load(new RandomAccessReaderAdapter(in));
            }

            private void readIndexEntry(ObjectDataInput in, Data key) throws IOException {
                int nodeId = in.readInt();
                keyToNodeIdMap.put(key, nodeId);
            }

            String getName() {
                return indexConfig.getName();
            }
        }

        private void readData(ObjectDataInput in) throws IOException {
            config = in.readObject();

            entryCount = in.readInt();

            // read indexes
            int indexCount = in.readInt();
            indexes = new ArrayList<>(indexCount);
            for (int j = 0; j < indexCount; j++) {
                // index name can be null
                String name = in.readString();
                var indexConfig = config.getVectorIndexConfigs().stream()
                        .filter(ic -> Objects.equals(ic.getName(), name))
                        .findFirst().orElseThrow();

                IndexReplicationStateHolder indexState = new IndexReplicationStateHolder(indexConfig);
                indexState.readData(in);
                indexes.add(indexState);
            }

            // read KV part + index data associated with key
            entries = new HashMap<>(entryCount);
            for (int j = 0; j < entryCount; j++) {
                var key = IOUtil.readData(in);
                var value = IOUtil.readData(in);
                entries.put(key, value);
                for (IndexReplicationStateHolder index : indexes) {
                    index.readIndexEntry(in, key);
                }
            }
        }
    }

    // used for serialization to avoid copying data multiple times
    private transient Map<String, VectorCollectionStorage> storageMap;

    // used for deserialization - cannot create full VectorCollectionStorage yet
    private transient Map<String, CollectionReplicationStateHolder> dataMap;

    public ReplicationStateHolder() {
    }

    public ReplicationStateHolder(Map<String, VectorCollectionStorage> vcStorageMap) {
        storageMap = vcStorageMap;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        SerializationUtil.writeMapStringKey(storageMap, out, ReplicationStateHolder::writeVectorCollectionStorage);
    }

    private static void writeVectorCollectionStorage(ObjectDataOutput out, VectorCollectionStorage storage) throws IOException {
        // collection config. In theory redundant, but required to recreate indexes efficiently.
        // Used also to check for configuration consistency.
        out.writeObject(storage.getConfig());

        // collection size = number of KV entries.
        // Needed early to pre-create some structures with appropriate initial size
        out.writeInt(storage.getRecordStore().size());

        // Each collection entry is written in the following format:
        // - key (Data)
        // - value (Data)
        // - for each index in order:
        //   - index-specific data for the entry, currently this is always only nodeId (int)
        //
        // Index ordinal (int) for index specific data is not written currently.
        // May be needed if not all vectors are required to be present.
        // Vectors are not written per KV entry, as they can be deduplicated in some indexes.

        int indexCount = storage.getVectorIndexes().getSize();
        // deterministic order of indexes
        List<AbstractVectorIndex> indexOrdinals = new ArrayList<>(indexCount);
        storage.getVectorIndexes().forEachIndex(indexOrdinals::add);

        // write indexes: general data, vectors, serialized index
        out.writeInt(indexCount);
        for (AbstractVectorIndex index : indexOrdinals) {
            out.writeString(index.indexName);
            writeIndex(out, index);
        }

        // write KV part + index data associated with key to avoid redundantly sending key multiple times
        var entryIt = storage.getRecordStore().mutationTolerantIterator();
        while (entryIt.hasNext()) {
            var entry = entryIt.next();
            IOUtil.writeData(out, entry.getKey());
            IOUtil.writeData(out, entry.getValue().getValue());
            for (AbstractVectorIndex ordinal : indexOrdinals) {
                writeIndexEntry(out, ordinal, entry.getKey());
            }
        }
    }

    private static void writeIndexEntry(ObjectDataOutput out, AbstractVectorIndex index, Data key) throws IOException {
        index.writeKeyToNodeIdMapping(out, key);
    }

    private static void writeIndex(ObjectDataOutput out, AbstractVectorIndex index) throws IOException {
        out.writeInt(index.idGenerator);

        // vectors
        index.vectorsSupplier.writeData(out);

        // keyToNodeId mapping is written alongside KV part

        // index
        index.indexBuilder.getGraph().save(out);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        dataMap = SerializationUtil.readMapStringKey(in, i -> {
            CollectionReplicationStateHolder collectionState = new CollectionReplicationStateHolder();
            collectionState.readData(i);
            return collectionState;
        });
    }

    public void apply(VectorCollectionService service, int partitionId) {
        for (var collectionState : dataMap.entrySet()) {
            String vectorCollectionName = collectionState.getKey();

            // Previous storage will not be needed anymore - it is being replaced with just received data
            // This should not happen if this member is the current owner of the partition.
            // Data loss would be possible if original storage contained data.
            service.destroyStorage(vectorCollectionName, partitionId);

            // Storage is made visible only after it is fully ready. Concurrent offloaded searches
            // should not be able to see storage which is just being migrated into this member.
            var storage = service.createStorage(vectorCollectionName, partitionId);
            storage.resetState(collectionState.getValue());
            service.attachStorage(storage);
        }
    }

    @Override
    public int getFactoryId() {
        return VectorCollectionSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return VectorCollectionSerializerHook.REPLICATION_STATE_HOLDER;
    }

    // visible for testing
    public Map<String, VectorCollectionStorage> getStorageMap() {
        return Collections.unmodifiableMap(storageMap);
    }

    static class RandomAccessReaderAdapter implements RandomAccessReader {

        private final ObjectDataInput in;

        RandomAccessReaderAdapter(ObjectDataInput in) {
            this.in = in;
        }

        @Override
        public void seek(long l) throws IOException {
            throw new UnsupportedOperationException("only used with the on-disk graph index");
        }

        @Override
        public long getPosition() {
            throw new UnsupportedOperationException("only used with the on-disk graph index");
        }

        @Override
        public int readInt() throws IOException {
            return in.readInt();
        }

        @Override
        public float readFloat() throws IOException {
            return in.readFloat();
        }

        @Override
        public void readFully(byte[] bytes) throws IOException {
            in.readFully(bytes);
        }

        @Override
        public void readFully(ByteBuffer byteBuffer) throws IOException {
            throw new UnsupportedOperationException("only used with native memory");
        }

        @Override
        public void readFully(float[] floats) throws IOException {
            throw new UnsupportedOperationException("only used with on-disk graph and quantization");
        }

        @Override
        public void readFully(long[] longs) throws IOException {
            throw new UnsupportedOperationException("only used with binary quantization vectors");
        }

        @Override
        public void read(int[] ints, int offset, int count) throws IOException {
            throw new UnsupportedOperationException("only used with on-disk graph");
        }

        @Override
        public void read(float[] floats, int offset, int count) throws IOException {
            int sizeInBytes = count * FLOAT_SIZE_IN_BYTES;
            var bytes = new byte[sizeInBytes];
            in.readFully(bytes, offset, sizeInBytes);
            ByteBuffer.wrap(bytes, offset, sizeInBytes).order(in.getByteOrder()).asFloatBuffer().get(floats);
        }

        @Override
        public void close() throws IOException {
            // this is just a wrapper, nothing to do here
        }
    }
}
