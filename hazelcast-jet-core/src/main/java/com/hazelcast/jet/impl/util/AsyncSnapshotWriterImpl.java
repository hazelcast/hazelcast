/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.util;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.core.PartitionAware;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.internal.serialization.impl.SerializationConstants;
import com.hazelcast.jet.impl.JetService;
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Bits;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.partition.IPartitionService;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class AsyncSnapshotWriterImpl implements AsyncSnapshotWriter {

    private static final int DEFAULT_CHUNK_SIZE = 128 * 1024;

    final int usableChunkSize; // this includes the serialization header for byte[], but not the terminator
    final byte[] serializedByteArrayHeader = new byte[3 * Bits.INT_SIZE_IN_BYTES];
    final byte[] valueTerminator;
    final AtomicInteger numConcurrentAsyncOps;

    private final IPartitionService partitionService;

    private final CustomByteArrayOutputStream[] buffers;
    private final int[] partitionKeys;
    private int partitionSequence;
    private final ILogger logger;
    private final NodeEngine nodeEngine;
    private final boolean useBigEndian;
    private final int memberCount;
    private Supplier<IMap<SnapshotDataKey, byte[]>> currentMap;
    private final AtomicReference<Throwable> lastError = new AtomicReference<>();
    private final AtomicInteger numActiveFlushes = new AtomicInteger();

    // stats
    private long totalKeys;
    private long totalChunks;
    private long totalPayloadBytes;

    private final ExecutionCallback<Object> callback = new ExecutionCallback<Object>() {
        @Override
        public void onResponse(Object response) {
            assert response == null : "put operation overwrote a previous value: " + response;
            numActiveFlushes.decrementAndGet();
            numConcurrentAsyncOps.decrementAndGet();
        }

        @Override
        public void onFailure(Throwable t) {
            logger.severe("Error writing to snapshot map '" + currentMap.get().getName() + "'", t);
            lastError.compareAndSet(null, t);
            numActiveFlushes.decrementAndGet();
            numConcurrentAsyncOps.decrementAndGet();
        }
    };

    public AsyncSnapshotWriterImpl(NodeEngine nodeEngine, int memberIndex, int memberCount) {
        this(DEFAULT_CHUNK_SIZE, nodeEngine, memberIndex, memberCount);
    }

    // for test
    AsyncSnapshotWriterImpl(int chunkSize, NodeEngine nodeEngine, int memberIndex, int memberCount) {
        this.nodeEngine = nodeEngine;
        this.partitionService = nodeEngine.getPartitionService();
        this.logger = nodeEngine.getLogger(getClass());
        this.memberCount = memberCount;

        useBigEndian = !nodeEngine.getHazelcastInstance().getConfig().getSerializationConfig().isUseNativeByteOrder()
                || ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN;

        Bits.writeInt(serializedByteArrayHeader, Bits.INT_SIZE_IN_BYTES, SerializationConstants.CONSTANT_TYPE_BYTE_ARRAY,
                useBigEndian);

        buffers = new CustomByteArrayOutputStream[partitionService.getPartitionCount()];
        for (int i = 0; i < buffers.length; i++) {
            buffers[i] = new CustomByteArrayOutputStream(chunkSize);
            buffers[i].write(serializedByteArrayHeader, 0, serializedByteArrayHeader.length);
        }

        JetService jetService = nodeEngine.getService(JetService.SERVICE_NAME);
        this.partitionKeys = jetService.getSharedPartitionKeys();
        this.partitionSequence = memberIndex;

        this.numConcurrentAsyncOps = jetService.numConcurrentAsyncOps();

        byte[] valueTerminatorWithHeader = nodeEngine.getSerializationService().toData(
                SnapshotDataValueTerminator.INSTANCE).toByteArray();
        valueTerminator = Arrays.copyOfRange(valueTerminatorWithHeader, HeapData.TYPE_OFFSET,
                valueTerminatorWithHeader.length);
        usableChunkSize = chunkSize - valueTerminator.length;
    }

    @Override
    public void setCurrentMap(String mapName) {
        assert isEmpty() : "writer not empty";

        if (currentMap != null && logger.isFineEnabled()) {
            logger.fine(String.format("Stats for %s: keys=%,d, chunks=%,d, bytes=%,d",
                    currentMap.get().getName(), totalKeys, totalChunks, totalPayloadBytes));
        }

        // lazily get map, since it forces creation of actual map upon request
        currentMap = Util.memoize(() -> nodeEngine.getHazelcastInstance().getMap(mapName));

        // reset stats
        totalKeys = totalChunks = totalPayloadBytes = 0;
    }

    @Override
    @CheckReturnValue
    public boolean offer(Entry<? extends Data, ? extends Data> entry) {
        int partitionId = partitionService.getPartitionId(entry.getKey());
        int length = entry.getKey().totalSize() + entry.getValue().totalSize() - 2 * HeapData.TYPE_OFFSET;

        // if single entry is larger than usableChunkSize, send it alone. We avoid adding it to the ByteArrayOutputStream,
        // since it will grow beyond maximum capacity and never shrink again.
        if (length > usableChunkSize) {
            return putAsyncToMap(partitionId, () -> {
                byte[] data = new byte[serializedByteArrayHeader.length + length + valueTerminator.length];
                int offset = 0;
                System.arraycopy(serializedByteArrayHeader, 0, data, offset, serializedByteArrayHeader.length);
                offset += serializedByteArrayHeader.length - Bits.INT_SIZE_IN_BYTES;

                Bits.writeInt(data, offset, length + valueTerminator.length, useBigEndian);
                offset += Bits.INT_SIZE_IN_BYTES;

                copyWithoutHeader(entry.getKey(), data, offset);
                offset += entry.getKey().totalSize() - HeapData.TYPE_OFFSET;

                copyWithoutHeader(entry.getValue(), data, offset);
                System.arraycopy(valueTerminator, 0, data, length, valueTerminator.length);

                return new HeapData(data);
            });
        }

        // if the buffer will exceed usableChunkSize after adding this entry, flush it first
        if (buffers[partitionId].size() + length > usableChunkSize && !flush(partitionId)) {
            return false;
        }

        // append to buffer
        writeWithoutHeader(entry.getKey(), buffers[partitionId]);
        writeWithoutHeader(entry.getValue(), buffers[partitionId]);
        totalKeys++;
        return true;
    }

    private void copyWithoutHeader(Data src, byte[] dst, int dstOffset) {
        byte[] bytes = src.toByteArray();
        System.arraycopy(bytes, HeapData.TYPE_OFFSET, dst, dstOffset, bytes.length - HeapData.TYPE_OFFSET);
    }

    private void writeWithoutHeader(Data src, OutputStream dst) {
        byte[] bytes = src.toByteArray();
        try {
            dst.write(bytes, HeapData.TYPE_OFFSET, bytes.length - HeapData.TYPE_OFFSET);
        } catch (IOException e) {
            throw new RuntimeException(e); // should never happen
        }
    }

    @CheckReturnValue
    private boolean flush(int partitionId) {
        return containsOnlyHeader(buffers[partitionId])
                || putAsyncToMap(partitionId, () -> getBufferContentsAndClear(buffers[partitionId]));
    }

    private boolean containsOnlyHeader(CustomByteArrayOutputStream buffer) {
        return buffer.size() == serializedByteArrayHeader.length;
    }

    private Data getBufferContentsAndClear(CustomByteArrayOutputStream buffer) {
        buffer.write(valueTerminator, 0, valueTerminator.length);
        final byte[] data = buffer.toByteArray();
        updateSerializedBytesLength(data);
        totalPayloadBytes += buffer.size;
        totalChunks++;
        buffer.reset();
        buffer.write(serializedByteArrayHeader, 0, serializedByteArrayHeader.length);
        return new HeapData(data);
    }

    private void updateSerializedBytesLength(byte[] data) {
        // update the array length at the beginning of the buffer
        // the length is the third int value in the serialized data
        Bits.writeInt(data, 2 * Bits.INT_SIZE_IN_BYTES, data.length - serializedByteArrayHeader.length, useBigEndian);
    }

    @CheckReturnValue
    private boolean putAsyncToMap(int partitionId, Supplier<Data> dataSupplier) {
        if (!Util.tryIncrement(numConcurrentAsyncOps, 1, JetService.MAX_PARALLEL_ASYNC_OPS)) {
            return false;
        }

        // we put a Data instance to the map directly to avoid the serialization of the byte array
        ICompletableFuture<Object> future = ((IMap) currentMap.get()).putAsync(
                new SnapshotDataKey(partitionKeys[partitionId], partitionSequence), dataSupplier.get());
        partitionSequence += memberCount;
        future.andThen(callback);
        numActiveFlushes.incrementAndGet();
        return true;
    }

    /**
     * Flush all partitions.
     *
     * @return {@code true} on success, {@code false} if we weren't able to
     * flush some partitions due to the limit on number of parallel async ops.
     */
    @Override
    @CheckReturnValue
    public boolean flush() {
        for (int i = 0; i < buffers.length; i++) {
            if (!flush(i)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean hasPendingAsyncOps() {
        return numActiveFlushes.get() > 0;
    }

    @Override
    public Throwable getError() {
        return lastError.getAndSet(null);
    }

    @Override
    public boolean isEmpty() {
        return numActiveFlushes.get() == 0 && Arrays.stream(buffers).allMatch(this::containsOnlyHeader);
    }

    int partitionKey(int partitionId) {
        return partitionKeys[partitionId];
    }

    public static final class SnapshotDataKey implements IdentifiedDataSerializable, PartitionAware {
        int partitionKey;
        int sequence;

        // for deserialization
        public SnapshotDataKey() {
        }

        SnapshotDataKey(int partitionKey, int sequence) {
            this.partitionKey = partitionKey;
            this.sequence = sequence;
        }

        @Override
        public Object getPartitionKey() {
            return partitionKey;
        }

        @Override
        public String toString() {
            return "SnapshotDataKey{partitionKey=" + partitionKey + ", sequence=" + sequence + '}';
        }

        @Override
        public int getFactoryId() {
            return JetInitDataSerializerHook.FACTORY_ID;
        }

        @Override
        public int getId() {
            return JetInitDataSerializerHook.ASYNC_SNAPSHOT_WRITER_SNAPSHOT_DATA_KEY;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeInt(partitionKey);
            out.writeInt(sequence);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            partitionKey = in.readInt();
            sequence = in.readInt();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SnapshotDataKey that = (SnapshotDataKey) o;
            return partitionKey == that.partitionKey &&
                    sequence == that.sequence;
        }

        @Override
        public int hashCode() {
            return Objects.hash(partitionKey, sequence);
        }
    }

    public static final class SnapshotDataValueTerminator implements IdentifiedDataSerializable {

        public static final IdentifiedDataSerializable INSTANCE = new SnapshotDataValueTerminator();

        private SnapshotDataValueTerminator() { }

        @Override
        public int getFactoryId() {
            return JetInitDataSerializerHook.FACTORY_ID;
        }

        @Override
        public int getId() {
            return JetInitDataSerializerHook.ASYNC_SNAPSHOT_WRITER_SNAPSHOT_DATA_VALUE_TERMINATOR;
        }

        @Override
        public void writeData(ObjectDataOutput out) {
        }

        @Override
        public void readData(ObjectDataInput in) {
        }
    }

    /**
     * Non-synchronized variant of {@code java.io.ByteArrayOutputStream} with capacity limit.
     */
    static class CustomByteArrayOutputStream extends OutputStream {

        private static final byte[] EMPTY_BYTE_ARRAY = {};

        private byte[] data;
        private int size;
        private int capacityLimit;

        CustomByteArrayOutputStream(int capacityLimit) {
            this.capacityLimit = capacityLimit;
            // initial capacity is 0. It will take several reallocations to reach typical capacity,
            // but it's also common to remain at 0 - for partitions not assigned to us.
            data = EMPTY_BYTE_ARRAY;
        }

        @Override
        public void write(int b) {
            ensureCapacity(size + 1);
            data[size] = (byte) b;
            size++;
        }

        public void write(@Nonnull byte[] b, int off, int len) {
            if ((off < 0) || (off > b.length) || (len < 0) || ((off + len) - b.length > 0)) {
                throw new IndexOutOfBoundsException("off=" + off + ", len=" + len);
            }
            ensureCapacity(size + len);
            System.arraycopy(b, off, data, size, len);
            size += len;
        }

        private void ensureCapacity(int minCapacity) {
            if (minCapacity - data.length > 0) {
                int newCapacity = data.length;
                do {
                    newCapacity = Math.max(1, newCapacity << 1);
                } while (newCapacity - minCapacity < 0);
                if (newCapacity - capacityLimit > 0) {
                    throw new IllegalStateException("buffer full");
                }
                data = Arrays.copyOf(data, newCapacity);
            }
        }

        void reset() {
            size = 0;
        }

        @Nonnull
        byte[] toByteArray() {
            return Arrays.copyOf(data, size);
        }

        int size() {
            return size;
        }
    }

    @Override
    public long getTotalPayloadBytes() {
        return totalPayloadBytes;
    }

    @Override
    public long getTotalKeys() {
        return totalKeys;
    }

    @Override
    public long getTotalChunks() {
        return totalChunks;
    }
}
