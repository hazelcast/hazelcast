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

package com.hazelcast.jet.impl.util;

import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.internal.nio.Bits;
import com.hazelcast.internal.partition.IPartitionService;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.internal.serialization.impl.SerializationConstants;
import com.hazelcast.jet.impl.JetServiceBackend;
import com.hazelcast.jet.impl.execution.SnapshotContext;
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.IMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.partition.PartitionAware;
import com.hazelcast.spi.impl.NodeEngine;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

public class AsyncSnapshotWriterImpl implements AsyncSnapshotWriter {

    public static final int DEFAULT_CHUNK_SIZE = 128 * 1024;

    final int usableChunkCapacity; // this includes the serialization header for byte[], but not the terminator
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
    private final SnapshotContext snapshotContext;
    private final String vertexName;
    private final int memberCount;
    private IMap<SnapshotDataKey, Object> currentMap;
    private long currentSnapshotId;
    private final AtomicReference<Throwable> firstError = new AtomicReference<>();
    private final AtomicInteger numActiveFlushes = new AtomicInteger();

    // stats
    private long totalKeys;
    private long totalChunks;
    private long totalPayloadBytes;

    private BiConsumer<Object, Throwable> putResponseConsumer = this::consumePutResponse;

    public AsyncSnapshotWriterImpl(NodeEngine nodeEngine,
                                   SnapshotContext snapshotContext,
                                   String vertexName,
                                   int memberIndex,
                                   int memberCount,
                                   SerializationService serializationService) {
        this(DEFAULT_CHUNK_SIZE, nodeEngine, snapshotContext, vertexName, memberIndex, memberCount, serializationService);
    }

    // for test
    AsyncSnapshotWriterImpl(int chunkSize,
                            NodeEngine nodeEngine,
                            SnapshotContext snapshotContext,
                            String vertexName,
                            int memberIndex,
                            int memberCount,
                            SerializationService serializationService) {
        if (Integer.bitCount(chunkSize) != 1) {
            throw new IllegalArgumentException("chunkSize must be a power of two, but is " + chunkSize);
        }
        this.nodeEngine = nodeEngine;
        this.partitionService = nodeEngine.getPartitionService();
        this.logger = nodeEngine.getLogger(getClass());
        this.snapshotContext = snapshotContext;
        this.vertexName = vertexName;
        this.memberCount = memberCount;
        currentSnapshotId = snapshotContext.currentSnapshotId();

        useBigEndian = !nodeEngine.getHazelcastInstance().getConfig().getSerializationConfig().isUseNativeByteOrder()
                || ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN;
        Bits.writeInt(serializedByteArrayHeader, Bits.INT_SIZE_IN_BYTES, SerializationConstants.CONSTANT_TYPE_BYTE_ARRAY,
                useBigEndian);

        buffers = createAndInitBuffers(chunkSize, partitionService.getPartitionCount(), serializedByteArrayHeader);
        JetServiceBackend jetServiceBackend = nodeEngine.getService(JetServiceBackend.SERVICE_NAME);
        this.partitionKeys = jetServiceBackend.getSharedPartitionKeys();
        this.partitionSequence = memberIndex;

        this.numConcurrentAsyncOps = jetServiceBackend.numConcurrentAsyncOps();

        byte[] valueTerminatorWithHeader = serializationService.toData(SnapshotDataValueTerminator.INSTANCE).toByteArray();
        valueTerminator = Arrays.copyOfRange(valueTerminatorWithHeader, HeapData.TYPE_OFFSET,
                valueTerminatorWithHeader.length);
        usableChunkCapacity = chunkSize - valueTerminator.length - serializedByteArrayHeader.length;
        if (usableChunkCapacity <= 0) {
            throw new IllegalArgumentException("too small chunk size: " + chunkSize);
        }
    }

    private static CustomByteArrayOutputStream[] createAndInitBuffers(
            int chunkSize,
            int partitionCount,
            byte[] serializedByteArrayHeader
    ) {
        CustomByteArrayOutputStream[] buffers = new CustomByteArrayOutputStream[partitionCount];
        for (int i = 0; i < buffers.length; i++) {
            buffers[i] = new CustomByteArrayOutputStream(chunkSize);
            buffers[i].write(serializedByteArrayHeader, 0, serializedByteArrayHeader.length);
        }
        return buffers;
    }

    private void consumePutResponse(Object response, Throwable throwable) {
        try {
            assert response == null : "put operation overwrote a previous value: " + response;
        } catch (AssertionError e) {
            throwable = e;
        }
        if (throwable != null) {
            logger.severe("Error writing to snapshot map", throwable);
            firstError.compareAndSet(null, throwable);
        }
        numActiveFlushes.decrementAndGet();
        numConcurrentAsyncOps.decrementAndGet();
    }

    @Override
    @CheckReturnValue
    public boolean offer(Entry<? extends Data, ? extends Data> entry) {
        int partitionId = partitionService.getPartitionId(entry.getKey());
        int length = entry.getKey().totalSize() + entry.getValue().totalSize() - 2 * HeapData.TYPE_OFFSET;

        // if the entry is larger than usableChunkSize, send it in its own chunk. We avoid adding it to the
        // ByteArrayOutputStream since it would expand it beyond its maximum capacity.
        if (length > usableChunkCapacity) {
            return putAsyncToMap(partitionId, () -> {
                byte[] data = new byte[serializedByteArrayHeader.length + length + valueTerminator.length];
                totalKeys++;
                int offset = 0;
                System.arraycopy(serializedByteArrayHeader, 0, data, offset, serializedByteArrayHeader.length);
                offset += serializedByteArrayHeader.length - Bits.INT_SIZE_IN_BYTES;

                Bits.writeInt(data, offset, length + valueTerminator.length, useBigEndian);
                offset += Bits.INT_SIZE_IN_BYTES;

                copyWithoutHeader(entry.getKey(), data, offset);
                offset += entry.getKey().totalSize() - HeapData.TYPE_OFFSET;

                copyWithoutHeader(entry.getValue(), data, offset);
                offset += entry.getValue().totalSize() - HeapData.TYPE_OFFSET;

                System.arraycopy(valueTerminator, 0, data, offset, valueTerminator.length);

                return new HeapData(data);
            });
        }

        // if the buffer after adding this entry and terminator would exceed the capacity limit, flush it first
        CustomByteArrayOutputStream buffer = buffers[partitionId];
        if (buffer.size() + length + valueTerminator.length > buffer.capacityLimit && !flushPartition(partitionId)) {
            return false;
        }

        // append to buffer
        writeWithoutHeader(entry.getKey(), buffer);
        writeWithoutHeader(entry.getValue(), buffer);
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
    private boolean flushPartition(int partitionId) {
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
        if (!initCurrentMap()) {
            return false;
        }

        if (!Util.tryIncrement(numConcurrentAsyncOps, 1, JetServiceBackend.MAX_PARALLEL_ASYNC_OPS)) {
            return false;
        }
        try {
            // we put a Data instance to the map directly to avoid the serialization of the byte array
            Data data = dataSupplier.get();
            totalPayloadBytes += data.dataSize();
            totalChunks++;
            CompletableFuture<Object> future = currentMap.putAsync(
                    new SnapshotDataKey(partitionKeys[partitionId], currentSnapshotId, vertexName, partitionSequence),
                    data).toCompletableFuture();
            partitionSequence += memberCount;
            future.whenComplete(putResponseConsumer);
            numActiveFlushes.incrementAndGet();
        } catch (HazelcastInstanceNotActiveException ignored) {
            return false;
        }
        return true;
    }

    private boolean initCurrentMap() {
        if (currentMap == null) {
            String mapName = snapshotContext.currentMapName();
            if (mapName == null) {
                return false;
            }
            currentMap = nodeEngine.getHazelcastInstance().getMap(mapName);
            this.currentSnapshotId = snapshotContext.currentSnapshotId();
        }
        return true;
    }

    /**
     * Flush all partitions and reset current map. No further items can be
     * offered until new snapshot is seen in {@link #snapshotContext}.
     *
     * @return {@code true} on success, {@code false} if we weren't able to
     * flush some partitions due to the limit on number of parallel async ops.
     */
    @Override
    @CheckReturnValue
    public boolean flushAndResetMap() {
        if (!initCurrentMap()) {
            return false;
        }

        for (int i = 0; i < buffers.length; i++) {
            if (!flushPartition(i)) {
                return false;
            }
        }

        // we're done
        currentMap = null;
        if (logger.isFineEnabled()) {
            logger.fine(String.format("Stats for %s: keys=%,d, chunks=%,d, bytes=%,d",
                    vertexName, totalKeys, totalChunks, totalPayloadBytes));
        }
        return true;
    }

    @Override
    public void resetStats() {
        totalKeys = totalChunks = totalPayloadBytes = 0;
    }

    @Override
    public boolean hasPendingAsyncOps() {
        return numActiveFlushes.get() > 0;
    }

    @Override
    public Throwable getError() {
        return firstError.getAndSet(null);
    }

    @Override
    public boolean isEmpty() {
        return numActiveFlushes.get() == 0 && Arrays.stream(buffers).allMatch(this::containsOnlyHeader);
    }

    int partitionKey(int partitionId) {
        return partitionKeys[partitionId];
    }

    public static final class SnapshotDataKey implements IdentifiedDataSerializable, PartitionAware {
        private int partitionKey;
        private long snapshotId;
        private String vertexName;
        private int sequence;

        // for deserialization
        public SnapshotDataKey() {
        }

        public SnapshotDataKey(int partitionKey, long snapshotId, String vertexName, int sequence) {
            this.partitionKey = partitionKey;
            this.snapshotId = snapshotId;
            this.vertexName = vertexName;
            this.sequence = sequence;
        }

        @Override
        public Object getPartitionKey() {
            return partitionKey;
        }

        public long snapshotId() {
            return snapshotId;
        }

        public String vertexName() {
            return vertexName;
        }

        @Override
        public String toString() {
            return "SnapshotDataKey{" +
                    "partitionKey=" + partitionKey +
                    ", snapshotId=" + snapshotId +
                    ", vertexName='" + vertexName + '\'' +
                    ", sequence=" + sequence +
                    '}';
        }

        @Override
        public int getFactoryId() {
            return JetInitDataSerializerHook.FACTORY_ID;
        }

        @Override
        public int getClassId() {
            return JetInitDataSerializerHook.ASYNC_SNAPSHOT_WRITER_SNAPSHOT_DATA_KEY;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeInt(partitionKey);
            out.writeLong(snapshotId);
            out.writeUTF(vertexName);
            out.writeInt(sequence);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            partitionKey = in.readInt();
            snapshotId = in.readLong();
            vertexName = in.readUTF();
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
                    snapshotId == that.snapshotId &&
                    sequence == that.sequence &&
                    Objects.equals(vertexName, that.vertexName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(partitionKey, snapshotId, vertexName, sequence);
        }
    }

    public static final class SnapshotDataValueTerminator implements IdentifiedDataSerializable {

        public static final IdentifiedDataSerializable INSTANCE = new SnapshotDataValueTerminator();

        private SnapshotDataValueTerminator() {
        }

        @Override
        public int getFactoryId() {
            return JetInitDataSerializerHook.FACTORY_ID;
        }

        @Override
        public int getClassId() {
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
