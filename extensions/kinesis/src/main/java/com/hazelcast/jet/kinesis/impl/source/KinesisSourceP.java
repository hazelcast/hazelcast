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

package com.hazelcast.jet.kinesis.impl.source;

import com.amazonaws.services.kinesis.AmazonKinesisAsync;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.Shard;
import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.internal.metrics.DynamicMetricsProvider;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsCollectionContext;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.BroadcastKey;
import com.hazelcast.jet.core.EventTimeMapper;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.retry.RetryStrategy;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.hazelcast.jet.Traversers.traverseStream;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.core.BroadcastKey.broadcastKey;
import static com.hazelcast.jet.impl.util.Util.toLocalTime;
import static com.hazelcast.jet.kinesis.impl.KinesisUtil.shardBelongsToRange;

public class KinesisSourceP<T> extends AbstractProcessor implements DynamicMetricsProvider {

    @Nonnull
    private final AmazonKinesisAsync kinesis;
    @Nonnull
    private final String stream;
    @Nonnull
    private final EventTimeMapper<? super T> eventTimeMapper;
    @Nonnull
    private final HashRange memberHashRange;
    @Nonnull
    private final HashRange processorHashRange;
    @Nonnull
    private final ShardStates shardStates = new ShardStates();
    @Nonnull
    private final ShardQueue shardQueue;
    @Nullable
    private final RangeMonitor monitor;
    @Nonnull
    private final List<ShardReader> shardReaders = new ArrayList<>();
    @Nonnull
    private final RetryStrategy retryStrategy;
    @Nonnull
    private final InitialShardIterators initialShardIterators;
    private final BiFunctionEx<? super Record, ? super Shard, ? extends T> projectionFn;

    private int id;
    private ILogger logger;

    private Traverser<Object> traverser = Traversers.empty();
    private Traverser<Entry<BroadcastKey<String>, ShardState>> snapshotTraverser;

    private int nextReader;

    public KinesisSourceP(
            @Nonnull AmazonKinesisAsync kinesis,
            @Nonnull String stream,
            @Nonnull EventTimePolicy<? super T> eventTimePolicy,
            @Nonnull HashRange memberHashRange,
            @Nonnull HashRange processorHashRange,
            @Nonnull ShardQueue shardQueue,
            @Nullable RangeMonitor monitor,
            @Nonnull RetryStrategy retryStrategy,
            @Nonnull InitialShardIterators initialShardIterators,
            BiFunctionEx<? super Record, ? super Shard, ? extends T> projectionFn
            ) {
        this.kinesis = Objects.requireNonNull(kinesis, "kinesis");
        this.stream = Objects.requireNonNull(stream, "stream");
        this.eventTimeMapper = new EventTimeMapper<>(eventTimePolicy);
        this.memberHashRange = Objects.requireNonNull(memberHashRange, "memberHashRange");
        this.processorHashRange = Objects.requireNonNull(processorHashRange, "processorHashRange");
        this.shardQueue = shardQueue;
        this.monitor = monitor;
        this.retryStrategy = retryStrategy;
        this.initialShardIterators = initialShardIterators;
        this.projectionFn = projectionFn;
    }

    @Override
    protected void init(@Nonnull Context context) throws Exception {
        super.init(context);

        logger = context.logger();
        id = context.globalProcessorIndex();

        if (logger.isFineEnabled()) {
            logger.fine("Processor " + id + " handles " + processorHashRange);
        }
    }

    @Override
    public boolean complete() {
        if (!emitFromTraverser(traverser)) {
            return false;
        }

        runMonitor();
        checkForNewShards();
        runReaders();

        return false;
    }

    private void runMonitor() {
        if (monitor != null) {
            monitor.run();
        }
    }

    private void checkForNewShards() {
        String shardId = shardQueue.pollExpired();
        if (shardId != null) {
            shardStates.remove(shardId);
            return;
        }
        Shard shard = shardQueue.pollAdded();
        if (shard != null) {
            addShardReader(shard);
        }
    }

    private void runReaders() {
        if (!shardReaders.isEmpty()) {
            long currentTime = System.nanoTime();
            for (int i = 0; i < shardReaders.size(); i++) {
                int currentReader = nextReader;
                ShardReader reader = shardReaders.get(currentReader);
                nextReader = incrementCircular(currentReader, shardReaders.size());

                ShardReader.Result result = reader.probe(currentTime);
                if (ShardReader.Result.HAS_DATA.equals(result)) {
                    Shard shard = reader.getShard();
                    traverser = reader.clearData()
                            .flatMap(record -> {
                                T payload = projectionFn.apply(record, shard);
                                return eventTimeMapper.flatMapEvent(
                                        payload,
                                        currentReader,
                                        record.getApproximateArrivalTimestamp().getTime());
                            });
                    Long watermark = eventTimeMapper.getWatermark(currentReader);
                    watermark = watermark < 0 ? null : watermark;
                    shardStates.update(shard, reader.getLastSeenSeqNo(), watermark);
                    emitFromTraverser(traverser);
                    return;
                } else if (ShardReader.Result.CLOSED.equals(result)) {
                    Shard shard = reader.getShard();
                    logger.info("Shard " + shard.getShardId() + " of stream " + stream + " closed");
                    shardStates.close(shard);
                    nextReader = 0;
                    traverser = removeShardReader(currentReader);
                    emitFromTraverser(traverser);
                    return;
                }
            }
        }

        traverser = eventTimeMapper.flatMapIdle();
        emitFromTraverser(traverser);
    }

    @Override
    public boolean saveToSnapshot() {
        if (!emitFromTraverser(traverser)) {
            return false;
        }

        if (snapshotTraverser == null) {
            snapshotTraverser = traverseStream(shardStates.snapshotEntries())
                    .onFirstNull(() -> {
                        snapshotTraverser = null;
                        if (getLogger().isFinestEnabled()) {
                            getLogger().finest("Finished saving snapshot. Saved shard states: " + shardStates);
                        }
                    });
        }
        return emitFromTraverserToSnapshot(snapshotTraverser);
    }

    @Override
    protected void restoreFromSnapshot(@Nonnull Object key, @Nonnull Object value) {
        ShardState state = (ShardState) value;

        String shardId = state.getShardId();
        BigInteger startingHashKey = state.getStartingHashKey();

        if (shardBelongsToRange(startingHashKey, processorHashRange)) {
            boolean closed = state.isClosed();
            String seqNo = state.getLastSeenSeqNo();
            Long watermark = state.getWatermark();
            shardStates.update(shardId, startingHashKey, closed, seqNo, watermark);
        }

        if (monitor != null && shardBelongsToRange(startingHashKey, memberHashRange)) {
            monitor.addKnownShard(shardId, startingHashKey);
        }
    }

    private void addShardReader(Shard shard) {
        String shardId = shard.getShardId();
        ShardState shardState = shardStates.get(shardId);
        if (!shardState.isClosed()) {
            int readerIndex = shardReaders.size();

            String lastSeenSeqNo = shardState.getLastSeenSeqNo();
            shardReaders.add(initShardReader(shard, lastSeenSeqNo));

            eventTimeMapper.addPartitions(1);

            Long watermark = shardState.getWatermark();
            if (watermark != null) {
                eventTimeMapper.restoreWatermark(readerIndex, watermark);
            }
        }
    }

    private Traverser<Object> removeShardReader(int index) {
        shardReaders.remove(index);
        return eventTimeMapper.removePartition(index);
    }

    @Nonnull
    private ShardReader initShardReader(Shard shard, String lastSeenSeqNo) {
        logger.info("Shard " + shard.getShardId() + " of stream " + stream + " assigned to processor instance " + id);
        return new ShardReader(kinesis, stream, shard, lastSeenSeqNo, retryStrategy, initialShardIterators, logger);
    }

    @Override
    public void provideDynamicMetrics(MetricDescriptor descriptor, MetricsCollectionContext context) {
        for (ShardReader shardReader : shardReaders) {
            shardReader.provideDynamicMetrics(descriptor.copy(), context);
        }
    }

    private static int incrementCircular(int v, int limit) {
        v++;
        if (v == limit) {
            v = 0;
        }
        return v;
    }

    private static class ShardStates {

        private final Map<String, ShardState> states = new HashMap<>();

        void update(Shard shard, String seqNo, Long watermark) {
            BigInteger startingHashKey = new BigInteger(shard.getHashKeyRange().getStartingHashKey());
            update(shard.getShardId(), startingHashKey, false, seqNo, watermark);
        }

        void close(Shard shard) {
            BigInteger startingHashKey = new BigInteger(shard.getHashKeyRange().getStartingHashKey());
            update(shard.getShardId(), startingHashKey, true, null, null);
        }

        void update(String shardId, BigInteger startingHashKey, boolean closed, String lastSeenSeqNo, Long watermark) {
            ShardState state = states.computeIfAbsent(shardId, ShardState::new);
            state.setStartingHashKey(startingHashKey);
            state.setClosed(closed);
            state.setLastSeenSeqNo(lastSeenSeqNo);
            state.setWatermark(watermark);
        }

        void remove(String shardId) {
            ShardState state = states.remove(shardId);
            if (state == null) {
                throw new JetException("Removing insistent state for shard " + shardId + ", shouldn't happen");
            }
        }

        ShardState get(String shardId) {
            return states.getOrDefault(shardId, ShardState.EMPTY);
        }

        Stream<Entry<BroadcastKey<String>, ShardState>> snapshotEntries() {
            return states.entrySet().stream()
                    .map(e -> entry(broadcastKey(e.getKey()), e.getValue()));
        }

        @Override
        public String toString() {
            return states.values().stream().map(ShardState::toString).collect(Collectors.joining(", "));
        }

    }

    public static final class ShardState implements IdentifiedDataSerializable {

        public static final ShardState EMPTY = new ShardState();

        private String shardId;
        private BigInteger startingHashKey;
        private boolean closed;
        private String lastSeenSeqNo;
        private Long watermark;

        public ShardState() {
        }

        public ShardState(String shardId) {
            this.shardId = shardId;
        }

        public String getShardId() {
            return shardId;
        }

        public void setShardId(String shardId) {
            this.shardId = shardId;
        }

        public BigInteger getStartingHashKey() {
            return startingHashKey;
        }

        public void setStartingHashKey(BigInteger startingHashKey) {
            this.startingHashKey = startingHashKey;
        }

        public boolean isClosed() {
            return closed;
        }

        public void setClosed(boolean closed) {
            this.closed = closed;
        }

        public String getLastSeenSeqNo() {
            return lastSeenSeqNo;
        }

        public void setLastSeenSeqNo(String lastSeenSeqNo) {
            this.lastSeenSeqNo = lastSeenSeqNo;
        }

        public Long getWatermark() {
            return watermark;
        }

        public void setWatermark(Long watermark) {
            this.watermark = watermark;
        }

        @Override
        public int getFactoryId() {
            return KinesisDataSerializerHook.FACTORY_ID;
        }

        @Override
        public int getClassId() {
            return KinesisDataSerializerHook.KINESIS_SOURCE_P_SHARD_STATE;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeUTF(shardId);
            out.writeObject(startingHashKey);
            out.writeBoolean(closed);
            out.writeUTF(lastSeenSeqNo);
            out.writeObject(watermark);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            shardId = in.readUTF();
            startingHashKey = in.readObject();
            closed = in.readBoolean();
            lastSeenSeqNo = in.readUTF();
            watermark = in.readObject();
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();

            sb.append(shardId).append(": ");
            sb.append("startingHashKey=").append(startingHashKey);
            sb.append(", closed=").append(closed);

            if (!closed) {
                sb.append(", lastSeenSeqNo=").append(lastSeenSeqNo);
                if (watermark != null) {
                    sb.append(", watermark=").append(toLocalTime(watermark));
                }
            }

            return sb.toString();
        }
    }
}
