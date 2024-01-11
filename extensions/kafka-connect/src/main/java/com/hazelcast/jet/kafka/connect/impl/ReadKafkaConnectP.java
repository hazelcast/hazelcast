/*
 * Copyright 2024 Hazelcast Inc.
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

package com.hazelcast.jet.kafka.connect.impl;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.internal.metrics.DynamicMetricsProvider;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsCollectionContext;
import com.hazelcast.internal.util.Timer;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.BroadcastKey;
import com.hazelcast.jet.core.EventTimeMapper;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.kafka.connect.impl.processorsupplier.ReadKafkaConnectProcessorSupplier;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.impl.NodeEngineImpl;
import org.apache.kafka.connect.source.SourceRecord;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.KAFKA_CONNECT_PREFIX;
import static com.hazelcast.internal.metrics.impl.ProviderHelper.provide;
import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.core.BroadcastKey.broadcastKey;
import static com.hazelcast.jet.impl.util.Util.getNodeEngine;

public class ReadKafkaConnectP<T> extends AbstractProcessor implements DynamicMetricsProvider {
    private final ILogger logger = Logger.getLogger(ReadKafkaConnectP.class);
    private SourceConnectorWrapper sourceConnectorWrapper;
    private final EventTimeMapper<T> eventTimeMapper;
    private final FunctionEx<SourceRecord, T> projectionFn;
    private Properties propertiesFromUser;
    private boolean snapshotInProgress;
    private Traverser<Entry<BroadcastKey<String>, State>> snapshotTraverser;
    private boolean snapshotsEnabled;
    private Traverser<?> traverser = Traversers.empty();
    private final LocalKafkaConnectStatsImpl localKafkaConnectStats = new LocalKafkaConnectStatsImpl();
    private int globalProcessorIndex;
    private int localProcessorIndex;
    private int processorOrder;
    private final AtomicInteger counter = new AtomicInteger();
    private boolean active = true;

    public ReadKafkaConnectP(@Nonnull EventTimePolicy<? super T> eventTimePolicy,
                             @Nonnull FunctionEx<SourceRecord, T> projectionFn) {
        Objects.requireNonNull(eventTimePolicy, "eventTimePolicy is required");
        Objects.requireNonNull(projectionFn, "projectionFn is required");
        this.eventTimeMapper = new EventTimeMapper<>(eventTimePolicy);
        this.projectionFn = projectionFn;
        eventTimeMapper.addPartitions(1);
    }

    public void setPropertiesFromUser(Properties propertiesFromUser) {
        this.propertiesFromUser = propertiesFromUser;
    }

    // Used for testing
    public void setSourceConnectorWrapper(SourceConnectorWrapper sourceConnectorWrapper) {
        this.sourceConnectorWrapper = sourceConnectorWrapper;
    }

    public void setProcessorOrder(int processorOrder) {
        this.processorOrder = processorOrder;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    @Override
    protected void init(@Nonnull Context context) {
        globalProcessorIndex = context.globalProcessorIndex();
        localProcessorIndex = context.localProcessorIndex();
        logger.info("Entering ReadKafkaConnectP init processorOrder=" +
                         processorOrder + " localProcessorIndex= " + localProcessorIndex);

        if (sourceConnectorWrapper == null) {
            sourceConnectorWrapper = new SourceConnectorWrapper(propertiesFromUser, processorOrder, context);
            sourceConnectorWrapper.setActiveStatusSetter(this::setActive);
        }
        snapshotsEnabled = context.snapshottingEnabled();
        NodeEngineImpl nodeEngine = getNodeEngine(context.hazelcastInstance());
        nodeEngine.getMetricsRegistry().registerDynamicMetricsProvider(this);
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    @Override
    public boolean complete() {
        if (!active) {
            return false;
        }
        // The taskConfig has not been received yet. We can not complete the processor
        if (!configurationReceived()) {
            return false;
        }
        if (snapshotInProgress) {
            return false;
        }
        if (!emitFromTraverser(traverser)) {
            return false;
        }

        long start = Timer.nanos();
        List<SourceRecord> sourceRecords = sourceConnectorWrapper.poll();

        logger.info("Total polled record size " + counter.addAndGet(sourceRecords.size()));

        long durationInNanos = Timer.nanosElapsed(start);
        localKafkaConnectStats.addSourceRecordPollDuration(Duration.ofNanos(durationInNanos));
        localKafkaConnectStats.incrementSourceRecordPoll(sourceRecords.size());
        this.traverser = sourceRecords.isEmpty() ? eventTimeMapper.flatMapIdle() :
                traverseIterable(sourceRecords)
                        .flatMap(sourceRecord -> {
                            long eventTime;
                            if (sourceRecord.timestamp() == null) {
                                eventTime = EventTimeMapper.NO_NATIVE_TIME;
                            } else {
                                eventTime = sourceRecord.timestamp();
                            }
                            T projectedRecord = projectionFn.apply(sourceRecord);
                            sourceConnectorWrapper.commitRecord(sourceRecord);
                            return eventTimeMapper.flatMapEvent(projectedRecord, 0, eventTime);
                        });
        emitFromTraverser(traverser);
        return false;
    }

    boolean configurationReceived() {
        return sourceConnectorWrapper.hasTaskConfiguration();
    }

    @Override
    public boolean saveToSnapshot() {
        logger.info("saveToSnapshot for globalProcessorIndex=" + globalProcessorIndex +
                         " localProcessorIndex= " + localProcessorIndex);

        if (!snapshotsEnabled) {
            return true;
        }
        snapshotInProgress = true;
        if (snapshotTraverser == null) {
            snapshotTraverser = Traversers.singleton(entry(snapshotKey(), sourceConnectorWrapper.copyState()))
                    .onFirstNull(() -> {
                        snapshotTraverser = null;
                        logger.finest("Finished saving snapshot");
                    });
        }
        return emitFromTraverserToSnapshot(snapshotTraverser);
    }

    private BroadcastKey<String> snapshotKey() {
        return broadcastKey("snapshot-" + globalProcessorIndex);
    }

    @Override
    protected void restoreFromSnapshot(@Nonnull Object key, @Nonnull Object value) {
        logger.info("restoreFromSnapshot key " + key + " value " + value);

        boolean forThisProcessor = snapshotKey().equals(key);
        if (forThisProcessor) {
            sourceConnectorWrapper.restoreState((State) value);
        }
    }

    @Override
    public boolean snapshotCommitFinish(boolean success) {
        try {
            if (success) {
                sourceConnectorWrapper.commit();
            }
        } finally {
            snapshotInProgress = false;
        }
        return true;
    }


    @Override
    public void close() {
        sourceConnectorWrapper.close();
        logger.info("Closed ReadKafkaConnectP");
    }

    public Map<String, LocalKafkaConnectStats> getStats() {
        Map<String, LocalKafkaConnectStats> connectStats = new HashMap<>();
        if (sourceConnectorWrapper != null && (sourceConnectorWrapper.hasTaskRunner())) {
            connectStats.put(sourceConnectorWrapper.getTaskRunnerName(), localKafkaConnectStats);
        }
        return connectStats;
    }

    @Override
    public void provideDynamicMetrics(MetricDescriptor descriptor, MetricsCollectionContext context) {
        if (sourceConnectorWrapper != null && (sourceConnectorWrapper.hasTaskRunner())) {
            descriptor.copy().withTag("task.runner", sourceConnectorWrapper.getTaskRunnerName());
        }
        provide(descriptor, context, KAFKA_CONNECT_PREFIX, getStats());
    }

    public static <T> ReadKafkaConnectProcessorSupplier processorSupplier(
            @Nonnull Properties propertiesFromUser,
            @Nonnull EventTimePolicy<? super T> eventTimePolicy,
            @Nonnull FunctionEx<SourceRecord, T> projectionFn) {
        return new ReadKafkaConnectProcessorSupplier() {
            private static final long serialVersionUID = 1L;

            @Nonnull
            @Override
            public Collection<ReadKafkaConnectP<?>> get(int localParallelismForMember) {
                return IntStream.range(0, localParallelismForMember)
                        .mapToObj(i -> {
                            ReadKafkaConnectP<T> processor = new ReadKafkaConnectP<>(eventTimePolicy, projectionFn);
                            processor.setPropertiesFromUser(propertiesFromUser);
                            return processor;
                        })
                        .collect(Collectors.toList());
            }
        };
    }
}
