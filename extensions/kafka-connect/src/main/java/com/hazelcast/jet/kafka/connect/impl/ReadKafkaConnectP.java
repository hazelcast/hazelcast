/*
 * Copyright 2023 Hazelcast Inc.
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

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.BroadcastKey;
import com.hazelcast.jet.core.EventTimeMapper;
import com.hazelcast.jet.core.EventTimePolicy;
import org.apache.kafka.connect.source.SourceRecord;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map.Entry;

import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.core.BroadcastKey.broadcastKey;

public class ReadKafkaConnectP extends AbstractProcessor {

    private final ConnectorWrapper connectorWrapper;
    private final EventTimeMapper<SourceRecord> eventTimeMapper;
    private TaskRunner taskRunner;
    private boolean snapshotInProgress;
    private Traverser<Entry<BroadcastKey<String>, TaskRunner.State>> snapshotTraverser;
    private boolean snapshotsEnabled;
    private int processorIndex;
    private Traverser<?> traverser;

    public ReadKafkaConnectP(ConnectorWrapper connectorWrapper, EventTimePolicy<? super SourceRecord> eventTimePolicy) {
        this.connectorWrapper = connectorWrapper;
        this.eventTimeMapper = new EventTimeMapper<>(eventTimePolicy);
        eventTimeMapper.addPartitions(1);

    }

    @Override
    protected void init(@Nonnull Context context) {
        taskRunner = connectorWrapper.createTaskRunner();
        snapshotsEnabled = context.snapshottingEnabled();
        processorIndex = context.globalProcessorIndex();
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    @Override
    public boolean complete() {
        if (snapshotInProgress) {
            return false;
        }
        if (traverser == null) {
            List<SourceRecord> sourceRecords = taskRunner.poll();
            this.traverser = traverser(sourceRecords)
                    .map(rec -> {
                        taskRunner.commitRecord((SourceRecord) rec);
                        return rec;
                    })
                    .onFirstNull(() -> traverser = null);
        }
        emitFromTraverser(traverser);
        return false;
    }

    @Nonnull
    public Traverser<?> traverser(List<SourceRecord> sourceRecords) {
        if (sourceRecords.isEmpty()) {
            return eventTimeMapper.flatMapIdle();
        }
        return traverseIterable(sourceRecords)
                .flatMap(rec -> {
                    long eventTime = rec.timestamp() == null ? 0 : rec.timestamp();
                    return eventTimeMapper.flatMapEvent(rec, 0, eventTime);
                });
    }

    @Override
    public boolean saveToSnapshot() {
        if (!snapshotsEnabled) {
            return true;
        }
        snapshotInProgress = true;
        if (snapshotTraverser == null) {
            snapshotTraverser = Traversers.singleton(entry(snapshotKey(), taskRunner.createSnapshot()))
                    .onFirstNull(() -> {
                        snapshotTraverser = null;
                        getLogger().finest("Finished saving snapshot");
                    });
        }
        return emitFromTraverserToSnapshot(snapshotTraverser);
    }

    private BroadcastKey<String> snapshotKey() {
        return broadcastKey("snapshot-" + processorIndex);
    }

    protected void restoreFromSnapshot(@Nonnull Object key, @Nonnull Object value) {
        boolean forThisProcessor = snapshotKey().equals(key);
        if (forThisProcessor) {
            taskRunner.restoreSnapshot((TaskRunner.State) value);
        }
    }

    @Override
    public boolean snapshotCommitFinish(boolean success) {
        try {
            if (success) {
                taskRunner.commit();
            }
        } finally {
            snapshotInProgress = false;
        }
        return true;
    }


    @Override
    public void close() {
        if (taskRunner != null) {
            taskRunner.stop();
        }
    }
}
