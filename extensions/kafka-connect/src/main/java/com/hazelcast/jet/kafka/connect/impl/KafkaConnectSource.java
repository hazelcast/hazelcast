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

import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;
import java.util.Map;

public class KafkaConnectSource {
    private static final ILogger LOGGER = Logger.getLogger(KafkaConnectSource.class);

    private final ConnectorWrapper connectorWrapper;

    private final TaskRunner taskRunner;

    public KafkaConnectSource(ConnectorWrapper connectorWrapper) {
        this.connectorWrapper = connectorWrapper;
        this.taskRunner = connectorWrapper.createTaskRunner();
    }

    public void fillBuffer(SourceBuilder.TimestampedSourceBuffer<SourceRecord> buf) {
        List<SourceRecord> records = taskRunner.poll();
        if (records == null) {
            return;
        }
        if (LOGGER.isFineEnabled()) {
            LOGGER.fine("Got " + records.size() + " records from task poll");
        }
        for (SourceRecord record : records) {
            addToBuffer(record, buf);
            taskRunner.commitRecord(record);
        }
    }

    private void addToBuffer(SourceRecord record, SourceBuilder.TimestampedSourceBuffer<SourceRecord> buf) {
        long ts = record.timestamp() == null ? 0 : record.timestamp();
        buf.add(record, ts);
    }

    public void destroy() {
        try {
            taskRunner.stop();
        } finally {
            // TODO: 01/03/2023 Probably need to be moved out of this class to some ProcessSupplier or somewhere else
            connectorWrapper.stop();
        }
    }

    public Map<Map<String, ?>, Map<String, ?>> createSnapshot() {
        return connectorWrapper.createSnapshot();
    }

    public void restoreSnapshot(List<Map<Map<String, ?>, Map<String, ?>>> snapshots) {
        connectorWrapper.restoreSnapshot(snapshots);
    }

}
