/*
 * Copyright 2026 Hazelcast Inc.
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
package com.hazelcast.jet.cdc.impl;

import tools.jackson.core.json.JsonReadFeature;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.MapperFeature;
import tools.jackson.databind.cfg.DateTimeFeature;
import tools.jackson.databind.json.JsonMapper;
import com.hazelcast.jet.JetException;
import io.debezium.engine.DebeziumEngine.RecordCommitter;
import io.debezium.engine.RecordChangeEvent;
import org.apache.kafka.connect.source.SourceRecord;

import java.nio.ByteBuffer;
import java.util.Map;

final class Utils {

    static final JsonMapper MAPPER = JsonMapper.builder()
            .enable(JsonReadFeature.ALLOW_UNESCAPED_CONTROL_CHARS)
            .enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES)
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .enable(DateTimeFeature.WRITE_DURATIONS_AS_TIMESTAMPS)
            .build();

    private Utils() {
    }

    static void markProcessed(RecordCommitter<RecordChangeEvent<SourceRecord>> committer,
                              RecordChangeEvent<SourceRecord> changeEvent) {
        try {
            committer.markProcessed(changeEvent);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new JetException("Unable to mark event as processed", e);
        }
    }

    static void markBatchFinished(RecordCommitter<RecordChangeEvent<SourceRecord>> committer) {
        try {
            committer.markBatchFinished();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new JetException("Unable to mark event batch as processed", e);
        }
    }

    static String decode(Map<ByteBuffer, ByteBuffer> partitionsToOffset) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<ByteBuffer, ByteBuffer> entry : partitionsToOffset.entrySet()) {
            ByteBuffer partition = entry.getKey();
            ByteBuffer offset = entry.getValue();
            sb.append(new String(partition.array())).append(" -> ").append(new String(offset.array()));
            sb.append(System.lineSeparator());
        }
        return sb.toString();
    }
}
