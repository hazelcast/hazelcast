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

import com.hazelcast.jet.cdc.RecordMappingFunction;
import org.apache.kafka.connect.data.Values;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.Serial;
import java.util.Map.Entry;

import static com.hazelcast.jet.Util.entry;

public class JsonRecordMappingFn implements RecordMappingFunction<Entry<String, String>> {

    @Serial
    private static final long serialVersionUID = 1L;

    @Override
    public Entry<String, String> applyEx(SourceRecord sourceRecord) {
        String keyJson = Values.convertToString(sourceRecord.keySchema(), sourceRecord.key());
        String valueJson = Values.convertToString(sourceRecord.valueSchema(), sourceRecord.value());
        return entry(keyJson, valueJson);
    }
}
