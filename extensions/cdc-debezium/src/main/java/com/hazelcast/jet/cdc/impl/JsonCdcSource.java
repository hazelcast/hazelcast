/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.cdc.impl;

import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.StreamSource;
import org.apache.kafka.connect.data.Values;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.Map.Entry;
import java.util.Properties;

import static com.hazelcast.jet.Util.entry;

public class JsonCdcSource extends CdcSource<Entry<String, String>> {

    public JsonCdcSource(Processor.Context context, Properties properties) {
        super(context, properties);
    }

    @Override
    protected Entry<String, String> mapToOutput(SourceRecord record) {
        String keyJson = Values.convertToString(record.keySchema(), record.key());
        String valueJson = Values.convertToString(record.valueSchema(), record.value());
        return entry(keyJson, valueJson);
    }

    public static StreamSource<Entry<String, String>> fromProperties(Properties properties) {
        String name = properties.getProperty("name");
        return SourceBuilder.timestampedStream(name, ctx -> new JsonCdcSource(ctx, properties))
                .fillBufferFn(JsonCdcSource::fillBuffer)
                .createSnapshotFn(CdcSource::createSnapshot)
                .restoreSnapshotFn(CdcSource::restoreSnapshot)
                .destroyFn(CdcSource::destroy)
                .build();
    }
}
