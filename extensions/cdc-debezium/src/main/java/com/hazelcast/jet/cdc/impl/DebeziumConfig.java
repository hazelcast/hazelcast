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

import com.hazelcast.jet.cdc.ChangeRecord;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.StreamSource;

import java.util.Objects;
import java.util.Properties;

public class DebeziumConfig {

    private final Properties properties = new Properties();

    public DebeziumConfig(String name, String connectorClass) {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(connectorClass, "connectorClass");

        properties.put("name", name);
        properties.put("connector.class", connectorClass);
        properties.put("database.history", CdcSource.DatabaseHistoryImpl.class.getName());
        properties.put("tombstones.on.delete", "false");
    }

    public void setProperty(String key, String value) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");

        properties.put(key, value);
    }

    public void setProperty(String key, int value) {
        setProperty(key, Integer.toString(value));
    }

    public void setProperty(String key, boolean value) {
        setProperty(key, Boolean.toString(value));
    }

    public void setProperty(String key, String... values) {
        Objects.requireNonNull(values, "values");
        for (int i = 0; i < values.length; i++) {
            Objects.requireNonNull(values[i], "values[" + i + "]");
        }
        setProperty(key, String.join(",", values));
    }

    public void check(PropertyRules rules) {
        rules.check(properties);
    }

    public StreamSource<ChangeRecord> createSource() {
        return createSource(properties);
    }

    private static StreamSource<ChangeRecord> createSource(Properties properties) {
        String name = properties.getProperty("name");
        return SourceBuilder.timestampedStream(name, ctx -> new CdcSource(properties))
                .fillBufferFn(CdcSource::fillBuffer)
                .createSnapshotFn(CdcSource::createSnapshot)
                .restoreSnapshotFn(CdcSource::restoreSnapshot)
                .destroyFn(CdcSource::destroy)
                .build();
    }
}
