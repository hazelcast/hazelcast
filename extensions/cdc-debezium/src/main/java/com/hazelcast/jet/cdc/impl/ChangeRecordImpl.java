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
import com.hazelcast.jet.cdc.Operation;
import com.hazelcast.jet.cdc.ParsingException;
import com.hazelcast.jet.cdc.RecordPart;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Objects;

public class ChangeRecordImpl implements ChangeRecord {

    private final String keyJson;
    private final String valueJson;

    private String json;
    private Long timestamp;
    private Operation operation;
    private RecordPart key;
    private RecordPart value;

    public ChangeRecordImpl(@Nonnull String keyJson, @Nonnull String valueJson) {
        this.keyJson = Objects.requireNonNull(keyJson, "keyJson");
        this.valueJson = Objects.requireNonNull(valueJson, "valueJson");
    }

    @Override
    public long timestamp() throws ParsingException {
        if (timestamp == null) {
            Long millis = get(value().toMap(), "__ts_ms", Long.class);
            if (millis == null) {
                throw new ParsingException("No parsable timestamp field found");
            }
            timestamp = millis;
        }
        return timestamp;
    }

    @Override
    @Nonnull
    public Operation operation() throws ParsingException {
        if (operation == null) {
            String opAlias = get(value().toMap(), "__op", String.class);
            operation = Operation.get(opAlias);
        }
        return operation;
    }

    @Override
    @Nonnull
    public RecordPart key() {
        if (key == null) {
            key = new RecordPartImpl(keyJson);
        }
        return key;
    }

    @Override
    @Nonnull
    public RecordPart value() {
        if (value == null) {
            value = new RecordPartImpl(valueJson);
        }
        return value;
    }

    @Override
    @Nonnull
    public String toJson() {
        if (json == null) {
            json = String.format("key:{%s}, value:{%s}", keyJson, valueJson);
        }
        return json;
    }

    public String getKeyJson() {
        return keyJson;
    }

    public String getValueJson() {
        return valueJson;
    }

    @Override
    public String toString() {
        return toJson();
    }

    private static <T> T get(Map<String, Object> map, String key, Class<T> clazz) {
        Object obj = map.get(key);
        if (obj != null && clazz.isInstance(obj)) {
            return (T) obj;
        }
        return null;
    }

}
