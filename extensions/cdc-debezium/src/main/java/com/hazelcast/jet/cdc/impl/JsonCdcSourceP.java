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

package com.hazelcast.jet.cdc.impl;

import com.hazelcast.jet.core.EventTimePolicy;
import org.apache.kafka.connect.data.Values;
import org.apache.kafka.connect.source.SourceRecord;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map.Entry;
import java.util.Properties;

import static com.hazelcast.jet.Util.entry;

public class JsonCdcSourceP extends CdcSourceP<Entry<String, String>> {

    public JsonCdcSourceP(
            @Nonnull Properties properties,
            @Nonnull EventTimePolicy<? super Entry<String, String>> eventTimePolicy
    ) {
        super(properties, eventTimePolicy);
    }

    @Nullable
    @Override
    protected Entry<String, String> map(SourceRecord record) {
        String keyJson = Values.convertToString(record.keySchema(), record.key());
        String valueJson = Values.convertToString(record.valueSchema(), record.value());
        return entry(keyJson, valueJson);
    }
}
