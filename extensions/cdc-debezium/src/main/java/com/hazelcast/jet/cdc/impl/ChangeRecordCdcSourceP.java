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

import com.hazelcast.jet.cdc.ChangeRecord;
import com.hazelcast.jet.core.EventTimePolicy;
import io.debezium.transforms.ExtractNewRecordState;
import org.apache.kafka.connect.data.Values;
import org.apache.kafka.connect.source.SourceRecord;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;

public class ChangeRecordCdcSourceP extends CdcSourceP<ChangeRecord> {

    public static final String DB_SPECIFIC_EXTRA_FIELDS_PROPERTY = "db.specific.extra.fields";

    private final SequenceExtractor sequenceExtractor;
    private final ExtractNewRecordState<SourceRecord> transform;

    public ChangeRecordCdcSourceP(
            @Nonnull Properties properties,
            @Nonnull EventTimePolicy<? super ChangeRecord> eventTimePolicy
    ) {
        super(properties, eventTimePolicy);

        try {
            sequenceExtractor = newInstance(properties.getProperty(SEQUENCE_EXTRACTOR_CLASS_PROPERTY),
                    "sequence extractor ");
            transform = initTransform(properties.getProperty(DB_SPECIFIC_EXTRA_FIELDS_PROPERTY));
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    @Nullable
    @Override
    protected ChangeRecord map(SourceRecord record) {
        record = transform.apply(record);
        if (record == null) {
            return null;
        }

        long sequenceSource = sequenceExtractor.source(record.sourcePartition(), record.sourceOffset());
        long sequenceValue = sequenceExtractor.sequence(record.sourceOffset());
        String keyJson = Values.convertToString(record.keySchema(), record.key());
        String valueJson = Values.convertToString(record.valueSchema(), record.value());
        return new ChangeRecordImpl(sequenceSource, sequenceValue, keyJson, valueJson);
    }

    private static ExtractNewRecordState<SourceRecord> initTransform(String dbSpecificExtraFields) {
        ExtractNewRecordState<SourceRecord> transform = new ExtractNewRecordState<>();

        Map<String, String> config = new HashMap<>();
        config.put("add.fields", String.join(",", extraFields(dbSpecificExtraFields)));
        config.put("delete.handling.mode", "rewrite");
        transform.configure(config);

        return transform;
    }

    private static Collection<String> extraFields(String dbSpecificExtraFields) {
        Set<String> extraFields = new HashSet<>(Arrays.asList("db", "table", "op", "ts_ms"));
        if (dbSpecificExtraFields != null) {
            extraFields.addAll(Arrays.asList(dbSpecificExtraFields.split(",")));
        }
        return extraFields;
    }
}
