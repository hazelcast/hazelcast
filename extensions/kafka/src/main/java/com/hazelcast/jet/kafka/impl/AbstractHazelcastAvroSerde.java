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

package com.hazelcast.jet.kafka.impl;

import org.apache.avro.Schema;

import java.util.Map;

public abstract class AbstractHazelcastAvroSerde {
    public static final String OPTION_KEY_AVRO_SCHEMA = "keyAvroSchema";
    public static final String OPTION_VALUE_AVRO_SCHEMA = "valueAvroSchema";

    protected Schema getSchema(Map<String, ?> configs, boolean isKey) {
        Object schema = configs.get(isKey ? OPTION_KEY_AVRO_SCHEMA : OPTION_VALUE_AVRO_SCHEMA);
        if (schema == null) {
            throw new IllegalArgumentException("Schema must be provided for " + (isKey ? "key" : "value"));
        } else if (schema instanceof Schema) {
            return (Schema) schema;
        } else if (schema instanceof String) {
            return new Schema.Parser().parse((String) schema);
        } else {
            throw new IllegalArgumentException("Provided schema cannot be recognized");
        }
    }
}
