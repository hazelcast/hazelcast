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
package com.hazelcast.jet.pulsar.sql;

import com.hazelcast.core.HazelcastJsonValue;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.SchemaInfo;

import java.nio.ByteBuffer;

/** Encodes SQL JSON values as UTF-8 bytes without adding a JSON wrapper. */
final class PulsarJsonSchema implements Schema<HazelcastJsonValue> {

    @Override
    public byte[] encode(HazelcastJsonValue value) {
        return value == null ? null : Schema.STRING.encode(value.toString());
    }

    @Override
    public HazelcastJsonValue decode(byte[] bytes) {
        return bytes == null ? null : new HazelcastJsonValue(Schema.STRING.decode(bytes));
    }

    @Override
    public HazelcastJsonValue decode(ByteBuffer bytes) {
        return bytes == null ? null : new HazelcastJsonValue(Schema.STRING.decode(bytes));
    }

    @Override
    public SchemaInfo getSchemaInfo() {
        return Schema.BYTES.getSchemaInfo();
    }

    @Override
    @SuppressWarnings("checkstyle:SuperClone")
    public Schema<HazelcastJsonValue> clone() {
        // The schema has no mutable state, so readers and writers can share it.
        return this;
    }
}
