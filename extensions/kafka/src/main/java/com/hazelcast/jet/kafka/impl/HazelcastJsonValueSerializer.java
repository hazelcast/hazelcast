/*
 * Copyright 2021 Hazelcast Inc.
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

import com.hazelcast.core.HazelcastJsonValue;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class HazelcastJsonValueSerializer implements Serializer<HazelcastJsonValue> {
    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) { }

    @Override
    public byte[] serialize(final String topic, final HazelcastJsonValue data) {
        if (data == null || data.toString() == null) {
            return null;
        }

        return data.toString().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public void close() { }
}
