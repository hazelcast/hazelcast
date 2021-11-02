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
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class HazelcastJsonValueDeserializer implements Deserializer<HazelcastJsonValue> {
    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) { }

    @Override
    public HazelcastJsonValue deserialize(final String topic, final byte[] data) {
        if (data == null || data.length == 0) {
            return null;
        }

        return new HazelcastJsonValue(new String(data));
    }

    @Override
    public void close() { }
}
