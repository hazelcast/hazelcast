/*
 * Copyright 2025 Hazelcast Inc.
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

package com.hazelcast.jet.sql.impl.connector.kafka.model;

import com.hazelcast.test.TestJavaSerializationUtils;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * Kafka deserializer for any java-serializable Object.
 */
public class JavaDeserializer implements Deserializer<Object> {
    public void configure(Map map, boolean b) { }

    @Override
    public void close() { }

    @Override
    public Object deserialize(String topic, byte[] data) {
        try {
            return TestJavaSerializationUtils.deserialize(data);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
