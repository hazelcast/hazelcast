/*
 * Copyright 2024 Hazelcast Inc.
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
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

/**
 * Kafka serializer for any java-serializable Object.
 */
public class JavaSerializer implements Serializer<Object> {
    public void configure(Map map, boolean b) { }

    @Override
    public void close() { }

    @Override
    public byte[] serialize(String s, Object o) {
        try {
            return TestJavaSerializationUtils.serialize((Serializable) o);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
