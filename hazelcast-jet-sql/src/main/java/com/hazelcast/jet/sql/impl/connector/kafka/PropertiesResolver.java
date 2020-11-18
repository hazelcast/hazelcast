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

package com.hazelcast.jet.sql.impl.connector.kafka;

import com.hazelcast.jet.sql.impl.connector.SqlConnector;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.DoubleDeserializer;
import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.FloatDeserializer;
import org.apache.kafka.common.serialization.FloatSerializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.ShortDeserializer;
import org.apache.kafka.common.serialization.ShortSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.AVRO_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.JAVA_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.JSON_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FORMAT;

final class PropertiesResolver {

    static final String KEY_SERIALIZER = "key.serializer";
    static final String KEY_DESERIALIZER = "key.deserializer";
    static final String VALUE_SERIALIZER = "value.serializer";
    static final String VALUE_DESERIALIZER = "value.deserializer";

    static final String AVRO_SERIALIZER = "io.confluent.kafka.serializers.KafkaAvroSerializer";
    static final String AVRO_DESERIALIZER = "io.confluent.kafka.serializers.KafkaAvroDeserializer";

    static final String JSON_SERIALIZER = ByteArraySerializer.class.getCanonicalName();
    static final String JSON_DESERIALIZER = ByteArrayDeserializer.class.getCanonicalName();

    private PropertiesResolver() {
    }

    static Properties resolveProperties(Map<String, String> options) {
        Properties properties = new Properties();
        properties.putAll(options);

        properties(options, true).forEach(properties::putIfAbsent);
        properties(options, false).forEach(properties::putIfAbsent);

        return properties;
    }

    private static Map<String, String> properties(Map<String, String> options, boolean isKey) {
        Map<String, String> properties = new HashMap<>();

        String serializer = isKey ? KEY_SERIALIZER : VALUE_SERIALIZER;
        String deserializer = isKey ? KEY_DESERIALIZER : VALUE_DESERIALIZER;

        String format = options.get(isKey ? OPTION_KEY_FORMAT : OPTION_VALUE_FORMAT);
        if (JAVA_FORMAT.equals(format)) {
            String clazz = options.get(isKey ? SqlConnector.OPTION_KEY_CLASS : SqlConnector.OPTION_VALUE_CLASS);

            if (Short.class.getName().equals(clazz) || short.class.getName().equals(clazz)) {
                properties.put(serializer, ShortSerializer.class.getCanonicalName());
                properties.put(deserializer, ShortDeserializer.class.getCanonicalName());
            } else if (Integer.class.getName().equals(clazz) || int.class.getName().equals(clazz)) {
                properties.put(serializer, IntegerSerializer.class.getCanonicalName());
                properties.put(deserializer, IntegerDeserializer.class.getCanonicalName());
            } else if (Long.class.getName().equals(clazz) || long.class.getName().equals(clazz)) {
                properties.put(serializer, LongSerializer.class.getCanonicalName());
                properties.put(deserializer, LongDeserializer.class.getCanonicalName());
            } else if (Float.class.getName().equals(clazz) || float.class.getName().equals(clazz)) {
                properties.put(serializer, FloatSerializer.class.getCanonicalName());
                properties.put(deserializer, FloatDeserializer.class.getCanonicalName());
            } else if (Double.class.getName().equals(clazz) || double.class.getName().equals(clazz)) {
                properties.put(serializer, DoubleSerializer.class.getCanonicalName());
                properties.put(deserializer, DoubleDeserializer.class.getCanonicalName());
            } else if (String.class.getName().equals(clazz)) {
                properties.put(serializer, StringSerializer.class.getCanonicalName());
                properties.put(deserializer, StringDeserializer.class.getCanonicalName());
            }
        } else if (AVRO_FORMAT.equals(format)) {
            properties.put(serializer, AVRO_SERIALIZER);
            properties.put(deserializer, AVRO_DESERIALIZER);
        } else if (JSON_FORMAT.equals(format)) {
            properties.put(serializer, JSON_SERIALIZER);
            properties.put(deserializer, JSON_DESERIALIZER);
        }

        return properties;
    }
}
