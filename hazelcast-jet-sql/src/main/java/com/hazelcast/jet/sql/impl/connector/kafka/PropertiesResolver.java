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

    // using strings instead of canonical names to not fail without Kafka on the classpath

    private static final String SHORT_SERIALIZER = "org.apache.kafka.common.serialization.ShortSerializer";
    private static final String SHORT_DESERIALIZER = "org.apache.kafka.common.serialization.ShortDeserializer";

    private static final String INT_SERIALIZER = "org.apache.kafka.common.serialization.IntegerSerializer";
    private static final String INT_DESERIALIZER = "org.apache.kafka.common.serialization.IntegerDeserializer";

    private static final String LONG_SERIALIZER = "org.apache.kafka.common.serialization.LongSerializer";
    private static final String LONG_DESERIALIZER = "org.apache.kafka.common.serialization.LongDeserializer";

    private static final String FLOAT_SERIALIZER = "org.apache.kafka.common.serialization.FloatSerializer";
    private static final String FLOAT_DESERIALIZER = "org.apache.kafka.common.serialization.FloatDeserializer";

    private static final String DOUBLE_SERIALIZER = "org.apache.kafka.common.serialization.DoubleSerializer";
    private static final String DOUBLE_DESERIALIZER = "org.apache.kafka.common.serialization.DoubleDeserializer";

    private static final String STRING_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
    private static final String STRING_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";

    private static final String AVRO_SERIALIZER = "io.confluent.kafka.serializers.KafkaAvroSerializer";
    private static final String AVRO_DESERIALIZER = "io.confluent.kafka.serializers.KafkaAvroDeserializer";

    private static final String BYTE_ARRAY_SERIALIZER = "org.apache.kafka.common.serialization.ByteArraySerializer";
    private static final String BYTE_ARRAY_DESERIALIZER = "org.apache.kafka.common.serialization.ByteArrayDeserializer";

    private PropertiesResolver() {
    }

    static Properties resolveProperties(Map<String, String> options) {
        Properties properties = new Properties();
        properties.putAll(options);

        withSerdeProperties(true, options, properties);
        withSerdeProperties(false, options, properties);

        return properties;
    }

    private static void withSerdeProperties(
            boolean isKey,
            Map<String, String> options,
            Properties properties
    ) {
        String serializer = isKey ? KEY_SERIALIZER : VALUE_SERIALIZER;
        String deserializer = isKey ? KEY_DESERIALIZER : VALUE_DESERIALIZER;

        String format = options.get(isKey ? OPTION_KEY_FORMAT : OPTION_VALUE_FORMAT);
        if (format == null && isKey) {
            properties.putIfAbsent(serializer, BYTE_ARRAY_SERIALIZER);
            properties.putIfAbsent(deserializer, BYTE_ARRAY_DESERIALIZER);
        } else if (JAVA_FORMAT.equals(format)) {
            String clazz = options.get(isKey ? SqlConnector.OPTION_KEY_CLASS : SqlConnector.OPTION_VALUE_CLASS);
            if (Short.class.getName().equals(clazz) || short.class.getName().equals(clazz)) {
                properties.putIfAbsent(serializer, SHORT_SERIALIZER);
                properties.putIfAbsent(deserializer, SHORT_DESERIALIZER);
            } else if (Integer.class.getName().equals(clazz) || int.class.getName().equals(clazz)) {
                properties.putIfAbsent(serializer, INT_SERIALIZER);
                properties.putIfAbsent(deserializer, INT_DESERIALIZER);
            } else if (Long.class.getName().equals(clazz) || long.class.getName().equals(clazz)) {
                properties.putIfAbsent(serializer, LONG_SERIALIZER);
                properties.putIfAbsent(deserializer, LONG_DESERIALIZER);
            } else if (Float.class.getName().equals(clazz) || float.class.getName().equals(clazz)) {
                properties.putIfAbsent(serializer, FLOAT_SERIALIZER);
                properties.putIfAbsent(deserializer, FLOAT_DESERIALIZER);
            } else if (Double.class.getName().equals(clazz) || double.class.getName().equals(clazz)) {
                properties.putIfAbsent(serializer, DOUBLE_SERIALIZER);
                properties.putIfAbsent(deserializer, DOUBLE_DESERIALIZER);
            } else if (String.class.getName().equals(clazz)) {
                properties.putIfAbsent(serializer, STRING_SERIALIZER);
                properties.putIfAbsent(deserializer, STRING_DESERIALIZER);
            }
        } else if (AVRO_FORMAT.equals(format)) {
            properties.putIfAbsent(serializer, AVRO_SERIALIZER);
            properties.putIfAbsent(deserializer, AVRO_DESERIALIZER);
        } else if (JSON_FORMAT.equals(format)) {
            properties.putIfAbsent(serializer, BYTE_ARRAY_SERIALIZER);
            properties.putIfAbsent(deserializer, BYTE_ARRAY_DESERIALIZER);
        }
    }
}
