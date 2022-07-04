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

package com.hazelcast.jet.sql.impl.connector.kafka;

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.jet.kafka.impl.HazelcastJsonValueDeserializer;
import com.hazelcast.jet.kafka.impl.HazelcastJsonValueSerializer;
import com.hazelcast.jet.sql.impl.connector.SqlConnector;
import com.hazelcast.jet.sql.impl.connector.keyvalue.JavaClassNameResolver;

import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.AVRO_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.JAVA_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.JSON_FLAT_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_CLASS;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_CLASS;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FORMAT;

final class PropertiesResolver {

    static final String KEY_SERIALIZER = "key.serializer";
    static final String KEY_DESERIALIZER = "key.deserializer";
    static final String VALUE_SERIALIZER = "value.serializer";
    static final String VALUE_DESERIALIZER = "value.deserializer";

    private static final Set<String> NON_KAFKA_OPTIONS = new HashSet<String>() {{
        add(OPTION_KEY_FORMAT);
        add(OPTION_KEY_CLASS);
        add(OPTION_VALUE_FORMAT);
        add(OPTION_VALUE_CLASS);
    }};

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

    private static final String JSON_SERIALIZER = HazelcastJsonValueSerializer.class.getName();
    private static final String JSON_DESERIALIZER = HazelcastJsonValueDeserializer.class.getName();

    private PropertiesResolver() {
    }

    static Properties resolveConsumerProperties(Map<String, String> options) {
        Properties properties = from(options);

        withSerdeConsumerProperties(true, options, properties);
        withSerdeConsumerProperties(false, options, properties);

        return properties;
    }

    static Properties resolveProducerProperties(Map<String, String> options) {
        Properties properties = from(options);

        withSerdeProducerProperties(true, options, properties);
        withSerdeProducerProperties(false, options, properties);

        return properties;
    }

    private static Properties from(Map<String, String> options) {
        Properties properties = new Properties();
        for (Entry<String, String> entry : options.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();

            if (!NON_KAFKA_OPTIONS.contains(key)) {
                properties.put(key, value);
            }
        }
        return properties;
    }

    private static void withSerdeConsumerProperties(
            boolean isKey,
            Map<String, String> options,
            Properties properties
    ) {
        String deserializer = isKey ? KEY_DESERIALIZER : VALUE_DESERIALIZER;

        String format = options.get(isKey ? OPTION_KEY_FORMAT : OPTION_VALUE_FORMAT);
        if (format == null && isKey) {
            properties.putIfAbsent(deserializer, BYTE_ARRAY_DESERIALIZER);
        } else if (AVRO_FORMAT.equals(format)) {
            properties.putIfAbsent(deserializer, AVRO_DESERIALIZER);
        } else if (JSON_FLAT_FORMAT.equals(format)) {
            properties.putIfAbsent(deserializer, BYTE_ARRAY_DESERIALIZER);
        } else if (JAVA_FORMAT.equals(format)) {
            String clazz = options.get(isKey ? SqlConnector.OPTION_KEY_CLASS : SqlConnector.OPTION_VALUE_CLASS);
            String deserializerClass = resolveDeserializer(clazz);
            if (deserializerClass != null) {
                properties.putIfAbsent(deserializer, deserializerClass);
            }
        } else {
            String resolvedClass = JavaClassNameResolver.resolveClassName(format);
            if (resolvedClass != null) {
                String deserializerClass = resolveDeserializer(resolvedClass);
                if (deserializerClass != null) {
                    properties.putIfAbsent(deserializer, deserializerClass);
                }
            }
        }
    }

    private static String resolveDeserializer(String clazz) {
        if (Short.class.getName().equals(clazz) || short.class.getName().equals(clazz)) {
            return SHORT_DESERIALIZER;
        } else if (Integer.class.getName().equals(clazz) || int.class.getName().equals(clazz)) {
            return INT_DESERIALIZER;
        } else if (Long.class.getName().equals(clazz) || long.class.getName().equals(clazz)) {
            return LONG_DESERIALIZER;
        } else if (Float.class.getName().equals(clazz) || float.class.getName().equals(clazz)) {
            return FLOAT_DESERIALIZER;
        } else if (Double.class.getName().equals(clazz) || double.class.getName().equals(clazz)) {
            return DOUBLE_DESERIALIZER;
        } else if (String.class.getName().equals(clazz)) {
            return STRING_DESERIALIZER;
        } else if (HazelcastJsonValue.class.getName().equals(clazz)) {
            return JSON_DESERIALIZER;
        } else {
            return null;
        }
    }

    private static void withSerdeProducerProperties(
            boolean isKey,
            Map<String, String> options,
            Properties properties
    ) {
        String serializer = isKey ? KEY_SERIALIZER : VALUE_SERIALIZER;

        String format = options.get(isKey ? OPTION_KEY_FORMAT : OPTION_VALUE_FORMAT);
        if (format == null && isKey) {
            properties.putIfAbsent(serializer, BYTE_ARRAY_SERIALIZER);
        } else if (AVRO_FORMAT.equals(format)) {
            properties.putIfAbsent(serializer, AVRO_SERIALIZER);
        } else if (JSON_FLAT_FORMAT.equals(format)) {
            properties.putIfAbsent(serializer, BYTE_ARRAY_SERIALIZER);
        } else if (JAVA_FORMAT.equals(format)) {
            String clazz = options.get(isKey ? SqlConnector.OPTION_KEY_CLASS : SqlConnector.OPTION_VALUE_CLASS);
            String serializerClass = resolveSerializer(clazz);
            if (serializerClass != null) {
                properties.putIfAbsent(serializer, serializerClass);
            }
        } else {
            String resolvedClass = JavaClassNameResolver.resolveClassName(format);
            if (resolvedClass != null) {
                String serializerClass = resolveSerializer(resolvedClass);
                if (serializerClass != null) {
                    properties.putIfAbsent(serializer, serializerClass);
                }
            }
        }
    }

    private static String resolveSerializer(String clazz) {
        if (Short.class.getName().equals(clazz) || short.class.getName().equals(clazz)) {
            return SHORT_SERIALIZER;
        } else if (Integer.class.getName().equals(clazz) || int.class.getName().equals(clazz)) {
            return INT_SERIALIZER;
        } else if (Long.class.getName().equals(clazz) || long.class.getName().equals(clazz)) {
            return LONG_SERIALIZER;
        } else if (Float.class.getName().equals(clazz) || float.class.getName().equals(clazz)) {
            return FLOAT_SERIALIZER;
        } else if (Double.class.getName().equals(clazz) || double.class.getName().equals(clazz)) {
            return DOUBLE_SERIALIZER;
        } else if (String.class.getName().equals(clazz)) {
            return STRING_SERIALIZER;
        } else if (HazelcastJsonValue.class.getName().equals(clazz)) {
            return JSON_SERIALIZER;
        } else {
            return null;
        }
    }
}
