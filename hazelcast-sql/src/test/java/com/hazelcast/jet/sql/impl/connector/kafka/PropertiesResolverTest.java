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

package com.hazelcast.jet.sql.impl.connector.kafka;

import com.hazelcast.jet.kafka.HazelcastKafkaAvroDeserializer;
import com.hazelcast.jet.kafka.HazelcastKafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
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
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Map;
import java.util.Properties;

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.AVRO_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.JAVA_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.JSON_FLAT_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_AVRO_SCHEMA;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_CLASS;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_AVRO_SCHEMA;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_CLASS;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.kafka.PropertiesResolver.KEY_DESERIALIZER;
import static com.hazelcast.jet.sql.impl.connector.kafka.PropertiesResolver.KEY_SERIALIZER;
import static com.hazelcast.jet.sql.impl.connector.kafka.PropertiesResolver.VALUE_DESERIALIZER;
import static com.hazelcast.jet.sql.impl.connector.kafka.PropertiesResolver.VALUE_SERIALIZER;
import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(JUnitParamsRunner.class)
public class PropertiesResolverTest {

    private static final String UNKNOWN_FORMAT = "unknown";
    private static final Schema DUMMY_SCHEMA = SchemaBuilder.record("jet.sql").fields().endRecord();

    @Test
    public void test_consumerProperties_absentFormat() {
        assertThat(resolveConsumerProperties(emptyMap()))
                .containsExactlyEntriesOf(Map.of(KEY_DESERIALIZER, ByteArrayDeserializer.class.getCanonicalName()));
    }

    @Test
    public void test_producerProperties_absentFormat() {
        assertThat(resolveProducerProperties(emptyMap()))
                .containsExactlyEntriesOf(Map.of(KEY_SERIALIZER, ByteArraySerializer.class.getCanonicalName()));
    }

    @Test
    public void when_consumerProperties_formatIsUnknown_then_itIsIgnored() {
        // key
        Map<String, String> keyOptions = Map.of(OPTION_KEY_FORMAT, UNKNOWN_FORMAT);
        assertThat(resolveConsumerProperties(keyOptions)).isEmpty();

        // value
        Map<String, String> valueOptions = Map.of(
                OPTION_KEY_FORMAT, UNKNOWN_FORMAT,
                OPTION_VALUE_FORMAT, UNKNOWN_FORMAT
        );
        assertThat(resolveConsumerProperties(valueOptions)).isEmpty();
    }

    @Test
    public void when_producerProperties_formatIsUnknown_then_itIsIgnored() {
        // key
        Map<String, String> keyOptions = Map.of(OPTION_KEY_FORMAT, UNKNOWN_FORMAT);
        assertThat(resolveProducerProperties(keyOptions)).isEmpty();

        // value
        Map<String, String> valueOptions = Map.of(
                OPTION_KEY_FORMAT, UNKNOWN_FORMAT,
                OPTION_VALUE_FORMAT, UNKNOWN_FORMAT
        );
        assertThat(resolveProducerProperties(valueOptions)).isEmpty();
    }

    @SuppressWarnings("unused")
    private Object[] consumerValues() {
        return new Object[]{
                new Object[]{Short.class.getName(), ShortDeserializer.class.getCanonicalName()},
                new Object[]{short.class.getName(), ShortDeserializer.class.getCanonicalName()},
                new Object[]{Integer.class.getName(), IntegerDeserializer.class.getCanonicalName()},
                new Object[]{int.class.getName(), IntegerDeserializer.class.getCanonicalName()},
                new Object[]{Long.class.getName(), LongDeserializer.class.getCanonicalName()},
                new Object[]{long.class.getName(), LongDeserializer.class.getCanonicalName()},
                new Object[]{Float.class.getName(), FloatDeserializer.class.getCanonicalName()},
                new Object[]{float.class.getName(), FloatDeserializer.class.getCanonicalName()},
                new Object[]{Double.class.getName(), DoubleDeserializer.class.getCanonicalName()},
                new Object[]{double.class.getName(), DoubleDeserializer.class.getCanonicalName()},
                new Object[]{String.class.getName(), StringDeserializer.class.getCanonicalName()},
        };
    }

    @Test
    @Parameters(method = "consumerValues")
    public void test_consumerProperties_java(String clazz, String deserializer) {
        // key
        assertThat(resolveConsumerProperties(Map.of(
                OPTION_KEY_FORMAT, JAVA_FORMAT,
                OPTION_KEY_CLASS, clazz
        ))).containsExactlyEntriesOf(Map.of(KEY_DESERIALIZER, deserializer));

        // value
        assertThat(resolveConsumerProperties(Map.of(
                OPTION_KEY_FORMAT, UNKNOWN_FORMAT,
                OPTION_VALUE_FORMAT, JAVA_FORMAT,
                OPTION_VALUE_CLASS, clazz)
        )).containsExactlyEntriesOf(Map.of(VALUE_DESERIALIZER, deserializer));
    }

    @SuppressWarnings("unused")
    private Object[] producerValues() {
        return new Object[]{
                new Object[]{Short.class.getName(), ShortSerializer.class.getCanonicalName()},
                new Object[]{short.class.getName(), ShortSerializer.class.getCanonicalName()},
                new Object[]{Integer.class.getName(), IntegerSerializer.class.getCanonicalName()},
                new Object[]{int.class.getName(), IntegerSerializer.class.getCanonicalName()},
                new Object[]{Long.class.getName(), LongSerializer.class.getCanonicalName()},
                new Object[]{long.class.getName(), LongSerializer.class.getCanonicalName()},
                new Object[]{Float.class.getName(), FloatSerializer.class.getCanonicalName()},
                new Object[]{float.class.getName(), FloatSerializer.class.getCanonicalName()},
                new Object[]{Double.class.getName(), DoubleSerializer.class.getCanonicalName()},
                new Object[]{double.class.getName(), DoubleSerializer.class.getCanonicalName()},
                new Object[]{String.class.getName(), StringSerializer.class.getCanonicalName()},
        };
    }

    @Test
    @Parameters(method = "producerValues")
    public void test_producerProperties_java(String clazz, String serializer) {
        // key
        assertThat(resolveProducerProperties(Map.of(
                OPTION_KEY_FORMAT, JAVA_FORMAT,
                OPTION_KEY_CLASS, clazz
        ))).containsExactlyEntriesOf(Map.of(KEY_SERIALIZER, serializer));

        // value
        assertThat(resolveProducerProperties(Map.of(
                OPTION_KEY_FORMAT, UNKNOWN_FORMAT,
                OPTION_VALUE_FORMAT, JAVA_FORMAT,
                OPTION_VALUE_CLASS, clazz)
        )).containsExactlyEntriesOf(Map.of(VALUE_SERIALIZER, serializer));
    }

    @SuppressWarnings("unused")
    private Object[] classes() {
        return new Object[]{
                new Object[]{Short.class.getName()},
                new Object[]{short.class.getName()},
                new Object[]{Integer.class.getName()},
                new Object[]{int.class.getName()},
                new Object[]{Long.class.getName()},
                new Object[]{long.class.getName()},
                new Object[]{Float.class.getName()},
                new Object[]{float.class.getName()},
                new Object[]{Double.class.getName()},
                new Object[]{double.class.getName()},
                new Object[]{String.class.getName()},
        };
    }

    @Test
    @Parameters(method = "classes")
    public void when_consumerProperties_javaPropertyIsDefined_then_itsNotOverwritten(String clazz) {
        // key
        Map<String, String> keyOptions = Map.of(
                OPTION_KEY_FORMAT, JAVA_FORMAT,
                OPTION_KEY_CLASS, clazz,
                KEY_DESERIALIZER, "deserializer"
        );

        assertThat(resolveConsumerProperties(keyOptions))
                .containsExactlyEntriesOf(Map.of(KEY_DESERIALIZER, "deserializer"));

        // value
        Map<String, String> valueOptions = Map.of(
                OPTION_KEY_FORMAT, UNKNOWN_FORMAT,
                OPTION_VALUE_FORMAT, JAVA_FORMAT,
                OPTION_VALUE_CLASS, clazz,
                VALUE_DESERIALIZER, "deserializer"
        );

        assertThat(resolveConsumerProperties(valueOptions))
                .containsExactlyEntriesOf(Map.of(VALUE_DESERIALIZER, "deserializer"));
    }

    @Test
    @Parameters(method = "classes")
    public void when_producerProperties_javaPropertyIsDefined_then_itsNotOverwritten(String clazz) {
        // key
        Map<String, String> keyOptions = Map.of(
                OPTION_KEY_FORMAT, JAVA_FORMAT,
                OPTION_KEY_CLASS, clazz,
                KEY_SERIALIZER, "serializer"
        );

        assertThat(resolveProducerProperties(keyOptions))
                .containsExactlyEntriesOf(Map.of(KEY_SERIALIZER, "serializer"));

        // value
        Map<String, String> valueOptions = Map.of(
                OPTION_KEY_FORMAT, UNKNOWN_FORMAT,
                OPTION_VALUE_FORMAT, JAVA_FORMAT,
                OPTION_VALUE_CLASS, clazz,
                VALUE_SERIALIZER, "serializer"
        );

        assertThat(resolveProducerProperties(valueOptions))
                .containsExactlyEntriesOf(Map.of(VALUE_SERIALIZER, "serializer"));
    }

    @Test
    public void test_consumerProperties_avro() {
        // key
        assertThat(PropertiesResolver.resolveConsumerProperties(Map.of(
                OPTION_KEY_FORMAT, AVRO_FORMAT
        ), DUMMY_SCHEMA, null)).containsExactlyInAnyOrderEntriesOf(Map.of(
                KEY_DESERIALIZER, HazelcastKafkaAvroDeserializer.class.getCanonicalName(),
                OPTION_KEY_AVRO_SCHEMA, DUMMY_SCHEMA
        ));

        // value
        assertThat(PropertiesResolver.resolveConsumerProperties(Map.of(
                OPTION_KEY_FORMAT, UNKNOWN_FORMAT,
                OPTION_VALUE_FORMAT, AVRO_FORMAT
        ), null, DUMMY_SCHEMA)).containsExactlyInAnyOrderEntriesOf(Map.of(
                VALUE_DESERIALIZER, HazelcastKafkaAvroDeserializer.class.getCanonicalName(),
                OPTION_VALUE_AVRO_SCHEMA, DUMMY_SCHEMA
        ));
    }

    @Test
    public void test_consumerProperties_avro_schemaRegistry() {
        // key
        assertThat(resolveConsumerProperties(Map.of(
                OPTION_KEY_FORMAT, AVRO_FORMAT,
                "schema.registry.url", "http://localhost:8081"
        ))).containsExactlyInAnyOrderEntriesOf(Map.of(
                KEY_DESERIALIZER, KafkaAvroDeserializer.class.getCanonicalName(),
                "schema.registry.url", "http://localhost:8081"
        ));

        // value
        assertThat(resolveConsumerProperties(Map.of(
                OPTION_KEY_FORMAT, UNKNOWN_FORMAT,
                OPTION_VALUE_FORMAT, AVRO_FORMAT,
                "schema.registry.url", "http://localhost:8081"
        ))).containsExactlyInAnyOrderEntriesOf(Map.of(
                VALUE_DESERIALIZER, KafkaAvroDeserializer.class.getCanonicalName(),
                "schema.registry.url", "http://localhost:8081"
        ));
    }

    @Test
    public void test_producerProperties_avro() {
        // key
        assertThat(PropertiesResolver.resolveProducerProperties(Map.of(
                OPTION_KEY_FORMAT, AVRO_FORMAT
        ), DUMMY_SCHEMA, null)).containsExactlyInAnyOrderEntriesOf(Map.of(
                KEY_SERIALIZER, HazelcastKafkaAvroSerializer.class.getCanonicalName(),
                OPTION_KEY_AVRO_SCHEMA, DUMMY_SCHEMA
        ));

        // value
        assertThat(PropertiesResolver.resolveProducerProperties(Map.of(
                OPTION_KEY_FORMAT, UNKNOWN_FORMAT,
                OPTION_VALUE_FORMAT, AVRO_FORMAT
        ), null, DUMMY_SCHEMA)).containsExactlyInAnyOrderEntriesOf(Map.of(
                VALUE_SERIALIZER, HazelcastKafkaAvroSerializer.class.getCanonicalName(),
                OPTION_VALUE_AVRO_SCHEMA, DUMMY_SCHEMA
        ));
    }

    @Test
    public void test_producerProperties_avro_schemaRegistry() {
        // key
        assertThat(resolveProducerProperties(Map.of(
                OPTION_KEY_FORMAT, AVRO_FORMAT,
                "schema.registry.url", "http://localhost:8081"
        ))).containsExactlyInAnyOrderEntriesOf(Map.of(
                KEY_SERIALIZER, KafkaAvroSerializer.class.getCanonicalName(),
                "schema.registry.url", "http://localhost:8081"
        ));

        // value
        assertThat(resolveProducerProperties(Map.of(
                OPTION_KEY_FORMAT, UNKNOWN_FORMAT,
                OPTION_VALUE_FORMAT, AVRO_FORMAT,
                "schema.registry.url", "http://localhost:8081"
        ))).containsExactlyInAnyOrderEntriesOf(Map.of(
                VALUE_SERIALIZER, KafkaAvroSerializer.class.getCanonicalName(),
                "schema.registry.url", "http://localhost:8081"
        ));
    }

    @Test
    public void when_consumerProperties_avroPropertyIsDefined_then_itsNotOverwritten() {
        // key
        assertThat(PropertiesResolver.resolveConsumerProperties(Map.of(
                OPTION_KEY_FORMAT, AVRO_FORMAT,
                KEY_DESERIALIZER, "deserializer"
        ), DUMMY_SCHEMA, null)).containsExactlyInAnyOrderEntriesOf(Map.of(
                KEY_DESERIALIZER, "deserializer",
                OPTION_KEY_AVRO_SCHEMA, DUMMY_SCHEMA
        ));

        // value
        assertThat(PropertiesResolver.resolveConsumerProperties(Map.of(
                OPTION_KEY_FORMAT, UNKNOWN_FORMAT,
                OPTION_VALUE_FORMAT, AVRO_FORMAT,
                VALUE_DESERIALIZER, "deserializer"
        ), null, DUMMY_SCHEMA)).containsExactlyInAnyOrderEntriesOf(Map.of(
                VALUE_DESERIALIZER, "deserializer",
                OPTION_VALUE_AVRO_SCHEMA, DUMMY_SCHEMA
        ));
    }

    @Test
    public void when_producerProperties_avroPropertyIsDefined_then_itsNotOverwritten() {
        // key
        assertThat(PropertiesResolver.resolveProducerProperties(Map.of(
                OPTION_KEY_FORMAT, AVRO_FORMAT,
                KEY_SERIALIZER, "serializer"
        ), DUMMY_SCHEMA, null)).containsExactlyInAnyOrderEntriesOf(Map.of(
                KEY_SERIALIZER, "serializer",
                OPTION_KEY_AVRO_SCHEMA, DUMMY_SCHEMA
        ));

        // value
        assertThat(PropertiesResolver.resolveProducerProperties(Map.of(
                OPTION_KEY_FORMAT, UNKNOWN_FORMAT,
                OPTION_VALUE_FORMAT, AVRO_FORMAT,
                VALUE_SERIALIZER, "serializer"
        ), null, DUMMY_SCHEMA)).containsExactlyInAnyOrderEntriesOf(Map.of(
                VALUE_SERIALIZER, "serializer",
                OPTION_VALUE_AVRO_SCHEMA, DUMMY_SCHEMA
        ));
    }

    @Test
    public void test_consumerProperties_json() {
        // key
        assertThat(resolveConsumerProperties(Map.of(OPTION_KEY_FORMAT, JSON_FLAT_FORMAT)))
                .containsExactlyEntriesOf(Map.of(KEY_DESERIALIZER, ByteArrayDeserializer.class.getCanonicalName()));

        // value
        assertThat(resolveConsumerProperties(Map.of(
                OPTION_KEY_FORMAT, UNKNOWN_FORMAT,
                OPTION_VALUE_FORMAT, JSON_FLAT_FORMAT
        ))).containsExactlyEntriesOf(Map.of(VALUE_DESERIALIZER, ByteArrayDeserializer.class.getCanonicalName()));
    }

    @Test
    public void test_producerProperties_json() {
        // key
        assertThat(resolveProducerProperties(Map.of(OPTION_KEY_FORMAT, JSON_FLAT_FORMAT)))
                .containsExactlyEntriesOf(Map.of(KEY_SERIALIZER, ByteArraySerializer.class.getCanonicalName()));

        // value
        assertThat(resolveProducerProperties(Map.of(
                OPTION_KEY_FORMAT, UNKNOWN_FORMAT,
                OPTION_VALUE_FORMAT, JSON_FLAT_FORMAT
        ))).containsExactlyEntriesOf(Map.of(VALUE_SERIALIZER, ByteArraySerializer.class.getCanonicalName()));
    }

    @Test
    public void when_consumerProperties_jsonPropertyIsDefined_then_itsNotOverwritten() {
        // key
        Map<String, String> keyOptions = Map.of(
                OPTION_KEY_FORMAT, JSON_FLAT_FORMAT,
                KEY_DESERIALIZER, "deserializer"
        );

        assertThat(resolveConsumerProperties(keyOptions))
                .containsExactlyEntriesOf(Map.of(KEY_DESERIALIZER, "deserializer"));

        // value
        Map<String, String> valueOptions = Map.of(
                OPTION_KEY_FORMAT, UNKNOWN_FORMAT,
                OPTION_VALUE_FORMAT, JSON_FLAT_FORMAT,
                VALUE_DESERIALIZER, "deserializer"
        );

        assertThat(resolveConsumerProperties(valueOptions))
                .containsExactlyEntriesOf(Map.of(VALUE_DESERIALIZER, "deserializer"));
    }

    @Test
    public void when_producerProperties_jsonPropertyIsDefined_then_itsNotOverwritten() {
        // key
        Map<String, String> keyOptions = Map.of(
                OPTION_KEY_FORMAT, JSON_FLAT_FORMAT,
                KEY_SERIALIZER, "serializer"
        );

        assertThat(resolveProducerProperties(keyOptions))
                .containsExactlyEntriesOf(Map.of(KEY_SERIALIZER, "serializer"));

        // value
        Map<String, String> valueOptions = Map.of(
                OPTION_KEY_FORMAT, UNKNOWN_FORMAT,
                OPTION_VALUE_FORMAT, JSON_FLAT_FORMAT,
                VALUE_SERIALIZER, "serializer"
        );

        assertThat(resolveProducerProperties(valueOptions))
                .containsExactlyEntriesOf(Map.of(VALUE_SERIALIZER, "serializer"));
    }

    private static Properties resolveConsumerProperties(Map<String, String> options) {
        return PropertiesResolver.resolveConsumerProperties(options, null, null);
    }

    private static Properties resolveProducerProperties(Map<String, String> options) {
        return PropertiesResolver.resolveProducerProperties(options, null, null);
    }
}
