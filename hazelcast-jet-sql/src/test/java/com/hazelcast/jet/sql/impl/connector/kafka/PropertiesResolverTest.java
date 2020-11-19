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

import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
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

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.AVRO_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.JAVA_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.JSON_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_CLASS;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FORMAT;
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

    @Test
    public void test_absentFormat() {
        assertThat(PropertiesResolver.resolveProperties(emptyMap()))
                .containsExactlyInAnyOrderEntriesOf(ImmutableMap.of(
                        KEY_SERIALIZER, ByteArraySerializer.class.getCanonicalName(),
                        KEY_DESERIALIZER, ByteArrayDeserializer.class.getCanonicalName()
                ));
    }

    @Test
    public void when_formatIsUnknown_then_itIsIgnored() {
        // key
        Map<String, String> keyOptions = ImmutableMap.of(OPTION_KEY_FORMAT, UNKNOWN_FORMAT);
        assertThat(PropertiesResolver.resolveProperties(keyOptions)).containsExactlyInAnyOrderEntriesOf(keyOptions);

        // value
        Map<String, String> valueOptions = ImmutableMap.of(
                OPTION_KEY_FORMAT, UNKNOWN_FORMAT,
                OPTION_VALUE_FORMAT, UNKNOWN_FORMAT
        );
        assertThat(PropertiesResolver.resolveProperties(valueOptions)).containsExactlyInAnyOrderEntriesOf(valueOptions);
    }

    @SuppressWarnings({"unused", "LineLength"})
    private Object[] values() {
        return new Object[]{
                new Object[]{Short.class.getName(), ShortSerializer.class.getCanonicalName(), ShortDeserializer.class.getCanonicalName()},
                new Object[]{short.class.getName(), ShortSerializer.class.getCanonicalName(), ShortDeserializer.class.getCanonicalName()},
                new Object[]{Integer.class.getName(), IntegerSerializer.class.getCanonicalName(), IntegerDeserializer.class.getCanonicalName()},
                new Object[]{int.class.getName(), IntegerSerializer.class.getCanonicalName(), IntegerDeserializer.class.getCanonicalName()},
                new Object[]{Long.class.getName(), LongSerializer.class.getCanonicalName(), LongDeserializer.class.getCanonicalName()},
                new Object[]{long.class.getName(), LongSerializer.class.getCanonicalName(), LongDeserializer.class.getCanonicalName()},
                new Object[]{Float.class.getName(), FloatSerializer.class.getCanonicalName(), FloatDeserializer.class.getCanonicalName()},
                new Object[]{float.class.getName(), FloatSerializer.class.getCanonicalName(), FloatDeserializer.class.getCanonicalName()},
                new Object[]{Double.class.getName(), DoubleSerializer.class.getCanonicalName(), DoubleDeserializer.class.getCanonicalName()},
                new Object[]{double.class.getName(), DoubleSerializer.class.getCanonicalName(), DoubleDeserializer.class.getCanonicalName()},
                new Object[]{String.class.getName(), StringSerializer.class.getCanonicalName(), StringDeserializer.class.getCanonicalName()},
        };
    }

    @Test
    @Parameters(method = "values")
    public void test_java(String clazz, String serializer, String deserializer) {
        // key
        assertThat(PropertiesResolver.resolveProperties(ImmutableMap.of(
                OPTION_KEY_FORMAT, JAVA_FORMAT,
                OPTION_KEY_CLASS, clazz
        ))).hasSize(4)
           .containsAllEntriesOf(ImmutableMap.of(KEY_SERIALIZER, serializer, KEY_DESERIALIZER, deserializer));

        // value
        assertThat(PropertiesResolver.resolveProperties(ImmutableMap.of(
                OPTION_KEY_FORMAT, UNKNOWN_FORMAT,
                OPTION_VALUE_FORMAT, JAVA_FORMAT,
                OPTION_VALUE_CLASS, clazz)
        )).hasSize(5)
          .containsAllEntriesOf(ImmutableMap.of(VALUE_SERIALIZER, serializer, VALUE_DESERIALIZER, deserializer));
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
    public void when_javaPropertyIsDefined_then_itsNotOverwritten(String clazz) {
        // key
        Map<String, String> keyOptions = ImmutableMap.of(
                OPTION_KEY_FORMAT, JAVA_FORMAT,
                OPTION_KEY_CLASS, clazz,
                KEY_SERIALIZER, "serializer",
                KEY_DESERIALIZER, "deserializer"
        );

        assertThat(PropertiesResolver.resolveProperties(keyOptions))
                .isNotSameAs(keyOptions)
                .containsExactlyInAnyOrderEntriesOf(keyOptions);

        // value
        Map<String, String> valueOptions = ImmutableMap.of(
                OPTION_KEY_FORMAT, UNKNOWN_FORMAT,
                OPTION_VALUE_FORMAT, JAVA_FORMAT,
                OPTION_VALUE_CLASS, clazz,
                VALUE_SERIALIZER, "serializer",
                VALUE_DESERIALIZER, "deserializer"
        );

        assertThat(PropertiesResolver.resolveProperties(valueOptions))
                .isNotSameAs(valueOptions)
                .containsExactlyInAnyOrderEntriesOf(valueOptions);
    }

    @Test
    public void test_avro() {
        // key
        assertThat(PropertiesResolver.resolveProperties(ImmutableMap.of(OPTION_KEY_FORMAT, AVRO_FORMAT)))
                .hasSize(3)
                .containsAllEntriesOf(ImmutableMap.of(
                        KEY_SERIALIZER, KafkaAvroSerializer.class.getCanonicalName(),
                        KEY_DESERIALIZER, KafkaAvroDeserializer.class.getCanonicalName()
                ));

        // value
        assertThat(PropertiesResolver.resolveProperties(ImmutableMap.of(
                OPTION_KEY_FORMAT, UNKNOWN_FORMAT,
                OPTION_VALUE_FORMAT, AVRO_FORMAT
        ))).hasSize(4)
           .containsAllEntriesOf(ImmutableMap.of(
                   VALUE_SERIALIZER, KafkaAvroSerializer.class.getCanonicalName(),
                   VALUE_DESERIALIZER, KafkaAvroDeserializer.class.getCanonicalName()
           ));
    }

    @Test
    public void when_avroPropertyIsDefined_then_itsNotOverwritten() {
        // key
        Map<String, String> keyOptions = ImmutableMap.of(
                OPTION_KEY_FORMAT, AVRO_FORMAT,
                KEY_SERIALIZER, "serializer",
                KEY_DESERIALIZER, "deserializer"
        );

        assertThat(PropertiesResolver.resolveProperties(keyOptions))
                .isNotSameAs(keyOptions)
                .containsExactlyInAnyOrderEntriesOf(keyOptions);

        // value
        Map<String, String> valueOptions = ImmutableMap.of(
                OPTION_KEY_FORMAT, UNKNOWN_FORMAT,
                OPTION_VALUE_FORMAT, AVRO_FORMAT,
                VALUE_SERIALIZER, "serializer",
                VALUE_DESERIALIZER, "deserializer"
        );

        assertThat(PropertiesResolver.resolveProperties(valueOptions))
                .isNotSameAs(valueOptions)
                .containsExactlyInAnyOrderEntriesOf(valueOptions);
    }

    @Test
    public void test_json() {
        // key
        assertThat(PropertiesResolver.resolveProperties(ImmutableMap.of(OPTION_KEY_FORMAT, JSON_FORMAT)))
                .hasSize(3)
                .containsAllEntriesOf(ImmutableMap.of(
                        KEY_SERIALIZER, ByteArraySerializer.class.getCanonicalName(),
                        KEY_DESERIALIZER, ByteArrayDeserializer.class.getCanonicalName()
                ));

        // value
        assertThat(PropertiesResolver.resolveProperties(ImmutableMap.of(
                OPTION_KEY_FORMAT, UNKNOWN_FORMAT,
                OPTION_VALUE_FORMAT, JSON_FORMAT
        ))).hasSize(4)
           .containsAllEntriesOf(ImmutableMap.of(
                   VALUE_SERIALIZER, ByteArraySerializer.class.getCanonicalName(),
                   VALUE_DESERIALIZER, ByteArrayDeserializer.class.getCanonicalName()
           ));
    }

    @Test
    public void when_jsonPropertyIsDefined_then_itsNotOverwritten() {
        // key
        Map<String, String> keyOptions = ImmutableMap.of(
                OPTION_KEY_FORMAT, JSON_FORMAT,
                KEY_SERIALIZER, "serializer",
                KEY_DESERIALIZER, "deserializer"
        );

        assertThat(PropertiesResolver.resolveProperties(keyOptions))
                .isNotSameAs(keyOptions)
                .containsExactlyInAnyOrderEntriesOf(keyOptions);

        // value
        Map<String, String> valueOptions = ImmutableMap.of(
                OPTION_KEY_FORMAT, UNKNOWN_FORMAT,
                OPTION_VALUE_FORMAT, JSON_FORMAT,
                VALUE_SERIALIZER, "serializer",
                VALUE_DESERIALIZER, "deserializer"
        );

        assertThat(PropertiesResolver.resolveProperties(valueOptions))
                .isNotSameAs(valueOptions)
                .containsExactlyInAnyOrderEntriesOf(valueOptions);
    }
}
