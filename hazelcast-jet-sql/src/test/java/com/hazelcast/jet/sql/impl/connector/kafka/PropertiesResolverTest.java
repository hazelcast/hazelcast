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
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
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
import static com.hazelcast.jet.sql.impl.connector.kafka.PropertiesResolver.AVRO_DESERIALIZER;
import static com.hazelcast.jet.sql.impl.connector.kafka.PropertiesResolver.AVRO_SERIALIZER;
import static com.hazelcast.jet.sql.impl.connector.kafka.PropertiesResolver.JSON_DESERIALIZER;
import static com.hazelcast.jet.sql.impl.connector.kafka.PropertiesResolver.JSON_SERIALIZER;
import static com.hazelcast.jet.sql.impl.connector.kafka.PropertiesResolver.KEY_DESERIALIZER;
import static com.hazelcast.jet.sql.impl.connector.kafka.PropertiesResolver.KEY_SERIALIZER;
import static com.hazelcast.jet.sql.impl.connector.kafka.PropertiesResolver.VALUE_DESERIALIZER;
import static com.hazelcast.jet.sql.impl.connector.kafka.PropertiesResolver.VALUE_SERIALIZER;
import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(JUnitParamsRunner.class)
@SuppressWarnings("LineLength")
public class PropertiesResolverTest {

    @Test
    public void when_formatIsAbsent_then_doesNotFail() {
        assertThat(PropertiesResolver.resolveProperties(emptyMap())).isEmpty();
    }

    @Test
    public void when_propertyIsDefined_then_itsNotOverwritten() {
        // key
        Map<String, String> keyOptions = ImmutableMap.of(
                OPTION_KEY_FORMAT, JAVA_FORMAT,
                OPTION_KEY_CLASS, short.class.getName(),
                KEY_SERIALIZER, "already-defined-key-serializer",
                KEY_DESERIALIZER, "already-defined-key-deserializer"
        );
        assertThat(PropertiesResolver.resolveProperties(keyOptions)).containsExactlyInAnyOrderEntriesOf(keyOptions);

        // value
        Map<String, String> valueOptions = ImmutableMap.of(
                OPTION_VALUE_FORMAT, JAVA_FORMAT,
                OPTION_VALUE_CLASS, short.class.getName(),
                VALUE_SERIALIZER, "already-defined-value-serializer",
                VALUE_DESERIALIZER, "already-defined-value-deserializer"
        );
        assertThat(PropertiesResolver.resolveProperties(valueOptions)).containsExactlyInAnyOrderEntriesOf(valueOptions);
    }

    @SuppressWarnings("unused")
    private Object[] javaValues() {
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
    @Parameters(method = "javaValues")
    public void test_java(String clazz, String serializer, String deserializer) {
        // key
        assertThat(PropertiesResolver.resolveProperties(ImmutableMap.of(OPTION_KEY_FORMAT, JAVA_FORMAT, OPTION_KEY_CLASS, clazz)))
                .hasSize(4)
                .containsAllEntriesOf(ImmutableMap.of(KEY_SERIALIZER, serializer, KEY_DESERIALIZER, deserializer));

        // value
        assertThat(PropertiesResolver.resolveProperties(ImmutableMap.of(OPTION_VALUE_FORMAT, JAVA_FORMAT, OPTION_VALUE_CLASS, clazz)))
                .hasSize(4)
                .containsAllEntriesOf(ImmutableMap.of(VALUE_SERIALIZER, serializer, VALUE_DESERIALIZER, deserializer));
    }

    @Test
    public void test_avro() {
        // key
        assertThat(PropertiesResolver.resolveProperties(ImmutableMap.of(OPTION_KEY_FORMAT, AVRO_FORMAT)))
                .hasSize(3)
                .containsAllEntriesOf(ImmutableMap.of(KEY_SERIALIZER, AVRO_SERIALIZER, KEY_DESERIALIZER, AVRO_DESERIALIZER));

        // value
        assertThat(PropertiesResolver.resolveProperties(ImmutableMap.of(OPTION_VALUE_FORMAT, AVRO_FORMAT)))
                .hasSize(3)
                .containsAllEntriesOf(ImmutableMap.of(VALUE_SERIALIZER, AVRO_SERIALIZER, VALUE_DESERIALIZER, AVRO_DESERIALIZER));
    }

    @Test
    public void test_json() {
        // key
        assertThat(PropertiesResolver.resolveProperties(ImmutableMap.of(OPTION_KEY_FORMAT, JSON_FORMAT)))
                .hasSize(3)
                .containsAllEntriesOf(ImmutableMap.of(KEY_SERIALIZER, JSON_SERIALIZER, KEY_DESERIALIZER, JSON_DESERIALIZER));

        // value
        assertThat(PropertiesResolver.resolveProperties(ImmutableMap.of(OPTION_VALUE_FORMAT, JSON_FORMAT)))
                .hasSize(3)
                .containsAllEntriesOf(ImmutableMap.of(VALUE_SERIALIZER, JSON_SERIALIZER, VALUE_DESERIALIZER, JSON_DESERIALIZER));
    }
}
