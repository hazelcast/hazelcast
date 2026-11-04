/*
 * Copyright 2026 Hazelcast Inc.
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
package com.hazelcast.jet.pulsar.sql;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.jet.pulsar.PulsarDataConnection;
import com.hazelcast.jet.pulsar.PulsarTestSupport;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.jet.sql.SqlTestSupport.SqlMapping;
import com.hazelcast.sql.SqlService;
import com.hazelcast.test.SerialTest;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Schema;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static com.hazelcast.jet.sql.SqlTestSupport.assertRowsEventuallyInAnyOrder;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.JAVA_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_CLASS;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FORMAT;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Locale.ROOT;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@SuppressWarnings("unchecked")
@SerialTest
public class BasicSqlWritingTest extends PulsarTestSupport {

    private HazelcastInstance[] instances;
    private SqlService sql;

    @BeforeEach
    public void setup() {
        var conf = smallInstanceConfig();
        conf.addDataConnectionConfig(PulsarDataConnection.pulsarDataConnectionConf("testPulsar",
                                                                                   pulsarContainer.getPulsarBrokerUrl(),
                                                                                   pulsarContainer.getHttpServiceUrl(),
                                                                                   true));
        instances = createHazelcastInstances(conf, 2);
        sql = instances[0].getSql();
    }

    @ParameterizedTest(name = "{0}, javaFormat={1}")
    @MethodSource("supportedValueTypes")
    <T> void insertsIntoPulsarAndSelectsSupportedType(ValueType type, boolean javaFormat) throws Exception {
        String topicName = "types_" + randomName().replace('-', '_');
        String mappingName = "pulsarTypes";
        String key = "test-key";
        new SqlMapping(mappingName, "testPulsar").externalName(topicName)
                .fields("__key VARCHAR", "this " + type.sqlType())
                .options(OPTION_KEY_FORMAT, "varchar", OPTION_VALUE_FORMAT,
                         javaFormat ? JAVA_FORMAT : type.sqlType().toLowerCase(ROOT))
                .optionsIf(javaFormat, OPTION_VALUE_CLASS, type.sqlValue().getClass().getName())
                .create(instances[0]);

        try (var reader = getClient().newReader(type.schema)
                .topic(topicName)
                .startMessageId(MessageId.earliest)
                .create()) {
            sql.executeUpdate("INSERT INTO " + mappingName + " (__key, this) VALUES (?, ?)", key, type.sqlValue());

            var message = reader.readNext(30, SECONDS);
            assertThat(message).as("message in Pulsar topic for %s", type.sqlType()).isNotNull();
            assertThat(message.getKey()).isEqualTo(key);
            assertThat(message.getValue()).isEqualTo(type.pulsarValue);

            assertRowsEventuallyInAnyOrder(instances[0], "SELECT __key, this FROM " + mappingName,
                    List.of(new SqlTestSupport.Row(key, type.sqlValue())));
        }
    }

    record JsonWrapper (String name, int number) {
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void readsJsonFromPulsarString(boolean useNativeJson) throws Exception {
        String topicName = RandomStringUtils.insecure().nextAlphabetic(20);
        String mappingName = "pulsarTypes";
        new SqlMapping(mappingName, "testPulsar").externalName(topicName)
                .fields("__key VARCHAR", "this JSON")
                .options(OPTION_KEY_FORMAT, "varchar", OPTION_VALUE_FORMAT, "json")
                .create(instances[0]);

        if (useNativeJson) {
            try (var producer = getClient().newProducer(Schema.JSON(JsonWrapper.class)).topic(topicName).create()) {
                producer.newMessage()
                        .key("key-1")
                        .value(new JsonWrapper("Żółw", 42))
                        .send();
            }
        } else {
            try (var producer = getClient().newProducer(Schema.STRING).topic(topicName).create()) {
                producer.newMessage()
                        .key("key-1")
                        .value(sampleJson())
                        .send();
            }
        }

        assertRowsEventuallyInAnyOrder(instances[0], "SELECT __key, this FROM " + mappingName,
                                       List.of(new SqlTestSupport.Row("key-1", new HazelcastJsonValue(sampleJson())))
        );
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void readsJsonObjectFromPulsarString(boolean useNativeJson) throws Exception {
        String topicName = RandomStringUtils.insecure().nextAlphabetic(20);
        String mappingName = "pulsarTypes";
        new SqlMapping(mappingName, "testPulsar").externalName(topicName)
                .fields("__key VARCHAR", "name varchar", "number int")
                .options(OPTION_KEY_FORMAT, "varchar", OPTION_VALUE_FORMAT, "json-flat")
                .create(instances[0]);

        if (useNativeJson) {
            try (var producer = getClient().newProducer(Schema.JSON(JsonWrapper.class)).topic(topicName).create()) {
                producer.newMessage()
                        .key("key-1")
                        .value(new JsonWrapper("Żółw", 42))
                        .send();
            }
        } else {
            try (var producer = getClient().newProducer(Schema.STRING).topic(topicName).create()) {
                producer.newMessage()
                        .key("key-1")
                        .value(sampleJson())
                        .send();
            }
        }

        assertRowsEventuallyInAnyOrder(instances[0], "SELECT __key, name, number FROM " + mappingName,
                                       List.of(new SqlTestSupport.Row("key-1", "Żółw", 42))
        );
    }

    private static Stream<Arguments> supportedValueTypes() {
        return Stream.of(ValueType.values())
                     .flatMap(type -> Stream.of(arguments(type, false),
                                                arguments(type, true)));
    }

    private static @NonNull String sampleJson() {
        return "{\"name\":\"Żółw\",\"number\":42}";
    }

    @SuppressWarnings("rawtypes")
    private enum ValueType {
        VARCHAR(Schema.STRING, "Żółw 'test'", "Żółw 'test'"),
        TINYINT(Schema.INT8, (byte) -123, (byte) -123),
        SMALLINT(Schema.INT16, (short) -12345, (short) -12345),
        INT(Schema.INT32, -123456789, -123456789),
        BIGINT(Schema.INT64, 1234567890123L, 1234567890123L),
        REAL(Schema.FLOAT, -123.5F, -123.5F),
        DOUBLE(Schema.DOUBLE, 123456.125D, 123456.125D),
        JSON(Schema.BYTES, new HazelcastJsonValue(sampleJson()), sampleJson().getBytes(UTF_8));

        final Schema schema;
        final Object sqlValue;
        final Object pulsarValue;

        ValueType(Schema schema, Object sqlValue, Object pulsarValue) {
            this.schema = schema;
            this.sqlValue = sqlValue;
            this.pulsarValue = pulsarValue;
        }

        Object sqlValue() {
            return sqlValue;
        }

        String sqlType() {
            return name();
        }
    }

    @Test
    public void sendsMessages(TestInfo testInfo) throws IOException {
        String testName = sanitizedTestName(testInfo);
        String fullName = "persistent://public/default/" + testName;
        createTopic(fullName, true);
        Map<String, MyMessage> results = Collections.synchronizedMap(new HashMap<>());
        try (var reader = newReader(testName, Schema.JSON(MyMessage.class), results)) {
            new SqlMapping("twoWayPulsar", "testPulsar").externalName("\"" + fullName + "\"")
                                                        .fields("__key VARCHAR", "title VARCHAR", "content VARCHAR")
                                                        .options(OPTION_KEY_FORMAT, "varchar", OPTION_VALUE_FORMAT, "json-flat")
                                                        .createOrReplace(instances[0]);

            List<MyMessage> messages = new ArrayList<>();
            messages.add(new MyMessage("Awake and Alive", "I'm awake, I'm alive, now I know what I believe inside!"));
            messages.add(new MyMessage("Hero", "I need a Hero to save me now!"));

            int i = 1;
            for (MyMessage message : messages) {
                sql.executeUpdate("INSERT INTO twoWayPulsar (__key, title, content) VALUES (?, ?, ?)",
                                  String.valueOf(i++), message.title, message.content);
            }


            reader.hasMessageAvailable();
            assertTrueEventually(() ->
                assertThat(results).containsAllEntriesOf(Map.of(
                    "1", messages.get(0),
                    "2", messages.get(1)
                )), 30);
        }
    }

    @Test
    public void sendsAndReadsMessages(TestInfo testInfo) {
        String testName = sanitizedTestName(testInfo);
        new SqlMapping("twoWayPulsar", "testPulsar").externalName(testName)
                                                    .fields("__key VARCHAR", "title VARCHAR", "content VARCHAR")
                                                    .options(OPTION_KEY_FORMAT, "varchar", OPTION_VALUE_FORMAT, "json-flat")
                                                    .createOrReplace(instances[0]);


        List<MyMessage> messages = new ArrayList<>();
        messages.add(new MyMessage("Awake and Alive", "I'm awake, I'm alive, now I know what I believe inside!"));
        messages.add(new MyMessage("Hero", "I need a Hero to save me now!"));

        int i = 1;
        for (MyMessage message : messages) {
            sql.executeUpdate("INSERT INTO twoWayPulsar (__key, title, content) VALUES (?, ?, ?)",
                              String.valueOf(i++), message.title, message.content);
        }

        i = 1;
        List<SqlTestSupport.Row> expected = new ArrayList<>();
        for (MyMessage message : messages) {
            expected.add(new SqlTestSupport.Row(String.valueOf(i++), message.title, message.content));
        }
        assertRowsEventuallyInAnyOrder(instances[0], "SELECT __key, title, content FROM twoWayPulsar", expected);
    }
}
