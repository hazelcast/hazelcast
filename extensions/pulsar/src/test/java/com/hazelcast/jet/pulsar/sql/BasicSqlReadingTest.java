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
import com.hazelcast.jet.pulsar.PulsarDataConnection;
import com.hazelcast.jet.pulsar.PulsarTestSupport;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.jet.sql.SqlTestSupport.SqlMapping;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.test.SerialTest;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.stream.IntStream;

import static com.hazelcast.jet.sql.SqlTestSupport.assertRowsEventuallyInAnyOrder;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_CLASS;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FORMAT;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SerialTest
public class BasicSqlReadingTest extends PulsarTestSupport {

    private HazelcastInstance[] instances;

    @BeforeEach
    public void setup() {
        var conf = smallInstanceConfig();
        conf.addDataConnectionConfig(PulsarDataConnection.pulsarDataConnectionConf("testPulsar",
                                                                                   pulsarContainer.getPulsarBrokerUrl(),
                                                                                   pulsarContainer.getHttpServiceUrl(),
                                                                                   true));
        instances = createHazelcastInstances(conf, 2);
    }

    @Test
    void throwsOnIncorrectKeyType() {
        assertThatThrownBy(() ->
            new SqlMapping("test", "testPulsar")
                .fields("__key INT")
                .fields("this VARCHAR")
                .options(OPTION_KEY_FORMAT, "varchar", OPTION_VALUE_FORMAT, "varchar")
                .createIfNotExists(instances[0])
        ).isInstanceOf(HazelcastSqlException.class)
            .hasMessageContaining("__key field must be of one of types: [varchar, character varying, char varying]");

        assertThatThrownBy(() ->
            new SqlMapping("test", "testPulsar")
                .fields("__key varchar")
                .fields("this VARCHAR")
                .options(OPTION_KEY_FORMAT, "int", OPTION_VALUE_FORMAT, "varchar")
                .createIfNotExists(instances[0])
        ).isInstanceOf(HazelcastSqlException.class)
            .hasMessageContaining("keyFormat option must be one of: `java` or [varchar, character varying, char varying]");
    }

    @Test
    void throwsOnIncorrectCompositeKeyType(TestInfo testInfo) {
        String testName = sanitizedTestName(testInfo);

        assertThatThrownBy(() ->
                               new SqlMapping("test", "testPulsar").externalName(testName)
                                                                   .fields("id VARCHAR EXTERNAL NAME \"__key.a\"")
                                                                   .fields("id2 INT EXTERNAL NAME \"__key.b\"")
                                                                   .fields("this VARCHAR")
                                                                   .options(OPTION_KEY_FORMAT, "java",
                                                                            OPTION_KEY_CLASS, Pojo.class.getName(),
                                                                            OPTION_VALUE_FORMAT, "varchar")
                                                                   .createIfNotExists(instances[0])
        ).hasMessageContaining("More than one field specified as part of __key field");
    }

    @Test
    void readsSimplePulsarSource(TestInfo testInfo) throws Exception {
        String testName = sanitizedTestName(testInfo);
        try (Producer<String> producer = getClient()
                    .newProducer(Schema.STRING)
                    .topic(testName)
                    .create()) {
            new SqlMapping("testPulsarMapping", "testPulsar").externalName(testName)
                .fields("__key VARCHAR", "this VARCHAR")
                .options(OPTION_KEY_FORMAT, "varchar", OPTION_VALUE_FORMAT, "varchar")
                .createOrReplace(instances[0]);
            for (int i = 1; i <= 10; i++) {
                producer.newMessage().key("key-" + i).value("value-" + i).send();
            }

            var expected = IntStream.range(1, 11)
                                    .mapToObj(i -> new SqlTestSupport.Row("key-" + i, "value-" + i))
                                    .toList();
            assertRowsEventuallyInAnyOrder(instances[0], "SELECT * FROM testPulsarMapping", expected);
        }
    }

    record Pojo (String a, int b) {
    }

    @Test
    void readsPojoFromPulsarSource(TestInfo testInfo) throws Exception {
        String testName = sanitizedTestName(testInfo);
        try (Producer<Pojo> producer = getClient()
                    .newProducer(Schema.JSON(Pojo.class))
                    .topic(testName)
                    .create()) {
            new SqlMapping("testPulsarMapping", "testPulsar").externalName(testName)
                                                      .fields("__key VARCHAR", "a VARCHAR", "b INT")
                                                      .options(OPTION_KEY_FORMAT, "varchar", OPTION_VALUE_FORMAT, "json-flat")
                                                      .createOrReplace(instances[0]);
            for (int i = 1; i <= 10; i++) {
                producer.newMessage().key("key-" + i).value(new Pojo("value-" + i, i)).send();
            }

            var expected = IntStream.rangeClosed(1, 10)
                                    .mapToObj(i -> new SqlTestSupport.Row("key-" + i, "value-" + i, i))
                                    .toList();
            assertRowsEventuallyInAnyOrder(instances[0], "SELECT * FROM testPulsarMapping", expected);
        }
    }

    @Test
    void noNullPointerExceptionWhenKeyFormatOptionIsOmitted(TestInfo testInfo) {
        String testName = sanitizedTestName(testInfo);

        // __key is declared but keyFormat is never set
        assertThatThrownBy(() ->
                               new SqlMapping("test", "testPulsar").externalName(testName)
                                                                   .fields("__key VARCHAR", "this VARCHAR")
                                                                   .options(OPTION_VALUE_FORMAT, "varchar")
                                                                   .createIfNotExists(instances[0])
        ).isInstanceOf(HazelcastSqlException.class)
            .hasMessageContaining("keyFormat option must be one of: `java`");
    }
}
