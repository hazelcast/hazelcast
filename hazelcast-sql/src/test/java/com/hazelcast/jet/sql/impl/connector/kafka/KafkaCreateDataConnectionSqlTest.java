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

import com.hazelcast.dataconnection.DataConnection;
import com.hazelcast.jet.kafka.KafkaDataConnection;
import com.hazelcast.jet.kafka.impl.KafkaTestSupport;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class KafkaCreateDataConnectionSqlTest extends SqlTestSupport {

    private static final KafkaTestSupport kafkaTestSupport = KafkaTestSupport.create();

    @BeforeClass
    public static void beforeClass() throws Exception {
        kafkaTestSupport.createKafkaCluster();
        initialize(1, null);
    }

    @AfterClass
    public static void afterClass() {

        kafkaTestSupport.shutdownKafkaCluster();
    }

    @Test
    public void test() {
        String dlName = randomName();
        instance().getSql().execute("CREATE DATA CONNECTION " + dlName + " TYPE Kafka SHARED " + options());

        DataConnection dataConnection = getNodeEngineImpl(
                instance()).getDataConnectionService().getAndRetainDataConnection(dlName, KafkaDataConnection.class);

        assertThat(dataConnection).isNotNull();
        assertThat(dataConnection.getConfig().getType()).isEqualTo("Kafka");
    }

    protected static String options() {
        return String.format("OPTIONS ( " +
                        "'bootstrap.servers' = '%s', " +
                        "'key.deserializer' = '%s', " +
                        "'key.serializer' = '%s', " +
                        "'value.serializer' = '%s', " +
                        "'value.deserializer' = '%s', " +
                        "'auto.offset.reset' = 'earliest') ",
                kafkaTestSupport.getBrokerConnectionString(),
                IntegerDeserializer.class.getCanonicalName(),
                IntegerSerializer.class.getCanonicalName(),
                StringSerializer.class.getCanonicalName(),
                StringDeserializer.class.getCanonicalName());
    }
}
