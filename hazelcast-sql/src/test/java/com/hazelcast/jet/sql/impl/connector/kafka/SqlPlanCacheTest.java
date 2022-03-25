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

import com.hazelcast.jet.kafka.impl.KafkaTestSupport;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.sql.SqlService;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FORMAT;
import static org.assertj.core.api.Assertions.assertThat;

public class SqlPlanCacheTest extends SqlTestSupport {

    private static final int INITIAL_PARTITION_COUNT = 4;

    private static KafkaTestSupport kafkaTestSupport;

    private static SqlService sqlService;

    @BeforeClass
    public static void setUpClass() throws IOException {
        initialize(1, null);
        sqlService = instance().getSql();

        kafkaTestSupport = KafkaTestSupport.create();
        kafkaTestSupport.createKafkaCluster();
    }

    @AfterClass
    public static void tearDownClass() {
        kafkaTestSupport.shutdownKafkaCluster();
    }

    @Test
    public void test_tableName() {
        String topicName = createRandomTopic();

        createMapping("kafka1", topicName, "int", "earliest");
        sqlService.execute("SELECT * FROM kafka1");
        assertThat(planCache(instance()).size()).isEqualTo(1);

        createMapping("kafka2", topicName, "int", "earliest");
        sqlService.execute("DROP MAPPING kafka1");
        assertThat(planCache(instance()).size()).isZero();
    }

    @Test
    public void test_topicName() {
        createMapping("kafka", createRandomTopic(), "int", "earliest");
        sqlService.execute("SELECT * FROM kafka");
        assertThat(planCache(instance()).size()).isEqualTo(1);

        createMapping("kafka", createRandomTopic(), "int", "earliest");
        assertThat(planCache(instance()).size()).isZero();
    }

    @Test
    public void test_fieldList() {
        String topicName = createRandomTopic();

        createMapping("kafka", topicName, "int", "earliest");
        sqlService.execute("SELECT * FROM kafka");
        assertThat(planCache(instance()).size()).isEqualTo(1);

        createMapping("kafka", topicName, "varchar", "earliest");
        assertThat(planCache(instance()).size()).isZero();
    }

    @Test
    public void test_options() {
        String topicName = createRandomTopic();

        createMapping("kafka", topicName, "int", "earliest");
        sqlService.execute("SELECT * FROM kafka");
        assertThat(planCache(instance()).size()).isEqualTo(1);

        createMapping("kafka", topicName, "int", "latest");
        assertThat(planCache(instance()).size()).isZero();
    }

    @Test
    public void test_dmlCaching() {
        String topicName = createRandomTopic();

        createMapping("kafka", topicName, "int", "earliest");
        sqlService.execute("INSERT INTO kafka VALUES(0)");
        assertThat(planCache(instance()).size()).isEqualTo(1);

        sqlService.execute("SINK INTO kafka VALUES(0)");
        assertThat(planCache(instance()).size()).isEqualTo(2);

        sqlService.execute("DROP MAPPING kafka");
        assertThat(planCache(instance()).size()).isZero();
    }

    private static String createRandomTopic() {
        String topicName = randomName();
        kafkaTestSupport.createTopic(topicName, INITIAL_PARTITION_COUNT);
        return topicName;
    }

    private static void createMapping(
            String tableName,
            String topicName,
            String valueFormat,
            String offset
    ) {
        sqlService.execute("CREATE OR REPLACE MAPPING " + tableName + " EXTERNAL NAME " + topicName + ' '
                + "TYPE " + KafkaSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ( "
                + '\'' + OPTION_VALUE_FORMAT + "'='" + valueFormat + '\''
                + ", 'bootstrap.servers'='" + kafkaTestSupport.getBrokerConnectionString() + '\''
                + ", 'auto.offset.reset'='" + offset + '\''
                + ")"
        );
    }
}
