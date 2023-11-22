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

import org.junit.Test;

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FORMAT;
import static org.assertj.core.api.Assertions.assertThat;

public class SqlPlanCacheTest extends KafkaSqlTestSupport {
    private static final int INITIAL_PARTITION_COUNT = 4;

    private static void createMapping(String tableName, String topicName, String valueFormat, String offset) {
        new SqlMapping(tableName, KafkaSqlConnector.class)
                .externalName(topicName)
                .options(OPTION_VALUE_FORMAT, valueFormat,
                         "bootstrap.servers", kafkaTestSupport.getBrokerConnectionString(),
                         "auto.offset.reset", offset)
                .createOrReplace();
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
        return createRandomTopic(INITIAL_PARTITION_COUNT);
    }
}
