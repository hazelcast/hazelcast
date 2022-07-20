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
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FORMAT;
import static java.util.Arrays.asList;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlKafkaAggregateTest extends SqlTestSupport {

    private static final int INITIAL_PARTITION_COUNT = 1;

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
    public void test_tumble() {
        String name = createRandomTopic();
        sqlService.execute("CREATE MAPPING " + name + ' '
                + "TYPE " + KafkaSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ( "
                + '\'' + OPTION_KEY_FORMAT + "'='int'"
                + ", '" + OPTION_VALUE_FORMAT + "'='varchar'"
                + ", 'bootstrap.servers'='" + kafkaTestSupport.getBrokerConnectionString() + '\''
                + ", 'auto.offset.reset'='earliest'"
                + ")"
        );
        sqlService.execute("INSERT INTO " + name + " VALUES" +
                "(0, 'value-0')" +
                ", (1, 'value-1')" +
                ", (2, 'value-2')" +
                ", (10, 'value-10')"
        );

        assertTipOfStream(
                "SELECT window_start, window_end, COUNT(*) FROM " +
                        "TABLE(TUMBLE(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(__key), 2)))" +
                        "  , DESCRIPTOR(__key)" +
                        "  , 2" +
                        ")) " +
                        "GROUP BY window_start, window_end",
                asList(
                        new Row(0, 2, 2L),
                        new Row(2, 4, 1L)
                )
        );
    }

    @Test
    public void test_hop() {
        String name = createRandomTopic();
        sqlService.execute("CREATE MAPPING " + name + ' '
                + "TYPE " + KafkaSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ( "
                + '\'' + OPTION_KEY_FORMAT + "'='int'"
                + ", '" + OPTION_VALUE_FORMAT + "'='varchar'"
                + ", 'bootstrap.servers'='" + kafkaTestSupport.getBrokerConnectionString() + '\''
                + ", 'auto.offset.reset'='earliest'"
                + ")"
        );
        sqlService.execute("INSERT INTO " + name + " VALUES" +
                "(0, 'value-0')" +
                ", (1, 'value-1')" +
                ", (2, 'value-2')" +
                ", (10, 'value-10')"
        );

        assertTipOfStream(
                "SELECT window_start, window_end, SUM(__key) FROM " +
                        "TABLE(HOP(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(__key), 2))), " +
                        "DESCRIPTOR(__key), 4, 2)) " +
                        "GROUP BY window_start, window_end",
                asList(
                        new Row(-2, 2, 1L),
                        new Row(0, 4, 3L),
                        new Row(2, 6, 2L)
                )
        );
    }

    private static String createRandomTopic() {
        String topicName = randomName();
        kafkaTestSupport.createTopic(topicName, INITIAL_PARTITION_COUNT);
        return topicName;
    }
}
