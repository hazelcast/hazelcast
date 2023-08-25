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

import com.hazelcast.core.HazelcastException;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FORMAT;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class KafkaDataConnectionIntegrationTest extends KafkaSqlTestSupport {

    private static final int PARTITION_COUNT = 1;

    @Test
    public void when_createNonSharedProducer_then_success() {
        String dlName = randomName();
        String name1 = createRandomTopic(PARTITION_COUNT);
        String name2 = createRandomTopic(PARTITION_COUNT);
        createSqlKafkaDataConnection(dlName, false);
        createKafkaMappingUsingDataConnection(name1, dlName, constructMappingOptions("int", "varchar"));
        createKafkaMappingUsingDataConnection(name2, dlName, constructMappingOptions("varchar", "int"));

        sqlService.execute("INSERT INTO " + name1 + " VALUES" +
                "(0, 'value-0')" +
                ", (1, 'value-1')" +
                ", (2, 'value-2')" +
                ", (10, 'value-10')"
        );

        assertTipOfStream(
                "SELECT window_start, window_end, MAX(__key) FROM " +
                        "TABLE(TUMBLE(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name1 + ", DESCRIPTOR(__key), 2)))" +
                        "  , DESCRIPTOR(__key)" +
                        "  , 2" +
                        ")) " +
                        "GROUP BY window_start, window_end",
                asList(
                        new Row(0, 2, 1),
                        new Row(2, 4, 2)
                )
        );

        sqlService.execute("INSERT INTO " + name2 + " VALUES" +
                "('value-0', 0)" +
                ", ('value-1', 1)" +
                ", ('value-2', 2)" +
                ", ('value-10', 10)"
        );

        assertTipOfStream(
                "SELECT window_start, window_end, COUNT(__key) FROM " +
                        "TABLE(TUMBLE(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name2 + ", DESCRIPTOR(this), 2)))" +
                        "  , DESCRIPTOR(this)" +
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
    public void when_createSharedProducer_then_success() {
        String dcName = randomName();
        String name = createRandomTopic(PARTITION_COUNT);
        createSqlKafkaDataConnection(dcName, true);
        createKafkaMappingUsingDataConnection(name, dcName, constructMappingOptions("int", "varchar"));

        try (SqlResult r = sqlService.execute("INSERT INTO " + name + " VALUES (0, 'value-0')")) {
            assertThat(r.updateCount()).isZero();
        }
    }

    @Test
    public void when_createSharedProducerWithOverriddenProperty_then_success() {
        String dlName = randomName();
        String name = createRandomTopic(PARTITION_COUNT);
        createSqlKafkaDataConnection(dlName, true);
        // not created
        createKafkaMappingUsingDataConnection(name, dlName,
                "OPTIONS ('" + OPTION_KEY_FORMAT + "'='int', '" + OPTION_VALUE_FORMAT + "'='varchar', "
                        + "'auto.offset.reset' = 'latest')"); // changed option

        // TODO: move this error to mapping creation level.
        assertThatThrownBy(() -> sqlService.execute(
                "INSERT INTO " + name + " VALUES" + "(0, 'value-0')"))
                .hasRootCauseInstanceOf(HazelcastException.class)
                .hasMessageContaining("For shared Kafka producer, please provide all serialization options");

    }

    /**
     * <a href="https://github.com/hazelcast/hazelcast/issues/24283">Issue 24283</a>
     */
    @Test
    public void when_creatingDataConnectionAndMapping_then_serdeDeterminesAutomatically() {
        String dlName = randomName();
        String name = createRandomTopic(PARTITION_COUNT);
        createSqlKafkaDataConnection(dlName, false); // connection is not shared
        createKafkaMappingUsingDataConnection(name, dlName, constructMappingOptions("int", "varchar"));

        try (SqlResult r = sqlService.execute("INSERT INTO " + name + " VALUES (0, 'value-0')")) {
            assertThat(r.updateCount()).isZero();
        }

        assertTipOfStream("SELECT * FROM " + name, singletonList(new Row(0, "value-0")));
    }
}
