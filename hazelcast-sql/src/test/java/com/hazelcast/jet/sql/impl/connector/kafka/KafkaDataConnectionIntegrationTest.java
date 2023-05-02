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

import static java.util.Arrays.asList;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class KafkaDataConnectionIntegrationTest extends KafkaSqlTestSupport {

    private static final int PARTITION_COUNT = 1;

    @Test
    public void test_nonShared() {
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
    public void test_shared() {
        String dlName = randomName();
        String name1 = createRandomTopic(PARTITION_COUNT);
        String name2 = createRandomTopic(PARTITION_COUNT);
        createSqlKafkaDataConnection(dlName, true);
        createKafkaMappingUsingDataConnection(name1, dlName, constructMappingOptions("int", "varchar"));
        createKafkaMappingUsingDataConnection(name2, dlName, constructMappingOptions("varchar", "int"));

        // TODO: move this error to mapping creation level.
        assertThatThrownBy(() -> sqlService.execute(
                        "INSERT INTO " + name1 + " VALUES" + "(0, 'value-0')"))
                .hasRootCauseInstanceOf(HazelcastException.class)
                .hasMessageContaining("Shared Kafka producer can be created only with data connection options");

    }

    /**
     * <a href="https://github.com/hazelcast/hazelcast/issues/24283">Issue 24283</a>
     */
    @Test
    public void when_creatingDataConnectionAndMapping_then_serdeDeterminesAutomatically() {
        String dlName = randomName();
        String name = createRandomTopic(PARTITION_COUNT);
        createSqlKafkaDataConnection(dlName, true);
        createKafkaMappingUsingDataConnection(name, dlName, constructMappingOptions("int", "varchar"));

        try (SqlResult r = sqlService.execute("CREATE JOB job AS SINK INTO " + name + " VALUES (0, 'value-0')")) {
            assertEquals(0, r.updateCount());
        }

        sqlService.execute("SELECT * FROM " + name).close();
    }
}
