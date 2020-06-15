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

package com.hazelcast.jet.cdc.mysql;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.accumulator.LongAccumulator;
import com.hazelcast.jet.cdc.ChangeRecord;
import com.hazelcast.jet.cdc.Operation;
import com.hazelcast.jet.cdc.RecordPart;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.test.annotation.NightlyTest;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.cdc.mysql.AbstractMySqlIntegrationTest.DATABASE;
import static com.hazelcast.jet.core.JobStatus.RUNNING;

@Category(NightlyTest.class)
public class MySqlListenBeforeExistIntegrationTest extends AbstractMySqlIntegrationTest {

    @Test
    public void testListenBeforeDatabaseExists() throws Exception {
        List<String> expectedRecords = Arrays.asList(
                "1001/0:INSERT:TableRow {id=1001, value1=someValue1, value2=someValue2, value3=null}"
        );

        StreamSource<ChangeRecord> source = sourceBuilder()
                .setDatabaseWhitelist(DATABASE)
                .build();

        Pipeline pipeline = pipeline(source);

        // when
        JetInstance jet = createJetMembers(2)[0];
        Job job = jet.newJob(pipeline);
        assertEqualsEventually(() -> job.getStatus(), RUNNING);

        try {
            //then
            createDb(DATABASE);
            createTableWithData(DATABASE, "someTable");
            insertToTable(DATABASE, "someTable", 1001, "someValue1", "someValue2");

            assertEqualsEventually(() -> mapResultsToSortedList(jet.getMap(SINK_MAP_NAME)), expectedRecords);
        } finally {
            job.cancel();
        }
    }

    @Test
    public void testListenBeforeTableExists() throws Exception {
        // given
        createDb(DATABASE);

        List<String> expectedRecords = Arrays.asList(
                "1001/0:INSERT:TableRow {id=1001, value1=someValue1, value2=someValue2, value3=null}"
        );

        StreamSource<ChangeRecord> source = sourceBuilder()
                .setDatabaseWhitelist(DATABASE)
                .setTableWhitelist(DATABASE + ".someTable")
                .build();

        Pipeline pipeline = pipeline(source);

        // when
        JetInstance jet = createJetMembers(2)[0];
        Job job = jet.newJob(pipeline);
        assertEqualsEventually(() -> job.getStatus(), RUNNING);

        try {
            //then
            createTableWithData(DATABASE, "someTable");
            insertToTable(DATABASE, "someTable", 1001, "someValue1", "someValue2");

            assertEqualsEventually(() -> mapResultsToSortedList(jet.getMap(SINK_MAP_NAME)), expectedRecords);
        } finally {
            job.cancel();
        }
    }

    @Test
    public void testListenBeforeColumnExists() throws Exception {
        // given
        createDb(DATABASE);
        createTableWithData(DATABASE, "someTable");
        insertToTable(DATABASE, "someTable", 1001, "someValue1", "someValue2");

        List<String> expectedRecords = Arrays.asList(
                "1001/0:INSERT:TableRow {id=1001, value1=someValue1, value2=someValue2, value3=null}",
                "1002/0:INSERT:TableRow {id=1002, value1=someValue4, value2=someValue5, value3=someValue6}"
        );

        StreamSource<ChangeRecord> source = sourceBuilder()
                .setDatabaseWhitelist(DATABASE)
                .setTableWhitelist(DATABASE + ".someTable")
                .build();

        Pipeline pipeline = pipeline(source);

        // when
        JetInstance jet = createJetMembers(2)[0];
        Job job = jet.newJob(pipeline);
        assertEqualsEventually(() -> job.getStatus(), RUNNING);

        try {
            assertEqualsEventually(() -> mapResultsToSortedList(jet.getMap(SINK_MAP_NAME)), Arrays.asList(
                    "1001/0:INSERT:TableRow {id=1001, value1=someValue1, value2=someValue2, value3=null}"
            ));
            //then
            insertNewColumnToTable(DATABASE, "someTable", "value_3");
            insertToTable(DATABASE, "someTable", 1002, "someValue4", "someValue5", "someValue6");

            assertEqualsEventually(() -> mapResultsToSortedList(jet.getMap(SINK_MAP_NAME)), expectedRecords);
        } finally {
            job.cancel();
        }
    }

    private Pipeline pipeline(StreamSource<ChangeRecord> source) {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(source)
                .withNativeTimestamps(0)
                .<ChangeRecord>customTransform("filter_timestamps", filterTimestampsProcessorSupplier())
                .setLocalParallelism(1)
                .groupingKey(record -> (Integer) record.key().toMap().get("id"))
                .mapStateful(
                        LongAccumulator::new,
                        (accumulator, customerId, record) -> {
                            long count = accumulator.get();
                            accumulator.add(1);
                            Operation operation = record.operation();
                            RecordPart value = record.value();
                            TableRow customer = value.toObject(TableRow.class);
                            return entry(customerId + "/" + count, operation + ":" + customer);
                        })
                .setLocalParallelism(1)
                .writeTo(Sinks.map(SINK_MAP_NAME));
        return pipeline;
    }

    private void createTableWithData(String dbName, String tableName) throws SQLException {
        try (Connection connection = DriverManager.getConnection(mysql.withDatabaseName(dbName).getJdbcUrl(),
                mysql.getUsername(), mysql.getPassword())) {
            connection
                    .prepareStatement("CREATE TABLE " + tableName + " (\n"
                            + "  id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,\n"
                            + "  value_1 VARCHAR(255) NOT NULL,\n"
                            + "  value_2 VARCHAR(255) NOT NULL\n"
                            + ")")
                    .executeUpdate();
            connection
                    .prepareStatement("ALTER TABLE " + tableName + " AUTO_INCREMENT = 1001 ;")
                    .executeUpdate();
        }
    }

    private void insertToTable(String dbName, String tableName, int id, String val1, String val2) throws SQLException {
        insertToTable(dbName, tableName, id, val1, val2, null);
    }

    private void insertToTable(String dbName, String tableName, int id, String val1, String val2, String val3)
            throws SQLException {
        StringBuilder statement = new StringBuilder();
        statement.append("INSERT INTO ").append(tableName).append(" VALUES ( ")
                .append(id).append(", '")
                .append(val1).append("', '")
                .append(val2).append("'");
        if (val3 != null) {
            statement.append(", '").append(val3).append("'");
        }
        statement.append(")");
        try (Connection connection = DriverManager.getConnection(mysql.withDatabaseName(dbName).getJdbcUrl(),
                mysql.getUsername(), mysql.getPassword())) {
            connection
                    .prepareStatement(statement.toString())
                    .executeUpdate();

        }
    }

    private void insertNewColumnToTable(String dbName, String tableName, String column) throws SQLException {
        try (Connection connection = DriverManager.getConnection(mysql.withDatabaseName(dbName).getJdbcUrl(),
                mysql.getUsername(), mysql.getPassword())) {
            connection
                    .prepareStatement("ALTER TABLE " + tableName + " ADD COLUMN " + column + " VARCHAR(255) NOT NULL;")
                    .executeUpdate();
        }
    }

}
