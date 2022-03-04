/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.cdc.postgres;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.accumulator.LongAccumulator;
import com.hazelcast.jet.cdc.ChangeRecord;
import com.hazelcast.jet.cdc.Operation;
import com.hazelcast.jet.cdc.RecordPart;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("SameParameterValue")
@Category(NightlyTest.class)
public class PostgresCdcListenBeforeExistsIntegrationTest extends AbstractPostgresCdcIntegrationTest {

    private static final String SCHEMA = "testSchema";
    private static final String SINK_MAP_NAME = "results";

    @Test
    public void listenBeforeSchemaExists() throws Exception {
        List<String> expectedRecords = Arrays.asList(
                "1001/0:(SYNC|INSERT):TableRow \\{id=1001, value1=someValue1, value2=someValue2, value3=null\\}"
        );

        StreamSource<ChangeRecord> source = sourceBuilder("source")
                .setSchemaWhitelist(SCHEMA)
                .build();
        Pipeline pipeline = pipeline(source);

        // when
        HazelcastInstance hz = createHazelcastInstances(2)[0];
        Job job = hz.getJet().newJob(pipeline);
        assertJobStatusEventually(job, RUNNING);

        assertReplicationSlotActive();

        try {
            //then
            createSchema(SCHEMA);
            createTableWithData(SCHEMA, "someTable");
            insertIntoTable(SCHEMA, "someTable", 1001, "someValue1", "someValue2");

            assertTrueEventually(() -> assertMatch(expectedRecords, mapResultsToSortedList(hz.getMap(SINK_MAP_NAME))));
        } finally {
            job.cancel();
            assertJobStatusEventually(job, JobStatus.FAILED);
        }
    }

    @Test
    public void listenBeforeTableExists() throws Exception {
        // given
        createSchema(SCHEMA);

        List<String> expectedRecords = Collections.singletonList(
                "1001/0:(SYNC|INSERT):TableRow \\{id=1001, value1=someValue1, value2=someValue2, value3=null\\}"
        );

        StreamSource<ChangeRecord> source = sourceBuilder("source")
                .setSchemaWhitelist(SCHEMA)
                .setTableWhitelist(SCHEMA + ".someTable")
                .build();

        Pipeline pipeline = pipeline(source);

        // when
        HazelcastInstance hz = createHazelcastInstances(2)[0];
        Job job = hz.getJet().newJob(pipeline);
        assertJobStatusEventually(job, RUNNING);
        assertReplicationSlotActive();

        try {
            //then
            createTableWithData(SCHEMA, "someTable");
            insertIntoTable(SCHEMA, "someTable", 1001, "someValue1", "someValue2");

            assertTrueEventually(() -> assertMatch(expectedRecords, mapResultsToSortedList(hz.getMap(SINK_MAP_NAME))));
        } finally {
            job.cancel();
            assertJobStatusEventually(job, JobStatus.FAILED);
        }
    }

    @Test
    public void listenBeforeColumnExists() throws Exception {
        // given
        createSchema(SCHEMA);
        createTableWithData(SCHEMA, "someTable");
        insertIntoTable(SCHEMA, "someTable", 1001, "someValue1", "someValue2");

        List<String> expectedRecords = Arrays.asList(
                "1001/0:(SYNC|INSERT):TableRow \\{id=1001, value1=someValue1, value2=someValue2, value3=null\\}",
                "1002/0:(SYNC|INSERT):TableRow \\{id=1002, value1=someValue4, value2=someValue5, value3=someValue6\\}"
        );

        StreamSource<ChangeRecord> source = sourceBuilder("source")
                .setSchemaWhitelist(SCHEMA)
                .setTableWhitelist(SCHEMA + ".someTable")
                .build();

        Pipeline pipeline = pipeline(source);

        // when
        HazelcastInstance hz = createHazelcastInstances(2)[0];
        Job job = hz.getJet().newJob(pipeline);
        assertJobStatusEventually(job, RUNNING);
        assertReplicationSlotActive();

        try {
            assertTrueEventually(() -> assertMatch(Collections.singletonList(
                    "1001/0:(SYNC|INSERT):TableRow \\{id=1001, value1=someValue1, value2=someValue2, value3=null\\}"),
                    mapResultsToSortedList(hz.getMap(SINK_MAP_NAME))));
            //then
            addColumnToTable(SCHEMA, "someTable", "value_3");
            insertIntoTable(SCHEMA, "someTable", 1002, "someValue4", "someValue5", "someValue6");

            assertTrueEventually(() -> assertMatch(expectedRecords, mapResultsToSortedList(hz.getMap(SINK_MAP_NAME))));
        } finally {
            job.cancel();
            assertJobStatusEventually(job, JobStatus.FAILED);
        }
    }

    private void createTableWithData(String schema, String table) throws SQLException {
        try (Connection connection = getConnection(postgres)) {
            Statement statement = connection.createStatement();
            statement.addBatch("SET search_path TO " + schema);
            statement.addBatch("CREATE TABLE " + table + " (\n"
                    + "  id SERIAL NOT NULL PRIMARY KEY,\n"
                    + "  value_1 VARCHAR(255) NOT NULL,\n"
                    + "  value_2 VARCHAR(255) NOT NULL\n"
                    + ")");
            statement.addBatch("ALTER SEQUENCE " + table + "_id_seq RESTART WITH 1001");
            statement.addBatch("ALTER TABLE " + table + " REPLICA IDENTITY FULL");
            statement.executeBatch();
        }
    }

    private void insertIntoTable(String schema, String table, int id, String val1, String val2) throws SQLException {
        insertIntoTable(schema, table, id, val1, val2, null);
    }

    private void insertIntoTable(String schema, String table, int id, String val1, String val2, String val3)
            throws SQLException {
        StringBuilder insertSql = new StringBuilder();
        insertSql.append("INSERT INTO ").append(table).append(" VALUES ( ")
                .append(id).append(", '")
                .append(val1).append("', '")
                .append(val2).append("'");
        if (val3 != null) {
            insertSql.append(", '").append(val3).append("'");
        }
        insertSql.append(")");

        try (Connection connection = getConnection(postgres)) {
            connection.setSchema(schema);
            Statement statement = connection.createStatement();
            statement.addBatch("SET search_path TO " + schema);
            statement.addBatch(insertSql.toString());
            statement.executeBatch();
        }
    }

    private void addColumnToTable(String schema, String table, String column) throws SQLException {
        try (Connection connection = getConnection(postgres)) {
            Statement statement = connection.createStatement();
            statement.addBatch("SET search_path TO " + schema);
            statement.addBatch("ALTER TABLE " + table + " ADD COLUMN " + column + " VARCHAR(255);");
            statement.executeBatch();
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
                        (accumulator, rowId, record) -> {
                            long count = accumulator.get();
                            accumulator.add(1);
                            Operation operation = record.operation();
                            RecordPart value = record.value();
                            TableRow row = value.toObject(TableRow.class);
                            return entry(rowId + "/" + count, operation + ":" + row);
                        })
                .setLocalParallelism(1)
                .peek()
                .writeTo(Sinks.map(SINK_MAP_NAME));
        return pipeline;
    }

    private void assertReplicationSlotActive() {
        assertTrueEventually(() -> {
            try (Connection connection = getConnection(postgres)) {
                PreparedStatement preparedStatement = connection.prepareStatement(
                        "select * from pg_replication_slots where slot_name = ? and database = ?");
                preparedStatement.setString(1, REPLICATION_SLOT_NAME);
                preparedStatement.setString(2, DATABASE_NAME);
                ResultSet resultSet = preparedStatement.executeQuery();
                assertTrue(resultSet.next() && resultSet.getBoolean("active"));
            }
        });
    }

}
