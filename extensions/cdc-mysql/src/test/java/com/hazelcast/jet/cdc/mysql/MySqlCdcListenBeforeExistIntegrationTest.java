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

package com.hazelcast.jet.cdc.mysql;

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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.core.JobStatus.RUNNING;

@Category(NightlyTest.class)
public class MySqlCdcListenBeforeExistIntegrationTest extends AbstractMySqlCdcIntegrationTest {

    private static final String DATABASE = "testDb";
    private static final String SINK_MAP_NAME = "resultsMap";

    @Test
    public void listenBeforeDatabaseExists() throws Exception {
        List<String> expectedRecords = Collections.singletonList(
                "1001/0:INSERT:TableRow {id=1001, value1=someValue1, value2=someValue2, value3=null}"
        );

        StreamSource<ChangeRecord> source = sourceBuilder("cdcMysql")
                .setDatabaseWhitelist(DATABASE)
                .build();

        Pipeline pipeline = pipeline(source);

        // when
        HazelcastInstance hz = createHazelcastInstances(2)[0];
        Job job = hz.getJet().newJob(pipeline);
        assertJobStatusEventually(job, RUNNING);

        try {
            //then
            createDb(DATABASE);
            createTableWithData(DATABASE, "someTable");
            insertToTable(DATABASE, "someTable", 1001, "someValue1", "someValue2");

            assertEqualsEventually(() -> mapResultsToSortedList(hz.getMap(SINK_MAP_NAME)), expectedRecords);
        } finally {
            job.cancel();
            assertJobStatusEventually(job, JobStatus.FAILED);
        }
    }

    @Test
    public void listenBeforeTableExists() throws Exception {
        // given
        createDb(DATABASE);

        List<String> expectedRecords = Collections.singletonList(
                "1001/0:INSERT:TableRow {id=1001, value1=someValue1, value2=someValue2, value3=null}"
        );

        StreamSource<ChangeRecord> source = sourceBuilder("cdcMysql")
                .setDatabaseWhitelist(DATABASE)
                .setTableWhitelist(DATABASE + ".someTable")
                .build();

        Pipeline pipeline = pipeline(source);

        // when
        HazelcastInstance hz = createHazelcastInstances(2)[0];
        Job job = hz.getJet().newJob(pipeline);
        assertJobStatusEventually(job, RUNNING);

        try {
            //then
            createTableWithData(DATABASE, "someTable");
            insertToTable(DATABASE, "someTable", 1001, "someValue1", "someValue2");

            assertEqualsEventually(() -> mapResultsToSortedList(hz.getMap(SINK_MAP_NAME)), expectedRecords);
        } finally {
            job.cancel();
            assertJobStatusEventually(job, JobStatus.FAILED);
        }
    }

    @Test
    public void listenBeforeColumnExists() throws Exception {
        // given
        createDb(DATABASE);
        createTableWithData(DATABASE, "someTable");
        insertToTable(DATABASE, "someTable", 1001, "someValue1", "someValue2");

        List<String> expectedRecords = Arrays.asList(
                "1001/0:INSERT:TableRow {id=1001, value1=someValue1, value2=someValue2, value3=null}",
                "1002/0:INSERT:TableRow {id=1002, value1=someValue4, value2=someValue5, value3=someValue6}"
        );

        StreamSource<ChangeRecord> source = sourceBuilder("cdcMysql")
                .setDatabaseWhitelist(DATABASE)
                .setTableWhitelist(DATABASE + ".someTable")
                .build();

        Pipeline pipeline = pipeline(source);

        // when
        HazelcastInstance hz = createHazelcastInstances(2)[0];
        Job job = hz.getJet().newJob(pipeline);
        assertJobStatusEventually(job, RUNNING);

        try {
            assertEqualsEventually(() -> mapResultsToSortedList(hz.getMap(SINK_MAP_NAME)), Collections.singletonList(
                    "1001/0:INSERT:TableRow {id=1001, value1=someValue1, value2=someValue2, value3=null}"
            ));
            //then
            addColumnToTable(DATABASE, "someTable", "value_3");
            insertToTable(DATABASE, "someTable", 1002, "someValue4", "someValue5", "someValue6");

            assertEqualsEventually(() -> mapResultsToSortedList(hz.getMap(SINK_MAP_NAME)), expectedRecords);
        } finally {
            job.cancel();
            assertJobStatusEventually(job, JobStatus.FAILED);
        }
    }

    private void createTableWithData(String database, String table) throws SQLException {
        try (Connection connection = getConnection(mysql, database)) {
            Statement statement = connection.createStatement();
            statement.addBatch("CREATE TABLE " + table + " (\n"
                            + "  id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,\n"
                            + "  value_1 VARCHAR(255) NOT NULL,\n"
                            + "  value_2 VARCHAR(255) NOT NULL\n"
                            + ")");
            statement.addBatch("ALTER TABLE " + table + " AUTO_INCREMENT = 1001 ;");
            statement.executeBatch();
        }
    }

    private void insertToTable(String database, String table, int id, String val1, String val2) throws SQLException {
        insertToTable(database, table, id, val1, val2, null);
    }

    private void insertToTable(String database, String table, int id, String val1, String val2, String val3)
            throws SQLException {
        StringBuilder statement = new StringBuilder();
        statement.append("INSERT INTO ").append(table).append(" VALUES ( ")
                .append(id).append(", '")
                .append(val1).append("', '")
                .append(val2).append("'");
        if (val3 != null) {
            statement.append(", '").append(val3).append("'");
        }
        statement.append(")");
        try (Connection connection = getConnection(mysql, database)) {
            connection.createStatement().execute(statement.toString());

        }
    }

    private void addColumnToTable(String database, String table, String column) throws SQLException {
        try (Connection connection = getConnection(mysql, database)) {
            connection.createStatement()
                    .execute("ALTER TABLE " + table + " ADD COLUMN " + column + " VARCHAR(255);");
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

}
