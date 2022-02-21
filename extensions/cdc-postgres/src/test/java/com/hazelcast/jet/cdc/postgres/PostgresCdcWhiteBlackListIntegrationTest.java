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
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.hazelcast.jet.Util.entry;

@Category(NightlyTest.class)
public class PostgresCdcWhiteBlackListIntegrationTest extends AbstractPostgresCdcIntegrationTest {

    private static final String SCHEMA_PREFIX = "schema";
    private static final String SINK_MAP_NAME = "resultsMap";

    @Before
    public void before() throws SQLException {
        createSchemaWithData(1);
        createSchemaWithData(2);
        createSchemaWithData(3);
    }

    @Test
    public void noWhiteBlacklist() throws Exception {
        StreamSource<ChangeRecord> source = sourceBuilder("source")
                .build();

        List<String> expectedRecords = allExpectedOperations();

        test(source, expectedRecords, 27);
    }

    @Test
    public void whitelistSchema() throws Exception {
        StreamSource<ChangeRecord> source = sourceBuilder("source")
                .setSchemaWhitelist(SCHEMA_PREFIX + "1", SCHEMA_PREFIX + "3")
                .build();

        List<String> expectedRecords = new ArrayList<>();
        expectedRecords.addAll(allDbExpectedOperations(1));
        expectedRecords.addAll(allDbExpectedOperations(3));

        test(source, expectedRecords, 18);
    }

    @Test
    public void blacklistSchema() throws Exception {
        StreamSource<ChangeRecord> source = sourceBuilder("source")
                .setSchemaBlacklist(SCHEMA_PREFIX + "1", SCHEMA_PREFIX + "3")
                .build();

        List<String> expectedRecords = allDbExpectedOperations(2);

        test(source, expectedRecords, 9);
    }

    @Test
    public void whitelistTable_sameDb() throws Exception {
        StreamSource<ChangeRecord> source = sourceBuilder("source")
                .setTableWhitelist(SCHEMA_PREFIX + "1.table0, " + SCHEMA_PREFIX + "1.table2")
                .build();

        List<String> expectedRecords = new ArrayList<>();
        expectedRecords.addAll(allTable0ExpectedOperations(1));
        expectedRecords.addAll(allTable2ExpectedOperations(1));

        test(source, expectedRecords, 6);
    }

    @Test
    public void whitelistTable_differentDb() throws Exception {
        StreamSource<ChangeRecord> source = sourceBuilder("source")
                .setTableWhitelist(SCHEMA_PREFIX + "1.table0, " + SCHEMA_PREFIX + "2.table1")
                .build();

        List<String> expectedRecords = new ArrayList<>();
        expectedRecords.addAll(allTable0ExpectedOperations(1));
        expectedRecords.addAll(allTable1ExpectedOperations(2));

        test(source, expectedRecords, 6);
    }

    @Test
    public void blacklistTable_sameDb() throws Exception {
        StreamSource<ChangeRecord> source = sourceBuilder("source")
                .setTableBlacklist(SCHEMA_PREFIX + "1.table0, " + SCHEMA_PREFIX + "1.table2")
                .build();

        List<String> expectedRecords = new ArrayList<>();
        expectedRecords.addAll(allTable1ExpectedOperations(1));
        expectedRecords.addAll(allDbExpectedOperations(2));
        expectedRecords.addAll(allDbExpectedOperations(3));

        test(source, expectedRecords, 21);
    }

    @Test
    public void blacklistTable_differentDb() throws Exception {
        StreamSource<ChangeRecord> source = sourceBuilder("source")
                .setTableBlacklist(SCHEMA_PREFIX + "1.table0, " + SCHEMA_PREFIX + "2.table1")
                .build();

        List<String> expectedRecords = new ArrayList<>();
        expectedRecords.addAll(allTable1ExpectedOperations(1));
        expectedRecords.addAll(allTable2ExpectedOperations(1));
        expectedRecords.addAll(allTable0ExpectedOperations(2));
        expectedRecords.addAll(allTable2ExpectedOperations(2));
        expectedRecords.addAll(allDbExpectedOperations(3));

        test(source, expectedRecords, 21);
    }

    @Test
    public void blacklistTableInWhitelistDb() throws Exception {
        StreamSource<ChangeRecord> source = sourceBuilder("source")
                .setSchemaWhitelist(SCHEMA_PREFIX + "1")
                .setTableBlacklist(SCHEMA_PREFIX + "1.table0")
                .build();

        List<String> expectedRecords = new ArrayList<>();
        expectedRecords.addAll(allTable1ExpectedOperations(1));
        expectedRecords.addAll(allTable2ExpectedOperations(1));

        test(source, expectedRecords, 6);
    }

    @Test
    public void blacklistColumn_sameTable() throws Exception {
        StreamSource<ChangeRecord> source = sourceBuilder("source")
                .setColumnBlacklist(SCHEMA_PREFIX + "1.table1.value_1, " + SCHEMA_PREFIX + "1.table1.value_2")
                .build();

        List<String> allTable1ExpectedOperations = allTable1ExpectedOperations(1);
        setNullToValue(allTable1ExpectedOperations, "value1");
        setNullToValue(allTable1ExpectedOperations, "value2");
        List<String> expectedRecords = new ArrayList<>();
        expectedRecords.addAll(allTable0ExpectedOperations(1));
        expectedRecords.addAll(allTable1ExpectedOperations);
        expectedRecords.addAll(allTable2ExpectedOperations(1));
        expectedRecords.addAll(allDbExpectedOperations(2));
        expectedRecords.addAll(allDbExpectedOperations(3));

        test(source, expectedRecords, 27);
    }

    @Test
    public void blacklistColumn_differentTable() throws Exception {
        StreamSource<ChangeRecord> source = sourceBuilder("source")
                .setColumnBlacklist(SCHEMA_PREFIX + "1.table1.value_2, " + SCHEMA_PREFIX + "1.table0.value_1")
                .build();

        List<String> allTable0ExpectedOperations = allTable0ExpectedOperations(1);
        setNullToValue(allTable0ExpectedOperations, "value1");
        List<String> allTable1ExpectedOperations = allTable1ExpectedOperations(1);
        setNullToValue(allTable1ExpectedOperations, "value2");
        List<String> expectedRecords = new ArrayList<>();
        expectedRecords.addAll(allTable0ExpectedOperations);
        expectedRecords.addAll(allTable1ExpectedOperations);
        expectedRecords.addAll(allTable2ExpectedOperations(1));
        expectedRecords.addAll(allDbExpectedOperations(2));
        expectedRecords.addAll(allDbExpectedOperations(3));

        test(source, expectedRecords, 27);
    }

    @Test
    public void blacklistColumnInWhitelistDb() throws Exception {
        StreamSource<ChangeRecord> source = sourceBuilder("source")
                .setSchemaWhitelist(SCHEMA_PREFIX + "1")
                .setColumnBlacklist(SCHEMA_PREFIX + "1.table1.value_2")
                .build();

        List<String> allTable1ExpectedOperations = allTable1ExpectedOperations(1);
        setNullToValue(allTable1ExpectedOperations, "value2");
        List<String> expectedRecords = new ArrayList<>();
        expectedRecords.addAll(allTable0ExpectedOperations(1));
        expectedRecords.addAll(allTable1ExpectedOperations);
        expectedRecords.addAll(allTable2ExpectedOperations(1));

        test(source, expectedRecords, 9);
    }

    @Test
    public void blacklistColumnInWhitelistDbAndTable() throws Exception {
        StreamSource<ChangeRecord> source = sourceBuilder("source")
                .setSchemaWhitelist(SCHEMA_PREFIX + "1")
                .setTableWhitelist(SCHEMA_PREFIX + "1.table1")
                .setColumnBlacklist(SCHEMA_PREFIX + "1.table1.value_2")
                .build();

        List<String> allTable1ExpectedOperations = allTable1ExpectedOperations(1);
        setNullToValue(allTable1ExpectedOperations, "value2");
        List<String> expectedRecords = new ArrayList<>();
        expectedRecords.addAll(allTable1ExpectedOperations);

        test(source, expectedRecords, 3);
    }

    private void test(StreamSource<ChangeRecord> source, List<String> expectedRecords, int expectedInitialOps)
            throws SQLException {
        Pipeline pipeline = pipeline(source);

        // when
        HazelcastInstance hz = createHazelcastInstances(2)[0];
        Job job = hz.getJet().newJob(pipeline);

        try {
            //then
            assertEqualsEventually(() -> hz.getMap(SINK_MAP_NAME).size(), expectedInitialOps);

            //when
            executeStatementsOnSchema(1);
            executeStatementsOnSchema(2);
            executeStatementsOnSchema(3);

            //then
            assertTrueEventually(() -> assertMatch(expectedRecords, mapResultsToSortedList(hz.getMap(SINK_MAP_NAME))));
        } finally {
            job.cancel();
            assertJobStatusEventually(job, JobStatus.FAILED);
        }
    }

    private Pipeline pipeline(StreamSource<ChangeRecord> source) {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(source)
                .withNativeTimestamps(0)
                .filter(t -> t.schema().startsWith(SCHEMA_PREFIX))
                .setLocalParallelism(1)
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

    private void createSchemaWithData(int schemaSuffix) throws SQLException {
        String schema = SCHEMA_PREFIX + schemaSuffix;
        createSchema(schema);
        try (Connection connection = getConnection(postgres)) {
            int dbId = schemaSuffix * 1000;
            for (int i = 0; i < 3; i++) {
                String table = "table" + i;
                int firstIdInTable = dbId + 100 * i + 1;
                Statement statement = connection.createStatement();
                statement.addBatch("SET search_path TO " + schema);
                statement.addBatch("CREATE TABLE " + table + " (\n"
                        + "  id SERIAL NOT NULL PRIMARY KEY,\n"
                        + "  value_1 VARCHAR(255) NOT NULL,\n"
                        + "  value_2 VARCHAR(255) NOT NULL,\n"
                        + "  value_3 VARCHAR(255) NOT NULL\n"
                        + ")");
                statement.addBatch("ALTER SEQUENCE " + table + "_id_seq RESTART WITH 1001");
                statement.addBatch("ALTER TABLE " + table + " REPLICA IDENTITY FULL");
                for (int j = 0; j < 3; j++) {
                    int id = firstIdInTable + j;
                    statement.addBatch("INSERT INTO " + table + " VALUES (\n"
                            + "  " + id + ", "
                            + "  '" + schema + "_" + table + "_val1_" + j + "',\n"
                            + "  '" + schema + "_" + table + "_val2_" + j + "',\n"
                            + "  '" + schema + "_" + table + "_val3_" + j + "'\n"
                            + ")");
                }
                statement.executeBatch();
            }
        }

    }

    private void executeStatementsOnSchema(int schemaSuffix) throws SQLException {
        String schema = SCHEMA_PREFIX + schemaSuffix;
        try (Connection connection = getConnection(postgres)) {
            int id = schemaSuffix * 1000 + 1;
            Statement statement = connection.createStatement();
            statement.addBatch("SET search_path TO " + schema);
            statement.addBatch("UPDATE table0 SET value_1='new_" + schema + "_table0_val1_0' WHERE id=" + id);
            id = schemaSuffix * 1000 + 100 + 2;
            statement.addBatch("UPDATE table1 SET value_2='new_" + schema + "_table1_val2_1' WHERE id=" + id);
            id = schemaSuffix * 1000 + 200 + 3;
            statement.addBatch("UPDATE table2 SET value_3='new_" + schema + "_table2_val3_2' WHERE id=" + id);

            id = schemaSuffix * 1000 + 4;
            statement.addBatch("INSERT INTO table0 VALUES (" + id + ", '" + schema + "_table0_val1_3', "
                    + "'" + schema + "_table0_val2_3', '" + schema + "_table0_val3_3')");
            id = schemaSuffix * 1000 + 100 + 4;
            statement.addBatch("INSERT INTO table1 VALUES (" + id + ", '" + schema + "_table1_val1_3', "
                    + "'" + schema + "_table1_val2_3', '" + schema + "_table1_val3_3')");
            id = schemaSuffix * 1000 + 200 + 4;
            statement.addBatch("INSERT INTO table2 VALUES (" + id + ", '" + schema + "_table2_val1_3', "
                    + "'" + schema + "_table2_val2_3', '" + schema + "_table2_val3_3')");

            id = schemaSuffix * 1000 + 4;
            statement.addBatch("DELETE FROM table0 WHERE id=" + id);
            id = schemaSuffix * 1000 + 100 + 4;
            statement.addBatch("DELETE FROM table1 WHERE id=" + id);
            id = schemaSuffix * 1000 + 200 + 4;
            statement.addBatch("DELETE FROM table2 WHERE id=" + id);
            statement.executeBatch();
        }
    }

    private List<String> allTable0ExpectedOperations(int schemaSuffix) {
        String schema = SCHEMA_PREFIX + schemaSuffix;
        int id1 = 1000 * schemaSuffix + 1;
        int id2 = 1000 * schemaSuffix + 2;
        int id3 = 1000 * schemaSuffix + 3;
        int id4 = 1000 * schemaSuffix + 4;
        return Arrays.asList(
                id1 + "/0:(SYNC|INSERT):TableRow \\{id=" + id1 + ", value1=" + schema + "_table0_val1_0, "
                        + "value2=" + schema + "_table0_val2_0, value3=" + schema + "_table0_val3_0\\}",
                id1 + "/1:UPDATE:TableRow \\{id=" + id1 + ", value1=new_" + schema + "_table0_val1_0, "
                        + "value2=" + schema + "_table0_val2_0, value3=" + schema + "_table0_val3_0\\}",
                id2 + "/0:(SYNC|INSERT):TableRow \\{id=" + id2 + ", value1=" + schema + "_table0_val1_1, "
                        + "value2=" + schema + "_table0_val2_1, value3=" + schema + "_table0_val3_1\\}",
                id3 + "/0:(SYNC|INSERT):TableRow \\{id=" + id3 + ", value1=" + schema + "_table0_val1_2, "
                        + "value2=" + schema + "_table0_val2_2, value3=" + schema + "_table0_val3_2\\}",
                id4 + "/0:(SYNC|INSERT):TableRow \\{id=" + id4 + ", value1=" + schema + "_table0_val1_3, "
                        + "value2=" + schema + "_table0_val2_3, value3=" + schema + "_table0_val3_3\\}",
                id4 + "/1:DELETE:TableRow \\{id=" + id4 + ", value1=" + schema + "_table0_val1_3, "
                        + "value2=" + schema + "_table0_val2_3, value3=" + schema + "_table0_val3_3\\}"
        );
    }

    private List<String> allTable1ExpectedOperations(int schemaSuffix) {
        String schema = SCHEMA_PREFIX + schemaSuffix;
        int id1 = 1000 * schemaSuffix + 101;
        int id2 = 1000 * schemaSuffix + 102;
        int id3 = 1000 * schemaSuffix + 103;
        int id4 = 1000 * schemaSuffix + 104;
        return Arrays.asList(
                id1 + "/0:(SYNC|INSERT):TableRow \\{id=" + id1 + ", value1=" + schema + "_table1_val1_0, "
                        + "value2=" + schema + "_table1_val2_0, value3=" + schema + "_table1_val3_0\\}",
                id2 + "/0:(SYNC|INSERT):TableRow \\{id=" + id2 + ", value1=" + schema + "_table1_val1_1, "
                        + "value2=" + schema + "_table1_val2_1, value3=" + schema + "_table1_val3_1\\}",
                id2 + "/1:UPDATE:TableRow \\{id=" + id2 + ", value1=" + schema + "_table1_val1_1, "
                        + "value2=new_" + schema + "_table1_val2_1, value3=" + schema + "_table1_val3_1\\}",
                id3 + "/0:(SYNC|INSERT):TableRow \\{id=" + id3 + ", value1=" + schema + "_table1_val1_2, "
                        + "value2=" + schema + "_table1_val2_2, value3=" + schema + "_table1_val3_2\\}",
                id4 + "/0:(SYNC|INSERT):TableRow \\{id=" + id4 + ", value1=" + schema + "_table1_val1_3, "
                        + "value2=" + schema + "_table1_val2_3, value3=" + schema + "_table1_val3_3\\}",
                id4 + "/1:DELETE:TableRow \\{id=" + id4 + ", value1=" + schema + "_table1_val1_3, "
                        + "value2=" + schema + "_table1_val2_3, value3=" + schema + "_table1_val3_3\\}"
        );
    }

    private List<String> allTable2ExpectedOperations(int schemaSuffix) {
        String schema = SCHEMA_PREFIX + schemaSuffix;
        int id1 = 1000 * schemaSuffix + 201;
        int id2 = 1000 * schemaSuffix + 202;
        int id3 = 1000 * schemaSuffix + 203;
        int id4 = 1000 * schemaSuffix + 204;
        return Arrays.asList(
                id1 + "/0:(SYNC|INSERT):TableRow \\{id=" + id1 + ", value1=" + schema + "_table2_val1_0, "
                        + "value2=" + schema + "_table2_val2_0, value3=" + schema + "_table2_val3_0\\}",
                id2 + "/0:(SYNC|INSERT):TableRow \\{id=" + id2 + ", value1=" + schema + "_table2_val1_1, "
                        + "value2=" + schema + "_table2_val2_1, value3=" + schema + "_table2_val3_1\\}",
                id3 + "/0:(SYNC|INSERT):TableRow \\{id=" + id3 + ", value1=" + schema + "_table2_val1_2, "
                        + "value2=" + schema + "_table2_val2_2, value3=" + schema + "_table2_val3_2\\}",
                id3 + "/1:UPDATE:TableRow \\{id=" + id3 + ", value1=" + schema + "_table2_val1_2, "
                        + "value2=" + schema + "_table2_val2_2, value3=new_" + schema + "_table2_val3_2\\}",
                id4 + "/0:(SYNC|INSERT):TableRow \\{id=" + id4 + ", value1=" + schema + "_table2_val1_3, "
                        + "value2=" + schema + "_table2_val2_3, value3=" + schema + "_table2_val3_3\\}",
                id4 + "/1:DELETE:TableRow \\{id=" + id4 + ", value1=" + schema + "_table2_val1_3, "
                        + "value2=" + schema + "_table2_val2_3, value3=" + schema + "_table2_val3_3\\}"
        );
    }

    private List<String> allDbExpectedOperations(int schemaSuffix) {
        List<String> list = new ArrayList<>();
        list.addAll(allTable0ExpectedOperations(schemaSuffix));
        list.addAll(allTable1ExpectedOperations(schemaSuffix));
        list.addAll(allTable2ExpectedOperations(schemaSuffix));
        return list;
    }

    private List<String> allExpectedOperations() {
        List<String> list = new ArrayList<>();
        list.addAll(allDbExpectedOperations(1));
        list.addAll(allDbExpectedOperations(2));
        list.addAll(allDbExpectedOperations(3));
        return list;
    }

    private void setNullToValue(List<String> list, String value) {
        for (int i = 0; i < list.size(); i++) {
            String content = list.get(i);
            String[] split1 = content.split(value + "=");
            String[] split2 = split1[1].split(",", 2);
            list.set(i, split1[0] + value + "=null," + split2[1]);
        }
    }
}
