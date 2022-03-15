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
public class MySqlCdcWhiteBlackListIntegrationTest extends AbstractMySqlCdcIntegrationTest {

    private static final String DB_PREFIX = "testDb";
    private static final String SINK_MAP_NAME = "resultsMap";

    @Before
    public void before() throws SQLException {
        createDbWithData(1);
        createDbWithData(2);
        createDbWithData(3);
    }

    @Test
    public void noWhiteBlacklist() throws Exception {
        StreamSource<ChangeRecord> source = sourceBuilder("source")
                .build();

        List<String> expectedRecords = allExpectedOperations();

        test(source, expectedRecords, 27);
    }

    @Test
    public void whitelistDatabase() throws Exception {
        StreamSource<ChangeRecord> source = sourceBuilder("source")
                .setDatabaseWhitelist(DB_PREFIX + "1", DB_PREFIX + "3")
                .build();

        List<String> expectedRecords = new ArrayList<>();
        expectedRecords.addAll(allDbExpectedOperations(1));
        expectedRecords.addAll(allDbExpectedOperations(3));

        test(source, expectedRecords, 18);
    }

    @Test
    public void blacklistDatabase() throws Exception {
        StreamSource<ChangeRecord> source = sourceBuilder("source")
                .setDatabaseBlacklist(DB_PREFIX + "1", DB_PREFIX + "3")
                .build();

        List<String> expectedRecords = allDbExpectedOperations(2);

        test(source, expectedRecords, 9);
    }

    @Test
    public void whitelistTable_sameDb() throws Exception {
        StreamSource<ChangeRecord> source = sourceBuilder("source")
                .setTableWhitelist(DB_PREFIX + "1.table0, " + DB_PREFIX + "1.table2")
                .build();

        List<String> expectedRecords = new ArrayList<>();
        expectedRecords.addAll(allTable0ExpectedOperations(1));
        expectedRecords.addAll(allTable2ExpectedOperations(1));

        test(source, expectedRecords, 6);
    }

    @Test
    public void whitelistTable_differentDb() throws Exception {
        StreamSource<ChangeRecord> source = sourceBuilder("source")
                .setTableWhitelist(DB_PREFIX + "1.table0, " + DB_PREFIX + "2.table1")
                .build();

        List<String> expectedRecords = new ArrayList<>();
        expectedRecords.addAll(allTable0ExpectedOperations(1));
        expectedRecords.addAll(allTable1ExpectedOperations(2));

        test(source, expectedRecords, 6);
    }

    @Test
    public void blacklistTable_sameDb() throws Exception {
        StreamSource<ChangeRecord> source = sourceBuilder("source")
                .setTableBlacklist(DB_PREFIX + "1.table0, " + DB_PREFIX + "1.table2")
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
                .setTableBlacklist(DB_PREFIX + "1.table0, " + DB_PREFIX + "2.table1")
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
                .setDatabaseWhitelist(DB_PREFIX + "1")
                .setTableBlacklist(DB_PREFIX + "1.table0")
                .build();

        List<String> expectedRecords = new ArrayList<>();
        expectedRecords.addAll(allTable1ExpectedOperations(1));
        expectedRecords.addAll(allTable2ExpectedOperations(1));

        test(source, expectedRecords, 6);
    }

    @Test
    public void blacklistColumn_sameTable() throws Exception {
        StreamSource<ChangeRecord> source = sourceBuilder("source")
                .setColumnBlacklist(DB_PREFIX + "1.table1.value_1, " + DB_PREFIX + "1.table1.value_2")
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
                .setColumnBlacklist(DB_PREFIX + "1.table1.value_2, " + DB_PREFIX + "1.table0.value_1")
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
                .setDatabaseWhitelist(DB_PREFIX + "1")
                .setColumnBlacklist(DB_PREFIX + "1.table1.value_2")
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
                .setDatabaseWhitelist(DB_PREFIX + "1")
                .setTableWhitelist(DB_PREFIX + "1.table1")
                .setColumnBlacklist(DB_PREFIX + "1.table1.value_2")
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
            executeStatementsOnDb(1);
            executeStatementsOnDb(2);
            executeStatementsOnDb(3);

            //then
            assertEqualsEventually(() -> mapResultsToSortedList(hz.getMap(SINK_MAP_NAME)), expectedRecords);
        } finally {
            job.cancel();
            assertJobStatusEventually(job, JobStatus.FAILED);
        }
    }

    private Pipeline pipeline(StreamSource<ChangeRecord> source) {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(source)
                .withNativeTimestamps(0)
                .filter(t -> t.database().startsWith(DB_PREFIX))
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
                .writeTo(Sinks.map(SINK_MAP_NAME));
        return pipeline;
    }

    private void createDbWithData(int dbSuffix) throws SQLException {
        String database = DB_PREFIX + dbSuffix;
        createDb(database);
        try (Connection connection = getConnection(mysql, database)) {
            int dbId = dbSuffix * 1000;
            for (int i = 0; i < 3; i++) {
                String table = "table" + i;
                int firstIdInTable = dbId + 100 * i + 1;
                Statement statement = connection.createStatement();
                statement.addBatch("CREATE TABLE " + table + " (\n"
                                + "  id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,\n"
                                + "  value_1 VARCHAR(255) NOT NULL,\n"
                                + "  value_2 VARCHAR(255) NOT NULL,\n"
                                + "  value_3 VARCHAR(255) NOT NULL\n"
                                + ")");
                statement.addBatch("ALTER TABLE " + table + " AUTO_INCREMENT = " + firstIdInTable + " ;");
                for (int j = 0; j < 3; j++) {
                    int id = firstIdInTable + j;
                    statement.addBatch("INSERT INTO " + table + " VALUES (\n"
                                    + "  " + id + ", "
                                    + "  '" + database + "_" + table + "_val1_" + j + "',\n"
                                    + "  '" + database + "_" + table + "_val2_" + j + "',\n"
                                    + "  '" + database + "_" + table + "_val3_" + j + "'\n"
                                    + ")");
                }
                statement.executeBatch();
            }
        }

    }

    private void executeStatementsOnDb(int dbSuffix) throws SQLException {
        String database = DB_PREFIX + dbSuffix;
        try (Connection connection = getConnection(mysql, database)) {
            int id = dbSuffix * 1000 + 1;
            Statement statement = connection.createStatement();
            statement.addBatch("UPDATE table0 SET value_1='new_" + database + "_table0_val1_0' WHERE id=" + id);
            id = dbSuffix * 1000 + 100 + 2;
            statement.addBatch("UPDATE table1 SET value_2='new_" + database + "_table1_val2_1' WHERE id=" + id);
            id = dbSuffix * 1000 + 200 + 3;
            statement.addBatch("UPDATE table2 SET value_3='new_" + database + "_table2_val3_2' WHERE id=" + id);

            id = dbSuffix * 1000 + 4;
            statement.addBatch("INSERT INTO table0 VALUES (" + id + ", '" + database + "_table0_val1_3', "
                    + "'" + database + "_table0_val2_3', '" + database + "_table0_val3_3')");
            id = dbSuffix * 1000 + 100 + 4;
            statement.addBatch("INSERT INTO table1 VALUES (" + id + ", '" + database + "_table1_val1_3', "
                    + "'" + database + "_table1_val2_3', '" + database + "_table1_val3_3')");
            id = dbSuffix * 1000 + 200 + 4;
            statement.addBatch("INSERT INTO table2 VALUES (" + id + ", '" + database + "_table2_val1_3', "
                    + "'" + database + "_table2_val2_3', '" + database + "_table2_val3_3')");

            id = dbSuffix * 1000 + 4;
            statement.addBatch("DELETE FROM table0 WHERE id=" + id);
            id = dbSuffix * 1000 + 100 + 4;
            statement.addBatch("DELETE FROM table1 WHERE id=" + id);
            id = dbSuffix * 1000 + 200 + 4;
            statement.addBatch("DELETE FROM table2 WHERE id=" + id);
            statement.executeBatch();
        }
    }

    private List<String> allTable0ExpectedOperations(int dbSuffix) {
        String db = DB_PREFIX + dbSuffix;
        int id1 = 1000 * dbSuffix + 1;
        int id2 = 1000 * dbSuffix + 2;
        int id3 = 1000 * dbSuffix + 3;
        int id4 = 1000 * dbSuffix + 4;
        return Arrays.asList(
                id1 + "/0:INSERT:TableRow {id=" + id1 + ", value1=" + db + "_table0_val1_0, "
                        + "value2=" + db + "_table0_val2_0, value3=" + db + "_table0_val3_0}",
                id1 + "/1:UPDATE:TableRow {id=" + id1 + ", value1=new_" + db + "_table0_val1_0, "
                        + "value2=" + db + "_table0_val2_0, value3=" + db + "_table0_val3_0}",
                id2 + "/0:INSERT:TableRow {id=" + id2 + ", value1=" + db + "_table0_val1_1, "
                        + "value2=" + db + "_table0_val2_1, value3=" + db + "_table0_val3_1}",
                id3 + "/0:INSERT:TableRow {id=" + id3 + ", value1=" + db + "_table0_val1_2, "
                        + "value2=" + db + "_table0_val2_2, value3=" + db + "_table0_val3_2}",
                id4 + "/0:INSERT:TableRow {id=" + id4 + ", value1=" + db + "_table0_val1_3, "
                        + "value2=" + db + "_table0_val2_3, value3=" + db + "_table0_val3_3}",
                id4 + "/1:DELETE:TableRow {id=" + id4 + ", value1=" + db + "_table0_val1_3, "
                        + "value2=" + db + "_table0_val2_3, value3=" + db + "_table0_val3_3}"
        );
    }

    private List<String> allTable1ExpectedOperations(int dbSuffix) {
        String db = DB_PREFIX + dbSuffix;
        int id1 = 1000 * dbSuffix + 101;
        int id2 = 1000 * dbSuffix + 102;
        int id3 = 1000 * dbSuffix + 103;
        int id4 = 1000 * dbSuffix + 104;
        return Arrays.asList(
                id1 + "/0:INSERT:TableRow {id=" + id1 + ", value1=" + db + "_table1_val1_0, "
                        + "value2=" + db + "_table1_val2_0, value3=" + db + "_table1_val3_0}",
                id2 + "/0:INSERT:TableRow {id=" + id2 + ", value1=" + db + "_table1_val1_1, "
                        + "value2=" + db + "_table1_val2_1, value3=" + db + "_table1_val3_1}",
                id2 + "/1:UPDATE:TableRow {id=" + id2 + ", value1=" + db + "_table1_val1_1, "
                        + "value2=new_" + db + "_table1_val2_1, value3=" + db + "_table1_val3_1}",
                id3 + "/0:INSERT:TableRow {id=" + id3 + ", value1=" + db + "_table1_val1_2, "
                        + "value2=" + db + "_table1_val2_2, value3=" + db + "_table1_val3_2}",
                id4 + "/0:INSERT:TableRow {id=" + id4 + ", value1=" + db + "_table1_val1_3, "
                        + "value2=" + db + "_table1_val2_3, value3=" + db + "_table1_val3_3}",
                id4 + "/1:DELETE:TableRow {id=" + id4 + ", value1=" + db + "_table1_val1_3, "
                        + "value2=" + db + "_table1_val2_3, value3=" + db + "_table1_val3_3}"
        );
    }

    private List<String> allTable2ExpectedOperations(int dbSuffix) {
        String db = DB_PREFIX + dbSuffix;
        int id1 = 1000 * dbSuffix + 201;
        int id2 = 1000 * dbSuffix + 202;
        int id3 = 1000 * dbSuffix + 203;
        int id4 = 1000 * dbSuffix + 204;
        return Arrays.asList(
                id1 + "/0:INSERT:TableRow {id=" + id1 + ", value1=" + db + "_table2_val1_0, "
                        + "value2=" + db + "_table2_val2_0, value3=" + db + "_table2_val3_0}",
                id2 + "/0:INSERT:TableRow {id=" + id2 + ", value1=" + db + "_table2_val1_1, "
                        + "value2=" + db + "_table2_val2_1, value3=" + db + "_table2_val3_1}",
                id3 + "/0:INSERT:TableRow {id=" + id3 + ", value1=" + db + "_table2_val1_2, "
                        + "value2=" + db + "_table2_val2_2, value3=" + db + "_table2_val3_2}",
                id3 + "/1:UPDATE:TableRow {id=" + id3 + ", value1=" + db + "_table2_val1_2, "
                        + "value2=" + db + "_table2_val2_2, value3=new_" + db + "_table2_val3_2}",
                id4 + "/0:INSERT:TableRow {id=" + id4 + ", value1=" + db + "_table2_val1_3, "
                        + "value2=" + db + "_table2_val2_3, value3=" + db + "_table2_val3_3}",
                id4 + "/1:DELETE:TableRow {id=" + id4 + ", value1=" + db + "_table2_val1_3, "
                        + "value2=" + db + "_table2_val2_3, value3=" + db + "_table2_val3_3}"
        );
    }

    private List<String> allDbExpectedOperations(int dbSuffix) {
        List<String> list = new ArrayList<>();
        list.addAll(allTable0ExpectedOperations(dbSuffix));
        list.addAll(allTable1ExpectedOperations(dbSuffix));
        list.addAll(allTable2ExpectedOperations(dbSuffix));
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
