/*
 * Copyright 2026 Hazelcast Inc.
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
package com.hazelcast.jet.cdc;

import com.hazelcast.collection.IList;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.cdc.TestUtils.StronglyTypedTransactionInfo;
import com.hazelcast.jet.cdc.TestUtils.Transaction;
import com.hazelcast.jet.cdc.TestUtils.TransactionInfo;
import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.test.OverridePropertyRule;
import org.apache.commons.lang3.NotImplementedException;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.JdbcDatabaseContainer;

import javax.annotation.Nonnull;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.hazelcast.jet.cdc.Operation.DELETE;
import static com.hazelcast.jet.cdc.Operation.INSERT;
import static com.hazelcast.jet.cdc.Operation.SYNC;
import static com.hazelcast.jet.cdc.Operation.UNSPECIFIED;
import static com.hazelcast.jet.cdc.Operation.UPDATE;
import static com.hazelcast.jet.cdc.TestUtils.executeSql;
import static com.hazelcast.jet.cdc.TestUtils.smallInstanceConfigWithCompactSerialization;
import static com.hazelcast.jet.cdc.TestUtils.standardConf;
import static com.hazelcast.jet.core.JobAssertions.assertThat;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static java.time.format.DateTimeFormatter.ISO_INSTANT;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class AbstractCdcDataTypesTest<C extends GenericContainer<?>> extends AbstractCdcIntegrationTest<C> {
    @ClassRule
    public static OverridePropertyRule overridePropertyRule = OverridePropertyRule.set("user.timezone", "UTC");

    protected Map<Integer, String> creationDates = new HashMap<>();

    @Nonnull
    protected abstract DebeziumCdcSources.Builder<ChangeRecord> conf(C container, String table, Map<String, String> options);

    @Override
    protected @Nonnull DebeziumCdcSources.Builder<ChangeRecord> basicConf(C container) {
        throw new UnsupportedOperationException();
    }

    protected String getAdaptivePrecisionMode() {
        return "adaptive";
    }

    @Before
    public void setup() {
        creationDates.clear();
    }

    @Test(timeout = 900000)
    public void basicTypes_precisionModeAdaptive() {
        // given
        Map<String, String> map = new HashMap<>();
        map.put("precisionMode", getAdaptivePrecisionMode());

        Pipeline pipeline = getPipeline(conf(container(), "%1$s.TX_TEMPORAL_USAGE".formatted(tablePrefix()), map).build(),
                ChangeRecord::table,
                (value, operation) -> {
                    Transaction customer = value.toObject(Transaction.class);
                    return new TransactionInfo(operation, customer);
                });

        // when
        HazelcastInstance hz = createHazelcastInstances(2)[0];
        Job job = hz.getJet().newJob(pipeline, standardConf());
        assertThat(job).eventuallyHasStatus(RUNNING);

        // then
        IList<TransactionInfo> results = hz.getList("results");
        waitForSnapshotEnd();

        // when
        performSetOfTemporalChanges(container());

        //then
        assertContainsAddedTemporalPrecisionModeAdaptive(results);
    }

    @Test
    public void basicTypes_precisionModeConnect() {
        // given
        Map<String, String> map = new HashMap<>();
        map.put("precisionMode", "connect");

        Pipeline pipeline = getPipeline(conf(container(), "%1$s.TX_TEMPORAL_USAGE".formatted(tablePrefix()), map).build(),
                ChangeRecord::table,
                (value, operation) -> {
                    Transaction customer = value.toObject(Transaction.class);
                    return new TransactionInfo(operation, customer);
                });

        // when
        HazelcastInstance hz = createHazelcastInstances(2)[0];
        Job job = hz.getJet().newJob(pipeline, standardConf());
        assertThat(job).eventuallyHasStatus(RUNNING);

        // then
        IList<TransactionInfo> results = hz.getList("results");
        waitForSnapshotEnd();

        // when
        performSetOfTemporalChanges(container());

        //then
        assertContainsAddedTemporalPrecisionModeConnect(results);
    }

    @Test
    public void stronglyTypedResultClass() {
        // given
        Map<String, String> map = new HashMap<>();
        map.put("precisionMode", getAdaptivePrecisionMode());

        final String table = tablePrefix() + ".TX_TEMPORAL_USAGE";
        final StreamSource<ChangeRecord> source = conf(container(), table, map).build();
        final Pipeline pipeline = getPipeline(source,
                ChangeRecord::table,
                (value, operation) -> {
                    TransactionStronglyTyped customer = value.toObject(TransactionStronglyTyped.class);
                    return new StronglyTypedTransactionInfo(operation, customer);
                });

        // when
        var conf = smallInstanceConfigWithCompactSerialization();
        var compactConf = conf.getSerializationConfig().getCompactSerializationConfig();
        compactConf.addSerializer(new TransactionStronglyTypedSerializer());
        HazelcastInstance hz = createHazelcastInstances(conf, 2)[0];
        Job job = hz.getJet().newJob(pipeline, standardConf());
        assertThat(job).eventuallyHasStatus(RUNNING);

        // then
        IList<StronglyTypedTransactionInfo> results = hz.getList("results");
        waitForSnapshotEnd();

        // when
        performSetOfTemporalChanges(container());

        //then
        assertContainsAddedWithConverters(results);
    }

    private List<TransactionInfo> addedRecordsWithTemporalPrecisionModeAdaptive() {
        return List.of(
                new TransactionInfo(UPDATE, new Transaction(1003, "2024-08-22T05:36:36.444Z",
                        "2024-08-04T01:30:00.777888999Z", "2024-08-04", 5400,
                                                            creationDates.get(1003))),
                new TransactionInfo(INSERT, new Transaction(1005,  "2024-08-03T02:30:00.999Z",
                        "2024-08-03T02:30:00.000111222Z", "2024-08-03", 9000,
                                                            creationDates.get(1005))),
                new TransactionInfo(DELETE, new Transaction(1005, "2024-08-03T02:30:00.999Z",
                        "2024-08-03T02:30:00.000111222Z", "2024-08-03", 9000,
                                                            creationDates.get(1005)))
                      );
    }

    private List<TransactionInfo> addedRecordsWithTemporalPrecisionModeConnect() {
        return List.of(
                // difference to adaptive: all are Date
                new TransactionInfo(UPDATE, new Transaction(1003, "2024-08-22T05:36:36.444+00:00",
                        "2024-08-04T01:30:00.777+00:00", "2024-08-04T00:00:00.000+00:00", 5400,
                                                            creationDates.get(1003))),
                new TransactionInfo(INSERT, new Transaction(1005,  "2024-08-03T02:30:00.999+00:00",
                        "2024-08-03T02:30:00.000+00:00", "2024-08-03T00:00:00.000+00:00", 9000,
                                                            creationDates.get(1005))),
                new TransactionInfo(DELETE, new Transaction(1005, "2024-08-03T02:30:00.999+00:00",
                        "2024-08-03T02:30:00.000+00:00", "2024-08-03T00:00:00.000+00:00", 9000,
                                                            creationDates.get(1005)))
        );
    }

    private List<StronglyTypedTransactionInfo> addedRecordsWithTemporalStronglyTyped() {
        return List.of(
                new StronglyTypedTransactionInfo(UPDATE, new TransactionStronglyTyped(1003, "2024-08-22 05:36:36.444",
                        "2024-08-04T01:30:00.777Z", "2024-08-04", "PT1H30M",
                                                                                      creationDates.get(1003))),
                new StronglyTypedTransactionInfo(INSERT, new TransactionStronglyTyped(1005,  "2024-08-03 02:30:00.999",
                        "2024-08-03T02:30:00.00Z", "2024-08-03", "PT2H30M",
                                                                                      creationDates.get(1005))),
                new StronglyTypedTransactionInfo(DELETE, new TransactionStronglyTyped(1005, "2024-08-03 02:30:00.999",
                        "2024-08-03T02:30:00.00Z", "2024-08-03", "PT2H30M",
                                                                                      creationDates.get(1005)))
        );
    }

    private Map<Integer, String> queryDates() {
        if (container instanceof JdbcDatabaseContainer<?> jdbcContainer) {
            try (var connection = DriverManager.getConnection(jdbcContainer.getJdbcUrl(), jdbcContainer.getUsername(),
                                                              jdbcContainer.getPassword());
                 var statement = connection.createStatement()) {

                try (ResultSet resultSet =
                             statement.executeQuery("SELECT ID, CREATE_DATETIME FROM %1$s.TX_TEMPORAL_USAGE"
                                                            .formatted(tablePrefix()))) {
                    Map<Integer, String> result = new HashMap<>();
                    while (resultSet.next()) {
                        int id = resultSet.getInt("ID");
                        Instant createDatetime = resultSet.getTimestamp("CREATE_DATETIME").toLocalDateTime()
                                                          .toInstant(ZoneOffset.UTC);
                        result.put(id, ISO_INSTANT.format(createDatetime));
                    }
                    return result;
                }
            } catch (SQLException e) {
                for (Throwable throwable : e) {
                    logger.severe("Exception while executing sql statement", throwable);
                }
                throw new RuntimeException(e);
            }
        } else {
            throw new NotImplementedException("Please override methods in AbstractCdcIntegrationTest");
        }
    }

    private <R> Pipeline getPipeline(StreamSource<ChangeRecord> source,
                                     FunctionEx<ChangeRecord, String> tableFn,
                                     BiFunctionEx<RecordPart, Operation, R> fn) {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(source)
                .withIngestionTimestamps()
                .filter(changeRecord -> changeRecord.operation() != UNSPECIFIED)
                .map(changeRecord -> {
                    assertThat(tableFn.apply(changeRecord)).isEqualToIgnoringCase("TX_TEMPORAL_USAGE");
                    Operation operation = changeRecord.operation();
                    RecordPart value = changeRecord.value();
                    assert value != null;
                    return fn.apply(value, operation);
                })
                .writeTo(Sinks.list("results"));
        pipeline.setPreserveOrder(true);
        return pipeline;
    }

    protected void performSetOfTemporalChanges(C container) {
        if (container instanceof JdbcDatabaseContainer<?> jdbcContainer) {
            executeSql(jdbcContainer.getJdbcUrl(), jdbcContainer.getUsername(), jdbcContainer.getPassword(),
                    "UPDATE %1$s.TX_TEMPORAL_USAGE SET TXN_DATETIME='2024-08-22 05:36:36.444' WHERE id=1003"
                            .formatted(tablePrefix()),
                    ("INSERT INTO %1$s.TX_TEMPORAL_USAGE (id, txn_datetime, datetime_with_precision, date, date_local, time) VALUES "
                            + "(1005, '2024-08-03 02:30:00.999', '2024-08-03 02:30:00.000111222', '2024-08-03', '2024-08-03', '02:30:00')")
                            .formatted(tablePrefix())
            );
            creationDates = queryDates();
            executeSql(jdbcContainer.getJdbcUrl(), jdbcContainer.getUsername(), jdbcContainer.getPassword(),
                       "DELETE FROM %1$s.TX_TEMPORAL_USAGE WHERE id=1005".formatted(tablePrefix())
                      );
        } else {
            throw new NotImplementedException("Please override methods in AbstractCdcIntegrationTest");
        }
    }

    protected void revertSetOfTemporalChanges(C container) {
        if (container instanceof JdbcDatabaseContainer<?> jdbcContainer) {
            executeSql(jdbcContainer.getJdbcUrl(), jdbcContainer.getUsername(), jdbcContainer.getPassword(),
                    "UPDATE %1$s.TX_TEMPORAL_USAGE SET TXN_DATETIME='2024-08-04 01:30:00.888' WHERE id=1003"
                            .formatted(tablePrefix())
            );
        } else {
            throw new NotImplementedException("Please override methods in AbstractCdcIntegrationTest");
        }
    }

    protected void assertContainsAddedTemporalPrecisionModeAdaptive(IList<TransactionInfo> results) {
        var expectedRecords = addedRecordsWithTemporalPrecisionModeAdaptive();
        assertTrueEventually(() -> {
            var copy = new ArrayList<>(results);
            var subset = copy.stream().filter(s -> s.op() != SYNC).toList();
            assertThat(subset)
                    .usingElementComparator(TransactionInfo.COMPARATOR)
                    .containsAll(expectedRecords);

            TransactionInfo.validateCreationDates(copy);
        }, ASSERT_TRUE_EVENTUALLY_TIMEOUT / 5);
    }

    protected void assertContainsAddedTemporalPrecisionModeConnect(IList<TransactionInfo> results) {
        var expectedRecords = addedRecordsWithTemporalPrecisionModeConnect();
        assertTrueEventually(() -> {
            var copy = new ArrayList<>(results);
            var subset = copy.stream().filter(s -> s.op() != SYNC).toList();
            assertThat(subset)
                    .usingElementComparator(TransactionInfo.COMPARATOR)
                    .containsAll(expectedRecords);

            TransactionInfo.validateCreationDates(copy);
        }, ASSERT_TRUE_EVENTUALLY_TIMEOUT / 5);
    }

    protected void assertContainsAddedWithConverters(IList<StronglyTypedTransactionInfo> results) {
        var expectedRecords = addedRecordsWithTemporalStronglyTyped();
        assertTrueEventually(() -> {
            var copy = new ArrayList<>(results);
            var subset = copy.stream().filter(s -> s.op() != SYNC).toList();
            assertThat(subset).usingElementComparator(StronglyTypedTransactionInfo.COMPARATOR)
                              .containsAll(expectedRecords);

            StronglyTypedTransactionInfo.validateCreationDates(copy);
        }, ASSERT_TRUE_EVENTUALLY_TIMEOUT / 5);
    }
}
