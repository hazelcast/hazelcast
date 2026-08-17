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
import com.hazelcast.jet.cdc.DebeziumCdcSources.Builder;
import com.hazelcast.jet.cdc.TestUtils.Customer;
import com.hazelcast.jet.cdc.TestUtils.CustomerInfo;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.impl.JobRepository;
import com.hazelcast.jet.json.JsonUtil;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamSource;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;

import static com.hazelcast.jet.cdc.Operation.INSERT;
import static com.hazelcast.jet.cdc.Operation.UNSPECIFIED;
import static com.hazelcast.jet.cdc.TestUtils.standardConf;
import static com.hazelcast.jet.core.JobAssertions.assertThat;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("ClassEscapesDefinedScope")
public abstract class AbstractBasicCdcIntegrationTest<C extends GenericContainer<?>>
        extends AbstractCdcIntegrationTest<C> {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractBasicCdcIntegrationTest.class);

    @Test
    public void basic() {
        // given
        Pipeline pipeline = getPipeline(getSource(container()));

        // when
        HazelcastInstance hz = createHazelcastInstances(2)[0];
        Job job = hz.getJet().newJob(pipeline, standardConf());
        assertThat(job).eventuallyHasStatus(RUNNING);
        LOG.info("Job started");

        // then
        IList<CustomerInfo> results = hz.getList("results");
        waitForSnapshotEnd();

        assertContainsInitialRecords(results);

        //when
        performSetOfChanges(container());

        //then
        assertContainsAddedRecords(results);
    }

    @Test
    public void withRestart() {
        // given
        Pipeline pipeline = getPipeline(getSource(container()));

        // when
        HazelcastInstance[] hazelcastInstances = createHazelcastInstances(2);
        HazelcastInstance hz = hazelcastInstances[0];
        Job job = hz.getJet().newJob(pipeline, standardConf());
        assertThat(job).eventuallyHasStatus(RUNNING);

        // then
        IList<CustomerInfo> results = hz.getList("results");
        waitForSnapshotEnd();

        //when
        performSetOfChanges(container());

        //then
        assertContainsAddedRecords(results);

        JobRepository jr = new JobRepository(hz);
        waitForNextSnapshot(jr, job.getId(), 10, false);

        job.restart();

        assertThat(job).eventuallyHasStatus(RUNNING);
        waitForNextSnapshot(jr, job.getId(), 10, false);
        waitForSnapshotEnd();

        performSecondSetOfChanges(container());
        assertContainsAddedRecordsAfterRestart(results);
        try {
            job.cancel();
        } catch (Exception ignored) {
        }
    }

    @Test
    @SuppressWarnings("checkstyle:NeedBraces")
    public void usingJson() {
        // given
        var source = basicConf(container())
                .json()
                .build();
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(source)
                .withIngestionTimestamps()
                .map(changeRecord -> {
                    Map<String, Object> value = JsonUtil.mapFrom(changeRecord.getValue());
                    assert value != null;
                    String op = (String) value.get("op");
                    if (op == null) {
                        return null;
                    }
                    if ("r".equalsIgnoreCase(op)) {
                        return new CustomerInfo(Operation.get(op), new Customer());
                    }

                    LOG.info("Record: {}", changeRecord.getValue());
                    var customerMap = value.get("after") != null
                            ? TestUtils.mapFrom(value.get("after"))
                            : TestUtils.mapFrom(value.get("before"));
                    var customer = new Customer((Integer) get(customerMap, "id"),
                            (String) get(customerMap, "first_name"),
                            (String) get(customerMap, "last_name"),
                            (String) get(customerMap, "email"));
                    return new CustomerInfo(Operation.get(op), customer);
                })
                .writeTo(Sinks.list("resultsJson"));
        pipeline.setPreserveOrder(true);

        // when
        HazelcastInstance hz = createHazelcastInstances(2)[0];
        Job job = hz.getJet().newJob(pipeline, standardConf());
        assertThat(job).eventuallyHasStatus(RUNNING);

        // then
        IList<CustomerInfo> results = hz.getList("resultsJson");
        waitForSnapshotEnd();
        JobRepository jr = new JobRepository(hz);
        waitForNextSnapshot(jr, job.getId(), 60, false);

        //when
        performSetOfChanges(container());

        //then
        assertContainsAddedRecords(results);
        try {
            job.cancel();
        } catch (Exception ignored) {
        }
    }

    private static Object get(Map<String, Object> map, String prop) {
        Object o = map.get(prop);
        if (o == null) {
            o = map.get(prop.toUpperCase(Locale.ROOT));
        }
        return o;
    }

    record FiltersMultipleTablesMetadata(String nonStandardTableIncludeProperty,
                                         FunctionEx<ChangeRecord, String> tableNameFn){
    }

    protected FiltersMultipleTablesMetadata filtersMultipleTablesMetadata() {
        return null;
    }

    @Test
    public void filtersMultipleTables() {
        // given
        String prefix = tablePrefix();
        Builder<ChangeRecord> conf = basicConf(container());
        FiltersMultipleTablesMetadata metadata = filtersMultipleTablesMetadata();
        if (metadata == null) {
            conf.setTableIncludeList(prefix + ".customers", prefix + ".products");
        } else {
            conf.setProperty(metadata.nonStandardTableIncludeProperty(),
                    "%1$s.customers,%1$s.products".formatted(prefix));
        }
        StreamSource<ChangeRecord> source = conf.build();

        FunctionEx<ChangeRecord, String> mapFn = metadata == null
                ? ChangeRecord::table
                : metadata.tableNameFn();
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(source)
                .withIngestionTimestamps()
                .filter(changeRecord -> changeRecord.operation() != UNSPECIFIED)
                .map(mapFn)
                .writeTo(Sinks.list("filtersMultipleTables"));
        pipeline.setPreserveOrder(true);

        // when
        HazelcastInstance hz = createHazelcastInstances(2)[0];
        Job job = hz.getJet().newJob(pipeline, standardConf());
        assertThat(job).eventuallyHasStatus(RUNNING);
        IList<CustomerInfo> results = hz.getList("filtersMultipleTables");
        waitForSnapshotEnd();

        performSetOfChanges(container());
        performChangeToProduct(container());

        // then
        assertTrueEventually(() -> assertThat(results).hasSizeGreaterThanOrEqualTo(2));
        assertTrueEventually(() -> assertThat(new HashSet<>(results)).hasSize(2));
        try {
            job.cancel();
        } catch (Exception ignored) {
        }
    }

    protected void assertContainsInitialRecords(IList<CustomerInfo> results) {
        var expectedRecords = initialRecords();
        assertTrueEventually(() -> assertThat(results).containsAll(expectedRecords));
    }

    protected void assertContainsAddedRecords(IList<CustomerInfo> results) {
        var expectedRecords = addedRecords();
        assertTrueEventually(() -> assertThat(results).containsAll(expectedRecords));
    }

    protected void assertContainsAddedRecordsAfterRestart(IList<CustomerInfo> results) {
        var expectedRecords = new ArrayList<>(addedRecords());
        expectedRecords.add(new CustomerInfo(INSERT,
                new Customer(1007 + testNumberModifier, "Darth", "Vader", "vader@empire.com")));
        assertTrueEventually(() -> assertThat(results).containsAll(expectedRecords));
    }

    @After
    public void revertChanges() {
        revertSetOfChanges(container());
        revertSecondSetOfChanges(container());
        revertChangeToProduct(container());
    }
}
