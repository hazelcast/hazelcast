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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.cdc.TestUtils.Customer;
import com.hazelcast.jet.cdc.TestUtils.CustomerInfo;
import com.hazelcast.function.ConsumerEx;
import com.hazelcast.jet.DataProducer;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.impl.JobRepository;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import eu.rekawek.toxiproxy.model.Toxic;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.TreeSet;
import java.util.UUID;

import static com.hazelcast.jet.cdc.Operation.UNSPECIFIED;
import static com.hazelcast.jet.cdc.TestNotificationChannel.waitForSnapshotEnd;
import static com.hazelcast.jet.cdc.TestUtils.smallInstanceConfigWithCompactSerialization;
import static com.hazelcast.jet.TestedVersions.TOXIPROXY_IMAGE;
import static com.hazelcast.test.DockerTestUtil.assumeDockerEnabled;
import static eu.rekawek.toxiproxy.model.ToxicDirection.DOWNSTREAM;
import static eu.rekawek.toxiproxy.model.ToxicDirection.UPSTREAM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

/**
 * Assumptions:
 * <ul>
 * <li>2 retries, 300 then 600ms
 * <li>1s connection timeout, 1s socket timeout
 * <li>Debezium tries to connect, 1s wasted. Then 300ms wait for second try, 1s wasted, 600ms wait for 2nd retry, 1s wasted.
 * <li> ~ 4s for connector to potentially fail
 * </ul>
 * <p>
 * Therefore, tests typically use 2s toxi (way below threshold) and 7s (above it).
 */
@Category({NightlyTest.class})
@RunWith(HazelcastSerialClassRunner.class)
public abstract class NetworkIssuesTest<C extends JdbcDatabaseContainer<?>> extends JetTestSupport {

    // 10 tests, each 10m (+-1m)
    @ClassRule
    public static Timeout globalTimeout = Timeout.seconds(10 * 11 * 60);

    @Rule
    public TestName testName = new TestName();

    @Rule
    public Timeout timeout = Timeout.seconds(10 * 60);

    protected Network network = Network.newNetwork();
    protected C dbContainer;
    protected Job job;
    protected ToxiproxyContainer toxiproxy;
    private HazelcastInstance hz;
    protected Proxy proxy;
    protected String uuidForNotifications;

    @BeforeClass
    public static void beforeClassCheckDocker() {
        assumeDockerEnabled();
    }

    @Before
    public void beforeTest() throws IOException {
        if (dbContainer == null) {
            startDbContainer();
        }

        toxiproxy = new ToxiproxyContainer(TOXIPROXY_IMAGE).withNetwork(network).withNetworkAliases("toxiproxy");
        toxiproxy.start();
        var toxiproxyClient = new ToxiproxyClient(toxiproxy.getHost(), toxiproxy.getControlPort());
        this.proxy = toxiproxyClient.createProxy("databaseProxy", "0.0.0.0:8666", "database:"
                + databasePort());

        HazelcastInstance[] hazelcastInstances = createHazelcastInstances(smallInstanceConfigWithCompactSerialization(), 2);
        hz = hazelcastInstances[0];
        uuidForNotifications = UUID.randomUUID().toString();
    }

    protected abstract int databasePort();
    protected abstract void startDbContainer();
    protected abstract StreamSource<ChangeRecord> prepareDefaultSource(int maxRetries);
    protected abstract String jdbcUrlWithAuth();

    @After
    public void afterTest() throws IOException {
        try {
            if (job != null) {
                job.cancel();
            }
        } catch (Exception ignored) {
        }
        stopDbContainer();
        proxy.delete();
        toxiproxy.close();
    }

    private void stopDbContainer() {
        if (dbContainer != null) {
            dbContainer.stop();
        }
    }

    @Test
    public void testJobFailWhenDbIsDownBeforeJobStart() {
        final StreamSource<ChangeRecord> source = prepareDefaultSource(2);
        stopDbContainer();

        final Pipeline pipeline = getPipeline(source);
        job = hz.getJet().newJob(pipeline, getJobConfig());

        assertTrueEventually(() -> assertEquals(JobStatus.FAILED, job.getStatus()));
    }

    @Test
    public void testJobSurvivesNetworkLatency_2s() throws Exception {
        testWithNetworkProxy(proxy -> {
            proxy.toxics().latency(DOWNSTREAM.name(), DOWNSTREAM, 500)
                 .setJitter(500);
            proxy.toxics().latency(UPSTREAM.name(), UPSTREAM, 500).setJitter(250);
        }, Duration.ofSeconds(2));
    }

    @Test
    public void testJobSurvivesNetworkLatency_7s() throws Exception {
        testWithNetworkProxy(proxy -> {
            proxy.toxics().latency(DOWNSTREAM.name(), DOWNSTREAM, 500)
                 .setJitter(500);
            proxy.toxics().latency(UPSTREAM.name(), UPSTREAM, 500).setJitter(250);
        }, Duration.ofSeconds(7));
    }

    @Test
    public void testJobSurvivesNetworkLatency_7s_high() throws Exception {
        testWithNetworkProxy(proxy -> {
            proxy.toxics().latency(DOWNSTREAM.name(), DOWNSTREAM, 2100)
                 .setJitter(500);
            proxy.toxics().latency(UPSTREAM.name(), UPSTREAM, 500).setJitter(250);
        }, Duration.ofSeconds(7));
    }

    @Test
    public void testJobSurvivesNetworkLatency_7s_very_high() throws Exception {
        testWithNetworkProxy(proxy -> {
            proxy.toxics().latency(DOWNSTREAM.name(), DOWNSTREAM, 1000)
                 .setJitter(500);
            proxy.toxics().latency(UPSTREAM.name(), UPSTREAM, 1000).setJitter(500);
        }, Duration.ofSeconds(7));
    }

    static <V> ConsumerEx<V> unchecked(ConsumerEx<V> function) {
        return function;
    }

    @Test
    public void testJobSurvivesNetworkDisruption_2s() throws Exception {
        testWithNetworkProxy(proxy -> {
            proxy.toxics().bandwidth(DOWNSTREAM.name(), DOWNSTREAM, 0);
            proxy.toxics().bandwidth(UPSTREAM.name(), UPSTREAM, 0);
        }, Duration.ofSeconds(2));
    }

    @Test
    public void testJobSurvivesNetworkDisruption_7s() throws Exception {
        testWithNetworkProxy(proxy -> {
            proxy.toxics().bandwidth(DOWNSTREAM.name(), DOWNSTREAM, 0);
            proxy.toxics().bandwidth(UPSTREAM.name(), UPSTREAM, 0);
        }, Duration.ofSeconds(7));
    }

    @Test(timeout = 7 * 60_000)
    public void testJobSurvivesTimeouts_7s() throws Exception {
        testWithNetworkProxy(proxy -> proxy.toxics().timeout(DOWNSTREAM.name(), DOWNSTREAM, 1000),
                Duration.ofSeconds(7));
    }

    @Test
    public void testJobSurvivesTimeouts_2s() throws Exception {
        testWithNetworkProxy(proxy -> proxy.toxics().timeout(DOWNSTREAM.name(), DOWNSTREAM, 1000),
                Duration.ofSeconds(1));
    }

    @Test
    public void testJobSurvivesTimeouts_2s_200ms() throws Exception {
        testWithNetworkProxy(proxy -> proxy.toxics().timeout(DOWNSTREAM.name(), DOWNSTREAM, 200),
                Duration.ofSeconds(2));
    }

    private void testWithNetworkProxy(ConsumerEx<Proxy> injectToxi, Duration toxiDuration) throws Exception {
        final StreamSource<ChangeRecord> source = prepareDefaultSource(-1);
        final Pipeline pipeline = getPipeline(source);
        job = hz.getJet().newJob(pipeline, getJobConfig());
        Thread.sleep(500);
        waitForSnapshotEnd(uuidForNotifications);

        DataProducer dataProducer = new DataProducer(jdbcUrlWithAuth(),
                id -> ("INSERT INTO inventory.customers (id, first_name, last_name, email) "
                        + "values (%s, 'FirstName%s', 'LastName%s', 'email_of_%s@mail.com');")
                        .formatted(id, id, id, id));

        JobRepository jobRepository = new JobRepository(hz);
        waitForNextSnapshot(jobRepository, job);
        dataProducer.start();
        Thread.sleep(700);

        injectToxi.accept(proxy);
        waitForNextSnapshot(jobRepository, job);
        Thread.sleep(toxiDuration.toMillis());
        proxy.toxics().getAll().forEach(unchecked(Toxic::remove));
        Thread.sleep(700);

        int itemsProduced = dataProducer.stop();
        assertThat(itemsProduced).isGreaterThan(0);

        var results = resultMap();

        assertTrueEventually(() -> assertThat(resultsWithoutInitial(results)).hasSize(itemsProduced));
    }

    private Collection<CustomerInfo> resultsWithoutInitial(IMap<Long, CustomerInfo> results) {
        final int firstId = 100000;
        return new TreeSet<>(results.values().stream()
                      .filter(c -> c.cust().id >= firstId)
                      .toList());
    }

    private JobConfig getJobConfig() {
        return new JobConfig()
                .setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE)
                .setSnapshotIntervalMillis(300);
    }

    protected @Nonnull Pipeline getPipeline(StreamSource<ChangeRecord> source) {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(source)
                .withIngestionTimestamps()
                .filter(changeRecord -> changeRecord.operation() != UNSPECIFIED)
                .writeTo(CdcSinks.map(resultMap(),
                                      changeRecord -> (long) changeRecord.nonNullValue().toObject(Customer.class).id,
                                      changeRecord -> {
                                          Operation operation = changeRecord.operation();
                                          return new CustomerInfo(operation, changeRecord.nonNullValue().toObject(Customer.class));
                                      }
                                     ));
        pipeline.setPreserveOrder(true);
        return pipeline;
    }

    private IMap<Long, CustomerInfo> resultMap() {
        assert hz != null;
        return hz.getMap(testName.getMethodName());
    }
}
