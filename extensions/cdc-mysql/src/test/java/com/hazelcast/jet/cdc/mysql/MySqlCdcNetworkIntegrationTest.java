/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.cdc.AbstractCdcIntegrationTest;
import com.hazelcast.jet.cdc.ChangeRecord;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.retry.RetryStrategies;
import com.hazelcast.jet.retry.RetryStrategy;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.NightlyTest;
import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import static com.hazelcast.jet.TestedVersions.TOXIPROXY_IMAGE;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.core.JobAssertions.assertThat;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.runners.Parameterized.Parameter;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;
import static org.testcontainers.containers.MySQLContainer.MYSQL_PORT;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({NightlyTest.class})
public class MySqlCdcNetworkIntegrationTest extends AbstractCdcIntegrationTest {

    private static final long RECONNECT_INTERVAL_MS = SECONDS.toMillis(1);
    private static final String NETWORK_ALIAS = "mysql";
    private static final String UPSTREAM = "mysql:3306";

    @Parameter(value = 0)
    public RetryStrategy reconnectBehavior;

    @Parameter(value = 1)
    public boolean resetStateOnReconnect;

    @Parameter(value = 2)
    public String testName;

    private MySQLContainer<?> mysql;

    @Parameters(name = "{2}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {RetryStrategies.never(), false, "fail"},
                {RetryStrategies.indefinitely(RECONNECT_INTERVAL_MS), false, "reconnect"},
                {RetryStrategies.indefinitely(RECONNECT_INTERVAL_MS), true, "reconnect w/ state reset"}
        });
    }

    @After
    public void after() {
        if (mysql != null) {
            mysql.stop();
        }
    }

    @Test
    public void when_noDatabaseToConnectTo() throws Exception {
        mysql = initMySql(null, null);
        int port = fixPortBinding(mysql, MYSQL_PORT);
        String containerIpAddress = mysql.getHost();
        stopContainer(mysql);

        Pipeline pipeline = initPipeline(containerIpAddress, port);

        // when job starts
        HazelcastInstance hz = createHazelcastInstances(2)[0];
        Job job = hz.getJet().newJob(pipeline);
        // then
        boolean neverReconnect = reconnectBehavior.getMaxAttempts() == 0;
        if (neverReconnect) {
            // then job fails
            assertJobFailsWithConnectException(job, false);
            assertTrue(hz.getMap("results").isEmpty());
        } else {
            // and can't connect to DB
            assertThat(job).eventuallyHasStatus(RUNNING);
            assertTrue(hz.getMap("results").isEmpty());

            // and DB starts
            mysql.start();
            try {
                // then source connects successfully
                assertEqualsEventually(() -> hz.getMap("results").size(), 4);
                assertEquals(RUNNING, job.getStatus());
            } finally {
                abortJob(job);
            }
        }
    }

    @Test
    public void when_networkDisconnectDuringSnapshotting_then_jetSourceIsStuckUntilReconnect() throws Exception {
        try (
                Network network = initNetwork();
                ToxiproxyContainer toxiproxy = initToxiproxy(network)
        ) {
            mysql = initMySql(network, null);
            Proxy proxy = initProxy(toxiproxy);

            String host = toxiproxy.getHost();
            Integer port = toxiproxy.getMappedPort(8666);
            Pipeline pipeline = initPipeline(host, port);
            // when job starts
            HazelcastInstance hz = createHazelcastInstances(2)[0];
            Job job = hz.getJet().newJob(pipeline);
            assertThat(job).eventuallyHasStatus(RUNNING);

            // and snapshotting is ongoing (we have no exact way of identifying
            // the moment, but random sleep will catch it at least some of the time)
            MILLISECONDS.sleep(ThreadLocalRandom.current().nextInt(0, 500));

            // and connection is cut
            setConnectionCut(proxy, true);

            // and some time passes
            MILLISECONDS.sleep(2 * RECONNECT_INTERVAL_MS);

            // and connection recovers
            setConnectionCut(proxy, false);

            // then connector manages to reconnect and finish snapshot
            try {
                assertEqualsEventually(() -> hz.getMap("results").size(), 4);
            } finally {
                abortJob(job);
            }
        }
    }

    @Test
    public void when_databaseShutdownDuringSnapshotting() throws Exception {
        mysql = initMySql(null, null);
        int port = fixPortBinding(mysql, MYSQL_PORT);

        Pipeline pipeline = initPipeline(mysql.getHost(), port);
        // when job starts
        HazelcastInstance hz = createHazelcastInstances(2)[0];
        Job job = hz.getJet().newJob(pipeline);
        assertThat(job).eventuallyHasStatus(RUNNING);

        // and snapshotting is ongoing (we have no exact way of identifying
        // the moment, but random sleep will catch it at least some of the time)
        MILLISECONDS.sleep(ThreadLocalRandom.current().nextInt(100, 500));

        // and DB is stopped
        stopContainer(mysql);

        boolean neverReconnect = reconnectBehavior.getMaxAttempts() == 0;
        if (neverReconnect) {
            // then job fails
            assertJobFailsWithConnectException(job, true);
        } else {
            // and DB is started anew
            mysql = initMySql(null, port);

            // then snapshotting finishes successfully
            try {
                assertEqualsEventually(() -> hz.getMap("results").size(), 4);
                assertEquals(RUNNING, job.getStatus());
            } finally {
                abortJob(job);
            }
        }
    }

    @Test
    public void when_networkDisconnectDuringBinlogRead_then_connectorReconnectsInternally() throws Exception {
        try (
                Network network = initNetwork();
                ToxiproxyContainer toxiproxy = initToxiproxy(network)
        ) {
            mysql = initMySql(network, null);
            Proxy proxy = initProxy(toxiproxy);

            String host = toxiproxy.getHost();
            Integer port = toxiproxy.getMappedPort(8666);
            Pipeline pipeline = initPipeline(host, port);
            // when connector is up and transitions to binlog reading
            HazelcastInstance hz = createHazelcastInstances(2)[0];
            Job job = hz.getJet().newJob(pipeline);
            assertEqualsEventually(() -> hz.getMap("results").size(), 4);
            insertRecords(mysql, 1005);
            assertEqualsEventually(() -> hz.getMap("results").size(), 5);

            // and the connection is cut
            setConnectionCut(proxy, true);

            // and some new events get generated in the DB
            insertRecords(mysql, 1006, 1007);

            // and some time passes
            MILLISECONDS.sleep(2 * RECONNECT_INTERVAL_MS);

            // and the connection is re-established
            setConnectionCut(proxy, false);

            // then the connector catches up
            try {
                assertEqualsEventually(() -> hz.getMap("results").size(), 7);
            } finally {
                abortJob(job);
            }
        }
    }

    @Test
    public void when_databaseShutdownDuringBinlogReading() throws Exception {
        mysql = initMySql(null, null);
        int port = fixPortBinding(mysql, MYSQL_PORT);

        Pipeline pipeline = initPipeline(mysql.getHost(), port);
        // when connector is up and transitions to binlog reading
        HazelcastInstance hz = createHazelcastInstances(2)[0];
        Job job = hz.getJet().newJob(pipeline);
        assertEqualsEventually(() -> hz.getMap("results").size(), 4);
        SECONDS.sleep(3);
        insertRecords(mysql, 1005);
        assertEqualsEventually(() -> hz.getMap("results").size(), 5);

        // and DB is stopped
        stopContainer(mysql);

        boolean neverReconnect = reconnectBehavior.getMaxAttempts() == 0;
        if (neverReconnect) {
            // then job fails
            assertJobFailsWithConnectException(job, true);
        } else {
            // and results are cleared
            hz.getMap("results").clear();
            assertEqualsEventually(() -> hz.getMap("results").size(), 0);

            // and DB is started anew
            mysql = initMySql(null, port);
            insertRecords(mysql, 1005, 1006, 1007);

            try {
                if (resetStateOnReconnect) {
                    // then job keeps running, connector starts freshly, including snapshotting
                    assertEqualsEventually(() -> hz.getMap("results").size(), 7);
                    assertEquals(RUNNING, job.getStatus());
                } else {
                    assertEqualsEventually(() -> hz.getMap("results").size(), 2);
                    assertEquals(RUNNING, job.getStatus());
                }
            } finally {
                abortJob(job);
            }
        }
    }

    private StreamSource<ChangeRecord> source(String host, int port) {
        return MySqlCdcSources.mysql("customers")
                .setDatabaseAddress(host)
                .setDatabasePort(port)
                .setDatabaseUser("debezium")
                .setDatabasePassword("dbz")
                .setClusterName("dbserver1").setDatabaseWhitelist("inventory")
                .setTableWhitelist("inventory." + "customers")
                .setReconnectBehavior(reconnectBehavior)
                .setShouldStateBeResetOnReconnect(resetStateOnReconnect)
                .build();
    }

    private Pipeline initPipeline(String host, int port) {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(source(host, port))
                .withNativeTimestamps(0)
                .map(r -> entry(requireNonNull(r.key()).toMap().get("id"), r.value().toJson()))
                .writeTo(Sinks.map("results"));
        return pipeline;
    }

    private void abortJob(Job job) {
        try {
            job.cancel();
            job.join();
        } catch (Exception e) {
            // ignore, cancellation exception expected
        }
    }

    @SuppressWarnings("resource")
    private MySQLContainer<?> initMySql(Network network, Integer fixedExposedPort) {
        MySQLContainer<?> mysql = namedTestContainer(
                new MySQLContainer<>(AbstractMySqlCdcIntegrationTest.DOCKER_IMAGE)
                        .withUsername("mysqluser")
                        .withPassword("mysqlpw")
        );
        if (fixedExposedPort != null) {
            Consumer<CreateContainerCmd> cmd = e -> requireNonNull(e.getHostConfig()).withPortBindings(
                    new PortBinding(Ports.Binding.bindPort(fixedExposedPort), new ExposedPort(MYSQL_PORT)));
            mysql = mysql.withCreateContainerCmdModifier(cmd);
        }
        if (network != null) {
            mysql = mysql.withNetwork(network)
                    .withNetworkAliases(NETWORK_ALIAS);
        }
        mysql.start();
        return mysql;
    }

    @SuppressWarnings("resource")
    private ToxiproxyContainer initToxiproxy(Network network) {
        ToxiproxyContainer toxiproxy = namedTestContainer(new ToxiproxyContainer(TOXIPROXY_IMAGE).withNetwork(network));
        toxiproxy.start();
        return toxiproxy;
    }

    private static Network initNetwork() {
        return Network.newNetwork();
    }

    private static Proxy initProxy(ToxiproxyContainer toxiproxy) throws IOException {
        ToxiproxyClient toxiproxyClient = new ToxiproxyClient(toxiproxy.getHost(), toxiproxy.getControlPort());
        return toxiproxyClient.createProxy(NETWORK_ALIAS, "0.0.0.0:8666", UPSTREAM);
    }

    private void setConnectionCut(Proxy proxy, boolean shouldCutConnection) throws IOException {
        if (shouldCutConnection) {
            proxy.toxics().bandwidth(ToxicDirection.DOWNSTREAM.name(), ToxicDirection.DOWNSTREAM, 0);
            proxy.toxics().bandwidth(ToxicDirection.UPSTREAM.name(), ToxicDirection.UPSTREAM, 0);
        } else {
            proxy.toxics().get(ToxicDirection.DOWNSTREAM.name()).remove();
            proxy.toxics().get(ToxicDirection.UPSTREAM.name()).remove();
        }
    }

    private static void insertRecords(MySQLContainer<?> mysql, int... ids) throws SQLException {
        try (Connection connection = AbstractMySqlCdcIntegrationTest.getConnection(mysql, "inventory")) {
            connection.setAutoCommit(false);
            Statement statement = connection.createStatement();
            for (int id : ids) {
                statement.addBatch("INSERT INTO customers VALUES (" + id + ", 'Jason', 'Bourne', " +
                        "'jason" + id + "@bourne.org')");
            }
            statement.executeBatch();
            connection.commit();
        }
    }

    @SuppressWarnings("StatementWithEmptyBody")
    private static void assertJobFailsWithConnectException(Job job, boolean lenient) throws InterruptedException {
        try {
            //wait for job to finish w/ timeout
            job.getFuture().get(5, SECONDS);
        } catch (TimeoutException te) {
            //explicitly cancelling the job because it has not completed so far
            job.cancel();

            if (lenient) {
                //ignore the timeout; not all tests are deterministic, sometimes we don't end up in the state
                //we actually want to test
            } else {
                fail("Connection failure not thrown");
            }
        } catch (ExecutionException ee) {
            //job completed exceptionally, as expected, we check the details of it
            assertThat(ee)
                    .hasCauseInstanceOf(JetException.class)
                    .hasStackTraceContaining("Failed to connect to database");
        }
    }

}
