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
import com.hazelcast.jet.Job;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.testcontainers.containers.MySQLContainer;

import javax.annotation.Nonnull;

import static com.hazelcast.jet.cdc.TestUtils.standardConf;
import static com.hazelcast.jet.TestedVersions.DEBEZIUM_MYSQL_IMAGE;
import static com.hazelcast.jet.core.JobAssertions.assertThat;
import static com.hazelcast.jet.core.JobStatus.FAILED;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.containers.MySQLContainer.MYSQL_PORT;

@Category({NightlyTest.class})
@RunWith(HazelcastSerialClassRunner.class)
@SuppressWarnings({"SqlNoDataSourceInspection", "SqlResolve"})
public class DebeziumMySqlTest extends AbstractBasicCdcIntegrationTest<MySQLContainer<?>> {

    @Test
    public void twoJobWithSameServerId() {
        // given
        Pipeline pipeline = getPipeline(basicConf(container()).setProperty("topic.prefix", "TEST1").build());
        Pipeline pipeline2 = getPipeline(basicConf(container()).setProperty("topic.prefix", "TEST2").build());

        // when
        HazelcastInstance hz = createHazelcastInstances(2)[0];
        Job job1 = hz.getJet().newJob(pipeline, standardConf());
        assertThat(job1).eventuallyHasStatus(RUNNING);
        Job job2 = hz.getJet().newJob(pipeline2, standardConf());

        // then
        assertTrueEventually(() ->
                assertThat(job1.getStatus() == FAILED || job2.getStatus() == FAILED).isTrue()
        );
    }

    @Override
    @SuppressWarnings("resource")
    public @Nonnull MySQLContainer<?> getContainer() {
        return new MySQLContainer<>(DEBEZIUM_MYSQL_IMAGE)
                .withDatabaseName("mysql")
                .withUsername("mysqluser")
                .withPassword("mysqlpw");
    }

    @Override
    @Nonnull
    protected DebeziumCdcSources.Builder<ChangeRecord> basicConf(MySQLContainer<?> container) {
        return DebeziumCdcSources
                .debezium("mysql", "io.debezium.connector.mysql.MySqlConnector")
                .withErrorMaxRetries(1)
                .setProperty("database.server.name", "server1")
                .setProperty("database.server.id", "184054")
                .setProperty("database.hostname", container.getHost())
                .setProperty("database.port", container.getMappedPort(MYSQL_PORT))
                .setProperty("database.user", "debezium")
                .setProperty("database.password", "dbz")
                .setProperty("table.include.list", "inventory.customers")
                .setProperty("include.schema.changes", true)
                .setProperty("topic.prefix", "TESTS")

                .setProperty("notification.enabled.channels", "TestNotificationChannel")
                .setProperty("notification.TestNotificationChannel.uuid", uuidForNotifications)

                .setProperty("heartbeat.interval.ms", 1000); // this will add Heartbeat messages to the stream
    }
}
