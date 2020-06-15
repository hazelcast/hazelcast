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
import com.hazelcast.jet.cdc.ChangeRecord;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.test.annotation.NightlyTest;
import java.sql.SQLException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.testcontainers.containers.MySQLContainer;

import static com.hazelcast.jet.core.JobStatus.FAILED;
import static com.hazelcast.test.HazelcastTestSupport.assertEqualsEventually;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testcontainers.containers.MySQLContainer.MYSQL_PORT;

@Category(NightlyTest.class)
public class MySqlAuthAndConnectionIntegrationTest extends JetTestSupport {

    @Rule
    public MySQLContainer<?> mysql = new MySQLContainer<>("debezium/example-mysql:1.2")
            .withUsername("mysqluser")
            .withPassword("mysqlpw");

    @Test
    public void testWrongDebeziumPassword() throws Exception {
        StreamSource<ChangeRecord> source = MySqlCdcSources.mysql("name")
                .setDatabaseAddress(mysql.getContainerIpAddress())
                .setDatabasePort(mysql.getMappedPort(MYSQL_PORT))
                .setDatabaseUser("debezium")
                .setDatabasePassword("wrongPassword")
                .setClusterName("dbserver1")
                .build();

        Pipeline pipeline = pipeline(source);

        JetInstance jet = createJetMembers(2)[0];

        // when
        Job job = jet.newJob(pipeline);
        // then
        assertThatThrownBy(() -> job.join())
                .hasRootCauseInstanceOf(SQLException.class)
                .hasStackTraceContaining("Access denied for user");
    }

    @Test
    public void testEmptyDebeziumPassword() throws Exception {
        StreamSource<ChangeRecord> source = MySqlCdcSources.mysql("name")
                .setDatabaseAddress(mysql.getContainerIpAddress())
                .setDatabasePort(mysql.getMappedPort(MYSQL_PORT))
                .setDatabaseUser("debezium")
                .setDatabasePassword("")
                .setClusterName("dbserver1")
                .build();

        Pipeline pipeline = pipeline(source);

        JetInstance jet = createJetMembers(2)[0];

        // when
        Job job = jet.newJob(pipeline);
        // then
        assertThatThrownBy(() -> job.join())
                .hasRootCauseInstanceOf(SQLException.class)
                .hasStackTraceContaining("Access denied for user");
    }

    @Test
    public void testIncorrectAddress() throws Exception {
        String containerIpAddress = mysql.getContainerIpAddress();
        String wrongContainerIpAddress = "172.17.5.10";
        if (containerIpAddress.equals(wrongContainerIpAddress)) {
            wrongContainerIpAddress = "172.17.5.20";
        }
        StreamSource<ChangeRecord> source = MySqlCdcSources.mysql("name")
                .setDatabaseAddress(wrongContainerIpAddress)
                .setDatabasePort(mysql.getMappedPort(MYSQL_PORT))
                .setDatabaseUser("debezium")
                .setDatabasePassword("dbz")
                .setClusterName("dbserver1")
                .build();

        Pipeline pipeline = pipeline(source);

        JetInstance jet = createJetMembers(2)[0];

        // when
        Job job = jet.newJob(pipeline);
        // then
        assertEqualsEventually(() -> job.getStatus(), FAILED);
    }

    @Test
    public void testIncorrectPort() throws Exception {
        int wrongPort = mysql.getMappedPort(MYSQL_PORT) + 1;
        StreamSource<ChangeRecord> source = MySqlCdcSources.mysql("name")
                .setDatabaseAddress(mysql.getContainerIpAddress())
                .setDatabasePort(wrongPort)
                .setDatabaseUser("debezium")
                .setDatabasePassword("dbz")
                .setClusterName("dbserver1")
                .build();

        Pipeline pipeline = pipeline(source);

        JetInstance jet = createJetMembers(2)[0];

        // when
        Job job = jet.newJob(pipeline);
        // then
        assertEqualsEventually(() -> job.getStatus(), FAILED);
    }

    private Pipeline pipeline(StreamSource<ChangeRecord> source) {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(source)
                .withoutTimestamps()
                .writeTo(Sinks.logger());
        return pipeline;
    }
}
