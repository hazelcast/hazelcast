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

package com.hazelcast.jet.cdc.postgres;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.cdc.ChangeRecord;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.SQLException;

import static com.hazelcast.jet.core.JobStatus.FAILED;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testcontainers.containers.PostgreSQLContainer.POSTGRESQL_PORT;

@Category(NightlyTest.class)
public class PostgresCdcAuthAndConnectionIntegrationTest extends AbstractPostgresCdcIntegrationTest {

    @Test
    public void wrongPassword() {
        StreamSource<ChangeRecord> source = PostgresCdcSources.postgres("name")
                .setDatabaseAddress(postgres.getContainerIpAddress())
                .setDatabasePort(postgres.getMappedPort(POSTGRESQL_PORT))
                .setDatabaseUser("postgres")
                .setDatabasePassword("wrongPassword")
                .setDatabaseName("postgres")
                .build();

        Pipeline pipeline = pipeline(source);

        JetInstance jet = createJetMembers(2)[0];

        // when
        Job job = jet.newJob(pipeline);
        // then
        assertThatThrownBy(job::join)
                .hasRootCauseInstanceOf(SQLException.class)
                .hasStackTraceContaining("password authentication failed for user \"postgres\"");
    }

    @Test
    public void incorrectAddress() {
        String containerIpAddress = postgres.getContainerIpAddress();
        String wrongContainerIpAddress = "172.17.5.10";
        if (containerIpAddress.equals(wrongContainerIpAddress)) {
            wrongContainerIpAddress = "172.17.5.20";
        }
        StreamSource<ChangeRecord> source = PostgresCdcSources.postgres("name")
                .setDatabaseAddress(wrongContainerIpAddress)
                .setDatabasePort(postgres.getMappedPort(POSTGRESQL_PORT))
                .setDatabaseUser("postgres")
                .setDatabasePassword("")
                .setDatabaseName("postgres")
                .build();

        Pipeline pipeline = pipeline(source);

        JetInstance jet = createJetMembers(2)[0];

        // when
        Job job = jet.newJob(pipeline);
        // then
        assertJobStatusEventually(job, FAILED);
    }

    @Test
    public void incorrectDatabaseName() {
        StreamSource<ChangeRecord> source = PostgresCdcSources.postgres("name")
                .setDatabaseAddress(postgres.getContainerIpAddress())
                .setDatabasePort(postgres.getMappedPort(POSTGRESQL_PORT))
                .setDatabaseUser("postgres")
                .setDatabasePassword("")
                .setDatabaseName("wrongDatabaseName")
                .build();

        Pipeline pipeline = pipeline(source);

        JetInstance jet = createJetMembers(2)[0];

        // when
        Job job = jet.newJob(pipeline);
        // then
        assertThatThrownBy(job::join)
                .hasRootCauseInstanceOf(SQLException.class)
                .hasStackTraceContaining("password authentication failed for user \"postgres\"");
    }

    private Pipeline pipeline(StreamSource<ChangeRecord> source) {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(source)
                .withoutTimestamps()
                .writeTo(Sinks.logger());
        return pipeline;
    }
}
