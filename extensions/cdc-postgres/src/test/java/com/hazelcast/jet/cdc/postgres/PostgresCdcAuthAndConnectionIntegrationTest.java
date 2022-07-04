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
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.cdc.ChangeRecord;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

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

        HazelcastInstance hz = createHazelcastInstances(2)[0];

        // when
        Job job = hz.getJet().newJob(pipeline);
        // then
        assertThatThrownBy(job::join)
                .hasRootCauseInstanceOf(JetException.class)
                .hasStackTraceContaining("password authentication failed for user \"postgres\"");
    }

    @Test
    public void incorrectDatabaseName() {
        StreamSource<ChangeRecord> source = PostgresCdcSources.postgres("name")
                .setDatabaseAddress(postgres.getContainerIpAddress())
                .setDatabasePort(postgres.getMappedPort(POSTGRESQL_PORT))
                .setDatabaseUser("postgres")
                .setDatabasePassword("postgres")
                .setDatabaseName("wrongDatabaseName")
                .build();

        Pipeline pipeline = pipeline(source);

        HazelcastInstance hz = createHazelcastInstances(2)[0];

        // when
        Job job = hz.getJet().newJob(pipeline);
        // then
        assertThatThrownBy(job::join)
                .hasRootCauseInstanceOf(JetException.class)
                .hasStackTraceContaining("database \"wrongDatabaseName\" does not exist");
    }

    private Pipeline pipeline(StreamSource<ChangeRecord> source) {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(source)
                .withoutTimestamps()
                .writeTo(Sinks.logger());
        return pipeline;
    }
}
