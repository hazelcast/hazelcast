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
package com.hazelcast.mapstore;

import com.hazelcast.config.Config;
import com.hazelcast.config.JavaSerializationFilterConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.sql.impl.connector.jdbc.SelectProcessorSupplier;
import com.hazelcast.test.jdbc.H2DatabaseProvider;
import org.h2.jdbc.JdbcSQLSyntaxErrorException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;

import static com.hazelcast.jet.core.ProcessorMetaSupplier.forceTotalParallelismOne;
import static com.hazelcast.jet.impl.JetServiceBackend.SQL_ARGUMENTS_KEY_NAME;
import static com.hazelcast.test.DockerTestUtil.assumeDockerEnabled;
import static java.util.Collections.emptyList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class H2GenericMapStoreIT extends GenericMapStoreIT {

    @BeforeAll
    public static void beforeClass() {
        assumeDockerEnabled();
        Config config = new Config();
        // H2 exceptions must be deserialised when SQL execution fails on another member.
        config.getSerializationConfig().setJavaSerializationFilterConfig(
                new JavaSerializationFilterConfig().setDefaultsDisabled(true));
        initializeBeforeClass(new H2DatabaseProvider(), config);
    }

    @Test
    public void testRemoteJdbcExceptionCompletesJob() {
        DAG dag = new DAG();
        dag.newVertex("select-missing-table", forceTotalParallelismOne(
                new SelectProcessorSupplier(TEST_DATABASE_REF,
                        "SELECT * FROM ctt1400_missing_table", new int[0], emptyList()),
                instances()[1].getCluster().getLocalMember().getAddress()));
        JobConfig jobConfig = new JobConfig().setArgument(SQL_ARGUMENTS_KEY_NAME, emptyList());
        Job job = instance().getJet().newLightJob(dag, jobConfig);

        try {
            assertThatThrownBy(() -> job.getFuture().get(10, SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .hasRootCauseInstanceOf(JdbcSQLSyntaxErrorException.class)
                    .hasStackTraceContaining("ctt1400_missing_table");
        } finally {
            // A rejected exception leaves the job pending; do not hang the remaining cleanup.
            if (!job.getFuture().isDone()) {
                for (HazelcastInstance member : instances()) {
                    member.getLifecycleService().terminate();
                }
            }
        }
    }
}
