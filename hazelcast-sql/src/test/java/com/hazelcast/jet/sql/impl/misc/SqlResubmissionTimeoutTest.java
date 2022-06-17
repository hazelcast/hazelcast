/*
 * Copyright 2021 Hazelcast Inc.
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

package com.hazelcast.jet.sql.impl.misc;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientSqlResubmissionMode;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlStatement;
import com.hazelcast.test.ClusterFailureTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({SlowTest.class, ParallelJVMTest.class})
public class SqlResubmissionTimeoutTest extends SqlResubmissionTestSupport {
    private static final int SLOW_MAP_SIZE = 100;

    private static final int INITIAL_CLUSTER_SIZE = 1;
    private static final int INITIAL_FAILURE_SIZE = 20;
    private static final Config INSTANCE_CONFIG = smallInstanceConfig()
            .setProperty(ClusterProperty.PARTITION_COUNT.getName(), "111");

    public ClusterFailureTestSupport.MultipleFailingInstanceClusterFailure clusterFailure =
            new ClusterFailureTestSupport.NodeShutdownClusterMultipleFailure();
    public ClientSqlResubmissionMode resubmissionMode = ClientSqlResubmissionMode.RETRY_ALL;

    @Test
    public void when_totalTimeOfResubmissionEnds_then_fail() throws InterruptedException {
        clusterFailure.initialize(INITIAL_CLUSTER_SIZE, INITIAL_FAILURE_SIZE, INSTANCE_CONFIG);
        createMap(clusterFailure.getFailingInstance(), SLOW_MAP_NAME, SLOW_MAP_SIZE, SlowFieldAccessObject::new,
                SlowFieldAccessObject.class);
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProperty("hazelcast.client.invocation.timeout.seconds", "5");
        clientConfig.getSqlConfig().setSqlResubmissionMode(resubmissionMode);
        HazelcastInstance client = clusterFailure.createClient(clientConfig);
        SqlStatement statement = new SqlStatement("update " + SLOW_MAP_NAME + " set field = field + 1");

        Thread failingThread = new Thread(() -> {
            try {
                Thread.sleep(SLOW_ACCESS_TIME_MILLIS / 2);
                for (int i = 0; i < INITIAL_FAILURE_SIZE; i++) {
                    clusterFailure.fail();
                }
            } catch (InterruptedException e) {
            }
        });
        failingThread.start();
        try {
            assertThrows(HazelcastSqlException.class, () -> client.getSql().execute(statement));
        } finally {
            failingThread.join();
            clusterFailure.cleanUp();
        }
    }
}
