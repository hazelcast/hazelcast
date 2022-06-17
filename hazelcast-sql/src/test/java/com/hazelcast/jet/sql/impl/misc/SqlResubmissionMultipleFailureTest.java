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
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParametrizedRunner.class)
@Parameterized.UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({SlowTest.class, ParallelJVMTest.class})
public class SqlResubmissionMultipleFailureTest extends SqlResubmissionTestSupport {
    private static final int INITIAL_CLUSTER_SIZE = 1;
    private static final int INITIAL_FAILURE_SIZE = 20;
    private static final Config SMALL_INSTANCE_CONFIG = smallInstanceConfig()
            .setProperty(ClusterProperty.PARTITION_COUNT.getName(), "111");

    @Parameterized.Parameter(0)
    public ClusterFailureTestSupport.MultipleFailingInstanceClusterFailure clusterFailure;

    @Parameterized.Parameter(1)
    public ClientSqlResubmissionMode resubmissionMode;

    private HazelcastInstance client;

    @Parameterized.Parameters(name = "clusterFailure:{0}, resubmissionMode:{1}")
    public static Collection<Object[]> parameters() {
        List<Object[]> res = new ArrayList<>();
        List<ClusterFailureTestSupport.MultipleFailingInstanceClusterFailure> failures = Arrays.asList(
                new ClusterFailureTestSupport.NodeShutdownClusterMultipleFailure()
        );
        for (ClientSqlResubmissionMode mode : ClientSqlResubmissionMode.values()) {
            for (ClusterFailureTestSupport.MultipleFailingInstanceClusterFailure failure : failures) {
                res.add(new Object[]{failure, mode});
            }
        }
        return res;
    }

    @Before
    public void initFailure() {
        clusterFailure.initialize(INITIAL_CLUSTER_SIZE, INITIAL_FAILURE_SIZE, SMALL_INSTANCE_CONFIG);
        createMap(clusterFailure.getFailingInstance(), SLOW_MAP_NAME, SLOW_MAP_SIZE, SlowFieldAccessObject::new,
                SlowFieldAccessObject.class);
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getSqlConfig().setSqlResubmissionMode(resubmissionMode);
        client = clusterFailure.createClient(clientConfig);
    }

    @Test
    public void when_failingUpdate() throws InterruptedException {
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
            if (shouldFailModifyingQuery(resubmissionMode)) {
                assertThrows(HazelcastSqlException.class, () -> client.getSql().execute(statement));
            } else {
                assertEquals(0, client.getSql().execute(statement).updateCount());
            }
        } finally {
            failingThread.join();
            clusterFailure.cleanUp();
        }
    }
}
