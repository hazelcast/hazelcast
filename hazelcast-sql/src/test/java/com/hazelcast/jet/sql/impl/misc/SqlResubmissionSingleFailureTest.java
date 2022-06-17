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
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
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
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParametrizedRunner.class)
@Parameterized.UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({SlowTest.class, ParallelJVMTest.class})
public class SqlResubmissionSingleFailureTest extends SqlResubmissionTestSupport {
    private static final int INITIAL_CLUSTER_SIZE = 1;
    private static final Config SMALL_INSTANCE_CONFIG = smallInstanceConfig();

    @Parameterized.Parameter(0)
    public ClusterFailureTestSupport.SingleFailingInstanceClusterFailure clusterFailure;

    @Parameterized.Parameter(1)
    public ClientSqlResubmissionMode resubmissionMode;

    private HazelcastInstance client;

    @Parameterized.Parameters(name = "clusterFailure:{0}, resubmissionMode:{1}")
    public static Collection<Object[]> parameters() {
        List<Object[]> res = new ArrayList<>();
        List<ClusterFailureTestSupport.SingleFailingInstanceClusterFailure> failures = Arrays.asList(
                new ClusterFailureTestSupport.NodeReplacementClusterFailure(),
                new ClusterFailureTestSupport.NodeShutdownClusterFailure(),
                new ClusterFailureTestSupport.NetworkProblemClusterFailure(),
                new ClusterFailureTestSupport.NodeTerminationClusterFailure()
        );
        for (ClientSqlResubmissionMode mode : ClientSqlResubmissionMode.values()) {
            for (ClusterFailureTestSupport.SingleFailingInstanceClusterFailure failure : failures) {
                res.add(new Object[]{failure, mode});
            }
        }
        return res;
    }

    @Before
    public void initFailure() {
        clusterFailure.initialize(INITIAL_CLUSTER_SIZE, SMALL_INSTANCE_CONFIG);
        createMap(clusterFailure.getNotFailingInstance(), COMMON_MAP_NAME, COMMON_MAP_SIZE, UUID::randomUUID, UUID.class);
        createMap(clusterFailure.getFailingInstance(), SLOW_MAP_NAME, SLOW_MAP_SIZE, SlowFieldAccessObject::new,
                SlowFieldAccessObject.class);
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getSqlConfig().setSqlResubmissionMode(resubmissionMode);
        client = clusterFailure.createClient(clientConfig);
    }

    @Test
    public void when_failingSelectAfterSomeDataIsFetched() {
        SqlStatement statement = new SqlStatement("select * from " + COMMON_MAP_NAME);
        statement.setCursorBufferSize(1);
        SqlResult rows = client.getSql().execute(statement);

        try {
            if (shouldFailAfterSomeDataIsFetched(resubmissionMode)) {
                assertThrows(HazelcastSqlException.class, () -> {
                    countWithFailureInTheMiddle(rows);
                });
            } else {
                int count = countWithFailureInTheMiddle(rows);
                logger.info("Count: " + count);
                assertTrue(COMMON_MAP_SIZE < count);
            }
        } finally {
            clusterFailure.cleanUp();
        }
    }

    private int countWithFailureInTheMiddle(SqlResult rows) {
        int count = 0;
        for (SqlRow row : rows) {
            if (count == COMMON_MAP_SIZE / 2) {
                logger.info("Half of the map is fetched, time to fail");
                clusterFailure.fail();
            }
            count++;
        }
        return count;
    }

    @Test
    public void when_failingSelectBeforeAnyDataIsFetched() throws InterruptedException {
        SqlStatement statement = new SqlStatement("select field from " + SLOW_MAP_NAME);
        statement.setCursorBufferSize(SLOW_MAP_SIZE);
        Thread failingThread = new Thread(() -> {
            try {
                Thread.sleep(SLOW_ACCESS_TIME_MILLIS / 2);
            } catch (InterruptedException e) {
            }
            clusterFailure.fail();
        });
        failingThread.start();
        try {
            if (shouldFailBeforeAnyDataIsFetched(resubmissionMode)) {
                assertThrows(HazelcastSqlException.class, () -> client.getSql().execute(statement));
            } else {
                assertEquals(SLOW_MAP_SIZE, count(client.getSql().execute(statement)));
            }
        } finally {
            failingThread.join();
            clusterFailure.cleanUp();
        }
    }

    private int count(SqlResult rows) {
        int count = 0;
        for (SqlRow row : rows) {
            count++;
        }
        return count;
    }

    @Test
    public void when_failingUpdate() throws InterruptedException {
        SqlStatement statement = new SqlStatement("update " + SLOW_MAP_NAME + " set field = field + 1");

        Thread failingThread = new Thread(() -> {
            try {
                Thread.sleep(SLOW_ACCESS_TIME_MILLIS / 2);
            } catch (InterruptedException e) {
            }
            clusterFailure.fail();
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
