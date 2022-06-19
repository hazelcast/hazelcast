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

import static org.junit.Assert.assertTrue;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientSqlResubmissionMode;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlStatement;
import com.hazelcast.sql.impl.client.SqlClientResult;
import com.hazelcast.test.ClusterFailureTestSupport;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.SlowTest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(HazelcastParametrizedRunner.class)
@Parameterized.UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({SlowTest.class, ParallelJVMTest.class})
public class SqlResubmissionStressTest extends SqlResubmissionTestSupport {
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

    private volatile Step step = Step.BEFORE_EXECUTE;
    private volatile boolean finish = false;
    private volatile long lastExecutionTime = 0;

    private Runnable cyclicFailure = () -> {
        boolean failureNeeded = true;

        while (!finish) {
            Step localStep = step;
            switch (localStep) {
                case BEFORE_FAIL:
                    try {
                        Thread.sleep(lastExecutionTime / 2);
                    } catch (InterruptedException e) {
                    }
                    step = Step.BEFORE_RECOVER;
                    failureNeeded = false;
                    clusterFailure.fail();
                    break;
                case BEFORE_RECOVER:
                    failureNeeded = true;
                    clusterFailure.recover();
                    step = Step.BEFORE_EXECUTE;
                    break;
            }
        }
        if (failureNeeded) {
            clusterFailure.fail();
        }
    };

    @Before
    public void initFailure() {
        clusterFailure.initialize(INITIAL_CLUSTER_SIZE, SMALL_INSTANCE_CONFIG);
        createMap(clusterFailure.getNotFailingInstance(), COMMON_MAP_NAME, COMMON_MAP_SIZE, IntHolder::new,
                IntHolder.class);
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getSqlConfig().setSqlResubmissionMode(resubmissionMode);
        client = clusterFailure.createClient(clientConfig);
        step = Step.BEFORE_EXECUTE;
        finish = false;
        lastExecutionTime = 0;
    }

    @Test
    public void when_failingSelectBeforeAnyDataIsFetched() throws InterruptedException {
        SqlStatement statement = new SqlStatement("select * from " + COMMON_MAP_NAME);

        Thread failingThread = new Thread(cyclicFailure);
        failingThread.start();
        try {
            if (shouldFailBeforeAnyDataIsFetched(resubmissionMode)) {
                assertThrows(HazelcastSqlException.class, () -> {
                    executeInLoop(statement, result -> false);
                });
                finish = true;
            } else {
                try {
                    executeInLoop(statement, result -> ((SqlClientResult) result).wasResubmission());
                } finally {
                    finish = true;
                }
            }
        } finally {
            failingThread.join();
            clusterFailure.cleanUp();
        }
    }

    private void executeInLoop(SqlStatement statement, Function<SqlResult, Boolean> shouldBreakFunction) {
        while (!finish) {
            Step localStep = step;
            if (localStep == Step.BEFORE_EXECUTE) {
                step = Step.BEFORE_FAIL;
                try {
                    long start = System.nanoTime();
                    SqlResult result = client.getSql().execute(statement);
                    lastExecutionTime = (System.nanoTime() - start) / 1_000_000;
                    if (shouldBreakFunction.apply(result)) {
                        break;
                    }
                } catch (RuntimeException e) {
                    if (e.getMessage() != null && e.getMessage().contains("CREATE MAPPING")) {
                        continue;
                    }
                    throw e;
                }
            }
        }
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
                clusterFailure.fail();
            }
            count++;
        }
        return count;
    }

    enum Step {
        BEFORE_FAIL,
        BEFORE_EXECUTE,
        BEFORE_RECOVER,
    }
}
