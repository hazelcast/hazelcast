/*
 * Copyright 2023 Hazelcast Inc.
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

package com.hazelcast.sql.impl.client;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientSqlResubmissionMode;
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
import java.util.function.Predicate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParametrizedRunner.class)
@Parameterized.UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({SlowTest.class, ParallelJVMTest.class})
public class SqlResubmissionTest extends SqlResubmissionTestSupport {
    private static final int INITIAL_CLUSTER_SIZE = 1;

    @Parameterized.Parameter
    public ClusterFailureTestSupport.SingleFailingInstanceClusterFailure clusterFailure;

    @Parameterized.Parameter(1)
    public ClientSqlResubmissionMode resubmissionMode;

    private HazelcastInstance client;

    private volatile State state;
    private volatile boolean done;

    /**
     * Elapsed time from the test start until the last execution, in millis.
     */
    private volatile long lastExecutionTime;

    @Parameterized.Parameters(name = "clusterFailure:{0}, mode:{1}")
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
        clusterFailure.initialize(INITIAL_CLUSTER_SIZE, smallInstanceConfig());
        createMap(clusterFailure.getNotFailingInstance(), COMMON_MAP_NAME, COMMON_MAP_SIZE, IntHolder::new,
                IntHolder.class);
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getSqlConfig().setResubmissionMode(resubmissionMode);

        client = clusterFailure.createClient(clientConfig);
        state = State.BEFORE_EXECUTE;
        done = false;
        lastExecutionTime = 0;
    }

    @Test
    public void when_failingSelectBeforeAnyDataIsFetched() throws InterruptedException {
        SqlStatement statement = new SqlStatement("select * from " + COMMON_MAP_NAME);
        testStatement(statement, shouldFailBeforeAnyDataIsFetched(resubmissionMode));
    }

    @Test
    public void when_failingUpdate() throws InterruptedException {
        SqlStatement statement = new SqlStatement("update " + COMMON_MAP_NAME + " set field = 1");
        testStatement(statement, shouldFailNonSelectQuery(resubmissionMode));
    }

    private void testStatement(SqlStatement statement, boolean shouldFail)
            throws InterruptedException {
        Thread failureControlThread = new Thread(cyclicFailure);
        failureControlThread.start();
        try {
            if (shouldFail) {
                assertThrows(HazelcastSqlException.class, () -> executeInLoop(statement, result -> false));
            } else {
                executeInLoop(statement, result -> ((SqlClientResult) result).wasResubmission());
            }
        } finally {
            done = true;
            failureControlThread.join();
            clusterFailure.cleanUp();
        }
    }

    private void executeInLoop(SqlStatement statement, Predicate<SqlResult> shouldBreakFunction) {
        while (true) {
            State localState = state;
            if (localState == State.BEFORE_EXECUTE) {
                state = State.BEFORE_FAIL;
                try {
                    long start = System.nanoTime();
                    SqlResult result = client.getSql().execute(statement);
                    lastExecutionTime = (System.nanoTime() - start) / 1_000_000;
                    if (shouldBreakFunction.test(result)) {
                        break;
                    }
                } catch (RuntimeException e) {
                    // Can be removed after merging https://github.com/hazelcast/hazelcast/pull/21639 - migration of
                    // SQL catalog to IMap.
                    if (e.getMessage() != null && e.getMessage().contains("CREATE MAPPING")) {
                        continue;
                    }
                    throw e;
                }
            }
        }
    }

    private final Runnable cyclicFailure = () -> {
        boolean failAfterDone = true;

        while (!done) {
            State localState = state;
            switch (localState) {
                case BEFORE_FAIL:
                    try {
                        Thread.sleep(lastExecutionTime / 2);
                    } catch (InterruptedException e) {
                        // ignored
                    }
                    state = State.BEFORE_RECOVER;
                    failAfterDone = false;
                    clusterFailure.fail();
                    break;
                case BEFORE_RECOVER:
                    failAfterDone = true;
                    clusterFailure.recover();
                    state = State.BEFORE_EXECUTE;
                    break;
            }
        }
        if (failAfterDone) {
            clusterFailure.fail();
        }
    };

    @Test
    public void when_failingSelectAfterSomeDataIsFetched() {
        SqlStatement statement = new SqlStatement("select * from " + COMMON_MAP_NAME + " ORDER BY __key");
        statement.setCursorBufferSize(1);
        SqlResult rows = client.getSql().execute(statement);

        try {
            // In this test we expect the rows to be: 0, 1, 2, ... X, 0, 1, 2, ... N.
            // X is the row after which the query was resubmitted.
            // N is total number of rows.
            // The code below asserts this. We don't know the value X, but we know the value N.
            boolean resubmitted = false;
            int expectedValue = 0;
            int rowsSeen = 0;
            for (SqlRow r : rows) {
                int rowValue = r.getObject("__key");
                if (rowsSeen++ == COMMON_MAP_SIZE  / 2) {
                    clusterFailure.fail();
                }
                if (expectedValue > 0 && rowValue == 0) {
                    assertFalse("rows restarted from 0 for the 2nd time", resubmitted);
                    resubmitted = true;
                    expectedValue = 0;
                }
                assertEquals(expectedValue, rowValue);
                expectedValue++;
            }
            assertEquals(COMMON_MAP_SIZE, expectedValue);
            assertTrue("resubmission didn't happen", resubmitted);
        } catch (HazelcastSqlException e) {
            // This may be expected (for example: when resubmissionMode == NEVER), so we need to check if in current
            // resubmissionMode an exception should be thrown.
           if (!shouldFailAfterSomeDataIsFetched(resubmissionMode)) {
               throw e;
           } // else the error is expected
        } finally {
            clusterFailure.cleanUp();
        }
    }

    enum State {
        BEFORE_FAIL,
        BEFORE_EXECUTE,
        BEFORE_RECOVER,
    }
}
