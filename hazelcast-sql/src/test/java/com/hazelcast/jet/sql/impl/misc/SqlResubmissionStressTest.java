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
import java.util.UUID;
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

    @Before
    public void initFailure() {
        clusterFailure.initialize(INITIAL_CLUSTER_SIZE, SMALL_INSTANCE_CONFIG);
        createMap(clusterFailure.getNotFailingInstance(), COMMON_MAP_NAME, COMMON_MAP_SIZE, UUID::randomUUID, UUID.class);
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getSqlConfig().setSqlResubmissionMode(resubmissionMode);
        client = clusterFailure.createClient(clientConfig);
    }

    private volatile Step step = Step.BEFORE_EXECUTE;
    private volatile boolean finish = false;
    private volatile long lastExecutionTime = 0;

    @Test
    public void when_failingSelectBeforeAnyDataIsFetched() throws InterruptedException {
        SqlStatement statement = new SqlStatement("select * from " + COMMON_MAP_NAME);
        step = Step.BEFORE_EXECUTE;
        finish = false;

        Thread failingThread = new Thread(() -> {
            Step localStep = null;
            boolean failureNeeded = true;

            while (!finish) {
                localStep = step;
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
        });

        failingThread.start();
        try {
            if (shouldFailBeforeAnyDataIsFetched(resubmissionMode)) {
                assertThrows(HazelcastSqlException.class, () -> {
                    Step localStep = null;
                    while (!finish) {
                        localStep = step;
                        if (localStep == Step.BEFORE_EXECUTE) {
                            step = Step.BEFORE_FAIL;
                            try {
                                long start = System.nanoTime();
                                client.getSql().execute(statement);
                                lastExecutionTime = (System.nanoTime() - start) / 1_000_000;
                            } catch (RuntimeException e) {
                                e.printStackTrace();
                                throw e;
                            }
                        }
                    }
                });
                finish = true;
            } else {
                Step localStep = null;
                while (!finish) {
                    localStep = step;
                    if (localStep == Step.BEFORE_EXECUTE) {
                        step = Step.BEFORE_FAIL;
                        try {
                            long start = System.nanoTime();
                            System.err.println("---->EXECUTE");
                            SqlResult result = client.getSql().execute(statement);
                            lastExecutionTime = (System.nanoTime() - start) / 1_000_000;
                            if (((SqlClientResult) result).wasResubmission()) {
                                break;
                            }
                        } catch (RuntimeException e) {
                            if (e.getMessage() != null && e.getMessage().contains("CREATE MAPPING")) {
                                continue;
                            }
                            finish = true;
                            throw e;
                        }
                    }
                }
                finish = true;
            }
        } finally {
            failingThread.join();
            clusterFailure.cleanUp();
        }
    }

    enum Step {
        BEFORE_FAIL,
        BEFORE_EXECUTE,
        BEFORE_RECOVER,
    }
}
