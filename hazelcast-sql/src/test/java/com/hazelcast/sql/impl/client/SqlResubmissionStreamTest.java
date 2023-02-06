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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParametrizedRunner.class)
@Parameterized.UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({SlowTest.class, ParallelJVMTest.class})
public class SqlResubmissionStreamTest extends SqlResubmissionTestSupport {
    private static final int INITIAL_CLUSTER_SIZE = 1;
    private static final int VALUES_PER_SECOND = 10000;

    @Parameterized.Parameter
    public ClusterFailureTestSupport.SingleFailingInstanceClusterFailure clusterFailure;

    @Parameterized.Parameter(1)
    public ClientSqlResubmissionMode resubmissionMode;

    private HazelcastInstance client;

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
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getSqlConfig().setResubmissionMode(resubmissionMode);
        client = clusterFailure.createClient(clientConfig);
    }

    @Test(timeout = 10_000)
    public void when_failingSelectAfterSomeDataIsFetched() {
        SqlStatement statement = new SqlStatement("select * from table(generate_stream(" + VALUES_PER_SECOND + "))");
        SqlResult rows = client.getSql().execute(statement);

        try {
            // In this test we expect the rows to be: 0, 1, 2, ... X, 0, 1, 2, ... N.
            boolean resubmitted = false;
            long expectedValue = 0;
            int rowsSeen = 0;
            for (SqlRow r : rows) {
                long rowValue = r.getObject(0);
                if (rowsSeen++ == VALUES_PER_SECOND / 2) {
                    clusterFailure.fail();
                }
                if (expectedValue > 0 && rowValue == 0) {
                    assertFalse("rows restarted from 0 for the 2nd time", resubmitted);
                    resubmitted = true;
                    expectedValue = 0;
                }
                assertEquals(expectedValue, rowValue);
                expectedValue++;
                if (resubmitted) {
                    break;
                }
            }
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
}
