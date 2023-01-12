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
import com.hazelcast.sql.SqlStatement;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.concurrent.TimeUnit;

@RunWith(HazelcastParametrizedRunner.class)
@Parameterized.UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({SlowTest.class, ParallelJVMTest.class})
public class SqlResubmissionTimeoutTest extends SqlResubmissionTestSupport {
    private static final int FAILING_MAP_SIZE = 100;

    @Parameterized.Parameter
    public Integer timeout;

    @Parameterized.Parameters(name = "timeout:{0}")
    public static Object[] parameters() {
        return new Object[]{1, 10, 30};
    }

    @BeforeClass
    public static void setUpClass() {
        initialize(1, null);
    }

    @Test
    public void when_totalTimeOfResubmissionEnds_then_fail() {
        createMap(instance(), FAILING_MAP_NAME, FAILING_MAP_SIZE, FailingDuringFieldAccessObject::new,
                FailingDuringFieldAccessObject.class);

        HazelcastInstance client = createClient();

        long startTime = System.currentTimeMillis();
        SqlStatement statement = new SqlStatement("update " + FAILING_MAP_NAME + " set field = field + 1");
        assertThrows(HazelcastSqlException.class, () -> client.getSql().execute(statement));
        long duration = System.currentTimeMillis() - startTime;

        assertGreaterOrEquals("timeout", TimeUnit.MILLISECONDS.toSeconds(duration), timeout);

        client.shutdown();
    }

    private HazelcastInstance createClient() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProperty("hazelcast.client.invocation.timeout.seconds", timeout + "");
        clientConfig.getSqlConfig().setResubmissionMode(ClientSqlResubmissionMode.RETRY_ALL);
        return factory().newHazelcastClient(clientConfig);
    }
}
