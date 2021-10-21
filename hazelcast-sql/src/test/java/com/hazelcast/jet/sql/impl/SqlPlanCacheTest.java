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

package com.hazelcast.jet.sql.impl;

import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.jet.sql.impl.connector.test.TestBatchSqlConnector;
import org.junit.BeforeClass;
import org.junit.Test;

import static java.util.Arrays.stream;
import static org.assertj.core.api.Assertions.assertThat;

public class SqlPlanCacheTest extends SqlTestSupport {

    @BeforeClass
    public static void setUpClass() {
        initialize(2, null);

        // effectively disable periodic plan cache validation
        stream(instances()).forEach(instance -> nodeEngine(instance)
                .getSqlService()
                .getInternalService()
                .getStateRegistryUpdater()
                .setStateCheckFrequency(Long.MAX_VALUE)
        );
    }

    @Test
    public void test_distributedInvalidation() {
        TestBatchSqlConnector.create(instance().getSql(), "t", 1);

        instances()[0].getSql().execute("SELECT * FROM t");
        assertThat(planCache(instances()[0]).size()).isEqualTo(1);

        instances()[1].getSql().execute("SELECT * FROM t");
        assertThat(planCache(instances()[1]).size()).isEqualTo(1);

        instances()[0].getSql().execute("DROP MAPPING t");
        assertThat(planCache(instances()[0]).size()).isEqualTo(0);
        // ReplicatedMap listeners are executed asynchronously
        assertTrueEventually(() -> assertThat(planCache(instances()[1]).size()).isEqualTo(0));
    }
}
