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

package com.hazelcast.jet.sql;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HazelcastParametrizedRunner.class)
@Parameterized.UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlTestSupportTest extends SqlTestSupport {

    @Parameterized.Parameter
    public boolean useClient;

    @Parameterized.Parameters(name = "useClient={0}")
    public static Collection<Object> data() {
        return asList(false, true);
    }

    @Before
    public void before() {
        if (useClient) {
            initializeWithClient(1, null, null);
        } else {
            initialize(1, null);
        }
    }

    @After
    public void after() throws Exception {
        tearDown();
        // these tests check cleanup in SimpleTestInClusterSupport
        // to check that in isolated way we use initialize/initializeWithClient per test method
        // unlike standard invocation per class.
        supportAfter();
        supportAfterClass();
    }

    private HazelcastInstance hz() {
        return useClient ? client() : instance();
    }

    @Test
    public void mappingShouldNotBeVisibleAfterFirstRestart() {
        // given
        createMapping();

        // when
        supportAfter();

        // then
        mappingDoesNotExist();
    }

    @Test
    public void mappingShouldNotBeVisibleAfterSecondRestart() {
        // given
        createMapping();
        supportAfter();
        createMapping();

        // when
        supportAfter();

        // then
        mappingDoesNotExist();
    }

    private void createMapping() {
        String sqlCreateMapping = String.format(
                "CREATE MAPPING mymap (__key BIGINT, this VARCHAR) "
                        + "TYPE IMap OPTIONS('keyFormat' = 'bigint', 'valueFormat' = 'varchar')");
        hz().getSql().execute(sqlCreateMapping);
    }

    private void mappingDoesNotExist() {
        assertThatThrownBy(() -> hz().getSql().execute("select * from mymap"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageContaining("Object 'mymap' not found, did you forget to CREATE MAPPING?");
    }
}
