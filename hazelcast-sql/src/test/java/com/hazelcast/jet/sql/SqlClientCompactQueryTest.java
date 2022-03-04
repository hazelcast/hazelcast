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

package com.hazelcast.jet.sql;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.FilteringClassLoader;
import com.hazelcast.jet.sql.impl.connector.map.IMapSqlConnector;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import example.serialization.EmployeeDTO;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.List;

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.COMPACT_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_COMPACT_TYPE_NAME;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FORMAT;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that the member can handle queries on Compact when it does not have the necessary classes on the server
 */
@RunWith(HazelcastParametrizedRunner.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlClientCompactQueryTest extends HazelcastTestSupport {
    @Parameterized.Parameter
    public InMemoryFormat inMemoryFormat;

    @Parameterized.Parameter(1)
    public boolean clusterHaveUserClasses;

    public TestHazelcastFactory factory = new TestHazelcastFactory();

    @Parameterized.Parameters(name = "inMemoryFormat:{0}, clusterHaveUserClasses:{1}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {InMemoryFormat.BINARY, true},
                {InMemoryFormat.BINARY, false},
                {InMemoryFormat.OBJECT, true},
                {InMemoryFormat.OBJECT, false}
        });
    }

    @Before
    public void setup() {
        MapConfig mapConfig = new MapConfig("default").setInMemoryFormat(inMemoryFormat);
        Config config = smallInstanceConfig().addMapConfig(mapConfig);
        config.getSerializationConfig().getCompactSerializationConfig().setEnabled(true);
        if (!clusterHaveUserClasses) {
            List<String> excludes = singletonList("example.serialization");
            FilteringClassLoader classLoader = new FilteringClassLoader(excludes, null);
            config.setClassLoader(classLoader);
        }
        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);
    }

    @After
    public void cleanup() {
        factory.terminateAll();
    }

    @Test
    public void testQueryOnPrimitive() {
        HazelcastInstance client = factory.newHazelcastClient(clientConfig());
        IMap<Integer, Object> map = client.getMap("test");
        for (int i = 0; i < 10; i++) {
            map.put(i, new EmployeeDTO(i, i));
        }

        client.getSql().execute("CREATE MAPPING " + "test" + '('
                + "__key INTEGER"
                + ", age INTEGER"
                + ", \"rank\" INTEGER"
                + ", id BIGINT"
                + ", isHired BOOLEAN"
                + ", isFired BOOLEAN"
                + ") TYPE " + IMapSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ("
                + '\'' + OPTION_KEY_FORMAT + "'='" + "int" + '\''
                + ", '" + OPTION_VALUE_FORMAT + "'='" + COMPACT_FORMAT + '\''
                + ", '" + OPTION_VALUE_COMPACT_TYPE_NAME + "'='" + EmployeeDTO.class.getName() + '\''
                + ")");

        SqlResult result = client.getSql().execute("SELECT * FROM test WHERE age >= 5");

        assertThat(result).hasSize(5);
    }

    @Test
    public void testQueryOnPrimitive_selectValue() {
        HazelcastInstance client = factory.newHazelcastClient(clientConfig());
        IMap<Integer, Object> map = client.getMap("test");
        for (int i = 0; i < 10; i++) {
            map.put(i, new EmployeeDTO(i, i));
        }

        client.getSql().execute("CREATE MAPPING " + "test" + '('
                + "__key INTEGER"
                + ", age INTEGER"
                + ", \"rank\" INTEGER"
                + ", id BIGINT"
                + ", isHired BOOLEAN"
                + ", isFired BOOLEAN"
                + ") TYPE " + IMapSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ("
                + '\'' + OPTION_KEY_FORMAT + "'='int'"
                + ", '" + OPTION_VALUE_FORMAT + "'='" + COMPACT_FORMAT + '\''
                + ", '" + OPTION_VALUE_COMPACT_TYPE_NAME + "'='" + EmployeeDTO.class.getName() + '\''
                + ")");

        SqlResult result = client.getSql().execute("SELECT this FROM test WHERE age >= 5");

        assertThat(result).hasSize(5);
    }

    private static ClientConfig clientConfig() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getSerializationConfig().getCompactSerializationConfig().setEnabled(true);
        return clientConfig;
    }
}
