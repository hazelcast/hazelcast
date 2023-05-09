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

package com.hazelcast.jet.sql.impl.connector.jdbc;

import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.jdbc.H2DatabaseProvider;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assumptions.assumeThat;

@RunWith(HazelcastParametrizedRunner.class)
public class CreateDataConnectionJdbcSqlConnectorTest extends JdbcSqlTestSupport {

    @Parameterized.Parameter
    public String value;

    @Parameterized.Parameters(name = "value={0}")
    public static List<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {"NOT SHARED"},
                {"SHARED"}
        });
    }

    @BeforeClass
    public static void beforeClass() {
        initialize(new H2DatabaseProvider());
    }

    @Test
    public void createDataConnection() {

        String connectionName = randomName();
        String sql = String.format("CREATE DATA CONNECTION %s TYPE JDBC %s OPTIONS('jdbcUrl'='%s')",
                connectionName, value, dbConnectionUrl);

        assertThatCode(() -> execute(sql))
                .doesNotThrowAnyException();

        assertThatCode(() -> execute("SHOW RESOURCES FOR " + connectionName))
                .doesNotThrowAnyException();
    }

    @Test
    public void createDataConnectionWithPasswordInProperties() {
        assumeThat(databaseProvider).isNotInstanceOfAny(
                H2DatabaseProvider.class // H2 doesn't allow passing user and password in properties
        );

        String connectionName = randomName();
        String sql = String.format(
                "CREATE DATA CONNECTION %s TYPE JDBC %s "
                        + "OPTIONS('jdbcUrl'='%s', 'user'='%s', 'password'='%s')",
                connectionName, value,
                databaseProvider.noAuthJdbcUrl(), databaseProvider.user(), databaseProvider.password()
        );

        assertThatCode(() -> execute(sql))
                .doesNotThrowAnyException();

        assertThatCode(() -> execute("SHOW RESOURCES FOR " + connectionName))
                .doesNotThrowAnyException();
    }
}
