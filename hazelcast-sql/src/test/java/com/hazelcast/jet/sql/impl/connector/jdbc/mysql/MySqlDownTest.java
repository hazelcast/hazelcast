/*
 * Copyright 2025 Hazelcast Inc.
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

package com.hazelcast.jet.sql.impl.connector.jdbc.mysql;

import com.hazelcast.jet.sql.impl.connector.jdbc.JdbcSqlTestSupport;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.jdbc.MySQLDatabaseProvider;
import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.jet.TestedVersions.TOXIPROXY_IMAGE;
import static com.hazelcast.jet.sql.SqlJsonTestSupport.jsonArray;
import static org.assertj.core.api.Assertions.assertThat;

@Category(NightlyTest.class)
public class MySqlDownTest extends JdbcSqlTestSupport {

    // Create a common docker network so that containers can communicate
    @Rule
    public Network network = Network.newNetwork();

    @Rule
    public ToxiproxyContainer toxiproxy = new ToxiproxyContainer(TOXIPROXY_IMAGE)
            .withNetwork(network);

    private MySQLDatabaseProvider mySQLDatabaseProvider;

    private final String networkAlias = "mysql";

    @Before
    public void setUp() throws Exception {
        mySQLDatabaseProvider = new MySQLDatabaseProvider();
        mySQLDatabaseProvider.setNetwork(network, networkAlias);
        initialize(mySQLDatabaseProvider);
    }

    @After
    public void tearDownProxy() {
        if (mySQLDatabaseProvider != null) {
            mySQLDatabaseProvider.shutdown();
            mySQLDatabaseProvider = null;
        }
    }

    @Test
    public void showDataConnectionsShouldReturnQuickly() throws Exception {
        createTable("my_table");

        final ToxiproxyClient toxiproxyClient = new ToxiproxyClient(toxiproxy.getHost(), toxiproxy.getControlPort());
        final Proxy proxy = toxiproxyClient.createProxy(networkAlias, "0.0.0.0:8666", "mysql:3306");

        String host = toxiproxy.getHost();
        Integer port = toxiproxy.getMappedPort(8666);
        String url = dbConnectionUrl.replaceFirst("localhost:\\d+", host + ":" + port);

        sqlService.executeUpdate("CREATE DATA CONNECTION mysql TYPE Jdbc OPTIONS ('jdbcUrl'= '" + url + "' )");
        sqlService.executeUpdate("CREATE MAPPING my_table DATA CONNECTION mysql");

        proxy.toxics().timeout("timeout", ToxicDirection.UPSTREAM, 0);
        // Couldn't find a better way to ensure the timeout toxi is active
        // In the worst case the toxic won't be active and the test will pass (occasionally)
        Thread.sleep(1000);

        ExecutorService executor = Executors.newSingleThreadExecutor();

        SqlResult result = executor.submit(() -> sqlService.execute("SHOW DATA CONNECTIONS"))
                .get(5, TimeUnit.SECONDS);


        assertThat(allRows(result)).containsExactly(
                new Row("mysql", "Jdbc", jsonArray("Table")),
                new Row("testDatabaseRef", "Jdbc", jsonArray("Table")));

        result = executor.submit(() -> sqlService.execute("SHOW MAPPINGS"))
                .get(5, TimeUnit.SECONDS);

        assertThat(allRows(result)).containsExactly(new Row("my_table"));
    }
}
