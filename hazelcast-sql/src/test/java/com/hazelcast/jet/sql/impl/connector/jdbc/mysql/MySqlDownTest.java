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
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.testcontainers.containers.ToxiproxyContainer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.jet.sql.SqlJsonTestSupport.jsonArray;
import static org.assertj.core.api.Assertions.assertThat;

@Category(NightlyTest.class)
public class MySqlDownTest extends JdbcSqlTestSupport {

    private static MySQLDatabaseProvider mySQLDatabaseProvider;

    private ToxiproxyContainer toxiproxy;

    @BeforeClass
    public static void beforeClass() {
        mySQLDatabaseProvider = new MySQLDatabaseProvider();
        initialize(mySQLDatabaseProvider);
    }

    @Before
    public void setUp() throws Exception {
        toxiproxy = new ToxiproxyContainer("ghcr.io/shopify/toxiproxy:2.5.0")
                .withNetwork(mySQLDatabaseProvider.getNetwork());

        toxiproxy.start();
    }

    @After
    public void tearDownProxy() {
        if (toxiproxy != null) {
            toxiproxy.stop();
            toxiproxy = null;
        }
    }

    @Test
    public void showDataConnectionsShouldReturnQuickly() throws Exception {
        createTable("my_table");

        final ToxiproxyClient toxiproxyClient = new ToxiproxyClient(toxiproxy.getHost(), toxiproxy.getControlPort());
        final Proxy proxy = toxiproxyClient.createProxy("mysql", "0.0.0.0:8666", "mysql:3306");

        String host = toxiproxy.getHost();
        Integer port = toxiproxy.getMappedPort(8666);
        String url = dbConnectionUrl.replaceFirst("localhost:\\d+", host + ":" + port);

        sqlService.execute("CREATE DATA CONNECTION mysql TYPE Jdbc OPTIONS ('jdbcUrl'= '" + url + "' )");
        sqlService.execute("CREATE MAPPING my_table DATA CONNECTION mysql");

        proxy.toxics().timeout("timeout", ToxicDirection.UPSTREAM, 0);
        // Couldn't find a better way to ensure the timeout toxi is active
        // In the worst case the toxic won't be active and the test will pass (occasionally)
        Thread.sleep(1000);

        ExecutorService executor = Executors.newSingleThreadExecutor();

        SqlResult result = executor.submit(() -> sqlService.execute("SHOW DATA CONNECTIONS"))
                .get(5, TimeUnit.SECONDS);


        assertThat(allRows(result)).containsExactly(
                new Row("mysql", "jdbc", jsonArray("Table")),
                new Row("testDatabaseRef", "jdbc", jsonArray("Table")));

        result = executor.submit(() -> sqlService.execute("SHOW MAPPINGS"))
                .get(5, TimeUnit.SECONDS);

        assertThat(allRows(result)).containsExactly(new Row("my_table"));
    }
}
