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

package com.hazelcast.mapstore;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.ExternalDataStoreConfig;
import com.hazelcast.datastore.JdbcDataStoreFactory;
import com.hazelcast.internal.util.FilteringClassLoader;
import com.hazelcast.jet.sql.impl.connector.jdbc.JdbcSqlTestSupport;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.jdbc.PostgresDatabaseProvider;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.assertj.core.util.Lists.newArrayList;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public class GenericMapStorePostgreSQLUpsertTest extends GenericMapStoreIntegrationTest {

    // Shadow the parent's @BeforeClass method by using the same method name
    @BeforeClass
    public static void beforeClass() {
        databaseProvider = new PostgresDatabaseProvider();
        dbConnectionUrl = databaseProvider.createDatabase(JdbcSqlTestSupport.class.getName());

        Config config = smallInstanceConfig();
        // Need to set filtering class loader so the members don't deserialize into class but into GenericRecord
        config.setClassLoader(new FilteringClassLoader(newArrayList("org.example"), null));

        config.addExternalDataStoreConfig(
                new ExternalDataStoreConfig(TEST_DATABASE_REF)
                        .setClassName(JdbcDataStoreFactory.class.getName())
                        .setProperty("jdbcUrl", dbConnectionUrl)
        );


        ClientConfig clientConfig = new ClientConfig();

        initializeWithClient(2, config, clientConfig);
        sqlService = instance().getSql();
    }
}
