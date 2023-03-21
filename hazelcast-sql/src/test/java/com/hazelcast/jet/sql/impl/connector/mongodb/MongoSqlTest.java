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
package com.hazelcast.jet.sql.impl.connector.mongodb;

import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.sql.SqlService;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.testcontainers.containers.MongoDBContainer;

import static com.hazelcast.test.DockerTestUtil.assumeDockerEnabled;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public abstract class MongoSqlTest extends SqlTestSupport {
    private static final String TEST_MONGO_VERSION = System.getProperty("test.mongo.version", "6.0.3");

    public static final MongoDBContainer mongoContainer
            = new MongoDBContainer("mongo:" + TEST_MONGO_VERSION);

    protected static SqlService sqlService;
    protected static MongoClient mongoClient;
    protected static MongoDatabase database;
    protected static String databaseName;
    protected static String collectionName;

    @Rule
    public final TestName testName = new TestName();

    @BeforeClass
    public static void beforeClass() throws InterruptedException {
        assumeDockerEnabled();
        mongoContainer.start();

        initialize(2, null);
        sqlService = instance().getSql();
        mongoClient = MongoClients.create(mongoContainer.getConnectionString());
        databaseName = randomName();
        database = mongoClient.getDatabase(databaseName);
    }

    @AfterClass
    public static void close() {
        if (mongoClient != null) {
            mongoClient.close();
        }
        if (mongoContainer != null) {
            mongoContainer.stop();
        }
    }

    @Before
    public void setup() {
        collectionName = randomName();
    }

    protected void execute(String sql, Object... arguments) {
        sqlService.execute(sql, arguments).close();
    }

    protected static String options() {
        return String.format("OPTIONS ( 'database' = '%s', 'connectionString' = '%s', 'idColumn' = 'id') ", databaseName,
                mongoContainer.getConnectionString());
    }

}
