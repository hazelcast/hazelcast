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
package com.hazelcast.jet.mongodb.dataconnection;

import com.hazelcast.config.DataConnectionConfig;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.test.IgnoreInJenkinsOnWindows;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.util.ArrayList;

import static com.hazelcast.jet.mongodb.AbstractMongoTest.TEST_MONGO_VERSION;
import static com.hazelcast.jet.mongodb.impl.Mappers.defaultCodecRegistry;
import static com.hazelcast.test.DockerTestUtil.assumeDockerEnabled;
import static java.util.Collections.singletonList;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class, IgnoreInJenkinsOnWindows.class})
public class MongoDataConnectionAuthTest extends SimpleTestInClusterSupport {

    private static final String USERNAME = "user123";
    private static final String PASSWORD = "password123";
    private static final String DATABASE = "MongoDataConnectionAuthTest";
    private static final String TEST_COLLECTION = "testCollection";
    private static final Logger LOGGER = LoggerFactory.getLogger(MongoDataConnectionAuthTest.class);
    private static final GenericContainer<?> mongoContainer = new GenericContainer<>("mongo:" + TEST_MONGO_VERSION)
            .withEnv("MONGO_INITDB_ROOT_USERNAME", USERNAME)
            .withEnv("MONGO_INITDB_ROOT_PASSWORD", PASSWORD)
            .withEnv("MONGO_INITDB_DATABASE", DATABASE)
            .withExposedPorts(27017)
            .withLogConsumer(new Slf4jLogConsumer(LOGGER))
            .waitingFor(Wait.forLogMessage("(?i).*Waiting for connections*.*", 1));

    @BeforeClass
    public static void setUp() {
        assumeDockerEnabled();
        mongoContainer.start();
    }

    private static DataConnectionConfig getDataConnectionConfig() {
        return new DataConnectionConfig("mongoDB")
                .setType("MongoDB")
                .setName("mongoDB")
                .setProperty("database", DATABASE)
                .setProperty("host", mongoContainer.getHost() + ":" + mongoContainer.getMappedPort(27017))
                .setProperty("username", USERNAME)
                .setProperty("password", PASSWORD)
                .setShared(true);
    }

    @AfterClass
    public static void clear() {
        if (mongoContainer != null) {
            mongoContainer.stop();
        }
    }

    @Test
    public void should_connect_with_credentials() {
        // given / when
        try (MongoClient mongo = mongoClient()) {
            mongo.getDatabase(DATABASE).createCollection(TEST_COLLECTION);
        }
        MongoDataConnection dataConnection = new MongoDataConnection(getDataConnectionConfig());
        MongoClient client = dataConnection.getClient();

        // then
        client.getDatabase(DATABASE).getCollection(TEST_COLLECTION).find().into(new ArrayList<>());
        // no exception should be thrown
        dataConnection.destroy();
    }

    private MongoClient mongoClient() {
        MongoCredential credential = MongoCredential.createCredential(USERNAME, "admin", PASSWORD.toCharArray());
        ServerAddress address = new ServerAddress(mongoContainer.getHost() + ":" + mongoContainer.getMappedPort(27017));
        return MongoClients.create(MongoClientSettings.builder()
                                                      .codecRegistry(defaultCodecRegistry())
                                                      .applyToClusterSettings(s -> s.hosts(singletonList(
                                                              address)))
                                                      .credential(credential)
                                                      .build());
    }

}
