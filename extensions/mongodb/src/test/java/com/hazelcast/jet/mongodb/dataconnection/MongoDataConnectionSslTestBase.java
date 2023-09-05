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

import com.github.dockerjava.api.command.InspectContainerResponse;
import com.hazelcast.config.DataConnectionConfig;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.test.IgnoreInJenkinsOnWindows;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import javax.annotation.Nonnull;
import javax.net.ssl.SSLContext;
import java.net.URL;
import java.util.ArrayList;

import static com.hazelcast.jet.mongodb.AbstractMongoTest.TEST_MONGO_VERSION;
import static com.hazelcast.jet.mongodb.impl.Mappers.defaultCodecRegistry;
import static com.hazelcast.test.DockerTestUtil.assumeDockerEnabled;
import static org.testcontainers.containers.BindMode.READ_WRITE;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class, IgnoreInJenkinsOnWindows.class})
public abstract class MongoDataConnectionSslTestBase extends SimpleTestInClusterSupport {

    protected static final String DATABASE = "MongoDataConnectionSslTest";
    protected static final String TEST_COLLECTION = "testCollection";
    protected static final Logger LOGGER = LoggerFactory.getLogger(MongoDataConnectionSslTestBase.class);
    protected static String connectionString;
    protected static String trustStoreLocation;
    protected static String keyStoreLocation;

    @BeforeClass
    public static void setUp() {
        URL resourceTS = MongoDataConnectionSslTestBase.class.getResource("/certs/ca.p12");
        URL resourceKS = MongoDataConnectionSslTestBase.class.getResource("/certs/localhost.p12");
        trustStoreLocation = resourceTS.getFile();
        keyStoreLocation = resourceKS.getFile();
        assumeDockerEnabled();
    }

    @Test
    public void should_connect_with_ssl() {
        // given / when
        try (MongoDBContainer mongoContainer = new MyMongoContainer()
                .withEnv("MONGO_INITDB_DATABASE", DATABASE)
                .withClasspathResourceMapping("certs/localhost.pem", "/data/ssl/key.pem", READ_WRITE)
                .withClasspathResourceMapping("certs/ca-cert.pem", "/data/ssl/ca.pem", READ_WRITE)
                .withClasspathResourceMapping(getConfigurationFile(), "/etc/mongo/mongod.conf", READ_WRITE)
                .withExposedPorts(27017)
                .withLogConsumer(new Slf4jLogConsumer(LOGGER))
                .withCommand("--config", "/etc/mongo/mongod.conf")
        ) {
            mongoContainer.start();
            connectionString = "mongodb://" + mongoContainer.getHost() + ":" + mongoContainer.getMappedPort(27017)
                    + "/";

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
    }

    protected abstract String getConfigurationFile();

    protected abstract DataConnectionConfig getDataConnectionConfig();

    @Nonnull
    protected abstract SSLContext getSslContext();

    private MongoClient mongoClient() {
        return MongoClients.create(MongoClientSettings
                .builder()
                .applyConnectionString(new ConnectionString(connectionString))
                .applyToSslSettings(b -> {
                    b.enabled(true).invalidHostNameAllowed(true);
                    b.context(getSslContext());
                })
                .codecRegistry(defaultCodecRegistry())
                .build());
    }
    private static class MyMongoContainer extends MongoDBContainer {
        MyMongoContainer() {
            super("mongo:" + TEST_MONGO_VERSION);
        }
        @Override
        public void configure() {
        }

        @Override
        protected void containerIsStarted(InspectContainerResponse containerInfo, boolean reused) {
        }
    }

}
