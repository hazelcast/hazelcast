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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;

import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static com.hazelcast.jet.mongodb.AbstractMongoTest.TEST_MONGO_VERSION;
import static com.hazelcast.jet.mongodb.impl.Mappers.defaultCodecRegistry;
import static com.hazelcast.test.DockerTestUtil.assumeDockerEnabled;
import static org.testcontainers.containers.BindMode.READ_WRITE;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class, IgnoreInJenkinsOnWindows.class})
public class MongoDataConnectionSslTest extends SimpleTestInClusterSupport {

    private static final String DATABASE = "MongoDataConnectionSslTest";
    private static final String TEST_COLLECTION = "testCollection";
    private static final Logger LOGGER = LoggerFactory.getLogger(MongoDataConnectionSslTest.class);
    private static final MongoDBContainer mongoContainer = new MyMongoContainer()
            .withEnv("MONGO_INITDB_DATABASE", DATABASE)
            .withClasspathResourceMapping("certs/localhost.pem", "/data/ssl/key.pem", READ_WRITE)
            .withClasspathResourceMapping("certs/ca-cert.pem", "/data/ssl/ca.pem", READ_WRITE)
            .withClasspathResourceMapping("tlsMongo.yaml", "/etc/mongo/mongod.conf", READ_WRITE)
            .withExposedPorts(27017)
            .withLogConsumer(new Slf4jLogConsumer(LOGGER))
            .withCommand("--config", "/etc/mongo/mongod.conf"
//                    , "--tlsAllowInvalidCertificates", "--tlsAllowInvalidHostnames"
                    );
    private static String connectionString;

    @BeforeClass
    public static void setUp() {
        try (
                InputStream resource = MongoDataConnectionSslTest.class.getResourceAsStream("/certs/localhost.p12");
                InputStream resourceTS = MongoDataConnectionSslTest.class.getResourceAsStream("/certs/ca.p12")
        ) {
            File tempFile = File.createTempFile("MongoDataConnectionSslTest", "jks");
            File tempFileTS = File.createTempFile("MongoDataConnectionSslTest", "jks");
            Files.copy(resource, tempFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
            Files.copy(resourceTS, tempFileTS.toPath(), StandardCopyOption.REPLACE_EXISTING);

            System.setProperty("https.protocols", "TLSv1,TLSv1.1,TLSv1.2,TLSv1.3");
            System.setProperty("javax.net.debug", "ssl:handshake");
            System.setProperty("javax.net.ssl.trustStore", tempFileTS.toString());
//            System.setProperty("javax.net.ssl.trustStoreType", "pkcs12");
            System.setProperty("javax.net.ssl.trustStorePassword", "123456");
//            System.setProperty("javax.net.ssl.keyStore", tempFile.toString());
//            System.setProperty("javax.net.ssl.keyStoreType", "pkcs12");
//            System.setProperty("javax.net.ssl.keyStorePassword", "123456");
        } catch (IOException e) {
            throw rethrow(e);
        }
        assumeDockerEnabled();
        mongoContainer.start();
        connectionString = "mongodb://" + mongoContainer.getHost() + ":" + mongoContainer.getMappedPort(27017)
        + "/";
    }

    private static DataConnectionConfig getDataConnectionConfig() {
        return new DataConnectionConfig("mongoDB")
                .setType("Mongo")
                .setName("mongoDB")
                .setProperty("database", DATABASE)
                .setProperty("enableSsl", "true")
                .setProperty("invalidHostNameAllowed", "true")
                .setProperty("connectionString", connectionString)
                .setShared(true);
    }

    @AfterClass
    public static void clear() {
//        if (mongoContainer != null) {
//            mongoContainer.stop();
//        }
    }

    @Test
    public void should_connect_with_ssl() {
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
        return MongoClients.create(MongoClientSettings
                .builder()
                .applyConnectionString(new ConnectionString(connectionString))
                .applyToSslSettings(b -> b.enabled(true).invalidHostNameAllowed(true))
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
