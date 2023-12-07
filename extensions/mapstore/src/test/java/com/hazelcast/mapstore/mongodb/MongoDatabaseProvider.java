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
package com.hazelcast.mapstore.mongodb;

import com.hazelcast.test.jdbc.TestDatabaseProvider;
import com.hazelcast.test.jdbc.TestDatabaseRecordProvider;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import org.testcontainers.containers.MongoDBContainer;

import java.util.Properties;

import static com.hazelcast.internal.nio.IOUtil.closeResource;
import static com.hazelcast.jet.mongodb.impl.Mappers.defaultCodecRegistry;

public class MongoDatabaseProvider implements TestDatabaseProvider {

    static final String TEST_MONGO_VERSION = System.getProperty("test.mongo.version", "7.0.2");

    private MongoDBContainer mongoContainer;
    private MongoClient mongoClient;
    private MongoDatabase database;

    @Override
    public String createDatabase(String dbName) {
        mongoContainer  = new MongoDBContainer("mongo:" + TEST_MONGO_VERSION);
        mongoContainer.start();
        String connectionString = mongoContainer.getConnectionString();
        mongoClient = MongoClients.create(connectionString);
        database = mongoClient.getDatabase(dbName).withCodecRegistry(defaultCodecRegistry());
        return connectionString;
    }

    @Override
    public String url() {
        return mongoContainer.getConnectionString();
    }

    @Override
    public Properties properties() {
        Properties props = new Properties();
        props.setProperty("connectionString", mongoContainer.getConnectionString());
        props.setProperty("database", database.getName());
        return props;
    }

    @Override
    public String dataConnectionType() {
        return "Mongo";
    }

    MongoDatabase database() {
        return database;
    }

    @Override
    public void shutdown() {
        closeResource(mongoClient);
        closeResource(mongoContainer);
    }

    @Override
    public TestDatabaseRecordProvider recordProvider() {
        return new MongoObjectProvider(this);
    }
}
