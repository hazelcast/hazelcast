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
package com.hazelcast.jet.mongodb.datalink;

import com.hazelcast.config.DataLinkConfig;
import com.hazelcast.datalink.DataLinkBase;
import com.hazelcast.datalink.DataLinkResource;
import com.hazelcast.spi.annotation.Beta;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.Preconditions.checkState;

/**
 * Creates a MongoDB DataLink.
 *
 * According to {@link MongoClient} documentation, the client object is responsible for maintaining connection pool,
 * so this data link just returns one, cached client.
 *
 * @since 5.3
 */
@Beta
public class MongoDataLink extends DataLinkBase {
    /**
     * Name of a property which holds connection string to the mongodb instance.
     */
    public static final String CONNECTION_STRING_PROPERTY = "connectionString";
    /**
     * Name of a property with a database name hint.
     * This is used as a <strong>hint</strong> only; {@link #listResources} will return only collection from db with this
     * name, but client is not restricted to this database. It can be used to specify database to which SQL Mappings
     * will point.
     */
    public static final String DATABASE_PROPERTY = "database";
    private volatile MongoClient mongoClient;
    private final String databaseName;

    /**
     * Creates a new data link based on given config.
     */
    public MongoDataLink(DataLinkConfig config) {
        super(config);
        if (config.isShared()) {
            this.mongoClient = new CloseableMongoClient(createClient(config), this::release);
        }
        this.databaseName = config.getProperty(DATABASE_PROPERTY);
    }

    private MongoClient createClient(DataLinkConfig config) {
        String connectionString = config.getProperty(CONNECTION_STRING_PROPERTY);
        checkNotNull(connectionString, "connectionString property cannot be null");
        return MongoClients.create(connectionString);
    }

    /**
     * Returns an instance of {@link MongoClient}.
     *
     * If client is {@linkplain DataLinkConfig#isShared()}  shared} and there will be still some usages of given client,
     * the {@linkplain MongoClient#close()} method won't take an effect.
     */
    @Nonnull
    public MongoClient getClient() {
        if (getConfig().isShared()) {
            retain();
            checkState(mongoClient != null, "Mongo client should not be closed at this point");
            return mongoClient;
        } else {
            MongoClient client = createClient(getConfig());
            return new CloseableMongoClient(client, client::close);
        }
    }

    /**
     * Returns the database name hint.
     */
    public String getDatabaseName() {
        return databaseName;
    }

    /**
     * Lists all MongoDB collections in all databases.
     */
    @Nonnull
    @Override
    public List<DataLinkResource> listResources() {
        List<DataLinkResource> resources = new ArrayList<>();
        try (MongoClient client = getClient()) {
            if (databaseName != null) {
                MongoDatabase mongoDatabase = client.getDatabase(databaseName);
                addResources(resources, mongoDatabase);
            } else {
                for (String databaseName : client.listDatabaseNames()) {
                    MongoDatabase database = client.getDatabase(databaseName);
                    addResources(resources, database);
                }
            }
        }
        return resources;
    }

    private static void addResources(List<DataLinkResource> resources, MongoDatabase database) {
        for (String collectionName : database.listCollectionNames()) {
            resources.add(new DataLinkResource("collection", database.getName() + "." + collectionName));
        }
    }

    /**
     * Closes underlying client.
     */
    @Override
    public void destroy() {
        if (mongoClient != null) {
            ((CloseableMongoClient) mongoClient).unwrap().close();
            mongoClient = null;
        }
    }

    /**
     * Helper method to create new {@link MongoDataLink} with given name and connection string.
     */
    @Nonnull
    public static DataLinkConfig mongoDataLinkConf(String name, String connectionString) {
        DataLinkConfig dataLinkConfig = new DataLinkConfig();
        dataLinkConfig.setName(name);
        dataLinkConfig.setShared(true);
        dataLinkConfig.setProperty(CONNECTION_STRING_PROPERTY, connectionString);
        dataLinkConfig.setType("MongoDB");
        return dataLinkConfig;
    }
}
