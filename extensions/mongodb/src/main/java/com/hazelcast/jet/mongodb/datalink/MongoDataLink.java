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
public class MongoDataLink extends DataLinkBase {
    /**
     * Name of a property which holds connection string to the mongodb instance.
     */
    public static final String CONNECTION_STRING_PROPERTY = "connectionString";
    private MongoClient mongoClient;

    /**
     * Creates a new data link based on given config.
     */
    public MongoDataLink(DataLinkConfig config) {
        super(config);
        this.mongoClient = new CloseableMongoClient(createClient(config), this::release);
    }

    private MongoClient createClient(DataLinkConfig config) {
        String connectionString = config.getProperty(CONNECTION_STRING_PROPERTY);
        checkNotNull(connectionString, "connectionString property cannot be null");
        return MongoClients.create(connectionString);
    }

    /**
     * Returns an instance of {@link MongoClient}.
     *
     * If client is {@linkplain DataLinkConfig#isShared() shared} and there will be still some usages of given client,
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
     * Lists all MongoDB collections in all databases.
     */
    @Nonnull
    @Override
    public List<DataLinkResource> listResources() {
        List<DataLinkResource> resources = new ArrayList<>();
        MongoClient client = getClient();

        for (String databaseName : client.listDatabaseNames()) {
            MongoDatabase database = client.getDatabase(databaseName);
            for (String collectionName : database.listCollectionNames()) {
                resources.add(new DataLinkResource("collection", databaseName + "." + collectionName));
            }
        }
        return resources;
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
        dataLinkConfig.setClassName(MongoDataLink.class.getName());
        return dataLinkConfig;
    }

}
