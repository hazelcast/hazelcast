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
import com.hazelcast.datalink.DataLink;
import com.hazelcast.datalink.DataLinkResource;
import com.hazelcast.datalink.ReferenceCounter;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * Creates a MongoDB DataLink.
 *
 * According to {@link MongoClient} documentation, the client object is responsible for maintaining connection pool,
 * so this data link just returns one, cached client.
 *
 * @since 5.3
 */
public class MongoDbDataLink implements DataLink {
    /**
     * Name of a property which holds connection string to the mongodb instance.
     */
    public static final String CONNECTION_STRING_PROPERTY = "connectionString";
    protected final DataLinkConfig config;
    protected final ReferenceCounter refCounter;
    private MongoClient mongoClient;

    public MongoDbDataLink(DataLinkConfig config) {
        this.config = config;
        this.refCounter = new ReferenceCounter(this::destroy);

        String connectionString = config.getProperty(CONNECTION_STRING_PROPERTY);
        checkNotNull(connectionString, "connectionString property cannot be null");
        this.mongoClient = new CloseableMongoClient(MongoClients.create(connectionString), refCounter::release);
    }

    /**
     * Returns an instance of {@link MongoClient}.
     *
     * {@linkplain MongoClient#close()} won't take an effect if there will be still some usages of given client.
     */
    public MongoClient getClient() {
        refCounter.retain();
        return mongoClient;
    }

    /**
     * Returns name of this data link.
     */
    @Nonnull
    @Override
    public String getName() {
        return config.getName();
    }

    /**
     * Lists all MongoDB collections in all databases.
     */
    @Nonnull
    @Override
    public List<DataLinkResource> listResources() {
        List<DataLinkResource> resources = new ArrayList<>();
        for (String databaseName : mongoClient.listDatabaseNames()) {
            MongoDatabase database = mongoClient.getDatabase(databaseName);
            for (String collectionName : database.listCollectionNames()) {
                resources.add(new DataLinkResource("collection", collectionName));
            }
        }
        return resources;
    }

    /**
     * Returns configuration of this data link.
     */
    @Nonnull
    @Override
    public DataLinkConfig getConfig() {
        return config;
    }

    @Override
    public void retain() {
        refCounter.retain();
    }

    @Override
    public void release() {
        refCounter.release();

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

    public static DataLinkConfig mongoDataLinkConf(String name, String connectionString) {
        DataLinkConfig dataLinkConfig = new DataLinkConfig();
        dataLinkConfig.setName(name);
        dataLinkConfig.setShared(true);
        dataLinkConfig.setProperty(CONNECTION_STRING_PROPERTY, connectionString);
        dataLinkConfig.setClassName(MongoDbDataLink.class.getName());
        return dataLinkConfig;
    }

}
