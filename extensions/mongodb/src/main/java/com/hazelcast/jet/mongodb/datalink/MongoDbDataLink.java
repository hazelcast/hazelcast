/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.jet.mongodb.datalink;

import com.hazelcast.config.DataLinkConfig;
import com.hazelcast.datalink.DataLink;
import com.hazelcast.datalink.ReferenceCounter;
import com.hazelcast.datalink.Resource;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;

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
        this.refCounter = new ReferenceCounter(() -> {
            destroy();
            return null;
        });

        String connectionString = config.getProperty(CONNECTION_STRING_PROPERTY);
        checkNotNull(connectionString, "connectionString property cannot be null");
        this.mongoClient = new CloseableMongoClient(MongoClients.create(connectionString), refCounter::release);
    }

    public MongoClient getClient() {
        refCounter.retain();
        return mongoClient;
    }

    @Override
    public String getName() {
        return config.getName();
    }

    @Override
    public List<Resource> listResources() {
        List<Resource> resources = new ArrayList<>();
        for (String databaseName : mongoClient.listDatabaseNames()) {
            MongoDatabase database = mongoClient.getDatabase(databaseName);
            for (String collectionName : database.listCollectionNames()) {
                resources.add(new Resource("collection", collectionName));
            }
        }
        return resources;
    }

    @Override
    public DataLinkConfig getConfig() {
        return config;
    }

    @Override
    public void retain() {
        refCounter.retain();
    }

    @Override
    public void close() throws Exception {
        refCounter.release();
    }

    private void destroy() {
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
