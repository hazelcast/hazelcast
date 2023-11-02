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
import com.hazelcast.core.HazelcastException;
import com.hazelcast.dataconnection.DataConnectionBase;
import com.hazelcast.dataconnection.DataConnectionResource;
import com.hazelcast.jet.impl.util.ConcurrentMemoizingSupplier;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoClientSettings.Builder;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.connection.ConnectionPoolSettings;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static com.hazelcast.internal.util.Preconditions.checkState;
import static com.hazelcast.jet.mongodb.impl.Mappers.defaultCodecRegistry;
import static java.util.Collections.singletonList;

/**
 * Creates a MongoDB DataConnection.
 * <p>
 * According to {@link MongoClient} documentation, the client object is responsible for maintaining connection pool,
 * so this data connection just returns one, cached client.
 *
 * @since 5.3
 */
public class MongoDataConnection extends DataConnectionBase {

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

    /**
     * Name of the property holding username.
     */
    public static final String USERNAME_PROPERTY = "username";

    /**
     * Name of the property holding user password.
     */
    public static final String PASSWORD_PROPERTY = "password";

    /**
     * Name of a property which holds host:port address of the mongodb instance.
     */
    public static final String HOST_PROPERTY = "host";

    /**
     * Name of the property holding the name of the database in which user is created.
     * Default value is {@code admin}.
     */
    public static final String AUTH_DB_PROPERTY = "authDb";

    /**
     * Name of the property holding the minimum size of Mongo Client connection pool.
     * Default is 10.
     * @since 5.4
     */
    public static final String CONNECTION_POOL_MIN = "connectionPoolMinSize";

    /**
     * Name of the property holding the maximum size of Mongo Client connection pool.
     * Default is 10.
     * @since 5.4
     */
    public static final String CONNECTION_POOL_MAX = "connectionPoolMaxSize";

    private volatile ConcurrentMemoizingSupplier<MongoClient> mongoClientSup;
    private final String name;
    private final String connectionString;
    private final String databaseName;
    private final String username;
    private final String password;
    private final String host;
    private final String authDb;
    private final int connectionPoolMinSize;
    private final int connectionPoolMaxSize;

    private final SslConf sslConf;

    /**
     * Creates a new data connection based on given config.
     */
    @SuppressWarnings("DataFlowIssue")
    public MongoDataConnection(DataConnectionConfig config) {
        super(config);
        this.name = config.getName();
        this.connectionString = config.getProperty(CONNECTION_STRING_PROPERTY);
        this.databaseName = config.getProperty(DATABASE_PROPERTY);
        this.username = config.getProperty(USERNAME_PROPERTY);
        this.password = config.getProperty(PASSWORD_PROPERTY);
        this.host = config.getProperty(HOST_PROPERTY);
        this.authDb = config.getProperty(AUTH_DB_PROPERTY, "admin");
        this.connectionPoolMinSize = Integer.parseInt(config.getProperty(CONNECTION_POOL_MIN, "10"));
        this.connectionPoolMaxSize = Integer.parseInt(config.getProperty(CONNECTION_POOL_MAX, "10"));
        this.sslConf = new SslConf(config);

        checkState(connectionPoolMinSize <= connectionPoolMaxSize, "connection pool max size" +
                " cannot be lower than min size");

        checkState(allSame((username == null), (password == null), (host == null)),
        "You have to provide connectionString property or combination of username, password and host");

        if (config.isShared()) {
            this.mongoClientSup = new ConcurrentMemoizingSupplier<>(
                    () -> new CloseableMongoClient(createClient(), this::release));
        }
    }

    static boolean allSame(boolean... booleans) {
        if (booleans.length == 0) {
            return true;
        }
        boolean first = booleans[0];
        for (boolean aBoolean : booleans) {
            if (first != aBoolean) {
                return false;
            }
        }
        return true;
    }

    private MongoClient createClient() {
        try {
            Builder builder = MongoClientSettings.builder()
                                                 .codecRegistry(defaultCodecRegistry())
                                                 .applyToConnectionPoolSettings(this::connectionPoolSettings)
                                                 .applyToSslSettings(sslConf::apply);
            if (connectionString != null) {
                builder.applyConnectionString(new ConnectionString(connectionString));
            } else {
                var serverAddress = new ServerAddress(host);
                var credential = MongoCredential.createCredential(username, authDb, password.toCharArray());
                builder
                        .applyToClusterSettings(s -> s.hosts(singletonList(serverAddress)))
                        .credential(credential);
            }
            return MongoClients.create(builder.build());
        } catch (Exception e) {
            throw new HazelcastException("Unable to create Mongo client for data connection '" + name + "': "
                    + e.getMessage(), e);
        }
    }

    private void connectionPoolSettings(ConnectionPoolSettings.Builder builder) {
        builder.minSize(connectionPoolMinSize)
               .maxSize(connectionPoolMaxSize);
    }

    /**
     * Returns an instance of {@link MongoClient}.
     *
     * If client is {@linkplain DataConnectionConfig#isShared()} and there will be still some usages of given client,
     * the {@linkplain MongoClient#close()} method won't take an effect.
     */
    @Nonnull
    public MongoClient getClient() {
        if (getConfig().isShared()) {
            retain();
            // local copy to protect from nullifying the value between two instructions
            ConcurrentMemoizingSupplier<MongoClient> supplier = mongoClientSup;
            checkState(supplier != null, "Mongo client should not be closed at this point");
            return supplier.get();
        } else {
            MongoClient client = createClient();
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
    public List<DataConnectionResource> listResources() {
        List<DataConnectionResource> resources = new ArrayList<>();
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

    @Nonnull
    @Override
    public Collection<String> resourceTypes() {
        return Arrays.asList("Collection", "ChangeStream");
    }

    private static void addResources(List<DataConnectionResource> resources, MongoDatabase database) {
        for (String collectionName : database.listCollectionNames()) {
            resources.add(new DataConnectionResource("Collection", database.getName(), collectionName));
            resources.add(new DataConnectionResource("ChangeStream", database.getName(), collectionName));
        }
    }

    /**
     * Closes underlying client.
     */
    @Override
    public void destroy() {
        ConcurrentMemoizingSupplier<MongoClient> supplier = mongoClientSup;
        if (supplier != null) {
            mongoClientSup = null;
            MongoClient mongoClient = supplier.remembered();
            if (mongoClient != null) {
                ((CloseableMongoClient) mongoClient).unwrap().close();
            }
        }
    }

    /**
     * Helper method to create new {@link MongoDataConnection} with given name and connection string.
     */
    @Nonnull
    public static DataConnectionConfig mongoDataConnectionConf(String name, String connectionString) {
        DataConnectionConfig dataConnectionConfig = new DataConnectionConfig();
        dataConnectionConfig.setName(name);
        dataConnectionConfig.setShared(true);
        dataConnectionConfig.setProperty(CONNECTION_STRING_PROPERTY, connectionString);
        dataConnectionConfig.setType("Mongo");
        return dataConnectionConfig;
    }
}
