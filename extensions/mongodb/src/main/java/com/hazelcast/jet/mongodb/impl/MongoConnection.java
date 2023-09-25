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
package com.hazelcast.jet.mongodb.impl;

import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.mongodb.dataconnection.MongoDataConnection;
import com.hazelcast.jet.pipeline.DataConnectionRef;
import com.hazelcast.jet.retry.RetryStrategies;
import com.hazelcast.jet.retry.RetryStrategy;
import com.hazelcast.jet.retry.impl.RetryTracker;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.mongodb.MongoClientException;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoSocketException;
import com.mongodb.client.MongoClient;
import org.bson.BsonArray;
import org.bson.BsonString;
import org.bson.BsonValue;

import java.util.function.Consumer;

import java.io.Closeable;

import static com.hazelcast.internal.util.Preconditions.checkState;
import static com.hazelcast.jet.retry.IntervalFunction.exponentialBackoffWithCap;

/**
 * Manages connection to MongoDB, reconnects if necessary.
 */
class MongoConnection implements Closeable {
    @SuppressWarnings("checkstyle:MagicNumber")
    private static final RetryStrategy RETRY_STRATEGY =
            RetryStrategies.custom()
                           .intervalFunction(exponentialBackoffWithCap(100, 2.0, 3000))
                           .maxAttempts(20)
                           .build();

    private SupplierEx<? extends MongoClient> clientSupplier;
    private final DataConnectionRef dataConnectionRef;
    private final Consumer<MongoClient> afterConnection;
    private final RetryTracker connectionRetryTracker;
    private final ILogger logger = Logger.getLogger(MongoConnection.class);

    private MongoClient mongoClient;
    private MongoDataConnection dataConnection;
    private Exception lastException;

    MongoConnection(SupplierEx<? extends MongoClient> clientSupplier, DataConnectionRef dataConnectionRef,
                    Consumer<MongoClient> afterConnection) {
        this.clientSupplier = clientSupplier;
        this.dataConnectionRef = dataConnectionRef;
        this.afterConnection = afterConnection;
        this.connectionRetryTracker = new RetryTracker(RETRY_STRATEGY);
    }

    MongoClient client() {
        checkState(reconnectIfNecessary(), "should be connected");
        return mongoClient;
    }

    /**
     * @return true if there is a connection to Mongo after exiting this method
     */
    boolean reconnectIfNecessary() {
        if (mongoClient != null) {
            return true;
        }
        if (connectionRetryTracker.needsToWait()) {
            return false;
        }
        if (connectionRetryTracker.shouldTryAgain()) {
            try {
                mongoClient = clientSupplier.get();
                afterConnection.accept(mongoClient);
                connectionRetryTracker.reset();

                lastException = null;
                return true;
            } catch (MongoCommandException e) {
                BsonArray codes = codes(e);
                if (codes.contains(new BsonString("NonResumableChangeStreamError"))) {
                    throw new JetException("NonResumableChangeStreamError thrown by Mongo", e);
                }
                connectionRetryTracker.attemptFailed();
                logger.warning("Could not connect to MongoDB." + willRetryMessage(), e);
                lastException = e;
                return false;
            } catch (MongoClientException | MongoSocketException e) {
                lastException = e;

                connectionRetryTracker.attemptFailed();
                logger.warning("Could not connect to MongoDB due to client/socket error."
                        + willRetryMessage(), e);
                return false;
            } catch (Exception e) {
                throw new JetException("Cannot connect to MongoDB, seems to be non-transient error: " + e.getMessage(), e);
            }
        } else {
            throw new JetException("cannot connect to MongoDB", lastException);
        }
    }

    private String willRetryMessage() {
        return connectionRetryTracker.shouldTryAgain()
                ? " Operation will be retried in " + connectionRetryTracker.getNextWaitTimeMs() + "ms."
                : "";
    }

    private BsonArray codes(MongoCommandException e) {
        BsonValue errorLabels = e.getResponse().get("errorLabels");
        if (errorLabels != null && errorLabels.isArray()) {
            return errorLabels.asArray();
        } else {
            return new BsonArray();
        }
    }

    @Override
    public void close() {
        if (mongoClient != null) {
            mongoClient.close();
            mongoClient = null;
        }
        if (dataConnection != null) {
            dataConnection.release();
            dataConnection = null;
        }
    }

    /**
     * Assembles client supplier - if data connection is used, it will set {@link #clientSupplier} to
     * correct supplier based on the data connection.
     */
    public void assembleSupplier(NodeEngineImpl nodeEngine) {
        if (dataConnectionRef != null) {
            dataConnection = nodeEngine.getDataConnectionService().getAndRetainDataConnection(
                    dataConnectionRef.getName(), MongoDataConnection.class);
            clientSupplier = dataConnection::getClient;
        }
    }
}
