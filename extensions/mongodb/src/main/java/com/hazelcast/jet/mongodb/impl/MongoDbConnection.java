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

import com.hazelcast.function.ConsumerEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.retry.RetryStrategies;
import com.hazelcast.jet.retry.RetryStrategy;
import com.hazelcast.jet.retry.impl.RetryTracker;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.mongodb.client.MongoClient;

import static com.hazelcast.internal.util.Preconditions.checkState;
import static com.hazelcast.jet.retry.IntervalFunction.exponentialBackoffWithCap;

/***
 * Manages connection to MongoDB, reconnects if necessary.
 */
class MongoDbConnection {
    @SuppressWarnings("checkstyle:MagicNumber")
    private static final RetryStrategy RETRY_STRATEGY =
            RetryStrategies.custom()
                           .intervalFunction(exponentialBackoffWithCap(100, 2.0, 3000))
                           .maxAttempts(20)
                           .build();

    private final SupplierEx<? extends MongoClient> clientSupplier;
    private final ConsumerEx<MongoClient> afterConnection;
    private final RetryTracker connectionRetryTracker;
    private final ILogger logger = Logger.getLogger(MongoDbConnection.class);

    private MongoClient mongoClient;

    MongoDbConnection(SupplierEx<? extends MongoClient> clientSupplier, ConsumerEx<MongoClient> afterConnection) {
        this.clientSupplier = clientSupplier;
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
        if (mongoClient == null) {
            if (connectionRetryTracker.shouldTryAgain()) {
                try {
                    mongoClient = clientSupplier.get();
                    afterConnection.accept(mongoClient);
                    connectionRetryTracker.reset();
                    return true;
                } catch (Exception e) {
                    logger.warning("Could not connect to MongoDB", e);
                    connectionRetryTracker.attemptFailed();
                    return false;
                }
            } else {
                throw new JetException("cannot connect to MongoDB");
            }
        } else {
            return true;
        }
    }

    void close() {
        if (mongoClient != null) {
            mongoClient.close();
            mongoClient = null;
        }
    }
}
