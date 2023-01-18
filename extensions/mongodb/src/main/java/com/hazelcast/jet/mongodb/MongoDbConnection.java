package com.hazelcast.jet.mongodb;

import com.hazelcast.function.ConsumerEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.retry.RetryStrategies;
import com.hazelcast.jet.retry.RetryStrategy;
import com.hazelcast.jet.retry.impl.RetryTracker;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.mongodb.client.MongoClient;

import static com.hazelcast.jet.retry.IntervalFunction.exponentialBackoffWithCap;

/***
 * Manages connection to MongoDB, reconnects if necessary.
 */
class MongoDbConnection {
    @SuppressWarnings("checkstyle:MagicNumber")
    private static final RetryStrategy RETRY_STRATEGY = RetryStrategies.custom()
                                                                       .intervalFunction(exponentialBackoffWithCap(100
                                                                               , 2.0, 3000))
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
        boolean connected;
        do {
            connected = reconnectIfNecessary();
        } while (!connected);
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
