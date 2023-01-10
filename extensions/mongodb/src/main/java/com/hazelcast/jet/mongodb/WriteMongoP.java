/*
 * Copyright 2021 Hazelcast Inc.
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
package com.hazelcast.jet.mongodb;

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.impl.processor.TransactionPoolSnapshotUtility;
import com.hazelcast.jet.impl.processor.TwoPhaseSnapshotCommitUtility;
import com.hazelcast.jet.impl.processor.TwoPhaseSnapshotCommitUtility.TransactionalResource;
import com.hazelcast.jet.retry.RetryStrategies;
import com.hazelcast.jet.retry.impl.RetryTracker;
import com.hazelcast.logging.ILogger;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Updates;
import org.bson.BsonDocument;
import org.bson.conversions.Bson;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.Objects;

import static com.hazelcast.jet.Util.idToString;
import static com.hazelcast.jet.impl.util.LoggingUtil.logFine;

/**
 * Processor for writing to MongoDB
 *
 * TODO add more info
 * @param <I> type of saved item
 */
public class WriteMongoP<I> extends AbstractProcessor {

    private static final long RETRY_INTERVAL = 3_000;
    private static final int BATCH_SIZE = 1000;
    private final SupplierEx<? extends MongoClient> connectionSupplier;
    private final Class<I> documentType;
    private MongoClient mongoClient;
    private ClientSession clientSession;
    private final RetryTracker connectionRetryTracker;
    private ILogger logger;

    private int processorIndex;
    private TransactionPoolSnapshotUtility<MongoTransactionId, MongoTransaction> snapshotUtility;

    private final CollectionPicker<I> collectionPicker;

    private final BiFunctionEx<I, BsonDocument, Bson> documentIdentityFn;

    /**
     * Creates a new processor that will always insert to the same database and collection.
     */
    public WriteMongoP(
            SupplierEx<? extends MongoClient> connectionSupplier,
            String databaseName,
            String collectionName,
            Class<I> documentType,
            BiFunctionEx<I, BsonDocument, Bson> documentIdentityFn) {
        this.connectionSupplier = connectionSupplier;
        this.documentType = documentType;
        this.documentIdentityFn = documentIdentityFn;
        this.connectionRetryTracker = new RetryTracker(RetryStrategies.indefinitely(RETRY_INTERVAL));

        collectionPicker = new ConstantCollectionPicker(databaseName, collectionName);
    }

    /**
     * Creates a new processor that will choose database and collection for each item.
     */
    public WriteMongoP(
            SupplierEx<? extends MongoClient> connectionSupplier,
            FunctionEx<I, String> databaseNameSelectFn,
            FunctionEx<I, String> collectionNameSelectFn,
            Class<I> documentType,
            BiFunctionEx<I, BsonDocument, Bson> documentIdentityFn) {
        this.connectionSupplier = connectionSupplier;
        this.documentType = documentType;
        this.documentIdentityFn = documentIdentityFn;
        this.connectionRetryTracker = new RetryTracker(RetryStrategies.indefinitely(RETRY_INTERVAL));

        collectionPicker = new FunctionalCollectionPicker(databaseNameSelectFn, collectionNameSelectFn);
    }

    @Override
    protected void init(@Nonnull Context context) {
        logger = context.logger();
        processorIndex = context.globalProcessorIndex();

        snapshotUtility = new TransactionPoolSnapshotUtility<>(
                getOutbox(),
                context,
                false,
                context.processingGuarantee(),
                2,
                (procIndex, txnIndex) -> new MongoTransactionId(context, processorIndex, txnIndex),
                txId -> new MongoTransaction(txId, connectionSupplier.get(), logger),
                txId -> clientSession.commitTransaction(),
                txId -> clientSession.abortTransaction()
        );
    }

    @Override
    public boolean tryProcessWatermark(@Nonnull Watermark watermark) {
        return true;
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) {
        return snapshotUtility.tryProcess();
    }

    @Override
    @SuppressWarnings("unchecked")
    public void process(int ordinal, @Nonnull Inbox inbox) {
        if (!reconnectIfNecessary() || snapshotUtility.activeTransaction() == null) {
            return;
        }
        for (Object untypedItem : inbox) {
            I item = (I) untypedItem;

            MongoCollection<I> collection = collectionPicker.pick(item);
            BsonDocument asBsonDoc = Mappers.toBsonDocument(item);

            Bson idFilter = documentIdentityFn.apply(item, asBsonDoc);
            Bson[] updates = asBsonDoc.entrySet().stream()
                                      .filter(e -> "_id".equalsIgnoreCase(e.getKey()))
                                      .map(e -> Updates.set(e.getKey(), e.getValue()))
                                      .toArray(Bson[]::new);


            if (snapshotUtility.usesTransactionLifecycle()) {
                collection.updateMany(clientSession, idFilter.toBsonDocument(), Updates.combine(updates));
            } else {
                collection.updateMany(idFilter.toBsonDocument(), Updates.combine(updates));
            }
        }
    }

    @Override
    public void close() {
        if (mongoClient != null) {
            mongoClient.close();
        }
        snapshotUtility.close();
    }

    @Override
    public boolean snapshotCommitPrepare() {
        return snapshotUtility.snapshotCommitPrepare();
    }

    @Override
    public boolean snapshotCommitFinish(boolean success) {
        snapshotUtility.snapshotCommitFinish(success);
        return true;
    }

    @Override
    protected void restoreFromSnapshot(@Nonnull Object key, @Nonnull Object value) {
        snapshotUtility.restoreFromSnapshot(key, value);
    }

    boolean reconnectIfNecessary() {
        if (!isConnectionUp()) {
            if (connectionRetryTracker.shouldTryAgain()) {
                try {
                    mongoClient = connectionSupplier.get();
                    collectionPicker.refreshOnReconnect(mongoClient);
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

    private boolean isConnectionUp() {
        return mongoClient != null;
    }


    public static class MongoTransactionId implements TwoPhaseSnapshotCommitUtility.TransactionId, Serializable {

        private static final long serialVersionUID = 1L;

        private final int processorIndex;
        private final String mongoId;
        private final int hashCode;

        MongoTransactionId(long jobId, String jobName, @Nonnull String vertexId, int processorIndex,
                           int transactionIndex) {
            this.processorIndex = processorIndex;

            mongoId = "jet.job-" + idToString(jobId) + '.' + sanitize(jobName) + '.' + sanitize(vertexId) + '.'
                    + processorIndex + "-" + transactionIndex;
            hashCode = Objects.hash(jobId, vertexId, processorIndex);
        }


        MongoTransactionId(Context context, int processorIndex,
                           int transactionIndex) {
            this(context.jobId(), context.jobConfig().getName(), context.vertexName(), processorIndex, transactionIndex);
        }

        @Override
        public int index() {
            return processorIndex;
        }

        @Override
        public String toString() {
            return "MongoTransactionId{" +
                    "processorIndex=" + processorIndex +
                    ", mongoId='" + mongoId + '\'' +
                    ", hashCode=" + hashCode +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            MongoTransactionId that = (MongoTransactionId) o;
            return processorIndex == that.processorIndex
                    && hashCode == that.hashCode
                    && Objects.equals(mongoId, that.mongoId);
        }

        @Override
        public int hashCode() {
            return hashCode;
        }

        private static String sanitize(String s) {
            if (s == null) {
                return "";
            }
            return s.replaceAll("[^\\p{Alnum}.\\-_$#/{}\\[\\]]", "_");
        }
    }

    private final class MongoTransaction implements TransactionalResource<MongoTransactionId> {
        private final MongoClient mongoClient;
        private final ILogger logger;
        private final MongoTransactionId transactionId;
        private boolean txnInitialized;

        private MongoTransaction(
                MongoTransactionId transactionId,
                MongoClient mongoClient,
                ILogger logger) {
            this.transactionId = transactionId;
            this.mongoClient = mongoClient;
            this.logger = logger;
        }

        @Override
        public MongoTransactionId id() {
            return transactionId;
        }

        @Override
        public void begin() {
            if (!txnInitialized) {
                logFine(logger, "beginning transaction %s", transactionId);
                txnInitialized = true;
            }
            clientSession = mongoClient.startSession();
            clientSession.startTransaction();
        }

        @Override
        public void commit() {
            if (transactionId != null) {
                clientSession.commitTransaction();
            }
        }

        @Override
        public void rollback() {
            if (transactionId != null) {
                clientSession.abortTransaction();
            }
        }

        @Override
        public void release() {
            clientSession.close();
        }
    }

    private interface CollectionPicker<I> {
        MongoCollection<I> pick(I item);

        void refreshOnReconnect(MongoClient client);
    }

    private final class ConstantCollectionPicker implements CollectionPicker<I> {

        private final String databaseName;
        private final String collectionName;
        private MongoCollection<I> collection;

        private ConstantCollectionPicker(String databaseName, String collectionName) {
            this.databaseName = databaseName;
            this.collectionName = collectionName;
        }


        @Override
        public MongoCollection<I> pick(I item) {
            return collection;
        }

        @Override
        public void refreshOnReconnect(MongoClient client) {
            MongoDatabase database = client.getDatabase(databaseName);
            collection = database.getCollection(collectionName, documentType);
        }
    }

    private final class FunctionalCollectionPicker implements CollectionPicker<I> {

        private final FunctionEx<I, String> databaseNameSelectFn;
        private final FunctionEx<I, String> collectionNameSelectFn;
        private MongoClient mongoClient;

        private FunctionalCollectionPicker(FunctionEx<I, String> databaseNameSelectFn,
                                           FunctionEx<I, String> collectionNameSelectFn) {
            this.databaseNameSelectFn = databaseNameSelectFn;
            this.collectionNameSelectFn = collectionNameSelectFn;
        }


        @Override
        public MongoCollection<I> pick(I item) {
            String databaseName = databaseNameSelectFn.apply(item);
            String collectionName = collectionNameSelectFn.apply(item);

            MongoDatabase database = mongoClient.getDatabase(databaseName);
            return database.getCollection(collectionName, documentType);
        }

        @Override
        public void refreshOnReconnect(MongoClient client) {
            mongoClient = client;
        }
    }

}
