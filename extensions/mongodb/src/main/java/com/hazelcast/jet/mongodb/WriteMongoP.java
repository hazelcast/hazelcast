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

import com.hazelcast.function.ConsumerEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.impl.processor.TwoPhaseSnapshotCommitUtility;
import com.hazelcast.jet.impl.processor.TwoPhaseSnapshotCommitUtility.TransactionalResource;
import com.hazelcast.jet.impl.processor.UnboundedTransactionsProcessorUtility;
import com.hazelcast.jet.retry.RetryStrategies;
import com.hazelcast.jet.retry.RetryStrategy;
import com.hazelcast.jet.retry.impl.RetryTracker;
import com.hazelcast.logging.ILogger;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.WriteModel;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static com.hazelcast.jet.Util.idToString;
import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.impl.util.LoggingUtil.logFine;
import static com.hazelcast.jet.mongodb.Mappers.toBsonDocument;
import static com.hazelcast.jet.retry.IntervalFunction.exponentialBackoffWithCap;
import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Aggregates.project;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Projections.include;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;

/**
 * Processor for writing to MongoDB
 *
 * TODO add more info
 * @param <I> type of saved item
 */
public class WriteMongoP<I> extends AbstractProcessor {
    @SuppressWarnings("checkstyle:MagicNumber")
    private static final RetryStrategy RETRY_STRATEGY = RetryStrategies.custom()
                                                                      .intervalFunction(exponentialBackoffWithCap(100
                                                                              , 2.0, 3000))
                                                                      .maxAttempts(10)
                                                                      .build();
    private final SupplierEx<? extends MongoClient> connectionSupplier;
    private final Class<I> documentType;
    private MongoClient mongoClient;
    private final RetryTracker connectionRetryTracker;
    private ILogger logger;

    private int processorIndex;
    private UnboundedTransactionsProcessorUtility<MongoTransactionId, MongoTransaction> snapshotUtility;

    private final CollectionPicker<I> collectionPicker;

    private final FunctionEx<I, Object> documentIdentityFn;
    private final String documentIdentityFieldName;

    private final ReplaceOptions replaceOptions;

    private final Map<MongoTransactionId, MongoTransaction> activeTransactions = new HashMap<>();

    /**
     * Creates a new processor that will always insert to the same database and collection.
     */
    public WriteMongoP(
            SupplierEx<? extends MongoClient> connectionSupplier,
            String databaseName,
            String collectionName,
            Class<I> documentType,
            FunctionEx<I, Object> documentIdentityFn,
            ConsumerEx<ReplaceOptions> replaceOptionAdjuster,
            String documentIdentityFieldName) {
        this.connectionSupplier = connectionSupplier;
        this.documentType = documentType;
        this.documentIdentityFn = documentIdentityFn;
        this.documentIdentityFieldName = documentIdentityFieldName;

        ReplaceOptions options = new ReplaceOptions().upsert(true);
        if (replaceOptionAdjuster != null) {
            replaceOptionAdjuster.accept(options);
        }
        this.replaceOptions = options;
        this.connectionRetryTracker = new RetryTracker(RETRY_STRATEGY);

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
            FunctionEx<I, Object> documentIdentityFn,
            ConsumerEx<ReplaceOptions> replaceOptionAdjuster,
            String documentIdentityFieldName
    ) {
        this.connectionSupplier = connectionSupplier;
        this.documentType = documentType;
        this.documentIdentityFn = documentIdentityFn;
        this.documentIdentityFieldName = documentIdentityFieldName;
        ReplaceOptions options = new ReplaceOptions().upsert(true);
        if (replaceOptionAdjuster != null) {
            replaceOptionAdjuster.accept(options);
        }
        this.replaceOptions = options;
        this.connectionRetryTracker = new RetryTracker(RETRY_STRATEGY);

        collectionPicker = new FunctionalCollectionPicker(databaseNameSelectFn, collectionNameSelectFn);
    }

    @Override
    protected void init(@Nonnull Context context) {
        logger = context.logger();
        processorIndex = context.globalProcessorIndex();

        snapshotUtility = new UnboundedTransactionsProcessorUtility<>(
                getOutbox(),
                context,
                context.processingGuarantee(),
                () -> new MongoTransactionId(context, processorIndex),
                txId -> new MongoTransaction(txId, logger),
                txId -> {
                    MongoTransaction mongoTransaction = activeTransactions.get(txId);
                    refreshTransaction(mongoTransaction, true);
                },
                () -> {
                    for (MongoTransaction tx : activeTransactions.values()) {
                        refreshTransaction(tx, false);

                    }
                }
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

    private static final class MongoCollectionKey {
        private final String collectionName;
        private final MongoCollection<BsonDocument> collection;

        private MongoCollectionKey(MongoCollection<BsonDocument> collection) {
            this.collectionName = collection.getNamespace().getCollectionName();
            this.collection = collection;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            MongoCollectionKey that = (MongoCollectionKey) o;
            return Objects.equals(collectionName, that.collectionName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(collectionName);
        }

        @Override
        public String toString() {
            return collectionName;
        }
    }

    @Override
    public void process(int ordinal, @Nonnull Inbox inbox) {
        if (!reconnectIfNecessary()) {
            return;
        }
        MongoTransaction mongoTransaction = snapshotUtility.activeTransaction();

        ArrayList<I> items = new ArrayList<>();
        inbox.drainTo(items);

        @SuppressWarnings("DataFlowIssue")
        Map<MongoCollectionKey, List<I>> itemsPerCollection = items
                .stream()
                .map(e -> {
                    MongoCollection<BsonDocument> col = collectionPicker.pick(e);
                    return tuple2(new MongoCollectionKey(col), e);
                })
                .collect(groupingBy(Tuple2::f0, mapping(Tuple2::getValue, toList())));

        for (MongoCollectionKey collectionKey : itemsPerCollection.keySet()) {
            MongoCollection<BsonDocument> collection = collectionKey.collection;

            Set<Object> docsFound = queryForExistingIds(items, collection);
            List<WriteModel<BsonDocument>> writes = new ArrayList<>();

            for (I item : itemsPerCollection.get(collectionKey)) {
                Object id = documentIdentityFn.apply(item);

                if (docsFound.contains(id)) {
                    Bson filter = eq(documentIdentityFieldName, id);
                    ReplaceOneModel<BsonDocument> update =
                            new ReplaceOneModel<>(filter, toBsonDocument(item), replaceOptions);
                    writes.add(update);
                } else {
                    InsertOneModel<BsonDocument> insert = new InsertOneModel<>(toBsonDocument(item));
                    writes.add(insert);
                }
            }
            if (snapshotUtility.usesTransactionLifecycle()) {
                collection.bulkWrite(mongoTransaction.clientSession, writes);
            } else {
                collection.bulkWrite(writes);
            }
        }


    }

    private Set<Object> queryForExistingIds(ArrayList<I> items, MongoCollection<?> collection) {
        List<Object> identityValues = items.stream()
                                           .map(documentIdentityFn)
                                           .filter(Objects::nonNull)
                                           .collect(toList());
        Iterable<Document> foundIds = collection.aggregate(
                asList(project(include("_id")), match(in("_id", identityValues))),
                Document.class);
        Set<Object> docsFound = new HashSet<>();
        for (Document document : foundIds) {
            docsFound.add(document.get("_id"));
        }
        return docsFound;
    }

    @Override
    public void close() {
        if (mongoClient != null) {
            mongoClient.close();
        }
        snapshotUtility.close();
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    @Override
    public boolean snapshotCommitPrepare() {
        return snapshotUtility.snapshotCommitPrepare();
    }

    @Override
    public boolean snapshotCommitFinish(boolean success) {
        return snapshotUtility.snapshotCommitFinish(success);
    }

    @Override
    public boolean complete() {
        snapshotUtility.afterCompleted();
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

    private static class MongoTransactionId implements TwoPhaseSnapshotCommitUtility.TransactionId, Serializable {

        private static final long serialVersionUID = 1L;

        private final int processorIndex;
        private final String mongoId;
        private final int hashCode;

        MongoTransactionId(long jobId, String jobName, @Nonnull String vertexId, int processorIndex) {
            this.processorIndex = processorIndex;

            mongoId = "jet.job-" + idToString(jobId) + '.' + sanitize(jobName) + '.' + sanitize(vertexId) + '.'
                    + processorIndex;
            hashCode = Objects.hash(jobId, vertexId, processorIndex);
        }


        MongoTransactionId(Context context, int processorIndex) {
            this(context.jobId(), context.jobConfig().getName(), context.vertexName(), processorIndex);
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

    private void refreshTransaction(MongoTransaction transaction, boolean commit) {
        if (!snapshotUtility.usesTransactionLifecycle()) {
            return;
        }
        if (transaction == null) {
            return;
        }
        if (commit) {
            transaction.commit();
        } else {
            transaction.rollback();
        }
    }

    private final class MongoTransaction implements TransactionalResource<MongoTransactionId> {
        private ClientSession clientSession;
        private final ILogger logger;
        private final MongoTransactionId transactionId;
        private boolean txnInitialized;

        private MongoTransaction(
                MongoTransactionId transactionId,
                ILogger logger) {
            this.transactionId = transactionId;
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
//            refreshTransaction(true);
            clientSession = mongoClient.startSession();
            TransactionOptions options = TransactionOptions.builder()
                                                           .writeConcern(WriteConcern.MAJORITY).build();
            clientSession.startTransaction(options);
            activeTransactions.put(transactionId, this);
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
            if (clientSession != null) {
                clientSession.close();
            }
            activeTransactions.remove(transactionId);
        }
    }

    private interface CollectionPicker<I> {
        MongoCollection<BsonDocument> pick(I item);

        void refreshOnReconnect(MongoClient client);
    }

    private final class ConstantCollectionPicker implements CollectionPicker<I> {

        private final String databaseName;
        private final String collectionName;
        private MongoCollection<BsonDocument> collection;

        private ConstantCollectionPicker(String databaseName, String collectionName) {
            this.databaseName = databaseName;
            this.collectionName = collectionName;
        }


        @Override
        public MongoCollection<BsonDocument> pick(I item) {
            return collection;
        }

        @Override
        public void refreshOnReconnect(MongoClient client) {
            MongoDatabase database = client.getDatabase(databaseName);
            collection = database.getCollection(collectionName, BsonDocument.class);
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
        public MongoCollection<BsonDocument> pick(I item) {
            String databaseName = databaseNameSelectFn.apply(item);
            String collectionName = collectionNameSelectFn.apply(item);

            MongoDatabase database = mongoClient.getDatabase(databaseName);
            return database.getCollection(collectionName, BsonDocument.class);
        }

        @Override
        public void refreshOnReconnect(MongoClient client) {
            mongoClient = client;
        }
    }

}
