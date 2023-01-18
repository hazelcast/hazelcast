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
import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoSocketException;
import com.mongodb.ReadPreference;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.WriteModel;
import org.bson.Document;
import org.bson.conversions.Bson;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;

import static com.hazelcast.internal.util.EmptyStatement.ignore;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.jet.Util.idToString;
import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.jet.impl.util.LoggingUtil.logFine;
import static com.hazelcast.jet.mongodb.Mappers.defaultCodecRegistry;
import static com.hazelcast.jet.retry.IntervalFunction.exponentialBackoffWithCap;
import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Aggregates.project;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Projections.include;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;

/**
 * Processor for writing to MongoDB
 *
 * For each chunk of items, processor will choose proper collection and write ReplaceOne or InsertOne operation,
 * depending on the existence of a document with given {@link #documentIdentityFieldName} in the collection.
 *
 * All writes are postponed until transaction commit. This is on purpose, so that - in case of any
 * MongoDB's WriteError - we will be able to retry the commit.
 *
 * Transactional guarantees are provided using {@linkplain UnboundedTransactionsProcessorUtility}.
 *
 * @param <I> type of saved item
 */
public class WriteMongoP<I> extends AbstractProcessor {
    /**
     * Max number of items processed (written) in one invocation of {@linkplain #process}.
     */
    private static final int MAX_BATCH_SIZE = 2_000;
    @SuppressWarnings("checkstyle:MagicNumber")
    private static final RetryStrategy RETRY_STRATEGY = RetryStrategies.custom()
                                                                      .intervalFunction(exponentialBackoffWithCap(100
                                                                              , 2.0, 3000))
                                                                      .maxAttempts(20)
                                                                      .build();

    private static final long MAX_COMMIT_TIME = 10L;
    private static final TransactionOptions TRANSACTION_OPTIONS = TransactionOptions
            .builder()
            .writeConcern(WriteConcern.MAJORITY)
            .maxCommitTime(MAX_COMMIT_TIME, MINUTES)
            .readPreference(ReadPreference.primaryPreferred())
            .build();
    /**
     * Error code used by MongoDB when there is Transient Error during transaction commit - which means we can
     * try to re-process such transaction.
     */
    private static final int MONGODB_TRANSIENT_ERROR = 112;
    private final MongoDbConnection connection;
    private final Class<I> documentType;
    private ILogger logger;

    private UnboundedTransactionsProcessorUtility<MongoTransactionId, MongoTransaction> transactionUtility;

    private final CollectionPicker<I> collectionPicker;

    private final FunctionEx<I, Object> documentIdentityFn;
    private final String documentIdentityFieldName;

    private final ReplaceOptions replaceOptions;

    private final Map<MongoTransactionId, MongoTransaction> activeTransactions = new HashMap<>();

    /**
     * Creates a new processor that will always insert to the same database and collection.
     */
    public WriteMongoP(
            SupplierEx<? extends MongoClient> clientSupplier,
            String databaseName,
            String collectionName,
            Class<I> documentType,
            FunctionEx<I, Object> documentIdentityFn,
            ConsumerEx<ReplaceOptions> replaceOptionAdjuster,
            String documentIdentityFieldName) {
        this(clientSupplier, new ConstantCollectionPicker<>(databaseName, collectionName),
                documentType, documentIdentityFn, replaceOptionAdjuster, documentIdentityFieldName);
    }

    /**
     * Creates a new processor that will choose database and collection for each item.
     */
    public WriteMongoP(
            SupplierEx<? extends MongoClient> clientSupplier,
            FunctionEx<I, String> databaseNameSelectFn,
            FunctionEx<I, String> collectionNameSelectFn,
            Class<I> documentType,
            FunctionEx<I, Object> documentIdentityFn,
            ConsumerEx<ReplaceOptions> replaceOptionAdjuster,
            String documentIdentityFieldName
    ) {
       this(clientSupplier, new FunctionalCollectionPicker<>(databaseNameSelectFn, collectionNameSelectFn),
               documentType, documentIdentityFn, replaceOptionAdjuster, documentIdentityFieldName);
    }

    /**
     * Creates a new processor that will always insert to the same database and collection.
     */
    public WriteMongoP(
            SupplierEx<? extends MongoClient> clientSupplier,
            CollectionPicker<I> picker,
            Class<I> documentType,
            FunctionEx<I, Object> documentIdentityFn,
            ConsumerEx<ReplaceOptions> replaceOptionAdjuster,
            String documentIdentityFieldName) {
        this.connection = new MongoDbConnection(clientSupplier, client -> {
        });
        this.documentIdentityFn = documentIdentityFn;
        this.documentIdentityFieldName = documentIdentityFieldName;
        this.documentType = documentType;
        this.collectionPicker = picker;

        ReplaceOptions options = new ReplaceOptions().upsert(true);
        if (replaceOptionAdjuster != null) {
            replaceOptionAdjuster.accept(options);
        }
        this.replaceOptions = options;
    }

    @Override
    protected void init(@Nonnull Context context) {
        logger = context.logger();

        int processorIndex = context.globalProcessorIndex();
        transactionUtility = new UnboundedTransactionsProcessorUtility<>(
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
        return transactionUtility.tryProcess();
    }

    @Override
    @SuppressWarnings("unchecked")
    public void process(int ordinal, @Nonnull Inbox inbox) {
        if (!connection.reconnectIfNecessary()) {
            return;
        }
        MongoTransaction mongoTransaction = transactionUtility.activeTransaction();

        ArrayList<I> items = new ArrayList<>();
        for (Object item : inbox) {
            items.add((I) item);
            if (items.size() >= MAX_BATCH_SIZE) {
                break;
            }
        }

        try {
            @SuppressWarnings("DataFlowIssue")
            Map<MongoCollectionKey, List<I>> itemsPerCollection = items
                    .stream()
                    .map(e -> tuple2(collectionPicker.pick(e), e))
                    .collect(groupingBy(Tuple2::f0, mapping(Tuple2::getValue, toList())));

            for (MongoCollectionKey collectionKey : itemsPerCollection.keySet()) {
                MongoCollection<I> collection = collectionKey.get(connection.client(), documentType);

                Set<Object> docsFound = queryForExistingIds(items, collection);
                List<WriteModel<I>> writes = new ArrayList<>();

                for (I item : itemsPerCollection.get(collectionKey)) {
                    Object id = documentIdentityFn.apply(item);

                    WriteModel<I> write;
                    if (docsFound.contains(id)) {
                        Bson filter = eq(documentIdentityFieldName, id);
                        write = new ReplaceOneModel<>(filter, item, replaceOptions);
                    } else {
                        write = new InsertOneModel<>(item);
                    }
                    writes.add(write);
                }
                if (transactionUtility.usesTransactionLifecycle()) {
                    checkNotNull(mongoTransaction, "there is no active transaction");
                    mongoTransaction.addWrites(collectionKey, writes);
                } else {
                    collection.bulkWrite(writes);
                }
            }

            for (int i = 0; i < items.size(); i++) {
                inbox.remove();
            }
        } catch (Exception e) {
            logger.info("Unable to process Mongo Sink: " + e.getMessage());
            // not removing from inbox, so it will be retried
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
        connection.close();
        transactionUtility.close();
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    @Override
    public boolean snapshotCommitPrepare() {
        return transactionUtility.snapshotCommitPrepare();
    }

    @Override
    public boolean snapshotCommitFinish(boolean success) {
        return transactionUtility.snapshotCommitFinish(success);
    }

    @Override
    public boolean complete() {
        transactionUtility.afterCompleted();
        return true;
    }

    @Override
    protected void restoreFromSnapshot(@Nonnull Object key, @Nonnull Object value) {
        transactionUtility.restoreFromSnapshot(key, value);
    }

    private static class MongoTransactionId implements TwoPhaseSnapshotCommitUtility.TransactionId, Serializable {

        private static final long serialVersionUID = 1L;

        private final int processorIndex;
        private final String mongoId;
        private final int hashCode;

        MongoTransactionId(long jobId, String jobName, @Nonnull String vertexId, int processorIndex) {
            this.processorIndex = processorIndex;

            String mongoId = "jet.job-" + idToString(jobId) + '.' + sanitize(jobName) + '.' + sanitize(vertexId) + '.'
                    + processorIndex;

            this.mongoId = mongoId;
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
        if (!transactionUtility.usesTransactionLifecycle()) {
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
        private final RetryTracker commitRetryTracker;

        private final Map<MongoCollectionKey, List<WriteModel<I>>> documents = new HashMap<>();

        private MongoTransaction(
                MongoTransactionId transactionId,
                ILogger logger) {
            this.transactionId = transactionId;
            this.logger = logger;
            this.commitRetryTracker = new RetryTracker(RETRY_STRATEGY);
        }

        @Override
        public MongoTransactionId id() {
            return transactionId;
        }

        void addWrites(MongoCollectionKey key, List<WriteModel<I>> writes) {
            documents.computeIfAbsent(key, k -> new ArrayList<>()).addAll(writes);
        }

        @Override
        @SuppressWarnings("resource")
        public void begin() {
            if (!txnInitialized) {
                logFine(logger, "beginning transaction %s", transactionId);
                txnInitialized = true;
            }
            clientSession = connection.client().startSession();
            activeTransactions.put(transactionId, this);
        }

        @Override
        public void commit() {
            if (transactionId != null) {
                boolean success = false;
                while (commitRetryTracker.shouldTryAgain() && !success) {
                    try {
                        startTransactionQuietly();
                        for (Entry<MongoCollectionKey, List<WriteModel<I>>> entry :
                                documents.entrySet()) {

                            MongoCollection<I> collection = entry.getKey().get(connection.client(), documentType);
                            List<WriteModel<I>> writes = entry.getValue();

                            BulkWriteResult result = collection.bulkWrite(clientSession, writes);

                            if (!result.wasAcknowledged()) {
                                commitRetryTracker.attemptFailed();
                                break;
                            }
                        }
                        clientSession.commitTransaction();
                        commitRetryTracker.reset();
                        success = true;
                    } catch (MongoCommandException e) {
                        tryExternalRollback();
                        commitRetryTracker.attemptFailed();
                        if (!commitRetryTracker.shouldTryAgain() || e.getErrorCode() != MONGODB_TRANSIENT_ERROR) {
                            throw e;
                        }
                    } catch (MongoBulkWriteException e) {
                        tryExternalRollback();
                        throw new JetException(e);
                    } catch (MongoSocketException | IllegalStateException e) {
                        tryExternalRollback();
                        commitRetryTracker.attemptFailed();
                        if (!commitRetryTracker.shouldTryAgain()) {
                            throw e;
                        }
                    }
                }
            }
        }

        private void startTransactionQuietly() {
            try {
                clientSession.startTransaction(TRANSACTION_OPTIONS);
            } catch (IllegalStateException e) {
                if (!e.getMessage().contains("already in progress")) {
                    throw sneakyThrow(e);
                }
            }
        }

        @Override
        public void rollback() {
            if (transactionId != null) {
                tryExternalRollback();
                documents.clear();
            }
        }

        private void tryExternalRollback() {
            try {
                clientSession.abortTransaction();
            } catch (IllegalStateException e) {
                ignore(e);
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
        MongoCollectionKey pick(I item);
    }

    private static final class ConstantCollectionPicker<I> implements CollectionPicker<I> {

        private final MongoCollectionKey key;

        private ConstantCollectionPicker(String databaseName, String collectionName) {
            this.key = new MongoCollectionKey(databaseName, collectionName);
        }


        @Override
        public MongoCollectionKey pick(I item) {
            return key;
        }

    }

    private static final class FunctionalCollectionPicker<I> implements CollectionPicker<I> {

        private final FunctionEx<I, String> databaseNameSelectFn;
        private final FunctionEx<I, String> collectionNameSelectFn;

        private FunctionalCollectionPicker(FunctionEx<I, String> databaseNameSelectFn,
                                           FunctionEx<I, String> collectionNameSelectFn) {
            this.databaseNameSelectFn = databaseNameSelectFn;
            this.collectionNameSelectFn = collectionNameSelectFn;
        }


        @Override
        public MongoCollectionKey pick(I item) {
            String databaseName = databaseNameSelectFn.apply(item);
            String collectionName = collectionNameSelectFn.apply(item);
            return new MongoCollectionKey(databaseName, collectionName);
        }

    }

    private static final class MongoCollectionKey {
        private final @Nonnull String collectionName;
        private final @Nonnull String databaseName;

        MongoCollectionKey(@Nonnull String databaseName, @Nonnull String collectionName) {
            this.collectionName = collectionName;
            this.databaseName = databaseName;
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
            return collectionName.equals(that.collectionName) && databaseName.equals(that.databaseName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(collectionName, databaseName);
        }

        @Override
        public String toString() {
            return "MongoCollectionKey{" +
                    "collectionName='" + collectionName + '\'' +
                    ", databaseName='" + databaseName + '\'' +
                    '}';
        }

        @Nonnull
        public <I> MongoCollection<I> get(MongoClient mongoClient, Class<I> documentType) {
            return mongoClient.getDatabase(databaseName)
                              .getCollection(collectionName, documentType)
                              .withCodecRegistry(defaultCodecRegistry());
        }
    }

}
