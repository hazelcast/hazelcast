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
import com.hazelcast.jet.retry.RetryStrategy;
import com.hazelcast.jet.retry.impl.RetryTracker;
import com.hazelcast.logging.ILogger;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoException;
import com.mongodb.MongoServerException;
import com.mongodb.MongoSocketException;
import com.mongodb.TransactionOptions;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.WriteModel;
import org.bson.conversions.Bson;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

import static com.hazelcast.internal.util.EmptyStatement.ignore;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.jet.Util.idToString;
import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.jet.impl.util.LoggingUtil.logFine;
import static com.hazelcast.jet.mongodb.impl.Mappers.defaultCodecRegistry;
import static com.mongodb.MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL;
import static com.mongodb.client.model.Filters.eq;
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

    private final MongoDbConnection connection;
    private final Class<I> documentType;
    private ILogger logger;

    private UnboundedTransactionsProcessorUtility<MongoTransactionId, MongoTransaction> transactionUtility;

    private final CollectionPicker<I> collectionPicker;

    private final FunctionEx<I, Object> documentIdentityFn;
    private final String documentIdentityFieldName;

    private final ReplaceOptions replaceOptions;

    private final Map<MongoTransactionId, MongoTransaction> activeTransactions = new HashMap<>();
    private final RetryStrategy commitRetryStrategy;
    private final SupplierEx<TransactionOptions> transactionOptionsSup;

    /**
     * Creates a new processor that will always insert to the same database and collection.
     */
    public WriteMongoP(WriteMongoParams<I> params) {
        this.connection = new MongoDbConnection(params.clientSupplier, client -> {
        });
        this.documentIdentityFn = params.documentIdentityFn;
        this.documentIdentityFieldName = params.documentIdentityFieldName;
        this.documentType = params.documentType;

        ReplaceOptions options = new ReplaceOptions().upsert(true);
        params.getReplaceOptionAdjuster().accept(options);
        this.replaceOptions = options;
        this.commitRetryStrategy = params.commitRetryStrategy;
        this.transactionOptionsSup = params.transactionOptionsSup;

        if (params.databaseName != null) {
            this.collectionPicker = new ConstantCollectionPicker<>(params.databaseName, params.collectionName);
        } else {
            this.collectionPicker = new FunctionalCollectionPicker<>(params.databaseNameSelectFn,
                    params.collectionNameSelectFn);
        }
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
    public void process(int ordinal, @Nonnull Inbox inbox) {
        if (!connection.reconnectIfNecessary()) {
            return;
        }

        try {
            MongoTransaction mongoTransaction = transactionUtility.activeTransaction();

            ArrayList<I> items = drainItems(inbox);
            @SuppressWarnings("DataFlowIssue")
            Map<MongoCollectionKey, List<I>> itemsPerCollection = items
                    .stream()
                    .map(e -> tuple2(collectionPicker.pick(e), e))
                    .collect(groupingBy(Tuple2::f0, mapping(Tuple2::getValue, toList())));

            for (Map.Entry<MongoCollectionKey, List<I>> entry : itemsPerCollection.entrySet()) {
                MongoCollectionKey collectionKey = entry.getKey();
                MongoCollection<I> collection = collectionKey.get(connection.client(), documentType);

                List<WriteModel<I>> writes = new ArrayList<>();

                for (I item : entry.getValue()) {
                    Object id = documentIdentityFn.apply(item);

                    WriteModel<I> write;
                    if (id == null) {
                        write = new InsertOneModel<>(item);
                    } else {
                        Bson filter = eq(documentIdentityFieldName, id);
                        write = new ReplaceOneModel<>(filter, item, replaceOptions);
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
        } catch (MongoBulkWriteException e) {
            if (e.hasErrorLabel(TRANSIENT_TRANSACTION_ERROR_LABEL)) {
                logger.info("Unable to process element: " + e.getMessage());
                // not removing from inbox, so it will be retried
            } else {
                throw new JetException(e);
            }
        } catch (MongoSocketException | MongoServerException e) {
            logger.info("Unable to process Mongo Sink: " + e.getMessage());
            // not removing from inbox, so it will be retried
        } catch (Exception e) {
            throw new JetException(e);
        }
    }

    @SuppressWarnings("unchecked")
    private static <I> ArrayList<I> drainItems(Inbox inbox) {
        ArrayList<I> items = new ArrayList<>();
        for (Object item : inbox) {
            items.add((I) item);
            if (items.size() >= MAX_BATCH_SIZE) {
                break;
            }
        }
        return items;
    }

    @Override
    public void close() {
        try {
            connection.close();
        } catch (Throwable e) {
            logger.severe("Error while closing MongoDB connection", e);
        }
        try {
            transactionUtility.close();
        } catch (Throwable e) {
            logger.severe("Error while closing MongoDB transaction utility", e);
        }
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
            this.commitRetryTracker = new RetryTracker(commitRetryStrategy);
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
                    } catch (MongoException e) {
                        tryExternalRollback();
                        commitRetryTracker.attemptFailed();
                        if (!commitRetryTracker.shouldTryAgain() || !e.hasErrorLabel("TransientTransactionError")) {
                            throw e;
                        }
                    } catch (IllegalStateException e) {
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
                clientSession.startTransaction(transactionOptionsSup.get());
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
