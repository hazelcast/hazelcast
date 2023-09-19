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
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.jet.mongodb.WriteMode;
import com.hazelcast.jet.retry.RetryStrategies;
import com.hazelcast.jet.retry.RetryStrategy;
import com.hazelcast.jet.retry.impl.RetryTracker;
import com.hazelcast.logging.ILogger;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoClientException;
import com.mongodb.MongoException;
import com.mongodb.MongoServerException;
import com.mongodb.MongoSocketException;
import com.mongodb.TransactionOptions;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.client.model.WriteModel;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.conversions.Bson;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

import static com.hazelcast.internal.nio.IOUtil.closeResource;
import static com.hazelcast.internal.util.EmptyStatement.ignore;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.jet.Util.idToString;
import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.internal.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.jet.impl.util.LoggingUtil.logFine;
import static com.hazelcast.jet.mongodb.impl.Mappers.defaultCodecRegistry;
import static com.hazelcast.jet.mongodb.impl.MongoUtilities.checkCollectionExists;
import static com.hazelcast.jet.mongodb.impl.MongoUtilities.checkDatabaseExists;
import static com.hazelcast.jet.retry.IntervalFunction.exponentialBackoffWithCap;
import static com.mongodb.MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL;
import static com.mongodb.client.model.Filters.eq;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;

/**
 * Processor for writing to MongoDB
 * <p>
 * For each chunk of items, processor will choose proper collection and write ReplaceOne or InsertOne operation,
 * depending on the existence of a document with given {@link #documentIdentityFieldName} in the collection.
 * <p>
 * All writes are postponed until transaction commit. This is on purpose, so that - in case of any
 * MongoDB's WriteError - we will be able to retry the commit.
 * <p>
 * Transactional guarantees are provided using {@linkplain UnboundedTransactionsProcessorUtility}.
 *
 * <p>
 * Flow of the data in processor:
 * <ol>
 *     <li>Data arrives in some format IN in the inbox</li>
 *     <li>Data is mapped to intermediate format I using {@link #intermediateMappingFn}; it's the target format of
 *          documents in the collection.</li>
 *     <li>Inbound items are now grouped per collection to which they point</li>
 *     <li>{@link #writeModelFn} creates a {@link WriteModel write model} for each item.
 *          By default this function is derived from {@link #writeMode}:
 *          <ul>
 *              <li>for {@linkplain WriteMode#INSERT_ONLY} it creates {@linkplain InsertOneModel}</li>
 *              <li>for {@linkplain WriteMode#UPDATE_ONLY} it creates {@linkplain UpdateOneModel} with upsert = false</li>
 *              <li>for {@linkplain WriteMode#UPSERT} it creates {@linkplain UpdateOneModel} with upsert = true</li>
 *              <li>for {@linkplain WriteMode#REPLACE} it creates {@linkplain ReplaceOneModel}</li>
 *          </ul>
 *          User can also provide own implementation, that takes item of type I and returns some {@link WriteModel}.
 *     </li>
 *
 *     <li>{@link WriteModel}s are then saved and will be used as a parameter for {@link MongoCollection#bulkWrite(List)
 *          MongoDB bulkWrite method}</li>
 *
 *     <li>Entities are recognized as the same if their {@link #documentIdentityFieldName} are the same.</li>
 * </ol>
 *
 * @param <IN> input type
 * @param <I> type of saved item
 */
public class WriteMongoP<IN, I> extends AbstractProcessor {
    /**
     * Max number of items processed (written) in one invocation of {@linkplain #process}.
     */
    private static final int MAX_BATCH_SIZE = 2_000;
    @SuppressWarnings("checkstyle:MagicNumber")
    private static final RetryStrategy BACKPRESSURE_RETRY_STRATEGY =
            RetryStrategies.custom()
                           .intervalFunction(exponentialBackoffWithCap(100, 2.0, 3000))
                           .build();

    private final MongoConnection connection;
    private final Class<I> documentType;

    /**
     * Mapping function from input type to some intermediate type.
     *
     * Can be used to avoid additional mapping steps before using this processor, e.g. SQL connector
     * passes JetSqlRow, which is then converted into {@linkplain org.bson.Document}, so that
     * {@link #documentIdentityFn} and {@link #documentIdentityFieldName} are easier to reason about.
     */
    private final FunctionEx<IN, I> intermediateMappingFn;
    private final RetryTracker backpressureTracker;
    private ILogger logger;

    private UnboundedTransactionsProcessorUtility<MongoTransactionId, MongoTransaction> transactionUtility;

    private final CollectionPicker<I> collectionPicker;

    private final FunctionEx<I, Object> documentIdentityFn;
    private final String documentIdentityFieldName;

    private final ReplaceOptions replaceOptions;

    private final Map<MongoTransactionId, MongoTransaction> activeTransactions = new HashMap<>();
    private final RetryStrategy commitRetryStrategy;
    private final SupplierEx<TransactionOptions> transactionOptionsSup;
    private final WriteMode writeMode;

    private final FunctionEx<I, WriteModel<I>> writeModelFn;

    /**
     * Creates a new processor that will always insert to the same database and collection.
     */
    public WriteMongoP(WriteMongoParams<I> params) {
        this.connection = new MongoConnection(params.clientSupplier, params.dataConnectionRef, client -> {
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
            this.collectionPicker = new ConstantCollectionPicker<>(params.checkExistenceOnEachConnect, params.databaseName,
                    params.collectionName);
        } else {
            this.collectionPicker = new FunctionalCollectionPicker<>(params.checkExistenceOnEachConnect,
                    params.databaseNameSelectFn, params.collectionNameSelectFn);
        }
        this.intermediateMappingFn = params.getIntermediateMappingFn();
        this.writeMode = params.getWriteMode();

        this.writeModelFn = params.getOptionalWriteModelFn().orElse(this::defaultWriteModelSupplier);
        this.backpressureTracker = new RetryTracker(BACKPRESSURE_RETRY_STRATEGY);
    }

    @Override
    protected void init(@Nonnull Context context) {
        logger = context.logger();
        connection.assembleSupplier(Util.getNodeEngine(context.hazelcastInstance()));

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
        if (backpressureTracker.needsToWait()) {
            return;
        }

        try {
            MongoTransaction mongoTransaction = transactionUtility.activeTransaction();

            ArrayList<IN> items = drainItems(inbox);
            Map<MongoCollectionKey, List<I>> itemsPerCollection = items
                    .stream()
                    .map(intermediateMappingFn)
                    .map(e -> tuple2(collectionPicker.pick(e), e))
                    .collect(groupingBy(Tuple2::requiredF0, mapping(Tuple2::requiredF1, toList())));

            for (Map.Entry<MongoCollectionKey, List<I>> entry : itemsPerCollection.entrySet()) {
                MongoCollectionKey collectionKey = entry.getKey();
                MongoCollection<I> collection = collectionKey.get(connection.client(), documentType);

                List<WriteModel<I>> writes = new ArrayList<>();

                for (I item : entry.getValue()) {
                    WriteModel<I> write = writeModelFn.apply(item);
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
            backpressureTracker.reset();
        } catch (MongoBulkWriteException e) {
            if (e.hasErrorLabel(TRANSIENT_TRANSACTION_ERROR_LABEL)) {
                logger.warning("Unable to process element: " + e.getMessage());
                backpressureTracker.attemptFailed();
                // not removing from inbox, so it will be retried
            } else {
                throw new JetException(e);
            }
        } catch (MongoSocketException | MongoClientException | MongoServerException e) {
            logger.warning("Unable to process Mongo Sink, will retry " + e.getMessage(), e);
            backpressureTracker.attemptFailed();
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

    private WriteModel<I> defaultWriteModelSupplier(I item) {
        Object id = documentIdentityFn.apply(item);
        switch (writeMode) {
            case INSERT_ONLY: return insert(item);
            case UPDATE_ONLY: return update(id, item, new UpdateOptions().upsert(false));
            case UPSERT:      return upsert(id, item);
            case REPLACE:     return replace(id, item);
            default: throw new IllegalStateException("unknown write mode");
        }
    }

    WriteModel<I> insert(I item) {
        return new InsertOneModel<>(item);
    }

    WriteModel<I> upsert(Object id, I item) {
        if (id == null) {
            return new InsertOneModel<>(item);
        } else {
            return update(id, item, new UpdateOptions().upsert(true));
        }
    }

    WriteModel<I> replace(Object id, I item) {
        if (id == null) {
            return new InsertOneModel<>(item);
        } else {
            Bson filter = eq(documentIdentityFieldName, id);
            return new ReplaceOneModel<>(filter, item, replaceOptions);
        }
    }

    WriteModel<I> update(Object id, I item, UpdateOptions opts) {
        Bson filter = eq(documentIdentityFieldName, id);
        BsonDocument doc = Mappers.toBsonDocument(item);
        List<Bson> updates = doc.entrySet().stream()
                                .map(e -> {
                                    String field = e.getKey();
                                    BsonValue value = e.getValue();
                                    return Updates.set(field, value);
                                })
                                .collect(toList());
        return new UpdateOneModel<>(filter, updates, opts);
    }

    @Override
    public void close() {
        closeResource(transactionUtility);
        closeResource(connection);
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

        private ConstantCollectionPicker(boolean throwOnNonExisting, String databaseName, String collectionName) {
            this.key = new MongoCollectionKey(throwOnNonExisting, databaseName, collectionName);
        }


        @Override
        public MongoCollectionKey pick(I item) {
            return key;
        }

    }

    private static final class FunctionalCollectionPicker<I> implements CollectionPicker<I> {

        private final FunctionEx<I, String> databaseNameSelectFn;
        private final FunctionEx<I, String> collectionNameSelectFn;
        private final boolean throwOnNonExisting;

        private FunctionalCollectionPicker(
                boolean throwOnNonExisting,
                FunctionEx<I, String> databaseNameSelectFn,
                FunctionEx<I, String> collectionNameSelectFn
        ) {
            this.throwOnNonExisting = throwOnNonExisting;
            this.databaseNameSelectFn = databaseNameSelectFn;
            this.collectionNameSelectFn = collectionNameSelectFn;
        }


        @Override
        public MongoCollectionKey pick(I item) {
            String databaseName = databaseNameSelectFn.apply(item);
            String collectionName = collectionNameSelectFn.apply(item);
            return new MongoCollectionKey(throwOnNonExisting, databaseName, collectionName);
        }

    }

    private static final class MongoCollectionKey {
        private final @Nonnull String collectionName;
        private final @Nonnull String databaseName;
        private final boolean checkExistenceOnReconnect;

        MongoCollectionKey(
                boolean checkExistenceOnReconnect,
                @Nonnull String databaseName,
                @Nonnull String collectionName) {
            this.checkExistenceOnReconnect = checkExistenceOnReconnect;
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
            if (checkExistenceOnReconnect) {
                checkDatabaseExists(mongoClient, databaseName);
            }
            MongoDatabase database = mongoClient.getDatabase(databaseName);
            if (checkExistenceOnReconnect) {
                checkCollectionExists(database, collectionName);
            }
            return database.getCollection(collectionName, documentType)
                           .withCodecRegistry(defaultCodecRegistry());
        }
    }

}
