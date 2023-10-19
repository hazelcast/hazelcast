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
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.mongodb.impl.DbCheckingPMetaSupplierBuilder;
import com.hazelcast.jet.mongodb.impl.WriteMongoP;
import com.hazelcast.jet.mongodb.impl.WriteMongoParams;
import com.hazelcast.jet.pipeline.DataConnectionRef;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.SinkBuilder;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.retry.RetryStrategies;
import com.hazelcast.jet.retry.RetryStrategy;
import com.mongodb.TransactionOptions;
import com.mongodb.client.MongoClient;
import com.mongodb.client.model.ReplaceOptions;
import org.bson.BsonDocument;
import org.bson.Document;

import javax.annotation.Nonnull;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.jet.impl.util.Util.checkNonNullAndSerializable;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;
import static com.hazelcast.jet.retry.IntervalFunction.exponentialBackoffWithCap;
import static com.mongodb.ReadPreference.primaryPreferred;
import static com.mongodb.WriteConcern.MAJORITY;
import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * See {@link MongoSinks#builder}
 *
 * @since 5.3
 *
 * @param <T> type of the items the sink will accept
 */
@SuppressWarnings({"UnusedReturnValue", "unused"})
public final class MongoSinkBuilder<T> {

    /**
     * Default transaction options used by the processors.
     */
    @SuppressWarnings("checkstyle:MagicNumber")
    public static final TransactionOptions DEFAULT_TRANSACTION_OPTION = TransactionOptions
            .builder()
            .writeConcern(MAJORITY)
            .maxCommitTime(10L, MINUTES)
            .readPreference(primaryPreferred())
            .build();

    /**
     * Default retry strategy used by the processors.
     */
    @SuppressWarnings("checkstyle:MagicNumber")
    public static final RetryStrategy DEFAULT_COMMIT_RETRY_STRATEGY = RetryStrategies
            .custom()
            .intervalFunction(exponentialBackoffWithCap(100, 2.0, 3000))
            .maxAttempts(20)
            .build();

    private final String name;
    private final WriteMongoParams<T> params = new WriteMongoParams<>();

    private int preferredLocalParallelism = 2;
    private ResourceChecks existenceChecks = ResourceChecks.ONCE_PER_JOB;

    /**
     * See {@link MongoSinks#builder}
     */
    MongoSinkBuilder(
            @Nonnull String name,
            @Nonnull Class<T> documentClass,
            @Nonnull SupplierEx<MongoClient> clientSupplier
    ) {
        this.name = checkNotNull(name, "sink name cannot be null");
        params.setClientSupplier(checkNonNullAndSerializable(clientSupplier, "clientSupplier"));
        params.setDocumentType(checkNotNull(documentClass, "document class cannot be null"));

        if (Document.class.isAssignableFrom(documentClass)) {
            identifyDocumentBy("_id", doc -> ((Document) doc).get("_id"));
        }
        if (BsonDocument.class.isAssignableFrom(documentClass)) {
            identifyDocumentBy("_id", doc -> ((BsonDocument) doc).get("_id"));
        }

        transactionOptions(() -> DEFAULT_TRANSACTION_OPTION);
        commitRetryStrategy(DEFAULT_COMMIT_RETRY_STRATEGY);
    }

    /**
     * See {@link MongoSinks#builder}
     */
    MongoSinkBuilder(
            @Nonnull String name,
            @Nonnull Class<T> documentClass,
            @Nonnull DataConnectionRef dataConnectionRef
            ) {
        this.name = checkNotNull(name, "sink name cannot be null");
        params.setDataConnectionRef(checkNonNullAndSerializable(dataConnectionRef, "dataConnectionRef"));
        params.setDocumentType(checkNotNull(documentClass, "document class cannot be null"));

        if (Document.class.isAssignableFrom(documentClass)) {
            identifyDocumentBy("_id", doc -> ((Document) doc).get("_id"));
        }
        if (BsonDocument.class.isAssignableFrom(documentClass)) {
            identifyDocumentBy("_id", doc -> ((BsonDocument) doc).get("_id"));
        }

        transactionOptions(() -> DEFAULT_TRANSACTION_OPTION);
        commitRetryStrategy(DEFAULT_COMMIT_RETRY_STRATEGY);
    }

    /**
     * @param selectDatabaseNameFn selects database name for each item individually
     * @param selectCollectionNameFn selects collection name for each item individually
     */
    @Nonnull
    public MongoSinkBuilder<T> into(
            @Nonnull FunctionEx<T, String> selectDatabaseNameFn,
            @Nonnull FunctionEx<T, String> selectCollectionNameFn
    ) {
        params.setDatabaseNameSelectFn(selectDatabaseNameFn);
        params.setCollectionNameSelectFn(selectCollectionNameFn);
        return this;
    }

    /**
     * @param databaseName database name to which objects will be inserted/updated.
     * @param collectionName collection name to which objects will be inserted/updated.
     */
    @Nonnull
    public MongoSinkBuilder<T> into(@Nonnull String databaseName, @Nonnull String collectionName) {
        params.setDatabaseName(databaseName);
        params.setCollectionName(collectionName);
        return this;
    }

    /**
     * See {@link SinkBuilder#preferredLocalParallelism(int)}.
     */
    @Nonnull
    public MongoSinkBuilder<T> preferredLocalParallelism(int preferredLocalParallelism) {
        this.preferredLocalParallelism = Vertex.checkLocalParallelism(preferredLocalParallelism);
        return this;
    }

    /**
     * Provides an option to adjust options used in replace action.
     * By default {@linkplain ReplaceOptions#upsert(boolean) upsert} is only enabled.
     */
    @Nonnull
    public MongoSinkBuilder<T> withCustomReplaceOptions(@Nonnull ConsumerEx<ReplaceOptions> adjustConsumer) {
        params.setReplaceOptionAdjuster(checkNonNullAndSerializable(adjustConsumer, "adjustConsumer"));
        return this;
    }

    /**
     * Sets the filter that decides which document in the collection is equal to processed document.
     * @param fieldName field name in the collection, that will be used for comparison
     * @param documentIdentityFn function that extracts ID from given item; will be compared against {@code fieldName}
     */
    @Nonnull
    public MongoSinkBuilder<T> identifyDocumentBy(
            @Nonnull String fieldName,
            @Nonnull FunctionEx<T, Object> documentIdentityFn) {
        checkNotNull(fieldName, "fieldName cannot be null");
        checkSerializable(documentIdentityFn, "documentIdentityFn");
        params.setDocumentIdentityFieldName(fieldName);
        params.setDocumentIdentityFn(documentIdentityFn);
        return this;
    }

    /**
     * Sets the retry strategy in case of commit failure.
     * <p>
     * MongoDB by default retries simple operations, but commits must be retried manually.
     * <p>
     * This option is taken into consideration only if
     * {@linkplain com.hazelcast.jet.config.ProcessingGuarantee#EXACTLY_ONCE} is used.
     * <p>
     * Default value is {@linkplain #DEFAULT_COMMIT_RETRY_STRATEGY}.
     */
    @Nonnull
    public MongoSinkBuilder<T> commitRetryStrategy(@Nonnull RetryStrategy commitRetryStrategy) {
        params.setCommitRetryStrategy(commitRetryStrategy);
        return this;
    }

    /**
     * Sets options which will be used by MongoDB transaction mechanism.
     * <p>
     * This option is taken into consideration only if
     * {@linkplain com.hazelcast.jet.config.ProcessingGuarantee#EXACTLY_ONCE} is used.
     * <p>
     * Default value is {@linkplain #DEFAULT_TRANSACTION_OPTION}.
     */
    @Nonnull
    public MongoSinkBuilder<T> transactionOptions(@Nonnull SupplierEx<TransactionOptions> transactionOptionsSup) {
        params.setTransactionOptionsSup(transactionOptionsSup);
        return this;
    }

    /**
     * Sets write mode used by the connector. Default value is {@linkplain WriteMode#REPLACE}.
     *
     * @see WriteMode#INSERT_ONLY
     * @see WriteMode#UPDATE_ONLY
     * @see WriteMode#UPSERT
     * @see WriteMode#REPLACE
     */
    @Nonnull
    public MongoSinkBuilder<T> writeMode(@Nonnull WriteMode writeMode) {
        params.setWriteMode(writeMode);
        return this;
    }

    /**
     * If {@link ResourceChecks#NEVER}, the database and collection will be automatically created on the first usage.
     * Otherwise, querying for a database or collection that don't exist will cause an error.
     * Default value is {@link ResourceChecks#ONCE_PER_JOB}.
     *
     * @since 5.4
     * @param checkResourceExistence mode of resource existence checks; whether exception should be thrown when
     *                               database or collection does not exist and when the check will be performed.
     */
    @Nonnull
    public MongoSinkBuilder<T> checkResourceExistence(ResourceChecks checkResourceExistence) {
        existenceChecks = checkResourceExistence;
        return this;
    }

    /**
     * Creates and returns the MongoDB {@link Sink} with the components you
     * supplied to this builder.
     */
    @Nonnull
    public Sink<T> build() {
        params.checkValid();
        final WriteMongoParams<T> localParams = this.params;
        localParams.setCheckExistenceOnEachConnect(existenceChecks == ResourceChecks.ON_EACH_CONNECT);

        return Sinks.fromProcessor(name, new DbCheckingPMetaSupplierBuilder()
                .withCheckResourceExistence(localParams.isCheckExistenceOnEachConnect())
                .withForceTotalParallelismOne(false)
                .withDatabaseName(localParams.getDatabaseName())
                .withCollectionName(localParams.getCollectionName())
                .withClientSupplier(localParams.getClientSupplier())
                .withDataConnectionRef(localParams.getDataConnectionRef())
                .withProcessorSupplier(ProcessorSupplier.of(() -> new WriteMongoP<>(localParams)))
                .withPreferredLocalParallelism(preferredLocalParallelism)
                .build());
    }

}
