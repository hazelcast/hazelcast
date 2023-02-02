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
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.mongodb.impl.WriteMongoP;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.SinkBuilder;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.retry.RetryStrategies;
import com.hazelcast.jet.retry.RetryStrategy;
import com.hazelcast.spi.annotation.Beta;
import com.mongodb.TransactionOptions;
import com.mongodb.client.MongoClient;
import com.mongodb.client.model.ReplaceOptions;
import org.bson.BsonDocument;
import org.bson.Document;

import javax.annotation.Nonnull;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.Preconditions.checkState;
import static com.hazelcast.jet.impl.util.Util.checkNonNullAndSerializable;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;
import static com.hazelcast.jet.retry.IntervalFunction.exponentialBackoffWithCap;
import static com.mongodb.ReadPreference.primaryPreferred;
import static com.mongodb.WriteConcern.MAJORITY;
import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * See {@link MongoDBSinks#builder}
 *
 * @since 5.3
 *
 * @param <T> type of the items the sink will accept
 */
@Beta
@SuppressWarnings("UnusedReturnValue")
public final class MongoDBSinkBuilder<T> {

    @SuppressWarnings("checkstyle:MagicNumber")
    private static final TransactionOptions DEFAULT_TRANSACTION_OPTION = TransactionOptions
            .builder()
            .writeConcern(MAJORITY)
            .maxCommitTime(10L, MINUTES)
            .readPreference(primaryPreferred())
            .build();

    @SuppressWarnings("checkstyle:MagicNumber")
    private static final RetryStrategy DEFAULT_COMMIT_RETRY_STRATEGY = RetryStrategies
            .custom()
            .intervalFunction(exponentialBackoffWithCap(100, 2.0, 3000))
            .maxAttempts(20)
            .build();

    private final InternalSerializationService serializationService = new DefaultSerializationServiceBuilder().build();
    private final String name;
    private final SupplierEx<MongoClient> clientSupplier;
    private final Class<T> documentClass;

    private int preferredLocalParallelism = 2;

    private FunctionEx<T, String> selectDatabaseNameFn;
    private FunctionEx<T, String> selectCollectionNameFn;
    private String databaseName;
    private String collectionName;
    private String idFieldName;
    private FunctionEx<T, Object> documentIdentityFn;
    private ConsumerEx<ReplaceOptions> replaceOptionsChanger;
    private RetryStrategy commitRetryStrategy;
    private TransactionOptions transactionOptions;

    /**
     * See {@link MongoDBSinks#builder}
     */
    MongoDBSinkBuilder(
            @Nonnull String name,
            @Nonnull Class<T> documentClass,
            @Nonnull SupplierEx<MongoClient> clientSupplier
    ) {
        this.clientSupplier = checkNonNullAndSerializable(clientSupplier, "clientSupplier");
        this.documentClass = checkNotNull(documentClass, "document class cannot be null");
        this.name = checkNotNull(name, "sink name cannot be null");

        if (Document.class.isAssignableFrom(documentClass)) {
            identifyDocumentBy("_id", doc -> ((Document) doc).get("_id"));
        }
        if (BsonDocument.class.isAssignableFrom(documentClass)) {
            identifyDocumentBy("_id", doc -> ((BsonDocument) doc).get("_id"));
        }

        transactionOptions(DEFAULT_TRANSACTION_OPTION);
        commitRetryStrategy(DEFAULT_COMMIT_RETRY_STRATEGY);
    }

    /**
     * @param selectDatabaseNameFn selects database name for each item individually
     * @param selectCollectionNameFn selects collection name for each item individually
     */
    @Nonnull
    public MongoDBSinkBuilder<T> into(
            @Nonnull FunctionEx<T, String> selectDatabaseNameFn,
            @Nonnull FunctionEx<T, String> selectCollectionNameFn
    ) {
        checkSerializable(selectDatabaseNameFn, "selectDatabaseNameFn");
        checkSerializable(selectCollectionNameFn, "selectCollectionNameFn");
        this.selectDatabaseNameFn = selectDatabaseNameFn;
        this.selectCollectionNameFn = selectCollectionNameFn;
        return this;
    }

    /**
     * @param databaseName database name to which objects will be inserted/updated.
     * @param collectionName collection name to which objects will be inserted/updated.
     */
    @Nonnull
    public MongoDBSinkBuilder<T> into(@Nonnull String databaseName, @Nonnull String collectionName) {
        checkNotNull(databaseName, "databaseName cannot be null");
        checkNotNull(collectionName, "collectionName cannot be null");
        this.databaseName = databaseName;
        this.collectionName = collectionName;
        return this;
    }

    /**
     * See {@link SinkBuilder#preferredLocalParallelism(int)}.
     */
    @Nonnull
    public MongoDBSinkBuilder<T> preferredLocalParallelism(int preferredLocalParallelism) {
        this.preferredLocalParallelism = Vertex.checkLocalParallelism(preferredLocalParallelism);
        return this;
    }

    /**
     * Provides an option to adjust options used in replace action.
     * By default {@linkplain ReplaceOptions#upsert(boolean) upsert} is only enabled.
     */
    @Nonnull
    public MongoDBSinkBuilder<T> withCustomReplaceOptions(@Nonnull ConsumerEx<ReplaceOptions> adjustConsumer) {
        this.replaceOptionsChanger = checkNonNullAndSerializable(adjustConsumer, "adjustConsumer");
        return this;
    }

    /**
     * Sets the filter that decides which document in the collection is equal to processed document.
     * @param fieldName field name in the collection, that will be used for comparison
     * @param documentIdentityFn function that extracts ID from given item; will be compared against {@code fieldName}
     */
    @Nonnull
    public MongoDBSinkBuilder<T> identifyDocumentBy(
            @Nonnull String fieldName,
            @Nonnull FunctionEx<T, Object> documentIdentityFn) {
        checkNotNull(fieldName, "fieldName cannot be null");
        checkSerializable(documentIdentityFn, "documentIdentityFn");
        this.idFieldName = fieldName;
        this.documentIdentityFn = documentIdentityFn;
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
    public MongoDBSinkBuilder<T> commitRetryStrategy(@Nonnull RetryStrategy commitRetryStrategy) {
        this.commitRetryStrategy = commitRetryStrategy;
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
    public MongoDBSinkBuilder<T> transactionOptions(@Nonnull TransactionOptions transactionOptions) {
        this.transactionOptions = transactionOptions;
        return this;
    }

    /**
     * Creates and returns the MongoDB {@link Sink} with the components you
     * supplied to this builder.
     */
    @Nonnull
        public Sink<T> build() {
        checkNotNull(clientSupplier, "clientSupplier must be set");
        checkNotNull(documentIdentityFn, "documentIdentityFn must be set");
        checkNotNull(commitRetryStrategy, "commitRetryStrategy must be set");
        checkNotNull(transactionOptions, "transactionOptions must be set");

        final SupplierEx<MongoClient> clientSupplier = this.clientSupplier;
        final Class<T> documentClass = this.documentClass;
        final String databaseName = this.databaseName;
        final String collectionName = this.collectionName;
        final FunctionEx<T, String> selectDatabaseNameFn = this.selectDatabaseNameFn;
        final FunctionEx<T, String> selectCollectionNameFn = this.selectCollectionNameFn;
        final FunctionEx<T, Object> documentIdentityFn = this.documentIdentityFn;
        final String fieldName = this.idFieldName;
        final ConsumerEx<ReplaceOptions> updateOptionsChanger = this.replaceOptionsChanger == null
                ? ConsumerEx.noop()
                : this.replaceOptionsChanger;
        final RetryStrategy commitRetryStrategy = this.commitRetryStrategy;
        final byte[] transactionOptions = serializationService.toData(this.transactionOptions).toByteArray();

        checkState((databaseName == null) == (collectionName == null), "if one of [databaseName, collectionName]" +
                " is provided, so should the other one");
        checkState((selectDatabaseNameFn == null) == (selectCollectionNameFn == null),
                "if one of [selectDatabaseNameFn, selectCollectionNameFn] is provided, so should the other one");

        checkState((selectDatabaseNameFn == null) != (databaseName == null),
                "Only select*Fn or *Name functions should be called, never mixed");

        if (databaseName != null) {
            return Sinks.fromProcessor(
                    name,
                    ProcessorMetaSupplier.of(
                            preferredLocalParallelism,
                            () -> new WriteMongoP<>(
                                    clientSupplier, databaseName, collectionName,
                                    documentClass, documentIdentityFn, updateOptionsChanger, fieldName,
                                    commitRetryStrategy, transactionOptions
                            )
                    ));
        } else {
            return Sinks.fromProcessor(
                    name,
                    ProcessorMetaSupplier.of(
                            preferredLocalParallelism,
                            () -> new WriteMongoP<>(
                                    clientSupplier, selectDatabaseNameFn, selectCollectionNameFn,
                                    documentClass, documentIdentityFn, updateOptionsChanger, fieldName,
                                    commitRetryStrategy, transactionOptions
                            )
                    ));
        }
    }

}
