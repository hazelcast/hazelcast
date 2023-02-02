package com.hazelcast.jet.mongodb.impl;

import com.hazelcast.function.ConsumerEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.util.Preconditions;
import com.hazelcast.jet.retry.RetryStrategy;
import com.mongodb.TransactionOptions;
import com.mongodb.client.MongoClient;
import com.mongodb.client.model.ReplaceOptions;

import java.io.Serializable;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.Preconditions.checkState;
import static com.hazelcast.jet.impl.util.Util.checkNonNullAndSerializable;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;

@SuppressWarnings({"UnusedReturnValue", "unused"})
public class WriteMongoParams<I> implements Serializable {
    private static final InternalSerializationService SERIALIZATION_SERVICE = new DefaultSerializationServiceBuilder().build();

    SupplierEx<? extends MongoClient> clientSupplier;
    String databaseName;
    String collectionName;
    Class<I> documentType;
    FunctionEx<I, Object> documentIdentityFn;
    ConsumerEx<ReplaceOptions> replaceOptionAdjuster = ConsumerEx.noop();
    String documentIdentityFieldName;
    RetryStrategy commitRetryStrategy;
    byte[] transactionOptions;
    FunctionEx<I, String> databaseNameSelectFn;
    FunctionEx<I, String> collectionNameSelectFn;

    public WriteMongoParams() {
    }

    public SupplierEx<? extends MongoClient> getClientSupplier() {
        return clientSupplier;
    }

    public WriteMongoParams<I> setClientSupplier(SupplierEx<? extends MongoClient> clientSupplier) {
        this.clientSupplier = clientSupplier;
        return this;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public WriteMongoParams<I> setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
        return this;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public WriteMongoParams<I> setCollectionName(String collectionName) {
        this.collectionName = collectionName;
        return this;
    }

    public Class<I> getDocumentType() {
        return documentType;
    }

    public WriteMongoParams<I> setDocumentType(Class<I> documentType) {
        this.documentType = documentType;
        return this;
    }

    public FunctionEx<I, Object> getDocumentIdentityFn() {
        return documentIdentityFn;
    }

    public WriteMongoParams<I> setDocumentIdentityFn(FunctionEx<I, Object> documentIdentityFn) {
        this.documentIdentityFn = documentIdentityFn;
        return this;
    }

    public ConsumerEx<ReplaceOptions> getReplaceOptionAdjuster() {
        return replaceOptionAdjuster;
    }

    public WriteMongoParams<I> setReplaceOptionAdjuster(ConsumerEx<ReplaceOptions> replaceOptionAdjuster) {
        checkNonNullAndSerializable(replaceOptionAdjuster, "replaceOptionAdjuster");
        this.replaceOptionAdjuster = replaceOptionAdjuster;
        return this;
    }

    public String getDocumentIdentityFieldName() {
        return documentIdentityFieldName;
    }

    public WriteMongoParams<I> setDocumentIdentityFieldName(String documentIdentityFieldName) {
        this.documentIdentityFieldName = documentIdentityFieldName;
        return this;
    }

    public RetryStrategy getCommitRetryStrategy() {
        return commitRetryStrategy;
    }

    public WriteMongoParams<I> setCommitRetryStrategy(RetryStrategy commitRetryStrategy) {
        this.commitRetryStrategy = commitRetryStrategy;
        return this;
    }

    public byte[] getTransactionOptions() {
        return transactionOptions;
    }

    public WriteMongoParams<I> setTransactionOptions(TransactionOptions transactionOptions) {
        this.transactionOptions = SERIALIZATION_SERVICE.toBytes(transactionOptions);
        return this;
    }

    public FunctionEx<I, String> getDatabaseNameSelectFn() {
        return databaseNameSelectFn;
    }

    public WriteMongoParams<I> setDatabaseNameSelectFn(FunctionEx<I, String> databaseNameSelectFn) {
        checkSerializable(databaseNameSelectFn, "databaseNameSelectFn");
        this.databaseNameSelectFn = databaseNameSelectFn;
        return this;
    }

    public FunctionEx<I, String> getCollectionNameSelectFn() {
        return collectionNameSelectFn;
    }

    public WriteMongoParams<I> setCollectionNameSelectFn(FunctionEx<I, String> collectionNameSelectFn) {
        checkSerializable(collectionNameSelectFn, "collectionNameSelectFn");
        this.collectionNameSelectFn = collectionNameSelectFn;
        return this;
    }

    public void checkValid() {
        checkNotNull(clientSupplier, "clientSupplier must be set");
        checkNotNull(documentIdentityFn, "documentIdentityFn must be set");
        checkNotNull(commitRetryStrategy, "commitRetryStrategy must be set");
        checkNotNull(transactionOptions, "transactionOptions must be set");


        checkState((databaseName == null) == (collectionName == null), "if one of [databaseName, collectionName]" +
                " is provided, so should the other one");
        checkState((databaseNameSelectFn == null) == (collectionNameSelectFn == null),
                "if one of [selectDatabaseNameFn, selectCollectionNameFn] is provided, so should the other one");

        checkState((databaseNameSelectFn == null) != (databaseName == null),
                "Only select*Fn or *Name functions should be called, never mixed");
    }
}
