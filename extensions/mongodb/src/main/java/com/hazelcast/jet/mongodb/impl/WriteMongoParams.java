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
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.mongodb.WriteMode;
import com.hazelcast.jet.pipeline.DataConnectionRef;
import com.hazelcast.jet.retry.RetryStrategy;
import com.mongodb.TransactionOptions;
import com.mongodb.client.MongoClient;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.WriteModel;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Optional;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.Preconditions.checkState;
import static com.hazelcast.jet.impl.util.Util.checkNonNullAndSerializable;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;
import static com.hazelcast.jet.pipeline.DataConnectionRef.dataConnectionRef;

@SuppressWarnings({"UnusedReturnValue", "unused"})
public class WriteMongoParams<I> implements Serializable {

    SupplierEx<? extends MongoClient> clientSupplier;
    DataConnectionRef dataConnectionRef;
    String databaseName;
    String collectionName;
    Class<I> documentType;
    @Nonnull
    FunctionEx<?, I> intermediateMappingFn = FunctionEx.identity();
    String documentIdentityFieldName;
    FunctionEx<I, Object> documentIdentityFn;
    @Nonnull
    ConsumerEx<ReplaceOptions> replaceOptionAdjuster = ConsumerEx.noop();
    RetryStrategy commitRetryStrategy;
    SupplierEx<TransactionOptions> transactionOptionsSup;
    FunctionEx<I, String> databaseNameSelectFn;
    FunctionEx<I, String> collectionNameSelectFn;
    @Nonnull
    WriteMode writeMode = WriteMode.REPLACE;
    FunctionEx<I, WriteModel<I>> writeModelFn;
    boolean checkExistenceOnEachConnect = true;

    public WriteMongoParams() {
    }

    @Nonnull
    public SupplierEx<? extends MongoClient> getClientSupplier() {
        return clientSupplier;
    }

    @Nonnull
    public WriteMongoParams<I> setClientSupplier(@Nullable SupplierEx<? extends MongoClient> clientSupplier) {
        this.clientSupplier = clientSupplier;
        return this;
    }

    @Nullable
    public DataConnectionRef getDataConnectionRef() {
        return dataConnectionRef;
    }

    @Nonnull
    public WriteMongoParams<I> setDataConnectionRef(@Nullable DataConnectionRef dataConnectionRef) {
        this.dataConnectionRef = dataConnectionRef;
        return this;
    }

    @Nonnull
    public WriteMongoParams<I> setDataConnectionRef(@Nullable String dataConnectionName) {
        if (dataConnectionName != null) {
            setDataConnectionRef(dataConnectionRef(dataConnectionName));
        }
        return this;
    }

    public void checkConnectivityOptionsValid() {
        boolean hasDataConnection = dataConnectionRef != null;
        boolean hasClientSupplier = clientSupplier != null;
        checkState(hasDataConnection || hasClientSupplier, "Client supplier or data connection ref should be provided");
        checkState(hasDataConnection != hasClientSupplier, "Only one of two should be provided: " +
                "Client supplier or data connection ref");
    }

    @Nonnull
    public String getDatabaseName() {
        return databaseName;
    }

    @Nonnull
    public WriteMongoParams<I> setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
        return this;
    }

    public String getCollectionName() {
        return collectionName;
    }

    @Nonnull
    public WriteMongoParams<I> setCollectionName(String collectionName) {
        this.collectionName = collectionName;
        return this;
    }

    @Nonnull
    public Class<I> getDocumentType() {
        return documentType;
    }

    @Nonnull
    public WriteMongoParams<I> setDocumentType(@Nonnull Class<I> documentType) {
        this.documentType = documentType;
        return this;
    }

    @SuppressWarnings("unchecked")
    @Nonnull
    public <T> FunctionEx<T, I> getIntermediateMappingFn() {
        return (FunctionEx<T, I>) intermediateMappingFn;
    }

    @Nonnull
    public <IN> WriteMongoParams<I> setIntermediateMappingFn(FunctionEx<IN, I> intermediateMappingFn) {
        this.intermediateMappingFn = intermediateMappingFn;
        return this;
    }

    @Nonnull
    public FunctionEx<I, Object> getDocumentIdentityFn() {
        return documentIdentityFn;
    }

    @Nonnull
    public WriteMongoParams<I> setDocumentIdentityFn(@Nonnull FunctionEx<I, Object> documentIdentityFn) {
        this.documentIdentityFn = documentIdentityFn;
        return this;
    }

    @Nonnull
    public ConsumerEx<ReplaceOptions> getReplaceOptionAdjuster() {
        return replaceOptionAdjuster;
    }

    @Nonnull
    public WriteMongoParams<I> setReplaceOptionAdjuster(@Nonnull ConsumerEx<ReplaceOptions> replaceOptionAdjuster) {
        checkNonNullAndSerializable(replaceOptionAdjuster, "replaceOptionAdjuster");
        this.replaceOptionAdjuster = replaceOptionAdjuster;
        return this;
    }

    public String getDocumentIdentityFieldName() {
        return documentIdentityFieldName;
    }

    @Nonnull
    public WriteMongoParams<I> setDocumentIdentityFieldName(String documentIdentityFieldName) {
        this.documentIdentityFieldName = documentIdentityFieldName;
        return this;
    }

    public RetryStrategy getCommitRetryStrategy() {
        return commitRetryStrategy;
    }

    @Nonnull
    public WriteMongoParams<I> setCommitRetryStrategy(RetryStrategy commitRetryStrategy) {
        this.commitRetryStrategy = commitRetryStrategy;
        return this;
    }

    public SupplierEx<TransactionOptions> getTransactionOptionsSup() {
        return transactionOptionsSup;
    }

    @Nonnull
    public WriteMongoParams<I> setTransactionOptionsSup(SupplierEx<TransactionOptions> transactionOptionsSup) {
        this.transactionOptionsSup = transactionOptionsSup;
        return this;
    }

    public FunctionEx<I, String> getDatabaseNameSelectFn() {
        return databaseNameSelectFn;
    }

    @Nonnull
    public WriteMongoParams<I> setDatabaseNameSelectFn(FunctionEx<I, String> databaseNameSelectFn) {
        checkSerializable(databaseNameSelectFn, "databaseNameSelectFn");
        this.databaseNameSelectFn = databaseNameSelectFn;
        return this;
    }

    public FunctionEx<I, String> getCollectionNameSelectFn() {
        return collectionNameSelectFn;
    }

    @Nonnull
    public WriteMongoParams<I> setCollectionNameSelectFn(FunctionEx<I, String> collectionNameSelectFn) {
        checkSerializable(collectionNameSelectFn, "collectionNameSelectFn");
        this.collectionNameSelectFn = collectionNameSelectFn;
        return this;
    }

    @Nonnull
    public WriteMode getWriteMode() {
        return writeMode;
    }

    public WriteMongoParams<I> setWriteMode(@Nonnull WriteMode writeMode) {
        checkNotNull(writeMode, "writeMode cannot be null");
        this.writeMode = writeMode;
        return this;
    }

    public FunctionEx<I, WriteModel<I>> getWriteModelFn() {
        return writeModelFn;
    }

    public Optional<FunctionEx<I, WriteModel<I>>> getOptionalWriteModelFn() {
        return Optional.ofNullable(writeModelFn);
    }

    @Nonnull
    public WriteMongoParams<I> setWriteModelFn(FunctionEx<I, WriteModel<I>> writeModelFn) {
        this.writeModelFn = writeModelFn;
        return this;
    }

    public boolean isCheckExistenceOnEachConnect() {
        return checkExistenceOnEachConnect;
    }

    /**
     * If true, the database and collection existence checks will be performed on every reconnection.
     */
    public WriteMongoParams<I> setCheckExistenceOnEachConnect(boolean checkExistenceOnEachConnect) {
        this.checkExistenceOnEachConnect = checkExistenceOnEachConnect;
        return this;
    }

    public void checkValid() {
        checkConnectivityOptionsValid();
        checkNotNull(documentIdentityFn, "documentIdentityFn must be set");
        checkNotNull(commitRetryStrategy, "commitRetryStrategy must be set");
        checkNotNull(transactionOptionsSup, "transactionOptions must be set");


        checkState((databaseName == null) == (collectionName == null), "if one of [databaseName, collectionName]" +
                " is provided, so should the other one");
        checkState((databaseNameSelectFn == null) == (collectionNameSelectFn == null),
                "if one of [selectDatabaseNameFn, selectCollectionNameFn] is provided, so should the other one");

        checkState((databaseNameSelectFn == null) != (databaseName == null),
                "Only select*Fn or *Name functions should be called, never mixed");
    }
}
