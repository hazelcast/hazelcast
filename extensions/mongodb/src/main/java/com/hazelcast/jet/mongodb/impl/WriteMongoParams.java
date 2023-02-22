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
import com.hazelcast.jet.retry.RetryStrategy;
import com.mongodb.TransactionOptions;
import com.mongodb.client.MongoClient;
import com.mongodb.client.model.ReplaceOptions;

import javax.annotation.Nonnull;
import java.io.Serializable;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.Preconditions.checkState;
import static com.hazelcast.jet.impl.util.Util.checkNonNullAndSerializable;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;

@SuppressWarnings({"UnusedReturnValue", "unused"})
public class WriteMongoParams<I> implements Serializable {

    SupplierEx<? extends MongoClient> clientSupplier;
    String databaseName;
    String collectionName;
    Class<I> documentType;
    String documentIdentityFieldName;
    FunctionEx<I, Object> documentIdentityFn;
    @Nonnull
    ConsumerEx<ReplaceOptions> replaceOptionAdjuster = ConsumerEx.noop();
    RetryStrategy commitRetryStrategy;
    SupplierEx<TransactionOptions> transactionOptionsSup;
    FunctionEx<I, String> databaseNameSelectFn;
    FunctionEx<I, String> collectionNameSelectFn;

    public WriteMongoParams() {
    }

    @Nonnull
    public SupplierEx<? extends MongoClient> getClientSupplier() {
        return clientSupplier;
    }

    @Nonnull
    public WriteMongoParams<I> setClientSupplier(@Nonnull SupplierEx<? extends MongoClient> clientSupplier) {
        this.clientSupplier = clientSupplier;
        return this;
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
    public WriteMongoParams<I> setTransactionOptions(SupplierEx<TransactionOptions> transactionOptionsSup) {
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

    public void checkValid() {
        checkNotNull(clientSupplier, "clientSupplier must be set");
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
