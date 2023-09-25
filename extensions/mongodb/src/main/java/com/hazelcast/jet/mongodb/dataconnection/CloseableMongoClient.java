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
package com.hazelcast.jet.mongodb.dataconnection;

import com.mongodb.ClientSessionOptions;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.ClientSession;
import com.mongodb.client.ListDatabasesIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.connection.ClusterDescription;
import org.bson.Document;
import org.bson.conversions.Bson;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * MongoClient implementation that run given action on close and delegates all actions to some delegate.
 */
class CloseableMongoClient implements MongoClient {
    final MongoClient delegate;
    private final Runnable onClose;

    CloseableMongoClient(MongoClient delegate, Runnable onClose) {
        this.delegate = delegate;
        this.onClose = onClose;
    }

    MongoClient unwrap() {
        return delegate;
    }

    @Override
    public void close() {
        onClose.run();
    }

    // All the following methods are delegating actions to underlying client.

    @Nonnull
    @Override
    public MongoDatabase getDatabase(@Nonnull String databaseName) {
        return delegate.getDatabase(databaseName);
    }

    @Nonnull
    @Override
    public ClientSession startSession() {
        return delegate.startSession();
    }

    @Nonnull
    @Override
    public ClientSession startSession(@Nonnull ClientSessionOptions options) {
        return delegate.startSession(options);
    }

    @Nonnull
    @Override
    public MongoIterable<String> listDatabaseNames() {
        return delegate.listDatabaseNames();
    }

    @Nonnull
    @Override
    public MongoIterable<String> listDatabaseNames(@Nonnull ClientSession clientSession) {
        return delegate.listDatabaseNames(clientSession);
    }

    @Nonnull
    @Override
    public ListDatabasesIterable<Document> listDatabases() {
        return delegate.listDatabases();
    }

    @Nonnull
    @Override
    public ListDatabasesIterable<Document> listDatabases(@Nonnull ClientSession clientSession) {
        return delegate.listDatabases(clientSession);
    }

    @Nonnull
    @Override
    public <TResult> ListDatabasesIterable<TResult> listDatabases(@Nonnull Class<TResult> tResultClass) {
        return delegate.listDatabases(tResultClass);
    }

    @Nonnull
    @Override
    public <TResult> ListDatabasesIterable<TResult> listDatabases(@Nonnull ClientSession clientSession,
                                                                  @Nonnull Class<TResult> tResultClass) {
        return delegate.listDatabases(clientSession, tResultClass);
    }

    @Override
    @Nonnull
    public ChangeStreamIterable<Document> watch() {
        return delegate.watch();
    }

    @Override
    @Nonnull
    public <TResult> ChangeStreamIterable<TResult> watch(@Nonnull Class<TResult> tResultClass) {
        return delegate.watch(tResultClass);
    }

    @Override
    @Nonnull
    public ChangeStreamIterable<Document> watch(@Nonnull List<? extends Bson> pipeline) {
        return delegate.watch(pipeline);
    }

    @Override
    @Nonnull
    public <TResult> ChangeStreamIterable<TResult> watch(@Nonnull List<? extends Bson> pipeline,
                                                         @Nonnull Class<TResult> tResultClass) {
        return delegate.watch(pipeline, tResultClass);
    }

    @Override
    @Nonnull
    public ChangeStreamIterable<Document> watch(@Nonnull ClientSession clientSession) {
        return delegate.watch(clientSession);
    }

    @Override
    @Nonnull
    public <TResult> ChangeStreamIterable<TResult> watch(@Nonnull ClientSession clientSession,
                                                         @Nonnull Class<TResult> tResultClass) {
        return delegate.watch(clientSession, tResultClass);
    }

    @Override
    @Nonnull
    public ChangeStreamIterable<Document> watch(@Nonnull ClientSession clientSession,
                                                @Nonnull List<? extends Bson> pipeline) {
        return delegate.watch(clientSession, pipeline);
    }

    @Nonnull
    @Override
    public <TResult> ChangeStreamIterable<TResult> watch(
            @Nonnull ClientSession clientSession,
            @Nonnull List<? extends Bson> pipeline,
            @Nonnull Class<TResult> tResultClass
    ) {
        return delegate.watch(clientSession, pipeline, tResultClass);
    }

    @Nonnull
    @Override
    public ClusterDescription getClusterDescription() {
        return delegate.getClusterDescription();
    }
}
