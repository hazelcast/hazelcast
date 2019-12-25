/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.contrib.mongodb;

import com.hazelcast.function.ConsumerEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.SinkBuilder;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.InsertManyOptions;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.jet.impl.util.Util.checkSerializable;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * See {@link MongoDBSinks#builder(String, SupplierEx)}
 *
 * @param <T> type of the items the sink will accept
 */
public final class MongoDBSinkBuilder<T> {

    private final String name;
    private final SupplierEx<MongoClient> connectionSupplier;

    private FunctionEx<MongoClient, MongoDatabase> databaseFn;
    private FunctionEx<MongoDatabase, MongoCollection<T>> collectionFn;
    private ConsumerEx<MongoClient> destroyFn = ConsumerEx.noop();

    private boolean ordered = true;
    private boolean bypassValidation;
    private int preferredLocalParallelism = 2;

    /**
     * See {@link MongoDBSinks#builder(String, SupplierEx)}
     */
    MongoDBSinkBuilder(
            @Nonnull String name,
            @Nonnull SupplierEx<MongoClient> connectionSupplier
    ) {
        checkSerializable(connectionSupplier, "connectionSupplier");
        this.name = name;
        this.connectionSupplier = connectionSupplier;
    }

    /**
     * @param databaseFn creates/obtains a database using the given client.
     */
    public MongoDBSinkBuilder<T> databaseFn(@Nonnull FunctionEx<MongoClient, MongoDatabase> databaseFn) {
        checkSerializable(databaseFn, "databaseFn");
        this.databaseFn = databaseFn;
        return this;
    }

    /**
     * @param collectionFn creates/obtains a collection in the given database.
     */
    public MongoDBSinkBuilder<T> collectionFn(@Nonnull FunctionEx<MongoDatabase, MongoCollection<T>> collectionFn) {
        checkSerializable(collectionFn, "collectionFn");
        this.collectionFn = collectionFn;
        return this;
    }

    /**
     * @param destroyFn destroys the client.
     */
    public MongoDBSinkBuilder<T> destroyFn(@Nonnull ConsumerEx<MongoClient> destroyFn) {
        checkSerializable(destroyFn, "destroyFn");
        this.destroyFn = destroyFn;
        return this;
    }

    /**
     * @param ordered sets {@link InsertManyOptions#ordered(boolean)}.
     */
    public MongoDBSinkBuilder<T> ordered(boolean ordered) {
        this.ordered = ordered;
        return this;
    }

    /**
     * @param bypassValidation sets {@link
     *                         InsertManyOptions#bypassDocumentValidation(Boolean)}.
     */
    public MongoDBSinkBuilder<T> bypassValidation(boolean bypassValidation) {
        this.bypassValidation = bypassValidation;
        return this;
    }

    /**
     * See {@link SinkBuilder#preferredLocalParallelism(int)}.
     */
    public MongoDBSinkBuilder<T> preferredLocalParallelism(int preferredLocalParallelism) {
        this.preferredLocalParallelism = Vertex.checkLocalParallelism(preferredLocalParallelism);
        return this;
    }

    /**
     * Creates and returns the MongoDB {@link Sink} with the components you
     * supplied to this builder.
     */
    public Sink<T> build() {
        checkNotNull(connectionSupplier, "connectionSupplier must be set");
        checkNotNull(databaseFn, "databaseFn must be set");
        checkNotNull(collectionFn, "collectionFn must be set");

        SupplierEx<MongoClient> localConnectionSupplier = connectionSupplier;
        FunctionEx<MongoClient, MongoDatabase> localDatabaseFn = databaseFn;
        FunctionEx<MongoDatabase, MongoCollection<T>> localCollectionFn = collectionFn;
        ConsumerEx<MongoClient> localDestroyFn = destroyFn;

        boolean localOrdered = ordered;
        boolean localBypassValidation = bypassValidation;

        return SinkBuilder
                .sinkBuilder(name, ctx -> {
                    MongoClient client = localConnectionSupplier.get();
                    MongoCollection<T> collection = localCollectionFn.apply(localDatabaseFn.apply(client));
                    return new MongoSinkContext<>(client, collection, localDestroyFn, localOrdered, localBypassValidation);
                })
                .<T>receiveFn(MongoSinkContext::addDocument)
                .flushFn(MongoSinkContext::flush)
                .destroyFn(MongoSinkContext::close)
                .preferredLocalParallelism(preferredLocalParallelism)
                .build();

    }

    private static class MongoSinkContext<T> {

        final MongoClient client;
        final MongoCollection<T> collection;
        final ConsumerEx<MongoClient> destroyFn;
        final InsertManyOptions insertManyOptions;

        final List<T> documents;

        MongoSinkContext(
                MongoClient client,
                MongoCollection<T> collection,
                ConsumerEx<MongoClient> destroyFn,
                boolean ordered,
                boolean bypassValidation
        ) {
            this.client = client;
            this.collection = collection;
            this.destroyFn = destroyFn;
            this.insertManyOptions = new InsertManyOptions()
                    .ordered(ordered)
                    .bypassDocumentValidation(bypassValidation);

            documents = new ArrayList<>();
        }

        void addDocument(T document) {
            documents.add(document);
        }

        void flush() {
            collection.insertMany(documents, insertManyOptions);
            documents.clear();
        }

        void close() {
            destroyFn.accept(client);
        }
    }


}
