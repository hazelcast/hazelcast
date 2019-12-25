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
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.StreamSource;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;

/**
 * Top-level class for MongoDB custom source builders.
 * <p>
 * For details refer to the factory methods:
 * <ul>
 *      <li>{@link #batch(String, SupplierEx)}</li>
 *      <li>{@link #stream(String, SupplierEx)}</li>
 *      <li>{@link #streamDatabase(String, SupplierEx)}</li>
 *      <li>{@link #streamAll(String, SupplierEx)}</li>
 * </ul>
 *
 * @param <T> type of the queried items
 */
public final class MongoDBSourceBuilder<T> {

    private static final int BATCH_SIZE = 1024;

    private final String name;
    private final SupplierEx<? extends MongoClient> connectionSupplier;

    private FunctionEx<? super MongoClient, ? extends MongoDatabase> databaseFn;
    private FunctionEx<? super MongoDatabase, ? extends MongoCollection<? extends T>> collectionFn;
    private ConsumerEx<? super MongoClient> destroyFn = ConsumerEx.noop();

    private MongoDBSourceBuilder(
            @Nonnull String name,
            @Nonnull SupplierEx<? extends MongoClient> connectionSupplier
    ) {
        checkSerializable(connectionSupplier, "connectionSupplier");
        this.name = name;
        this.connectionSupplier = connectionSupplier;
    }

    /**
     * Returns a builder object that offers a step-by-step fluent API to build
     * a custom MongoDB {@link BatchSource} for the Pipeline API.
     * <p>
     * The created source will not be distributed, a single processor instance
     * will be created on an arbitrary member.
     * <p>
     * These are the callback functions you should provide to implement the
     * source's behavior:
     * <ol><li>
     * {@code connectionSupplier} supplies MongoDb client.
     * </li><li>
     * {@code databaseFn} creates/obtains a database using the given client.
     * </li><li>
     * {@code collectionFn} creates/obtains a collection in the given
     * database.
     * </li><li>
     * {@code searchFn} queries the collection and returns an iterable over the
     * result set.
     * </li><li>
     * {@code mapFn} transforms the queried items to the desired output items.
     * </li><li>
     * {@code destroyFn} destroys the client. It will be called upon completion
     * to release any resource. This component is optional.
     * </li></ol>
     * <p>
     * Here's an example that builds a simple source which queries all the
     * documents in a collection and emits the items as a string by transforming
     * each item to json.
     * <pre>{@code
     * BatchSource<String> source = MongoDBSourceBuilder
     *         .batch("batch-source",
     *                 () -> MongoClients.create("mongodb://127.0.0.1:27017"))
     *         .databaseFn(client -> client.getDatabase("databaseName"))
     *         .collectionFn(db -> db.getCollection("collectionName"))
     *         .searchFn(MongoCollection::find)
     *         .mapFn(Document::toJson)
     *         .build();
     * Pipeline p = Pipeline.create();
     * BatchStage<String> srcStage = p.readFrom(source);
     * }</pre>
     *
     * @param name               a descriptive name for the source (diagnostic purposes)
     * @param connectionSupplier a function that creates MongoDB client
     */
    @Nonnull
    public static MongoDBSourceBuilder<Void>.Batch<Void, Void> batch(
            @Nonnull String name,
            @Nonnull SupplierEx<? extends MongoClient> connectionSupplier
    ) {
        return new MongoDBSourceBuilder<Void>(name, connectionSupplier).new Batch<Void, Void>();
    }

    /**
     * Returns a builder object that offers a step-by-step fluent API to build
     * a custom MongoDB {@link StreamSource} for the Pipeline API.
     * <p>
     * The source watches the changes on all the collections across all the
     * databases instead of on a single collection thus trying to set {@code
     * collectionFn} and {@code databaseFn} will result in an exception.
     * <p>
     * Change stream is available for replica sets and sharded clusters that
     * use WiredTiger storage engine and replica set protocol version 1 (pv1).
     * Change streams can also be used on deployments which employ MongoDB's
     * encryption-at-rest feature. You cannot watch on system collections and
     * collections in admin, local and config databases.
     * <p>
     * Here's an example that builds a simple source which watches all changes
     * on all collections across all the databases and emits the full document
     * of the change.
     * <pre>{@code
     * StreamSource<? extends Document> streamAll = MongoDBSourceBuilder
     *         .streamAll("stream-all",
     *                 () -> MongoClients.create("mongodb://127.0.0.1:27017"))
     *         .searchFn(MongoClient::watch)
     *         .mapFn(ChangeStreamDocument::getFullDocument)
     *         .build();
     * Pipeline p = Pipeline.create();
     * StreamSourceStage<? extends Document> srcStage = p.readFrom(streamAll);
     * }</pre>
     * <p>
     * See {@link #stream(String, SupplierEx)}
     *
     * @param name               a descriptive name for the source (diagnostic purposes)
     * @param connectionSupplier a function that creates MongoDB client
     */
    @Nonnull
    public static MongoDBSourceBuilder<Void>.StreamAll<Void, Void> streamAll(
            @Nonnull String name,
            @Nonnull SupplierEx<? extends MongoClient> connectionSupplier
    ) {
        return new MongoDBSourceBuilder<Void>(name, connectionSupplier).new StreamAll<Void, Void>();
    }

    /**
     * Returns a builder object that offers a step-by-step fluent API to build
     * a custom MongoDB {@link StreamSource} for the Pipeline API.
     * <p>
     * The source watches the changes on all the collections in the database
     * instead of on a single collection thus trying to set {@code
     * collectionFn} will result in an exception.
     * <p>
     * Change stream is available for replica sets and sharded clusters that
     * use WiredTiger storage engine and replica set protocol version 1 (pv1).
     * Change streams can also be used on deployments which employ MongoDB's
     * encryption-at-rest feature. You cannot watch on system collections and
     * collections in admin, local and config databases.
     * <p>
     * Here's an example that builds a simple source which watches all changes
     * on all collections in the database and emits the full document of the
     * change.
     * <pre>{@code
     * StreamSource<? extends Document> streamDatabase = MongoDBSourceBuilder
     *         .streamDatabase("stream-database",
     *                 () -> MongoClients.create("mongodb://127.0.0.1:27017"))
     *         .databaseFn(client -> client.getDatabase("dbName"))
     *         .searchFn(MongoDatabase::watch)
     *         .mapFn(ChangeStreamDocument::getFullDocument)
     *         .build();
     * Pipeline p = Pipeline.create();
     * StreamSourceStage<? extends Document> srcStage = p.readFrom(streamDatabase);
     * }</pre>
     * <p>
     * See {@link #stream(String, SupplierEx)}
     *
     * @param name               a descriptive name for the source (diagnostic purposes)
     * @param connectionSupplier a function that creates MongoDB client
     */
    @Nonnull
    public static MongoDBSourceBuilder<Void>.StreamDatabase<Void, Void> streamDatabase(
            @Nonnull String name,
            @Nonnull SupplierEx<? extends MongoClient> connectionSupplier
    ) {
        return new MongoDBSourceBuilder<Void>(name, connectionSupplier).new StreamDatabase<Void, Void>();
    }

    /**
     * Returns a builder object that offers a step-by-step fluent API to build
     * a custom MongoDB {@link StreamSource} for the Pipeline API.
     * <p>
     * The created source will not be distributed, a single processor instance
     * will be created on an arbitrary member. The source provides native
     * timestamps using {@link ChangeStreamDocument#getClusterTime()} and fault
     * tolerance using {@link ChangeStreamDocument#getResumeToken()}.
     *
     * <p>
     * These are the callback functions you should provide to implement the
     * source's behavior:
     * <ol><li>
     * {@code connectionSupplier} supplies MongoDb client.
     * </li><li>
     * {@code databaseFn} creates/obtains a database using the given client.
     * </li><li>
     * {@code collectionFn} creates/obtains a collection in the given
     * database.
     * </li><li>
     * {@code searchFn} watches the changes on the collection and returns an
     * iterable over the changes.
     * </li><li>
     * {@code mapFn} transforms the queried items to the desired output items.
     * </li><li>
     * {@code destroyFn} destroys the client. It will be called upon completion
     * to release any resource. This component is optional.
     * </li></ol>
     * <p>
     * Change stream is available for replica sets and sharded clusters that
     * use WiredTiger storage engine and replica set protocol version 1 (pv1).
     * Change streams can also be used on deployments which employ MongoDB's
     * encryption-at-rest feature. You cannot watch on system collections and
     * collections in admin, local and config databases.
     * <p>
     * Here's an example that builds a simple source which watches all changes
     * on a collection and emits the full document of the change.
     * <pre>{@code
     * StreamSource<? extends Document> streamCollection = MongoDBSourceBuilder
     *         .stream("stream-collection",
     *                 () -> MongoClients.create("mongodb://127.0.0.1:27017"))
     *         .databaseFn(client -> client.getDatabase("dbName"))
     *         .collectionFn(db -> db.getCollection("collectionName"))
     *         .searchFn(MongoCollection::watch)
     *         .mapFn(ChangeStreamDocument::getFullDocument)
     *         .build();
     * Pipeline p = Pipeline.create();
     * StreamSourceStage<? extends Document> srcStage = p.readFrom(streamCollection);
     * }</pre>
     *
     * @param name               a descriptive name for the source (diagnostic purposes)
     * @param connectionSupplier a function that creates MongoDB client
     */
    @Nonnull
    public static MongoDBSourceBuilder<Void>.Stream<Void, Void> stream(
            @Nonnull String name,
            @Nonnull SupplierEx<? extends MongoClient> connectionSupplier
    ) {
        return new MongoDBSourceBuilder<Void>(name, connectionSupplier).new Stream<Void, Void>();
    }

    private static <T, U> SupplierEx<StreamContext<T, U>> contextFn(
            SupplierEx<? extends MongoClient> connectionSupplier,
            FunctionEx<? super MongoClient, ? extends MongoDatabase> databaseFn,
            FunctionEx<? super MongoDatabase, ? extends MongoCollection<? extends T>> collectionFn,
            ConsumerEx<? super MongoClient> destroyFn,
            FunctionEx<? super MongoCollection<? extends T>, ? extends ChangeStreamIterable<? extends T>> searchFn,
            FunctionEx<? super ChangeStreamDocument<? extends T>, U> mapFn,
            FunctionEx<? super MongoClient, ? extends BsonTimestamp> startAtOperationTimeFn

    ) {
        return () -> {
            MongoClient client = connectionSupplier.get();
            MongoDatabase database = databaseFn.apply(client);
            MongoCollection<? extends T> collection = collectionFn.apply(database);
            ChangeStreamIterable<? extends T> changeStreamIterable = searchFn.apply(collection);
            return new StreamContext<>(client, changeStreamIterable, mapFn, destroyFn, startAtOperationTimeFn);
        };
    }

    private static <T, U> SupplierEx<StreamContext<T, U>> contextFn(
            SupplierEx<? extends MongoClient> connectionSupplier,
            FunctionEx<? super MongoClient, ? extends MongoDatabase> databaseFn,
            ConsumerEx<? super MongoClient> destroyFn,
            FunctionEx<? super MongoDatabase, ? extends ChangeStreamIterable<? extends T>> searchFn,
            FunctionEx<? super ChangeStreamDocument<? extends T>, U> mapFn,
            FunctionEx<? super MongoClient, ? extends BsonTimestamp> startAtOperationTimeFn
    ) {
        return () -> {
            MongoClient client = connectionSupplier.get();
            MongoDatabase database = databaseFn.apply(client);
            ChangeStreamIterable<? extends T> changeStreamIterable = searchFn.apply(database);
            return new StreamContext<>(client, changeStreamIterable, mapFn, destroyFn, startAtOperationTimeFn);
        };
    }

    private static <T, U> SupplierEx<StreamContext<T, U>> contextFn(
            SupplierEx<? extends MongoClient> connectionSupplier,
            ConsumerEx<? super MongoClient> destroyFn,
            FunctionEx<? super MongoClient, ? extends ChangeStreamIterable<? extends T>> searchFn,
            FunctionEx<? super ChangeStreamDocument<? extends T>, U> mapFn,
            FunctionEx<? super MongoClient, ? extends BsonTimestamp> startAtOperationTimeFn
    ) {
        return () -> {
            MongoClient client = connectionSupplier.get();
            ChangeStreamIterable<? extends T> changeStreamIterable = searchFn.apply(client);
            return new StreamContext<>(client, changeStreamIterable, mapFn, destroyFn, startAtOperationTimeFn);
        };
    }

    private abstract class Base<T> {

        private Base() {
        }

        /**
         * @param databaseFn creates/obtains a database using the given client.
         */
        @Nonnull
        public Base<T> databaseFn(
                @Nonnull FunctionEx<MongoClient, MongoDatabase> databaseFn
        ) {
            checkSerializable(databaseFn, "databaseFn");
            MongoDBSourceBuilder.this.databaseFn = databaseFn;
            return this;
        }

        /**
         * @param collectionFn creates/obtains a collection in the given database.
         */
        @Nonnull
        @SuppressWarnings("unchecked")
        public <T_NEW> Base<T_NEW> collectionFn(
                @Nonnull FunctionEx<MongoDatabase, MongoCollection<T_NEW>> collectionFn
        ) {
            checkSerializable(collectionFn, "collectionFn");

            Base<T_NEW> newThis = (Base<T_NEW>) this;
            MongoDBSourceBuilder<T_NEW> newBuilder = (MongoDBSourceBuilder<T_NEW>) MongoDBSourceBuilder.this;
            newBuilder.collectionFn = collectionFn;
            return newThis;
        }

        /**
         * @param destroyFn destroys the client.
         */
        @Nonnull
        public Base<T> destroyFn(@Nonnull ConsumerEx<MongoClient> destroyFn) {
            checkSerializable(destroyFn, "destroyFn");
            MongoDBSourceBuilder.this.destroyFn = destroyFn;
            return this;
        }
    }

    /**
     * See {@link #batch(String, SupplierEx)}.
     *
     * @param <T> type of the queried documents
     * @param <U> type of the emitted objects
     */
    public final class Batch<T, U> extends Base<T> {

        private FunctionEx<? super MongoCollection<? extends T>, ? extends FindIterable<? extends T>> searchFn;
        private FunctionEx<? super T, U> mapFn;

        private Batch() {
        }

        /**
         * @param searchFn queries the collection and returns an iterable over
         *                 the result set
         */
        @Nonnull
        public Batch<T, U> searchFn(
                @Nonnull FunctionEx<? super MongoCollection<? extends T>, ? extends FindIterable<? extends T>> searchFn
        ) {
            checkSerializable(searchFn, "searchFn");
            this.searchFn = searchFn;
            return this;
        }

        /**
         * @param mapFn   transforms the queried document to the desired output
         *                object
         * @param <U_NEW> type of the emitted object
         */
        @Nonnull
        @SuppressWarnings("unchecked")
        public <U_NEW> Batch<T, U_NEW> mapFn(
                @Nonnull FunctionEx<? super T, U_NEW> mapFn
        ) {
            checkSerializable(mapFn, "mapFn");
            Batch<T, U_NEW> newThis = (Batch<T, U_NEW>) this;
            newThis.mapFn = mapFn;
            return newThis;
        }

        @Override @Nonnull
        public Batch<T, U> databaseFn(
                @Nonnull FunctionEx<MongoClient, MongoDatabase> databaseFn
        ) {
            return (Batch<T, U>) super.databaseFn(databaseFn);
        }

        @Override @Nonnull
        public <T_NEW> Batch<T_NEW, U> collectionFn(
                @Nonnull FunctionEx<MongoDatabase, MongoCollection<T_NEW>> collectionFn
        ) {
            return (Batch<T_NEW, U>) super.collectionFn(collectionFn);
        }

        @Override @Nonnull
        public Batch<T, U> destroyFn(
                @Nonnull ConsumerEx<MongoClient> destroyFn
        ) {
            return (Batch<T, U>) super.destroyFn(destroyFn);
        }

        /**
         * Creates and returns the MongoDB {@link BatchSource}.
         */
        @Nonnull
        public BatchSource<U> build() {
            checkNotNull(connectionSupplier, "connectionSupplier must be set");
            checkNotNull(databaseFn, "databaseFn must be set");
            checkNotNull(collectionFn, "collectionFn must be set");
            checkNotNull(searchFn, "searchFn must be set");
            checkNotNull(mapFn, "mapFn must be set");

            SupplierEx<? extends MongoClient> localConnectionSupplier = connectionSupplier;
            FunctionEx<? super MongoClient, ? extends MongoDatabase> localDatabaseFn = databaseFn;
            FunctionEx<? super MongoDatabase, ? extends MongoCollection<? extends T>> localCollectionFn
                    = (FunctionEx<? super MongoDatabase, ? extends MongoCollection<? extends T>>) collectionFn;
            ConsumerEx<? super MongoClient> localDestroyFn = destroyFn;
            FunctionEx<? super MongoCollection<? extends T>, ? extends FindIterable<? extends T>> localSearchFn = searchFn;
            FunctionEx<? super T, U> localMapFn = mapFn;

            return SourceBuilder
                    .batch(name, ctx -> {
                        MongoClient client = localConnectionSupplier.get();
                        MongoCollection<? extends T> collection = localCollectionFn.apply(localDatabaseFn.apply(client));
                        return new BatchContext<>(client, collection, localSearchFn, localMapFn, localDestroyFn);
                    })
                    .<U>fillBufferFn(BatchContext::fillBuffer)
                    .destroyFn(BatchContext::close)
                    .build();
        }
    }

    private abstract class StreamBase<T, U> extends Base<T> {

        FunctionEx<? super ChangeStreamDocument<? extends T>, U> mapFn;
        FunctionEx<? super MongoClient, ? extends BsonTimestamp> startAtOperationTimeFn;

        private StreamBase() {
        }

        /**
         * @param mapFn   transforms the change stream document to the desired
         *                output object
         * @param <U_NEW> type of the emitted objects
         */
        @Nonnull
        @SuppressWarnings("unchecked")
        public <U_NEW> StreamBase<T, U_NEW> mapFn(
                @Nonnull FunctionEx<? super ChangeStreamDocument<? extends T>, U_NEW> mapFn
        ) {
            checkSerializable(mapFn, "mapFn");
            StreamBase<T, U_NEW> newThis = (StreamBase<T, U_NEW>) this;
            newThis.mapFn = mapFn;
            return newThis;
        }

        /**
         * @param startAtOperationTimeFn obtains an operation time to start the
         *                               stream from. If the function is {@code
         *                               null} or returns {@code null} the
         *                               stream will start from the latest.
         */
        @Nonnull
        public StreamBase<T, U> startAtOperationTimeFn(
                @Nullable FunctionEx<? super MongoClient, ? extends BsonTimestamp> startAtOperationTimeFn
        ) {
            checkSerializable(startAtOperationTimeFn, "startAtOperationTimeFn");
            this.startAtOperationTimeFn = startAtOperationTimeFn;
            return this;
        }

        @Nonnull
        StreamSource<U> build(@Nonnull SupplierEx<StreamContext<T, U>> contextFn) {
            return SourceBuilder
                    .timestampedStream(name, ctx -> contextFn.get())
                    .<U>fillBufferFn(StreamContext::fillBuffer)
                    .createSnapshotFn(StreamContext::snapshot)
                    .restoreSnapshotFn(StreamContext::restore)
                    .destroyFn(StreamContext::close)
                    .build();
        }
    }

    /**
     * See {@link #streamAll(String, SupplierEx)}.
     *
     * @param <T> type of the queried documents
     * @param <U> type of the emitted objects
     */
    public final class StreamAll<T, U> extends StreamBase<T, U> {

        private FunctionEx<? super MongoClient, ? extends ChangeStreamIterable<? extends T>> searchFn;

        private StreamAll() {
        }

        /**
         * @param searchFn returns an iterable over the changes on all
         *                 collections across all databases
         * @param <T_NEW>  type of the queried documents
         */
        @Nonnull
        @SuppressWarnings("unchecked")
        public <T_NEW> StreamAll<T_NEW, U> searchFn(
                @Nonnull FunctionEx<? super MongoClient, ? extends ChangeStreamIterable<? extends T_NEW>> searchFn
        ) {
            checkSerializable(searchFn, "searchFn");
            StreamAll<T_NEW, U> newThis = (StreamAll<T_NEW, U>) this;
            newThis.searchFn = searchFn;
            return newThis;
        }

        @Override @Nonnull
        public <U_NEW> StreamAll<T, U_NEW> mapFn(
                @Nonnull FunctionEx<? super ChangeStreamDocument<? extends T>, U_NEW> mapFn
        ) {
            return (StreamAll<T, U_NEW>) super.mapFn(mapFn);
        }

        @Override @Nonnull
        public Base<T> databaseFn(
                @Nonnull FunctionEx<MongoClient, MongoDatabase> databaseFn
        ) {
            throw new IllegalArgumentException();
        }

        @Override @Nonnull
        public <T_NEW> Base<T_NEW> collectionFn(
                @Nonnull FunctionEx<MongoDatabase, MongoCollection<T_NEW>> collectionFn
        ) {
            throw new IllegalArgumentException();
        }

        @Override @Nonnull
        public StreamAll<T, U> destroyFn(
                @Nonnull ConsumerEx<MongoClient> destroyFn
        ) {
            return (StreamAll<T, U>) super.destroyFn(destroyFn);
        }

        @Override @Nonnull
        public StreamAll<T, U> startAtOperationTimeFn(
                @Nullable FunctionEx<? super MongoClient, ? extends BsonTimestamp> startAtOperationTimeFn
        ) {
            return (StreamAll<T, U>) super.startAtOperationTimeFn(startAtOperationTimeFn);
        }

        /**
         * Creates and returns the MongoDB {@link StreamSource} which watches
         * all collections across all databases.
         */
        @Nonnull
        public StreamSource<U> build() {
            checkNotNull(connectionSupplier, "connectionSupplier must be set");
            checkNotNull(searchFn, "searchFn must be set");
            checkNotNull(mapFn, "mapFn must be set");

            return build(contextFn(connectionSupplier, destroyFn, searchFn, mapFn, startAtOperationTimeFn));
        }
    }

    /**
     * See {@link #streamDatabase(String, SupplierEx)}.
     *
     * @param <T> type of the queried documents
     * @param <U> type of the emitted objects
     */
    public final class StreamDatabase<T, U> extends StreamBase<T, U> {

        private FunctionEx<? super MongoDatabase, ? extends ChangeStreamIterable<? extends T>> searchFn;

        private StreamDatabase() {
        }

        /**
         * @param searchFn returns an iterable over the changes on all
         *                 collections in the given database
         * @param <T_NEW>  type of the queried documents
         */
        @Nonnull
        @SuppressWarnings("unchecked")
        public <T_NEW> StreamDatabase<T_NEW, U> searchFn(
                @Nonnull FunctionEx<? super MongoDatabase, ? extends ChangeStreamIterable<? extends T_NEW>> searchFn
        ) {
            checkSerializable(searchFn, "searchFn");
            StreamDatabase<T_NEW, U> newThis = (StreamDatabase<T_NEW, U>) this;
            newThis.searchFn = searchFn;
            return newThis;
        }

        @Override @Nonnull
        public <U_NEW> StreamDatabase<T, U_NEW> mapFn(
                @Nonnull FunctionEx<? super ChangeStreamDocument<? extends T>, U_NEW> mapFn
        ) {
            return (StreamDatabase<T, U_NEW>) super.mapFn(mapFn);
        }

        @Override @Nonnull
        public StreamDatabase<T, U> databaseFn(
                @Nonnull FunctionEx<MongoClient, MongoDatabase> databaseFn
        ) {
            return (StreamDatabase<T, U>) super.databaseFn(databaseFn);
        }

        @Override @Nonnull
        public <T_NEW> Base<T_NEW> collectionFn(
                @Nonnull FunctionEx<MongoDatabase, MongoCollection<T_NEW>> collectionFn
        ) {
            throw new IllegalArgumentException();
        }

        @Override @Nonnull
        public StreamDatabase<T, U> destroyFn(
                @Nonnull ConsumerEx<MongoClient> destroyFn
        ) {
            return (StreamDatabase<T, U>) super.destroyFn(destroyFn);
        }

        @Override @Nonnull
        public StreamDatabase<T, U> startAtOperationTimeFn(
                @Nullable FunctionEx<? super MongoClient, ? extends BsonTimestamp> startAtOperationTimeFn
        ) {
            return (StreamDatabase<T, U>) super.startAtOperationTimeFn(startAtOperationTimeFn);
        }

        /**
         * Creates and returns the MongoDB {@link StreamSource} which watches
         * all collections in the given database.
         */
        @Nonnull
        public StreamSource<U> build() {
            checkNotNull(connectionSupplier, "connectionSupplier must be set");
            checkNotNull(databaseFn, "databaseFn must be set");
            checkNotNull(searchFn, "searchFn must be set");
            checkNotNull(mapFn, "mapFn must be set");

            return build(contextFn(connectionSupplier, databaseFn, destroyFn, searchFn, mapFn, startAtOperationTimeFn));
        }
    }

    /**
     * See {@link #stream(String, SupplierEx)}.
     *
     * @param <T> type of the queried documents
     * @param <U> type of the emitted objects
     */
    public final class Stream<T, U> extends StreamBase<T, U> {

        private FunctionEx<? super MongoCollection<? extends T>, ? extends ChangeStreamIterable<? extends T>> searchFn;

        private Stream() {
        }

        /**
         * @param searchFn returns an iterable over the changes on the given
         *                 collection
         */
        @Nonnull
        public Stream<T, U> searchFn(
                @Nonnull FunctionEx<? super MongoCollection<? extends T>, ? extends ChangeStreamIterable<? extends T>>
                        searchFn
        ) {
            checkSerializable(searchFn, "searchFn");
            this.searchFn = searchFn;
            return this;
        }

        @Override @Nonnull
        public <U_NEW> Stream<T, U_NEW> mapFn(
                @Nonnull FunctionEx<? super ChangeStreamDocument<? extends T>, U_NEW> mapFn
        ) {
            return (Stream<T, U_NEW>) super.mapFn(mapFn);
        }

        @Override @Nonnull
        public Stream<T, U> databaseFn(
                @Nonnull FunctionEx<MongoClient, MongoDatabase> databaseFn
        ) {
            return (Stream<T, U>) super.databaseFn(databaseFn);
        }

        @Override @Nonnull
        public <T_NEW> Stream<T_NEW, U> collectionFn(
                @Nonnull FunctionEx<MongoDatabase, MongoCollection<T_NEW>> collectionFn
        ) {
            return (Stream<T_NEW, U>) super.collectionFn(collectionFn);
        }

        @Override @Nonnull
        public Stream<T, U> destroyFn(
                @Nonnull ConsumerEx<MongoClient> destroyFn
        ) {
            return (Stream<T, U>) super.destroyFn(destroyFn);
        }

        @Override @Nonnull
        public Stream<T, U> startAtOperationTimeFn(
                @Nullable FunctionEx<? super MongoClient, ? extends BsonTimestamp> startAtOperationTimeFn
        ) {
            return (Stream<T, U>) super.startAtOperationTimeFn(startAtOperationTimeFn);
        }

        /**
         * Creates and returns the MongoDB {@link StreamSource} which watches
         * the given collection.
         */
        @Nonnull
        public StreamSource<U> build() {
            checkNotNull(connectionSupplier, "connectionSupplier must be set");
            checkNotNull(databaseFn, "databaseFn must be set");
            checkNotNull(collectionFn, "collectionFn must be set");
            checkNotNull(searchFn, "searchFn must be set");
            checkNotNull(mapFn, "mapFn must be set");

            SupplierEx<StreamContext<T, U>> contextFn = contextFn(connectionSupplier, databaseFn,
                    (FunctionEx<? super MongoDatabase, ? extends MongoCollection<? extends T>>) collectionFn, destroyFn,
                    searchFn, mapFn, startAtOperationTimeFn);

            return build(contextFn);
        }
    }

    private static class BatchContext<T, U> {

        final MongoClient client;
        final FunctionEx<? super T, U> mapFn;
        final ConsumerEx<? super MongoClient> destroyFn;

        final MongoCursor<? extends T> cursor;

        BatchContext(
                MongoClient client,
                MongoCollection<? extends T> collection,
                FunctionEx<? super MongoCollection<? extends T>, ? extends FindIterable<? extends T>> searchFn,
                FunctionEx<? super T, U> mapFn,
                ConsumerEx<? super MongoClient> destroyFn
        ) {
            this.client = client;
            this.mapFn = mapFn;
            this.destroyFn = destroyFn;

            cursor = searchFn.apply(collection).iterator();
        }

        void fillBuffer(SourceBuilder.SourceBuffer<U> buffer) {
            for (int i = 0; i < BATCH_SIZE; i++) {
                if (cursor.hasNext()) {
                    U item = mapFn.apply(cursor.next());
                    if (item != null) {
                        buffer.add(item);
                    }
                } else {
                    buffer.close();
                }
            }
        }

        void close() {
            cursor.close();
            destroyFn.accept(client);
        }
    }

    private static class StreamContext<T, U> {

        final MongoClient client;
        final FunctionEx<? super ChangeStreamDocument<? extends T>, U> mapFn;
        final ConsumerEx<? super MongoClient> destroyFn;
        final ChangeStreamIterable<? extends T> changeStreamIterable;
        final BsonTimestamp timestamp;

        BsonDocument resumeToken;
        MongoCursor<? extends ChangeStreamDocument<? extends T>> cursor;

        StreamContext(
                MongoClient client,
                ChangeStreamIterable<? extends T> changeStreamIterable,
                FunctionEx<? super ChangeStreamDocument<? extends T>, U> mapFn,
                ConsumerEx<? super MongoClient> destroyFn,
                FunctionEx<? super MongoClient, ? extends BsonTimestamp> startAtOperationTimeFn
        ) {
            this.client = client;
            this.changeStreamIterable = changeStreamIterable;
            this.mapFn = mapFn;
            this.destroyFn = destroyFn;

            this.timestamp = startAtOperationTimeFn == null ? null : startAtOperationTimeFn.apply(client);
        }

        void fillBuffer(SourceBuilder.TimestampedSourceBuffer<U> buffer) {
            if (cursor == null) {
                if (resumeToken != null) {
                    changeStreamIterable.resumeAfter(resumeToken);
                } else if (timestamp != null) {
                    changeStreamIterable.startAtOperationTime(timestamp);
                }
                cursor = changeStreamIterable.batchSize(BATCH_SIZE).iterator();
            }

            ChangeStreamDocument<? extends T> changeStreamDocument = null;
            for (int i = 0; i < BATCH_SIZE; i++) {
                changeStreamDocument = cursor.tryNext();
                if (changeStreamDocument == null) {
                    // we've exhausted the stream
                    break;
                }
                long clusterTime = clusterTime(changeStreamDocument);
                U item = mapFn.apply(changeStreamDocument);
                if (item != null) {
                    buffer.add(item, clusterTime);
                }
            }
            resumeToken = changeStreamDocument == null ? null : changeStreamDocument.getResumeToken();
        }

        long clusterTime(ChangeStreamDocument<? extends T> changeStreamDocument) {
            BsonTimestamp clusterTime = changeStreamDocument.getClusterTime();
            return clusterTime == null ? System.currentTimeMillis() : clusterTime.getValue();
        }

        void close() {
            cursor.close();
            destroyFn.accept(client);
        }

        BsonDocument snapshot() {
            return resumeToken;
        }

        void restore(List<BsonDocument> list) {
            resumeToken = list.get(0);
        }
    }

}
