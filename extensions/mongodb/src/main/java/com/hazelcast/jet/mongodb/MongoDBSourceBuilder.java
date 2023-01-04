/*
 * Copyright 2021 Hazelcast Inc.
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

import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.StreamSource;
import com.mongodb.client.MongoClient;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;
import static com.hazelcast.jet.mongodb.Mappers.streamToClass;
import static com.hazelcast.jet.mongodb.Mappers.toClass;

/**
 * Top-level class for MongoDB custom source builders.
 * <p>
 * For details refer to the factory methods:
 * <ul>
 *      <li>{@link #batch(String, SupplierEx)}</li>
 *      <li>{@link #stream(String, SupplierEx)}</li>
 * </ul>
 *
 */
public final class MongoDBSourceBuilder implements Serializable{ // TODO make non serializable

    private static final int BATCH_SIZE = 1024;

    private final String name;
    private final SupplierEx<? extends MongoClient> connectionSupplier;
    private final List<Bson> aggregates = new ArrayList<>();

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
    public static MongoDBSourceBuilder.Batch<Document> batch(
            @Nonnull String name,
            @Nonnull SupplierEx<? extends MongoClient> connectionSupplier
    ) {
        return new MongoDBSourceBuilder(name, connectionSupplier).new Batch<>();
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
    public static MongoDBSourceBuilder.Stream<Document> stream(
            @Nonnull String name,
            @Nonnull SupplierEx<? extends MongoClient> connectionSupplier
    ) {
        return new MongoDBSourceBuilder(name, connectionSupplier).new Stream<>();
    }

    private abstract static class Base<T> implements Serializable { // todo later - non serializable!
        protected Class<T> mongoType;

        protected String databaseName;
        protected String collectionName;

        private Base() {
        }

        @Nonnull
        public Base<T> database(String database) {
            databaseName = database;
            return this;
        }

        @Nonnull
        @SuppressWarnings("unchecked")
        public <T_NEW> Base<T_NEW> collection(String collectionName, Class<T_NEW> mongoType) {
            Base<T_NEW> newThis = (Base<T_NEW>) this;
            newThis.collectionName = collectionName;
            newThis.mongoType = mongoType;
            return newThis;
        }

        @Nonnull
        public Base<Document> collection(String collectionName) {
            return this.collection(collectionName, Document.class);
        }
    }

    /**
     * See {@link #batch(String, SupplierEx)}.
     *
     * @param <T> type of the emitted objects
     */
    public final class Batch<T> extends Base<T> {

        private FunctionEx<Document, T> mapFn;

        private Batch() {
        }

        @Nonnull
        public Batch<T> project (@Nullable Bson projection) {
            if (projection != null) {
                aggregates.add(Aggregates.project(projection).toBsonDocument());
            }
            return this;
        }

        @Nonnull
        public Batch<T> sort (@Nullable Bson sort) {
            if (sort != null) {
                aggregates.add(Aggregates.sort(sort).toBsonDocument());
            }
            return this;
        }

        @Nonnull
        public Batch<T> filter (@Nullable Bson filter) {
            if (filter != null) {
                aggregates.add(Aggregates.match(filter).toBsonDocument());
            }
            return this;
        }

        /**
         * @param mapFn   transforms the queried document to the desired output
         *                object
         * @param <T_NEW> type of the emitted object
         */
        @Nonnull
        @SuppressWarnings("unchecked")
        public <T_NEW> Batch<T_NEW> mapFn(@Nonnull FunctionEx<Document, T_NEW> mapFn) {
            checkSerializable(mapFn, "mapFn");
            Batch<T_NEW> newThis = (Batch<T_NEW>) this;
            newThis.mapFn = mapFn;
            return newThis;
        }

        @Override @Nonnull
        public Batch<T> database(String database) {
            return (Batch<T>) super.database(database);
        }

        @SuppressWarnings("unchecked")
        @Override @Nonnull
        public <T_NEW> Batch<T_NEW> collection(String collectionName, Class<T_NEW> mongoType) {

            Batch<T_NEW> newThis = (Batch<T_NEW>) this;
            newThis.collection(collectionName);
            newThis.mapFn = toClass(mongoType);
            return newThis;
        }

        @Override @Nonnull
        public Batch<Document> collection(String collectionName) {
            return (Batch<Document>) super.collection(collectionName, Document.class);
        }

        /**
         * Creates and returns the MongoDB {@link BatchSource}.
         */
        @Nonnull
        public BatchSource<T> build() {
            checkNotNull(connectionSupplier, "connectionSupplier must be set");
            checkNotNull(mapFn, "mapFn must be set");

            SupplierEx<? extends MongoClient> localConnectionSupplier = connectionSupplier;
            FunctionEx<Document, T> localMapFn = mapFn;
            return Sources.batchFromProcessor(name, ProcessorMetaSupplier.of(
                    () -> new ReadMongoP<>(localConnectionSupplier, aggregates, databaseName, collectionName, localMapFn)));
        }
    }

    /**
     * See {@link #stream(String, SupplierEx)}.
     *
     * @param <T> type of the queried documents
     */
    public final class Stream<T> extends Base<T> {

        FunctionEx<ChangeStreamDocument<Document>, T> mapFn;
        Long startAtOperationTime;

        private Stream() {
        }


        @Nonnull
        public Stream<T> project (@Nullable Bson projection) {
            if (projection != null) {
                aggregates.add(Aggregates.project(projection).toBsonDocument());
            }
            return this;
        }

        @Nonnull
        public Stream<T> sort (@Nullable Bson sort) {
            if (sort != null) {
                aggregates.add(Aggregates.sort(sort).toBsonDocument());
            }
            return this;
        }

        @Nonnull
        public Stream<T> filter (@Nullable Bson filter) {
            if (filter != null) {
                aggregates.add(Aggregates.match(filter).toBsonDocument());
            }
            return this;
        }

        @Nonnull
        public Stream<T> database(String database) {
            databaseName = database;
            return this;
        }

        @Nonnull
        @SuppressWarnings("unchecked")
        public <T_NEW> Stream<T_NEW> collection(String collectionName, Class<T_NEW> mongoType) {
            Stream<T_NEW> newThis = (Stream<T_NEW>) this;
            newThis.collectionName = collectionName;
            newThis.mapFn = streamToClass(mongoType);
            return newThis;
        }

        @Nonnull
        public Stream<Document> collection(String collectionName) {
            return collection(collectionName, Document.class);
        }

        @Nonnull
        @SuppressWarnings("unchecked")
        public <T_NEW> Stream<T_NEW> mapFn(
                @Nonnull FunctionEx<ChangeStreamDocument<Document>, T_NEW> mapFn
        ) {
            checkSerializable(mapFn, "mapFn");
            Stream<T_NEW> newThis = (Stream<T_NEW>) this;
            newThis.mapFn = mapFn;
            return newThis;
        }

        @Nonnull
        public Stream<T> startAtOperationTime(
                @Nullable BsonTimestamp startAtOperationTime
        ) {
            this.startAtOperationTime = startAtOperationTime == null ? null : startAtOperationTime.getValue();
            return this;
        }

        /**
         * Creates and returns the MongoDB {@link StreamSource} which watches
         * the given collection.
         */
        @Nonnull
        public StreamSource<T> build() {
            checkNotNull(connectionSupplier, "connectionSupplier must be set");
            checkNotNull(mapFn, "mapFn must be set");

            SupplierEx<? extends MongoClient> localConnectionSupplier = connectionSupplier;
            FunctionEx<ChangeStreamDocument<Document>, T> localMapFn = mapFn;

            MongoClient mongoClient = connectionSupplier.get();

            return Sources.streamFromProcessorWithWatermarks(name, true,
                    eventTimePolicy -> ProcessorMetaSupplier.of(
                        () -> new ReadMongoP<>(localConnectionSupplier, startAtOperationTime, eventTimePolicy, aggregates,
                                databaseName, collectionName, localMapFn)));
        }
    }
//
//    private static class BatchContext<T, U> {
//
//        final MongoClient client;
//        final FunctionEx<? super T, U> mapFn;
//        final ConsumerEx<? super MongoClient> destroyFn;
//
//        final MongoCursor<? extends T> cursor;
//
//        BatchContext(
//                MongoClient client,
//                MongoCollection<? extends T> collection,
//                FunctionEx<? super MongoCollection<? extends T>, ? extends FindIterable<? extends T>> searchFn,
//                FunctionEx<? super T, U> mapFn,
//                ConsumerEx<? super MongoClient> destroyFn
//        ) {
//            this.client = client;
//            this.mapFn = mapFn;
//            this.destroyFn = destroyFn;
//
//            cursor = searchFn.apply(collection).iterator();
//        }
//
//        void fillBuffer(SourceBuilder.SourceBuffer<U> buffer) {
//            for (int i = 0; i < BATCH_SIZE; i++) {
//                if (cursor.hasNext()) {
//                    U item = mapFn.apply(cursor.next());
//                    if (item != null) {
//                        buffer.add(item);
//                    }
//                } else {
//                    buffer.close();
//                }
//            }
//        }
//
//        void close() {
//            cursor.close();
//            destroyFn.accept(client);
//        }
//    }
//
//    private static class StreamContext<T, U> {
//
//        final MongoClient client;
//        final FunctionEx<? super ChangeStreamDocument<? extends T>, U> mapFn;
//        final ConsumerEx<? super MongoClient> destroyFn;
//        final ChangeStreamIterable<? extends T> changeStreamIterable;
//        final BsonTimestamp timestamp;
//
//        BsonDocument resumeToken;
//        MongoCursor<? extends ChangeStreamDocument<? extends T>> cursor;
//
//        StreamContext(
//                MongoClient client,
//                ChangeStreamIterable<? extends T> changeStreamIterable,
//                FunctionEx<? super ChangeStreamDocument<? extends T>, U> mapFn,
//                ConsumerEx<? super MongoClient> destroyFn,
//                FunctionEx<? super MongoClient, ? extends BsonTimestamp> startAtOperationTimeFn
//        ) {
//            this.client = client;
//            this.changeStreamIterable = changeStreamIterable;
//            this.mapFn = mapFn;
//            this.destroyFn = destroyFn;
//
//            this.timestamp = startAtOperationTimeFn == null ? null : startAtOperationTimeFn.apply(client);
//        }
//
//        void fillBuffer(SourceBuilder.TimestampedSourceBuffer<U> buffer) {
//            if (cursor == null) {
//                if (resumeToken != null) {
//                    changeStreamIterable.resumeAfter(resumeToken);
//                } else if (timestamp != null) {
//                    changeStreamIterable.startAtOperationTime(timestamp);
//                }
//                cursor = changeStreamIterable.batchSize(BATCH_SIZE).iterator();
//            }
//
//            ChangeStreamDocument<? extends T> changeStreamDocument = null;
//            for (int i = 0; i < BATCH_SIZE; i++) {
//                changeStreamDocument = cursor.tryNext();
//                if (changeStreamDocument == null) {
//                    // we've exhausted the stream
//                    break;
//                }
//                long clusterTime = clusterTime(changeStreamDocument);
//                U item = mapFn.apply(changeStreamDocument);
//                if (item != null) {
//                    buffer.add(item, clusterTime);
//                }
//            }
//            resumeToken = changeStreamDocument == null ? null : changeStreamDocument.getResumeToken();
//        }
//
//        long clusterTime(ChangeStreamDocument<? extends T> changeStreamDocument) {
//            BsonTimestamp clusterTime = changeStreamDocument.getClusterTime();
//            return clusterTime == null ? System.currentTimeMillis() : clusterTime.getValue();
//        }
//
//        void close() {
//            cursor.close();
//            destroyFn.accept(client);
//        }
//
//        BsonDocument snapshot() {
//            return resumeToken;
//        }
//
//        void restore(List<BsonDocument> list) {
//            resumeToken = list.get(0);
//        }
//    }

}
