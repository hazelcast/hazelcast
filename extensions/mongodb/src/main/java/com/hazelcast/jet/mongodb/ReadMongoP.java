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
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.BroadcastKey;
import com.hazelcast.jet.core.EventTimeMapper;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.retry.RetryStrategies;
import com.hazelcast.jet.retry.impl.RetryTracker;
import com.hazelcast.logging.ILogger;
import com.mongodb.MongoException;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.bson.BsonDocument;
import org.bson.BsonReader;
import org.bson.BsonTimestamp;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.conversions.Bson;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.internal.util.Preconditions.checkState;
import static com.hazelcast.jet.Traversers.singleton;
import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.config.ProcessingGuarantee.NONE;
import static com.hazelcast.jet.core.BroadcastKey.broadcastKey;
import static com.mongodb.MongoClientSettings.getDefaultCodecRegistry;
import static com.mongodb.client.model.Sorts.ascending;
import static java.util.Objects.requireNonNull;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

/**
 * Processor for reading from MongoDB
 *
 * TODO add more info
 * @param <T> type of elements in collection(s)
 * @param <I> type of emitted item
 */
public class ReadMongoP<T, I> extends AbstractProcessor {

    private static final long RETRY_INTERVAL = 3_000;
    private static final int BATCH_SIZE = 1_000;
    private ILogger logger;
    private final SupplierEx<? extends MongoClient> connectionSupplier;
    private final BsonTimestamp startAtTimestamp;
    private final EventTimeMapper<I> eventTimeMapper;
    private MongoClient mongoClient;

    private MongoDatabase database;
    private MongoCollection<T> collection;

    private final List<Bson> aggregates;
    private final Class<T> mongoType;
    private final String databaseName;
    private final String collectionName;

    private RetryTracker connectionRetryTracker;
    private Traverser<?> traverser;

    private int totalParallelism;
    private boolean snapshotsEnabled;
    private final FunctionEx<? super T, I> mapItemFn;
    private final FunctionEx<ChangeStreamDocument<T>, I> mapStreamFn;
    private ChunkedReader<I> reader;

    private boolean snapshotInProgress;
    private Traverser<? extends Entry<BroadcastKey<Integer>, ?>> snapshotTraverser;
    private int processorIndex;

    public ReadMongoP(
            SupplierEx<? extends MongoClient> connectionSupplier,
            List<Bson> aggregates,
            Class<T> mongoType,
            String databaseName,
            String collectionName,
            FunctionEx<? super T, I> mapItemFn
    ) {
        this.connectionSupplier = connectionSupplier;
        this.aggregates = aggregates;
        this.mongoType = mongoType;
        this.databaseName = databaseName;
        this.collectionName = collectionName;
        this.startAtTimestamp = null;
        this.eventTimeMapper = null;
        this.mapItemFn = mapItemFn;
        this.mapStreamFn = null;
        this.kind = Kind.BATCH;
    }

    public ReadMongoP(
            SupplierEx<? extends MongoClient> connectionSupplier,
            BsonTimestamp startAtTimestamp,
            EventTimePolicy<? super I> eventTimePolicy,
            List<Bson> aggregates,
            Class<T> mongoType,
            String databaseName,
            String collectionName,
            FunctionEx<ChangeStreamDocument<T>, I> mapStreamFn
    ) {
        this.connectionSupplier = connectionSupplier;
        this.startAtTimestamp = startAtTimestamp;
        this.eventTimeMapper = new EventTimeMapper<>(eventTimePolicy);
        this.aggregates = aggregates;
        this.mongoType = mongoType;
        this.databaseName = databaseName;
        this.collectionName = collectionName;
        this.mapStreamFn = mapStreamFn;
        this.mapItemFn = null;
        this.kind = Kind.STREAM;
    }

    @Override
    protected void init(@Nonnull Context context) {
        logger = context.logger();
        connectionRetryTracker = new RetryTracker(RetryStrategies.indefinitely(RETRY_INTERVAL));
        connect();
        totalParallelism = context.totalParallelism();
        processorIndex = context.globalProcessorIndex();
        this.snapshotsEnabled = context.jobConfig().getProcessingGuarantee() != NONE;

        if (snapshotsEnabled) {
            aggregates.add(Aggregates.sort(ascending("__id")).toBsonDocument());
        }

        CodecProvider pojoCodecProvider = PojoCodecProvider.builder().automatic(true).build();
        CodecRegistry pojoCodecRegistry = fromRegistries(getDefaultCodecRegistry(), fromProviders(pojoCodecProvider));
        if (databaseName != null) {
            this.database = mongoClient.getDatabase(databaseName).withCodecRegistry(pojoCodecRegistry);
        }
        if (this.collectionName != null) {
            checkState(databaseName != null, "you have to provide database name if collection name" +
                    " is specified");
            //noinspection ConstantValue false warn by intellij
            checkState(database != null, "database " + databaseName + " does not exists");
            this.collection = database.getCollection(collectionName, mongoType);
            this.collection = collection.withCodecRegistry(pojoCodecRegistry);
        }


        switch (kind) {
            case BATCH:
                reader = new BatchMongoTraverser(mapItemFn);
                break;
            case STREAM:
                reader = new StreamMongoTraverser(mapStreamFn, startAtTimestamp);
                break;
        }

        traverser = reader.nextChunkTraverser();
    }

    private enum Kind {
        BATCH,
        STREAM
    }

    private final Kind kind;

    @Override
    public boolean complete() {
        if (!emitFromTraverser(traverser)) {
            return false;
        }
        reconnectIfNecessary();

        this.traverser = reader.nextChunkTraverser();

        return kind == Kind.BATCH;
    }

    private int partition(ChangeStreamDocument<T> changeStreamDocument) {
        BsonDocument documentKey = requireNonNull(changeStreamDocument.getDocumentKey(), "required document key is missing");
        try (BsonReader reader = documentKey.asBsonReader()) {
            return reader.readObjectId().toHexString().hashCode()
                    % totalParallelism;

        }
    }

    private long clusterTime(ChangeStreamDocument<T> changeStreamDocument) {
        BsonTimestamp clusterTime = changeStreamDocument.getClusterTime();
        return clusterTime == null ? System.currentTimeMillis() : clusterTime.getValue();
    }

    private void reconnectIfNecessary() {
        if (!isConnectionUp()) {
            if (connectionRetryTracker.shouldTryAgain()) {
                connect();
            } else {
                throw new JetException("cannot connect to MongoDB");
            }
        }
    }

    private boolean isConnectionUp() {
        return mongoClient != null;
    }

    private void connect() {
        try {
            mongoClient = connectionSupplier.get();

            connectionRetryTracker.reset();
        } catch (Exception e) {
            logger.warning("Could not connect to MongoDB", e);
            connectionRetryTracker.attemptFailed();
        }
    }

    @Override
    public void close() {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }

    @Override
    public boolean saveToSnapshot() {
        if (!snapshotsEnabled) {
            return true;
        }
        if (!emitFromTraverser(traverser)) {
            return false;
        }
        snapshotInProgress = true;
        if (snapshotTraverser == null) {
            int partition = processorIndex % totalParallelism;
            snapshotTraverser = singleton(entry(broadcastKey(partition), reader.snapshot()))
                    .onFirstNull(() -> {
                        snapshotTraverser = null;
                        getLogger().finest("Finished saving snapshot.");
                    });
        }
        return emitFromTraverserToSnapshot(snapshotTraverser);
    }

    @Override
    protected void restoreFromSnapshot(@Nonnull Object key, @Nonnull Object value) {
        int keyInteger = (int) key;
        if (keyInteger % totalParallelism == processorIndex) {
            reader.restore(value);
        }
    }

    private interface ChunkedReader<I> {
        @Nonnull
        Traverser<? extends Object> nextChunkTraverser();

        @Nonnull
        Object snapshot();

        void restore(Object value);

    }

    private final class BatchMongoTraverser implements ChunkedReader<I> {
        private final Traverser<T> delegate;

        private BatchMongoTraverser(
                FunctionEx<? super T, I> mapFn
        ) {
            if (collection != null) {
                this.delegate = delegateForCollection(collection, mapFn);
            }
            else if (database != null) {
                this.delegate = delegateForDb(database, mapFn);
            }
            else {
                this.delegate = traverseIterable(mongoClient.listDatabaseNames())
                        .flatMap(name -> {
                            MongoDatabase db = mongoClient.getDatabase(name);
                            return delegateForDb(db, mapFn);
                        });
            }
        }

        private Traverser<T> delegateForCollection(MongoCollection<T> collection, FunctionEx<? super T, I> mapFn) {
            return traverseIterable(collection.aggregate(aggregates));
        }

        private Traverser<T> delegateForDb(MongoDatabase database, FunctionEx<? super T, I> mapFn) {
            MongoIterable<String> collectionsIterable = database.listCollectionNames();

            return traverseIterable(collectionsIterable)
                    .flatMap(colName -> delegateForCollection(database.getCollection(colName, mongoType), mapFn));
        }

        @Nonnull
        @Override
        public Traverser<I> nextChunkTraverser() {
            List<I> chunk = new ArrayList<>(BATCH_SIZE);
            T item;
            while ((item = delegate.next()) != null) {
                chunk.add(mapItemFn.apply(item));
            }

            return Traversers.traverseIterable(chunk);
        }

        @Nonnull
        @Override
        public Object snapshot() {
            return null;
        }

        @Override
        public void restore(Object value) {

        }
    }

    private final class StreamMongoTraverser implements ChunkedReader<I> {
        private final ChangeStreamIterable<T>  changeStream;
        private final FunctionEx<? super ChangeStreamDocument<T>, I> mapFn;
        private final BsonTimestamp startTimestamp;
        private MongoCursor<ChangeStreamDocument<T>> cursor;
        private BsonDocument resumeToken;

        private StreamMongoTraverser(
                FunctionEx<ChangeStreamDocument<T>, I> mapFn,
                BsonTimestamp startTimestamp) {
            this.mapFn = mapFn;
            this.startTimestamp = startTimestamp;
            this.changeStream = mongoClient.watch(aggregates, mongoType);
        }

        @Nonnull
        @Override
        public Traverser<Object> nextChunkTraverser() {
            Traverser<Object> traverser;
            try {
                if (cursor == null) {
                    if (resumeToken != null) {
                        changeStream.resumeAfter(resumeToken);
                    } else if (startTimestamp != null) {
                        changeStream.startAtOperationTime(startTimestamp);
                    }
                    cursor = changeStream.batchSize(BATCH_SIZE).iterator();
                }

                AtomicReference<ChangeStreamDocument<?>> lastDocument = new AtomicReference<>();
                traverser = Traversers.traverseIterator(cursor)
                                      .onFirstNull(() -> {
                                          ChangeStreamDocument<?> doc = lastDocument.get();
                                          resumeToken = doc == null ? null : doc.getResumeToken();
                                      })
                                      .flatMap(doc -> {
                                          lastDocument.set(doc);
                                          long eventTime = clusterTime(doc);
                                          I item = mapFn.apply(doc);
                                          int partition = partition(doc);
                                          return item == null
                                                  ? Traversers.empty()
                                                  : eventTimeMapper.flatMapEvent(item, partition, eventTime);
                                      });
            } catch (MongoException e) {
                throw new JetException("error while reading from mongodb", e);
            }
            return traverser;
        }

        @Nonnull
        @Override
        public Object snapshot() {
            return resumeToken;
        }

        @Override
        public void restore(Object value) {
            this.resumeToken = (BsonDocument) value;
        }
    }
}
