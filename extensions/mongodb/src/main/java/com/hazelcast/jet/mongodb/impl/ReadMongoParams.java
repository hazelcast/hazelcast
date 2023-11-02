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

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.pipeline.DataConnectionRef;
import com.mongodb.client.MongoClient;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.bson.BsonTimestamp;
import org.bson.Document;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.internal.util.Preconditions.checkState;
import static com.hazelcast.jet.impl.util.Util.checkNonNullAndSerializable;
import static com.hazelcast.jet.pipeline.DataConnectionRef.dataConnectionRef;

@SuppressWarnings({"UnusedReturnValue", "unused"})
public class ReadMongoParams<I> implements Serializable {
    final boolean stream;
    SupplierEx<? extends MongoClient> clientSupplier;
    DataConnectionRef dataConnectionRef;
    String databaseName;
    String collectionName;
    FunctionEx<Document, I> mapItemFn;

    Long startAtTimestamp;
    EventTimePolicy<? super I> eventTimePolicy;
    BiFunctionEx<ChangeStreamDocument<Document>, Long, I> mapStreamFn;
    boolean nonDistributed;
    private boolean checkExistenceOnEachConnect;
    private Aggregates aggregates = new Aggregates();

    public ReadMongoParams(boolean stream) {
        this.stream = stream;
    }

    public boolean isStream() {
        return stream;
    }

    public void checkConnectivityOptionsValid() {
        boolean hasDataConnection = dataConnectionRef != null;
        boolean hasClientSupplier = clientSupplier != null;
        checkState(hasDataConnection || hasClientSupplier, "Client supplier or data connection ref should be provided");
        checkState(hasDataConnection != hasClientSupplier, "Only one of two should be provided: " +
                "Client supplier or data connection ref");
    }

    @Nonnull
    public SupplierEx<? extends MongoClient> getClientSupplier() {
        return clientSupplier;
    }

    public ReadMongoParams<I> setClientSupplier(@Nonnull SupplierEx<? extends MongoClient> clientSupplier) {
        this.clientSupplier = clientSupplier;
        return this;
    }

    public DataConnectionRef getDataConnectionRef() {
        return dataConnectionRef;
    }

    public ReadMongoParams<I> setDataConnectionRef(DataConnectionRef dataConnectionRef) {
        this.dataConnectionRef = dataConnectionRef;
        return this;
    }

    @Nonnull
    public ReadMongoParams<I> setDataConnectionRef(@Nullable String dataConnectionName) {
        if (dataConnectionName != null) {
            setDataConnectionRef(dataConnectionRef(dataConnectionName));
        }
        return this;
    }

    @Nonnull
    public Aggregates getAggregates() {
        return aggregates;
    }

    @Nonnull
    public ReadMongoParams<I> setAggregates(Aggregates aggregates) {
        this.aggregates = aggregates;
        return this;
    }

    public ReadMongoParams<I> setFilter(Document filter) {
        this.aggregates.filter = filter;
        return this;
    }

    public ReadMongoParams<I> setProjection(Document projection) {
        this.aggregates.projection = projection;
        return this;
    }

    public ReadMongoParams<I> setSort(Document sort) {
        this.aggregates.sort = sort;
        return this;
    }

    @Nonnull
    public String getDatabaseName() {
        return databaseName;
    }

    @Nonnull
    public ReadMongoParams<I> setDatabaseName(@Nonnull String databaseName) {
        this.databaseName = databaseName;
        return this;
    }

    @Nonnull
    public String getCollectionName() {
        return collectionName;
    }

    public ReadMongoParams<I> setCollectionName(@Nonnull String collectionName) {
        this.collectionName = collectionName;
        return this;
    }

    public FunctionEx<Document, I> getMapItemFn() {
        return mapItemFn;
    }

    public ReadMongoParams<I> setMapItemFn(@Nonnull FunctionEx<Document, I> mapItemFn) {
        checkNonNullAndSerializable(mapItemFn, "mapFn");
        this.mapItemFn = mapItemFn;
        return this;
    }

    public BsonTimestamp getStartAtTimestamp() {
        return startAtTimestamp == null ? null : new BsonTimestamp(startAtTimestamp);
    }

    public ReadMongoParams<I> setStartAtTimestamp(BsonTimestamp startAtTimestamp) {
        this.startAtTimestamp = startAtTimestamp == null ? null : startAtTimestamp.getValue();
        return this;
    }

    public EventTimePolicy<? super I> getEventTimePolicy() {
        return eventTimePolicy;
    }

    public ReadMongoParams<I> setEventTimePolicy(EventTimePolicy<? super I> eventTimePolicy) {
        this.eventTimePolicy = eventTimePolicy;
        return this;
    }

    public BiFunctionEx<ChangeStreamDocument<Document>, Long, I> getMapStreamFn() {
        return mapStreamFn;
    }

    public ReadMongoParams<I> setMapStreamFn(BiFunctionEx<ChangeStreamDocument<Document>, Long, I> mapStreamFn) {
        this.mapStreamFn = mapStreamFn;
        return this;
    }

    public ReadMongoParams<I> setNonDistributed(boolean nonDistributed) {
        this.nonDistributed = nonDistributed;
        return this;
    }

    public boolean isNonDistributed() {
        return nonDistributed;
    }

    public boolean isCheckExistenceOnEachConnect() {
        return checkExistenceOnEachConnect;
    }

    /**
     * If true, the database and collection existence checks will be performed on every reconnection.
     */
    public ReadMongoParams<I> setCheckExistenceOnEachConnect(boolean checkExistenceOnEachConnect) {
        this.checkExistenceOnEachConnect = checkExistenceOnEachConnect;
        return this;
    }

    public static final class Aggregates implements Serializable {
        private Document filter;
        private Document projection;
        private Document sort;

        public List<Document> nonNulls() {
            var list = new ArrayList<Document>();
            if (filter != null) {
                list.add(filter);
            }
            if (projection != null) {
                list.add(projection);
            }
            if (sort != null) {
                list.add(sort);
            }
            return list;
        }

        public int indexAfterFilter() {
            return filter == null ? 0 : 1;
        }

        public void setFilter(Document filter) {
            this.filter = filter;
        }

        public void setProjection(Document projection) {
            this.projection = projection;
        }

        public void setSort(Document sort) {
            this.sort = sort;
        }

        public Document getFilter() {
            return filter;
        }

        public Document getProjection() {
            return projection;
        }

        public Document getSort() {
            return sort;
        }
    }
}
