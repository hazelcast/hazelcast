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

import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.EventTimePolicy;
import com.mongodb.client.MongoClient;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.bson.Document;
import org.bson.conversions.Bson;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.jet.impl.util.Util.checkNonNullAndSerializable;

@SuppressWarnings({"UnusedReturnValue", "unused"})
public class ReadMongoParams<I> implements Serializable {
    final boolean stream;
    SupplierEx<? extends MongoClient> clientSupplier;
    List<Bson> aggregates = new ArrayList<>();
    String databaseName;
    String collectionName;
    FunctionEx<Document, I> mapItemFn;

    Long startAtTimestamp;
    EventTimePolicy<? super I> eventTimePolicy;
    FunctionEx<ChangeStreamDocument<Document>, I> mapStreamFn;

    public ReadMongoParams(boolean stream) {
        this.stream = stream;
    }

    public boolean isStream() {
        return stream;
    }

    @Nonnull
    public SupplierEx<? extends MongoClient> getClientSupplier() {
        return clientSupplier;
    }

    public ReadMongoParams<I> setClientSupplier(@Nonnull SupplierEx<? extends MongoClient> clientSupplier) {
        this.clientSupplier = clientSupplier;
        return this;
    }

    @Nonnull
    public List<Bson> getAggregates() {
        return aggregates;
    }

    public ReadMongoParams<I> setAggregates(@Nonnull List<Bson> aggregates) {
        this.aggregates = aggregates;
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

    public Long getStartAtTimestamp() {
        return startAtTimestamp;
    }

    public ReadMongoParams<I> setStartAtTimestamp(Long startAtTimestamp) {
        this.startAtTimestamp = startAtTimestamp;
        return this;
    }

    public EventTimePolicy<? super I> getEventTimePolicy() {
        return eventTimePolicy;
    }

    public ReadMongoParams<I> setEventTimePolicy(EventTimePolicy<? super I> eventTimePolicy) {
        this.eventTimePolicy = eventTimePolicy;
        return this;
    }

    public FunctionEx<ChangeStreamDocument<Document>, I> getMapStreamFn() {
        return mapStreamFn;
    }

    public ReadMongoParams<I> setMapStreamFn(FunctionEx<ChangeStreamDocument<Document>, I> mapStreamFn) {
        this.mapStreamFn = mapStreamFn;
        return this;
    }

    public ReadMongoParams<I> addAggregate(@Nonnull Bson doc) {
        this.aggregates.add(doc);
        return this;
    }

}
