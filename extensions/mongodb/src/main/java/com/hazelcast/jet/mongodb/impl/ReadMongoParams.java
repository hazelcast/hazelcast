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
import java.util.Objects;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;

@SuppressWarnings({"UnusedReturnValue", "unused"})
public class ReadMongoParams<I> implements Serializable {
    private final boolean stream;
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

    public SupplierEx<? extends MongoClient> getClientSupplier() {
        return clientSupplier;
    }

    public ReadMongoParams<I> setClientSupplier(SupplierEx<? extends MongoClient> clientSupplier) {
        this.clientSupplier = clientSupplier;
        return this;
    }

    @Nonnull
    public List<Bson> getAggregates() {
        return aggregates;
    }

    public ReadMongoParams<I> setAggregates(List<Bson> aggregates) {
        this.aggregates = aggregates;
        return this;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public ReadMongoParams<I> setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
        return this;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public ReadMongoParams<I> setCollectionName(String collectionName) {
        this.collectionName = collectionName;
        return this;
    }

    public FunctionEx<Document, I> getMapItemFn() {
        return mapItemFn;
    }

    public ReadMongoParams<I> setMapItemFn(FunctionEx<Document, I> mapItemFn) {
        checkNotNull(mapItemFn, "mapFn argument cannot be null");
        checkSerializable(mapItemFn, "mapFn must be serializable");
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

    public ReadMongoParams<I> addAggregate(Bson doc) {
        this.aggregates.add(doc);
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ReadMongoParams)) {
            return false;
        }
        ReadMongoParams<?> that = (ReadMongoParams<?>) o;
        return Objects.equals(clientSupplier, that.clientSupplier) && Objects.equals(aggregates, that.aggregates) && Objects.equals(databaseName, that.databaseName) && Objects.equals(collectionName, that.collectionName) && Objects.equals(mapItemFn, that.mapItemFn) && Objects.equals(startAtTimestamp, that.startAtTimestamp) && Objects.equals(eventTimePolicy, that.eventTimePolicy) && Objects.equals(mapStreamFn, that.mapStreamFn);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clientSupplier, aggregates, databaseName, collectionName, mapItemFn, startAtTimestamp, eventTimePolicy, mapStreamFn);
    }
}
