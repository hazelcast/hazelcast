/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.query;

import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.util.IterationType;
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.impl.Versioned;
import com.hazelcast.projection.Projection;
import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.Predicate;

import javax.annotation.Nullable;
import java.io.IOException;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * Object representing a Query together with all possible co-variants: like a predicate, iterationType, etc.
 */
public class Query implements IdentifiedDataSerializable, Versioned {

    private String mapName;
    private Predicate predicate;
    private IterationType iterationType;
    private Aggregator aggregator;
    private Projection projection;
    private PartitionIdSet partitionIdSet;

    public Query() {
    }

    public Query(
            String mapName,
            Predicate predicate,
            IterationType iterationType,
            Aggregator aggregator,
            Projection projection,
            PartitionIdSet partitionIdSet
    ) {
        this.mapName = checkNotNull(mapName);
        this.predicate = checkNotNull(predicate);
        this.iterationType = checkNotNull(iterationType);

        this.aggregator = aggregator;
        this.projection = projection;
        if (aggregator != null && projection != null) {
            throw new IllegalArgumentException("It's forbidden to use a Projection with an Aggregator.");
        }
        this.partitionIdSet = partitionIdSet;
    }

    public String getMapName() {
        return mapName;
    }

    public Predicate getPredicate() {
        return predicate;
    }

    public IterationType getIterationType() {
        return iterationType;
    }

    public Aggregator getAggregator() {
        return aggregator;
    }

    public Class<? extends Result> getResultType() {
        if (isAggregationQuery()) {
            return AggregationResult.class;
        } else {
            return QueryResult.class;
        }
    }

    public boolean isAggregationQuery() {
        return aggregator != null;
    }

    public Projection getProjection() {
        return projection;
    }

    public boolean isProjectionQuery() {
        return projection != null;
    }

    @Nullable
    public PartitionIdSet getPartitionIdSet() {
        return partitionIdSet;
    }

    public Result createResult(SerializationService serializationService, long limit) {
        if (isAggregationQuery()) {
            Aggregator aggregatorClone = serializationService.toObject(serializationService.toData(aggregator));
            return new AggregationResult(aggregatorClone, serializationService);
        } else {
            return new QueryResult(iterationType, projection, serializationService, limit, predicate instanceof PagingPredicate);
        }
    }

    public static QueryBuilder of() {
        return new QueryBuilder();
    }

    public static QueryBuilder of(Query query) {
        return new QueryBuilder(query);
    }

    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.QUERY;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(mapName);
        out.writeObject(predicate);
        out.writeByte(iterationType.getId());
        out.writeObject(aggregator);
        out.writeObject(projection);
        if (out.getVersion().isGreaterOrEqual(Versions.V4_2)) {
            out.writeObject(partitionIdSet);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        this.mapName = in.readString();
        this.predicate = in.readObject();
        this.iterationType = IterationType.getById(in.readByte());
        this.aggregator = in.readObject();
        this.projection = in.readObject();
        if (in.getVersion().isGreaterOrEqual(Versions.V4_2)) {
            this.partitionIdSet = in.readObject();
        }
    }

    public static final class QueryBuilder {
        private String mapName;
        private Predicate predicate;
        private IterationType iterationType;
        private Aggregator aggregator;
        private Projection projection;
        private PartitionIdSet partitionIdSet;

        private QueryBuilder() {
        }

        private QueryBuilder(Query query) {
            this.mapName = query.mapName;
            this.predicate = query.predicate;
            this.iterationType = query.iterationType;
            this.aggregator = query.aggregator;
            this.projection = query.projection;
            this.partitionIdSet = query.partitionIdSet;
        }

        public QueryBuilder mapName(String mapName) {
            this.mapName = mapName;
            return this;
        }

        public QueryBuilder predicate(Predicate predicate) {
            this.predicate = predicate;
            return this;
        }

        public QueryBuilder iterationType(IterationType iterationType) {
            this.iterationType = iterationType;
            return this;
        }

        public QueryBuilder aggregator(Aggregator aggregator) {
            this.aggregator = aggregator;
            return this;
        }

        public QueryBuilder projection(Projection projection) {
            this.projection = projection;
            return this;
        }

        public QueryBuilder partitionIdSet(PartitionIdSet partitionIdSet) {
            this.partitionIdSet = partitionIdSet;
            return this;
        }

        public Query build() {
            return new Query(mapName, predicate, iterationType, aggregator, projection, partitionIdSet);
        }
    }
}
