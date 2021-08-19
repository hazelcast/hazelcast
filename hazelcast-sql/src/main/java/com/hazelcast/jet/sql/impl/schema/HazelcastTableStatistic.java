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

package com.hazelcast.jet.sql.impl.schema;

import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.Collections;
import java.util.List;

/**
 * Simple table statistics for Hazelcast tables.
 */
public class HazelcastTableStatistic implements Statistic {
    /** Row count that is fixed for the duration of query optimization process. */
    private final Long rowCount;

    public HazelcastTableStatistic(long rowCount) {
        this.rowCount = rowCount;
    }

    @Override
    public Double getRowCount() {
        return (double) rowCount;
    }

    @Override
    public boolean isKey(ImmutableBitSet columns) {
        // See getKeys().
        return false;
    }

    @Override
    public List<ImmutableBitSet> getKeys() {
        // We do not return any keys at the moment because the optimizer to be released do not use any of rules that may benefit
        // from unique keys. When it is time to implement more advanced things such as aggregations and joins, this statistic
        // will be very important, because it is used in a number of optimization rules.
        // See BuiltInMetadata.ColumnUniqueness and BuiltInMetadata.UniqueKeys.
        return Collections.emptyList();
    }

    @Override
    public List<RelReferentialConstraint> getReferentialConstraints() {
        // Hazelcast do not have referential constraints.
        return Collections.emptyList();
    }

    @Override
    public List<RelCollation> getCollations() {
        // Entries in IMap and ReplicatedMap are not sorted.
        return Collections.emptyList();
    }

    @Override
    public RelDistribution getDistribution() {
        // We do not use Calcite distributions, so just returning the default value here.
        return RelDistributionTraitDef.INSTANCE.getDefault();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{rowCount=" + rowCount + '}';
    }
}
