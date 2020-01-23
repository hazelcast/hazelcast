/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.calcite.opt.physical.agg;

import com.hazelcast.sql.impl.calcite.opt.OptUtils;
import com.hazelcast.sql.impl.calcite.opt.logical.AggregateLogicalRel;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.ArrayList;
import java.util.List;

/**
 * Collation of the given aggregate. Defines the collation and sorted prefix which should be passed to the physical aggregate.
 */
public final class AggregateCollation {
    /** Collation. */
    private final RelCollation collation;

    /** Prefix of sorted fields within the aggregate. */
    private final ImmutableBitSet sortedGroupSet;

    private AggregateCollation(RelCollation collation, ImmutableBitSet sortedGroupSet) {
        this.collation = collation;
        this.sortedGroupSet = sortedGroupSet;
    }

    public static AggregateCollation of(AggregateLogicalRel agg, RelNode input) {
        ImmutableBitSet groupSet = agg.getGroupSet();
        List<RelFieldCollation> inputFieldCollations = OptUtils.getCollation(input).getFieldCollations();

        return of(groupSet, inputFieldCollations);
    }

    public static AggregateCollation of(ImmutableBitSet groupSet, List<RelFieldCollation> inputFieldCollations) {
        List<Integer> groupSet0 = groupSet.toList();

        int minLen = Math.min(inputFieldCollations.size(), groupSet0.size());

        List<RelFieldCollation> resFieldCollations = new ArrayList<>(minLen);
        List<Integer> resSortedGroupSet = new ArrayList<>(minLen);

        for (int i = 0; i < minLen; i++) {
            RelFieldCollation inputFieldCollation = inputFieldCollations.get(i);
            int inputFieldCollationIndex = inputFieldCollation.getFieldIndex();

            if (inputFieldCollationIndex == groupSet0.get(i)) {
                resFieldCollations.add(inputFieldCollation);
                resSortedGroupSet.add(inputFieldCollationIndex);
            } else {
                break;
            }
        }

        return new AggregateCollation(RelCollations.of(resFieldCollations), ImmutableBitSet.of(resSortedGroupSet));
    }

    public RelCollation getCollation() {
        return collation;
    }

    public ImmutableBitSet getSortedGroupSet() {
        return sortedGroupSet;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{collation=" + collation + ", sortedGroupSet=" + sortedGroupSet + '}';
    }
}

