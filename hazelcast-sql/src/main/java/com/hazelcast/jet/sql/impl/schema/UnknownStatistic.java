/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.sql.impl.schema;


import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.List;

import static java.util.Collections.emptyList;

public final class UnknownStatistic implements Statistic {

    public static final UnknownStatistic INSTANCE = new UnknownStatistic();

    private UnknownStatistic() {
    }

    @Override
    public Double getRowCount() {
        return null;
    }

    @Override
    public boolean isKey(ImmutableBitSet columns) {
        return false;
    }

    @Override
    public List<ImmutableBitSet> getKeys() {
        return null;
    }

    @Override
    public List<RelReferentialConstraint> getReferentialConstraints() {
        return null;
    }

    @Override
    public List<RelCollation> getCollations() {
        return emptyList();
    }

    @Override
    public RelDistribution getDistribution() {
        return null;
    }
}
