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

package com.hazelcast.sql.impl.calcite.schema.statistic;

import org.apache.calcite.util.ImmutableBitSet;

import java.util.List;

/**
 * Simple table statistics.
 */
public class TableStatistic extends TableStatisticAdapter {
    /** Row count. */
    private final Long rowCount;

    public TableStatistic(long rowCount) {
        this((Long) rowCount);
    }

    public TableStatistic(Long rowCount) {
        this.rowCount = rowCount;
    }

    @Override
    public Double getRowCount() {
        return rowCount != null ? (double) rowCount : null;
    }

    @Override
    public List<ImmutableBitSet> getKeys() {
        // TODO: Do we need to reutrn __key here?
        return null;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{rowCount=" + rowCount + '}';
    }
}
