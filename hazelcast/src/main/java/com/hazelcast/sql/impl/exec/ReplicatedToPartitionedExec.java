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

package com.hazelcast.sql.impl.exec;

import com.hazelcast.internal.util.HashUtil;
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.sql.impl.row.hash.RowHashFunction;
import com.hazelcast.sql.impl.row.Row;

/**
 * Executor which converts replicated input to partitioned based on the given fields.
 */
public class ReplicatedToPartitionedExec extends AbstractFilterExec {
    /** Hash function. */
    private final RowHashFunction hashFunction;

    /** Owning partitions. */
    private final PartitionIdSet parts;

    public ReplicatedToPartitionedExec(int id, Exec upstream, RowHashFunction hashFunction, PartitionIdSet parts) {
        super(id, upstream);

        this.hashFunction = hashFunction;
        this.parts = parts;
    }

    @Override
    protected boolean eval(Row row) {
        int hash = hashFunction.getHash(row);
        int part = HashUtil.hashToIndex(hash, parts.getPartitionCount());

        return parts.contains(part);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{hashFunction=" + hashFunction + ", parts=" + parts
            + ", upstreamState=" + state + '}';
    }
}
