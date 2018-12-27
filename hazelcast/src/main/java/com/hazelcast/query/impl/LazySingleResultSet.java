/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.query.impl;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.impl.collections.LazySet;
import com.hazelcast.query.impl.collections.ReadOnlyMapDelegate;
import com.hazelcast.util.function.Supplier;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Multiple result set for Predicates.
 */
public class LazySingleResultSet extends LazySet<QueryableEntry> {

    private final Supplier<Map<Data, QueryableEntry>> resultSupplier;
    private final int estimatedSize;

    LazySingleResultSet(Supplier<Map<Data, QueryableEntry>> resultSupplier, int resultSize) {
        this.resultSupplier = resultSupplier;
        this.estimatedSize = resultSize;
    }

    @Nonnull
    @Override
    protected Set<QueryableEntry> initialize() {
        Map<Data, QueryableEntry> records = resultSupplier.get();
        return records == null ? Collections.<QueryableEntry>emptySet() : new ReadOnlyMapDelegate(records);
    }

    @Override
    public int estimatedSize() {
        return estimatedSize;
    }

}
