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

import com.hazelcast.util.function.Supplier;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class LazyDuplicateDetectingMultiResult<T> extends LazyMultiResultSet<T> {

    private final List<Supplier<Collection<T>>> resultSuppliers
            = new ArrayList<Supplier<Collection<T>>>();
    private int estimatedSize;

    @Override
    public void addResultSetSupplier(Supplier<Collection<T>> resultSupplier, int resultSize) {
        resultSuppliers.add(resultSupplier);
        estimatedSize += resultSize;
    }

    @Nonnull
    @Override
    protected Set<T> initialize() {
        if (resultSuppliers.isEmpty()) {
            return Collections.emptySet();
        }
        //Since duplicate detection required, we're paying copy cost again
        //TODO : check what can be done to prevent second copy cost
        Set<T> results = new HashSet<T>();
        for (Supplier<Collection<T>> resultSupplier : resultSuppliers) {
            Collection<T> result = resultSupplier.get();
            results.addAll(result);
        }
        return Collections.unmodifiableSet(results);
    }

    @Override
    public int estimatedSize() {
        return estimatedSize;
    }
}
