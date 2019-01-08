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

import com.hazelcast.query.impl.collections.ReadOnlyMultiCollectionDelegate;
import com.hazelcast.util.function.Supplier;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * Lazy Multiple result set for Predicates.
 */
public class LazyFastMultiResultSet<E> extends LazyMultiResultSet<E> {

    private final List<Supplier<Collection<E>>> resultSuppliers
            = new ArrayList<Supplier<Collection<E>>>();
    private int estimatedSize;

    @Override
    void addResultSetSupplier(Supplier<Collection<E>> resultSupplier, int resultSize) {
        resultSuppliers.add(resultSupplier);
        estimatedSize += resultSize;
    }

    @Nonnull
    @Override
    protected Set<E> initialize() {
        if (resultSuppliers.isEmpty()) {
            return Collections.emptySet();
        }
        List<Collection<E>> results = new LinkedList<Collection<E>>();
        int size = 0;
        for (Supplier<Collection<E>> resultSupplier : resultSuppliers) {
            Collection<E> result = resultSupplier.get();
            results.add(result);
            size += result.size();
        }
        return new ReadOnlyMultiCollectionDelegate(results, size);
    }

    @Override
    public int estimatedSize() {
        return estimatedSize;
    }

}
