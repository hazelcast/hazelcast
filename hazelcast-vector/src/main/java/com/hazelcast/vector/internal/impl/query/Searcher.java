/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.vector.internal.impl.query;

import com.hazelcast.client.impl.ClientEndpoint;
import com.hazelcast.internal.memory.Measurable;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.vector.SearchOptions;
import com.hazelcast.vector.SearchResults;
import com.hazelcast.vector.VectorValues;

import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;

/**
 * Strategy to search in distributed vector collection
 */
public interface Searcher extends Measurable {
    CompletableFuture<SearchResults<Data, Data>> search(String collectionName, VectorValues vectors, SearchOptions options,
                                                        @Nullable ClientEndpoint endpoint);

    default CompletableFuture<SearchResults<Data, Data>> search(
            String collectionName,
            VectorValues vectors,
            SearchOptions options
    ) {
        return search(collectionName, vectors, options, null);
    }

    default Searcher getBaseSearcher() {
        return this;
    }
}
