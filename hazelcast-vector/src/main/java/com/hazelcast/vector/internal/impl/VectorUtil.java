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

package com.hazelcast.vector.internal.impl;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.vector.SearchResult;
import com.hazelcast.vector.SearchResults;
import com.hazelcast.vector.VectorDocument;
import com.hazelcast.vector.impl.DataVectorDocument;
import com.hazelcast.vector.impl.InternalSearchResult;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class VectorUtil {
    private VectorUtil() {
    }

    public static <V> DataVectorDocument serialize(VectorDocument<V> vectorDocument,
                                                    SerializationService serializationService) {
        if (vectorDocument instanceof DataVectorDocument dataVectorDocument) {
            return dataVectorDocument;
        }
        if (vectorDocument == null) {
            return null;
        }

        Data valueData = serializationService.toData(vectorDocument.getValue());
        return new DataVectorDocument(valueData, vectorDocument.getVectors());
    }

    public static <V> VectorDocument<V> deserialize(VectorDocument<Data> dataVectorDocument,
                                                    SerializationService serializationService) {
        if (dataVectorDocument == null) {
            return null;
        }
        V userValue = serializationService.toObject(dataVectorDocument.getValue());
        return VectorDocument.of(userValue, dataVectorDocument.getVectors());
    }

    public static <K, V> SearchResults<K, V> deserialize(SearchResults<Data, Data> dataSearchResults,
                                            SerializationService serializationService) {
        List<SearchResult<K, V>> results = new ArrayList<>(dataSearchResults.size());
        for (Iterator<? extends SearchResult<Data, Data>> it = dataSearchResults.results(); it.hasNext(); ) {
            InternalSearchResult<Data, Data> r = (InternalSearchResult<Data, Data>) it.next();
            results.add(new SearchResultImpl<K, V>(r.id(), serializationService.toObject(r.getKey()), r.getScore())
                    .setValue(serializationService.toObject(r.getValue()))
                    .setVectors(r.getVectors()));
        }
        return new SearchResultsImpl<>(results);
    }
}
