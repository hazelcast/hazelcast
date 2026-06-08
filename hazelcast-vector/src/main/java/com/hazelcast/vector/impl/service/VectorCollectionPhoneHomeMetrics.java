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

package com.hazelcast.vector.impl.service;

import com.hazelcast.internal.util.phonehome.Metric;

public enum VectorCollectionPhoneHomeMetrics implements Metric {

    // int: count of vector collections configured
    VECTOR_COLLECTION_COUNT("vcc"),
    // int: count of vector collection indexes configured
    VECTOR_COLLECTION_INDEX_COUNT("vcic"),
    // int[]: vector index dimensions
    VECTOR_COLLECTION_INDEX_DIMENSIONS("vcid"),
    // long: heap usage of vector collections
    VECTOR_COLLECTION_HEAP_USAGE("vchu"),
    // int[]: total backup counts for vector collections
    VECTOR_COLLECTION_TOTAL_BACKUP_COUNTS("vctbc"),
    // boolean: is Vector API available
    VECTOR_COLLECTION_VECTOR_API_AVAILABLE("vcvaa");

    private final String query;

    VectorCollectionPhoneHomeMetrics(String query) {
        this.query = query;
    }

    @Override
    public String getQueryParameter() {
        return query;
    }
}
