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

package com.hazelcast.map.impl.query;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.monitor.impl.InternalIndexStats;
import com.hazelcast.query.impl.IndexCopyBehavior;
import com.hazelcast.query.impl.InternalIndex;
import com.hazelcast.query.impl.getters.Extractors;

/**
 * Provides storage-specific indexes to {@link com.hazelcast.query.impl.Indexes
 * Indexes}.
 */
public interface IndexProvider {

    /**
     * Creates a new index for the given attribute name.
     *
     * @param attributeName the attribute name to create the index for.
     * @param ordered       {@code true} to create an ordered index supporting
     *                      fast range queries, {@code false} to create an
     *                      unordered index supporting fast point queries only.
     * @param extractors    the extractors to extract values of the given
     *                      attribute.
     * @param ss            the serialization service to perform the
     *                      deserialization of entries while extracting values
     *                      from them.
     * @param copyBehavior  the desired index copy behaviour.
     * @param stats         the index stats instance to report the statistics to.
     * @return the created index instance.
     */
    InternalIndex createIndex(String attributeName, boolean ordered, Extractors extractors, InternalSerializationService ss,
                              IndexCopyBehavior copyBehavior, InternalIndexStats stats);

}
