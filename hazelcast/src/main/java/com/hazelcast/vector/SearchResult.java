/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.vector;

import com.hazelcast.spi.annotation.Beta;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Single vector search result
 * @param <K>
 * @param <V>
 *
 * @since 5.5
 */
@Beta
public interface SearchResult<K, V> {

    /**
     * @return id of the document
     */
    @Nonnull
    K getKey();

    /**
     * @return  the complete document payload, if it was requested to be included, otherwise
     *          {@code null}
     * @see     SearchOptions#isIncludeValue()
     */
    @Nullable
    V getDocument();

    /**
     * @return  the document's vectors, if requested to be included, otherwise {@code null}
     * @see     SearchOptions#isIncludeVectors()
     */
    @Nullable
    VectorValues getVectors();

    float getScore();
}
