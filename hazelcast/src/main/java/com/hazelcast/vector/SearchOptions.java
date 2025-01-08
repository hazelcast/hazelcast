/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.vector.impl.SearchOptionsImpl;

import java.util.Map;

/**
 * Vector search options
 *
 * @since 5.5
 */
@Beta
public interface SearchOptions {

    /**
     * @return  {@code true} if search results should include the user-supplied value or
     *          {@code false} if only keys should be returned.
     */
    boolean isIncludeValue();

    /**
     * @return  {@code true} if search results should include the vectors associated with
     *          each search result, {@code false} otherwise.
     */
    boolean isIncludeVectors();

    /**
     * @return the number of search results to return
     */
    int getLimit();

    /**
     * @return hints for search execution
     */
    Map<String, String> getHints();

    /**
     * @return SearchOptionsBuilder with the same settings
     * @since 6.0
     */
    SearchOptionsBuilder toBuilder();

    /**
     * @return builder of {@link SearchOptions}
     */
    static SearchOptionsBuilder builder() {
        return new SearchOptionsBuilder();
    }

    static SearchOptions of(int limit, boolean includeValue, boolean includeVectors) {
        return new SearchOptionsImpl(includeValue, includeVectors, limit, Map.of());
    }
}
