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
import com.hazelcast.vector.impl.SearchOptionsImpl;

import java.util.HashMap;
import java.util.Map;

/**
 * Vector search options builder
 *
 * @since 5.5
 */
@Beta
public class SearchOptionsBuilder {
    private static final int DEFAULT_SEARCH_LIMIT = 10;
    private boolean includeValue;
    private boolean includeVectors;
    private int limit = DEFAULT_SEARCH_LIMIT;
    private Map<String, String> hints;

    /**
     * Includes the user-supplied value in search results.
     *
     * @return this builder instance
     * @see SearchOptions#isIncludeValue()
     */
    public SearchOptionsBuilder includeValue() {
        this.includeValue = true;
        return this;
    }

    /**
     * Includes or excludes the user-supplied value in search results.
     *
     * @param includeValue {@code true} if search results should include the user-supplied value or
     *                     {@code false} if only keys should be returned
     * @return this builder instance
     * @see SearchOptions#isIncludeValue()
     */
    public SearchOptionsBuilder setIncludeValue(boolean includeValue) {
        this.includeValue = includeValue;
        return this;
    }

    /**
     * Includes the vectors associated with each search result in search results.
     *
     * @return this builder instance
     * @see SearchOptions#isIncludeVectors()
     */
    public SearchOptionsBuilder includeVectors() {
        this.includeVectors = true;
        return this;
    }

    /**
     * Includes or excludes the vectors associated with each search result in search results.
     *
     * @param includeVectors {@code true} if search results should include the vectors associated with
     *                       each search result, {@code false} otherwise
     * @return this builder instance
     * @see SearchOptions#isIncludeVectors()
     */
    public SearchOptionsBuilder setIncludeVectors(boolean includeVectors) {
        this.includeVectors = includeVectors;
        return this;
    }

    /**
     * Sets the number of search results to return.
     *
     * @param limit the number of search results to return. Must be positive.
     * @return this builder instance
     * @see SearchOptions#getLimit()
     */
    public SearchOptionsBuilder limit(int limit) {
        if (limit <= 0) {
            throw new IllegalArgumentException("Limit must be positive");
        }
        this.limit = limit;
        return this;
    }

    /**
     * Adds search hint. Hints allow fine-tuning of the query execution.
     *
     * @param hintName name of the hint
     * @param value    value of the hint
     * @param <T>      type of hint value
     * @return this builder instance
     * @see SearchOptions#getHints()
     */
    public <T> SearchOptionsBuilder hint(String hintName, T value) {
        if (hints == null) {
            hints = new HashMap<>();
        }
        hints.put(hintName, value.toString());
        return this;
    }

    /**
     * Adds search hint. Hints allow fine-tuning of the query execution.
     *
     * @param hint  hint definition
     * @param value value of the hint
     * @param <T>   type of hint value
     * @return this builder instance
     * @see SearchOptions#getHints()
     */
    public <T> SearchOptionsBuilder hint(Hint<T> hint, T value) {
        if (hints == null) {
            hints = new HashMap<>();
        }
        hints.put(hint.name(), value.toString());
        return this;
    }

    /**
     * Sets search hints. Hints allow fine-tuning of the query execution.
     *
     * @param hints map of hint names and values
     * @return this builder instance
     * @see SearchOptions#getHints()
     */
    public SearchOptionsBuilder hints(Map<String, String> hints) {
        if (this.hints == null) {
            this.hints = new HashMap<>();
        }
        this.hints.putAll(hints);
        return this;
    }

    /**
     * Creates {@link SearchOptions}
     *
     * @return SearchOptions instance
     */
    public SearchOptions build() {
        return new SearchOptionsImpl(includeValue, includeVectors, limit, hints);
    }
}
