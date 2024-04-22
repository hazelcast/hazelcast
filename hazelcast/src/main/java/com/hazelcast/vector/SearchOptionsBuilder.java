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
 * Search options builder
 *
 * @since 5.5
 */
@Beta
public class SearchOptionsBuilder {
    private boolean includePayload;
    private boolean includeVectors;
    private int limit = 1;
    private VectorValues vectors;
    private Map<String, String> hints;

    public SearchOptionsBuilder includePayload() {
        this.includePayload = true;
        return this;
    }

    public SearchOptionsBuilder setIncludePayload(boolean includePayload) {
        this.includePayload = includePayload;
        return this;
    }

    public SearchOptionsBuilder includeVectors() {
        this.includeVectors = true;
        return this;
    }

    public SearchOptionsBuilder setIncludeVectors(boolean includeVectors) {
        this.includeVectors = includeVectors;
        return this;
    }

    public SearchOptionsBuilder limit(int limit) {
        this.limit = limit;
        return this;
    }

    public SearchOptionsBuilder vectors(VectorValues vectors) {
        this.vectors = vectors;
        return this;
    }

    public SearchOptionsBuilder vector(float[] vector) {
        this.vectors = VectorValues.of(vector);
        return this;
    }

    public <T> SearchOptionsBuilder hint(String hintName, T value) {
        if (hints == null) {
            hints = new HashMap<>();
        }
        hints.put(hintName, value.toString());
        return this;
    }

    public <T> SearchOptionsBuilder hint(Hint<T> hint, T value) {
        if (hints == null) {
            hints = new HashMap<>();
        }
        hints.put(hint.name(), value.toString());
        return this;
    }

    public SearchOptionsBuilder hints(Map<String, String> hints) {
        if (this.hints == null) {
            this.hints = new HashMap<>();
        }
        this.hints.putAll(hints);
        return this;
    }

    public SearchOptions build() {
        return new SearchOptionsImpl(includePayload, includeVectors, limit, vectors, hints);
    }
}
