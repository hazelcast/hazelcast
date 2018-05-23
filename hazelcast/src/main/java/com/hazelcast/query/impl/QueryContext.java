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

/**
 * Provides the context for queries execution.
 */
public class QueryContext {
    protected Indexes indexes;

    /**
     * Creates a new query context with the given available indexes.
     *
     * @param indexes the indexes available for the query context.
     */
    public QueryContext(Indexes indexes) {
        this.indexes = indexes;
    }

    /**
     * Creates a new query context unattached to any indexes.
     */
    QueryContext() {
    }

    /**
     * Attaches this index context to the given indexes.
     *
     * @param indexes the indexes to attach to.
     */
    void attachTo(Indexes indexes) {
        this.indexes = indexes;
    }

    /**
     * Obtains the index available for the given attribute in this query
     * context.
     *
     * @param attributeName the name of the attribute to obtain the index for.
     * @return the obtained index or {@code null} if there is no index available
     * for the given attribute.
     */
    public Index getIndex(String attributeName) {
        if (indexes == null) {
            return null;
        } else {
            return indexes.getIndex(attributeName);
        }
    }

    /**
     * Applies the collected per-query stats, if any.
     */
    void applyPerQueryStats() {
        // do nothing
    }

}
