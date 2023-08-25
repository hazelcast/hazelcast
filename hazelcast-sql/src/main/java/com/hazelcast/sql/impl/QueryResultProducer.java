/*
 * Copyright 2023 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.sql.impl;

import com.hazelcast.sql.impl.row.JetSqlRow;

/**
 * Generic interface which produces iterator over results which are then delivered to users.
 * Returned iterator must provide rows which were not returned yet.
 */
public interface QueryResultProducer {
    /**
     * Get iterator over results. Subsequent calls must return the same instance.
     *
     * @return Iterator.
     */
    ResultIterator<JetSqlRow> iterator();

    /**
     * Notify the producer about an error.
     *
     * @param error Error.
     */
    void onError(QueryException error);
}
