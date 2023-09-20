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

package com.hazelcast.sql.impl.state;

import com.hazelcast.sql.impl.QueryResultProducer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry that tracks active jobs started from a member.
 */
public class QueryResultRegistry {

    /**
     * IDs of locally started jobs.
     */
    private final Map<Long, QueryResultProducer> results;

    public QueryResultRegistry() {
        this.results = new ConcurrentHashMap<>();
    }

    public QueryResultProducer store(long id, QueryResultProducer resultProducer) {
        return results.put(id, resultProducer);
    }

    @SuppressWarnings("unchecked")
    public <T extends QueryResultProducer> T remove(long id) {
        return (T) results.remove(id);
    }

    public int getResultCount() {
        return results.size();
    }

    public void shutdown() {
        results.clear();
    }
}
