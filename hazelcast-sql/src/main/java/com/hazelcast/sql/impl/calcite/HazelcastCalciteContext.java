/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.calcite;

import com.hazelcast.spi.impl.NodeEngine;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

import java.util.EnumMap;
import java.util.Map;

/**
 * Static context to keep local state during query planning.
 */
public final class HazelcastCalciteContext {
    /** Thread-local context. */
    private static final ThreadLocal<HazelcastCalciteContext> CTX = new ThreadLocal<>();

    /** Node engine. */
    private final NodeEngine nodeEngine;

    /** Cached data. */
    private final Map<Key, Object> data = new EnumMap<>(Key.class);

    private HazelcastCalciteContext(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
    }

    /**
     * @return Current context.
     */
    public static HazelcastCalciteContext get() {
        return CTX.get();
    }

    /**
     * Initialize the context.
     *
     * @param nodeEngine Node engine.
     */
    public static void initialize(NodeEngine nodeEngine) {
        assert CTX.get() == null;

        CTX.set(new HazelcastCalciteContext(nodeEngine));
    }

    /**
     * Clear the context.
     */
    public static void clear() {
        CTX.remove();

        RelMetadataQuery.THREAD_PROVIDERS.remove();
    }

    @SuppressWarnings("unchecked")
    public <T> T getData(Key key) {
        return (T) data.get(key);
    }

    public void setData(Key key, Object value) {
        data.put(key, value);
    }

    public enum Key {
        PARTITIONED_MAPS,
        REPLICATED_MAPS
    }
}
