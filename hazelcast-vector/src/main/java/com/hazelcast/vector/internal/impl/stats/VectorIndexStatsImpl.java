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

package com.hazelcast.vector.internal.impl.stats;

import java.util.Objects;

import com.hazelcast.internal.tpcengine.util.ReflectionUtil;

import java.lang.invoke.VarHandle;

import static com.hazelcast.internal.util.JVMUtil.OBJECT_HEADER_SIZE;

public class VectorIndexStatsImpl implements VectorIndexStats {
    public static final long FIXED_HEAP_BYTES_USED = OBJECT_HEADER_SIZE + 2 * Long.BYTES;

    private static final VarHandle QUERY_COUNT = ReflectionUtil.findVarHandle("queryCount", long.class);
    private static final VarHandle VISITED_NODES = ReflectionUtil.findVarHandle("visitedNodes", long.class);

    private volatile long queryCount;
    private volatile long visitedNodes;

    @Override
    public long getQueryCount() {
        return queryCount;
    }

    public void incrementQueryCount() {
        QUERY_COUNT.setOpaque(this, queryCount + 1);
    }

    @Override
    public long getVisitedNodes() {
        return visitedNodes;
    }

    public void addVisitedNodes(int nodes) {
        VISITED_NODES.setOpaque(this, visitedNodes + nodes);
    }

    // not invoked concurrently
    public VectorIndexStats add(VectorIndexStats other) {
        QUERY_COUNT.setOpaque(this, queryCount + other.getQueryCount());
        VISITED_NODES.setOpaque(this, visitedNodes + other.getVisitedNodes());
        return this;
    }

    public void set(VectorIndexStats other) {
        queryCount = other.getQueryCount();
        visitedNodes = other.getVisitedNodes();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        VectorIndexStatsImpl that = (VectorIndexStatsImpl) o;
        return queryCount == that.queryCount && visitedNodes == that.visitedNodes;
    }

    @Override
    public int hashCode() {
        return Objects.hash(queryCount, visitedNodes);
    }
}
