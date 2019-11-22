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

package com.hazelcast.sql.impl;

import com.hazelcast.sql.impl.physical.PhysicalNode;

/**
 * A single element of a {@link QueryExplain}
 */
public class QueryExplainElement {
    /** Owning fragment. */
    private final QueryFragment fragment;

    /** Node. */
    private final PhysicalNode node;

    /** Nesting level. */
    private final int level;

    public QueryExplainElement(QueryFragment fragment, PhysicalNode node, int level) {
        this.fragment = fragment;
        this.node = node;
        this.level = level;
    }

    public QueryFragment getFragment() {
        return fragment;
    }

    public PhysicalNode getNode() {
        return node;
    }

    public int getLevel() {
        return level;
    }
}
