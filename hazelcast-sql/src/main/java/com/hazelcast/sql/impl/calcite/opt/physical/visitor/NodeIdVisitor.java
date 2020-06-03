/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.calcite.opt.physical.visitor;

import com.hazelcast.sql.impl.calcite.opt.physical.PhysicalRel;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

/**
 * Visitor that assigns unique numbers to physical rels. These numbers are propagated to the plan and executor nodes
 * in order to facilitate compilation, monitoring and debugging.
 */
public class NodeIdVisitor extends PhysicalRelVisitorAdapter {
    /** Initial index. */
    private int curId;

    /** Map from the node to its indexes as they are found in the tree bottom-up. */
    private final IdentityHashMap<PhysicalRel, List<Integer>> idMap = new IdentityHashMap<>();

    public Map<PhysicalRel, List<Integer>> getIdMap() {
        return Collections.unmodifiableMap(idMap);
    }

    @Override
    protected void onNode(PhysicalRel rel) {
        int idx = curId++;

        // One rel may be located in several places in the tree. Hence, we use List instead of a single entry.
        idMap.computeIfAbsent(rel, (k) -> new ArrayList<>(1)).add(idx);
    }
}
