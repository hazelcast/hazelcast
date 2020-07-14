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

package com.hazelcast.sql.impl.calcite.opt.physical.index;

import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

public class IndexConjunctiveFilter {
    /** All nodes. */
    private final List<RexNode> nodes;

    /** Map from node to position. */
    private final IdentityHashMap<RexNode, Integer> nodeToPosition;

    public IndexConjunctiveFilter(List<RexNode> nodes) {
        this.nodes = nodes;

        nodeToPosition = new IdentityHashMap<>();

        for (int i = 0; i < nodes.size(); i++) {
            nodeToPosition.put(nodes.get(i), i);
        }
    }

    public List<RexNode> getNodes() {
        return nodes;
    }

    /**
     * Constructs the remainder filter that has all the original expressions except for the given ones.
     *
     * @param exclusions Exclusions.
     * @return Remainder.
     */
    public List<RexNode> exclude(Set<RexNode> exclusions) {
        List<RexNode> res = new ArrayList<>(nodes.size());

        for (RexNode node : nodes) {
            if (exclusions.contains(node)) {
                continue;
            }

            res.add(node);
        }

        return res;
    }
}
