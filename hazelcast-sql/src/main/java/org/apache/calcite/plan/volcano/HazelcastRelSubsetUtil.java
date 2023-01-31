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

package org.apache.calcite.plan.volcano;

import org.apache.calcite.rel.RelNode;

import java.util.List;

/**
 * Utility class to access package-private Calcite internals.
 */
public final class HazelcastRelSubsetUtil {

    private HazelcastRelSubsetUtil() {
    }

    /**
     * Gets all subsets from the node's set
     */
    public static List<RelSubset> getSubsets(RelNode node) {
        VolcanoPlanner planner = (VolcanoPlanner) node.getCluster().getPlanner();
        RelSet set = planner.getSet(node);
        return set.subsets;
    }

    /**
     * If the {@code node} is a {@link RelSubset}, return the best rel from it.
     *  Otherwise, return the node.
     */
    public static RelNode unwrapSubset(RelNode node) {
        if (node instanceof RelSubset) {
            RelNode best = ((RelSubset) node).getBest();
            if (best != null) {
                return unwrapSubset(best);
            }
        }
        return node;
    }
}
