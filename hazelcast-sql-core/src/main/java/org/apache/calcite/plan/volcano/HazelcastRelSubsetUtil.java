/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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
     * Gets all subsets from the input's set
     * @param input the input
     * @return the subset
     */
    public static List<RelSubset> getSubsets(RelNode input) {
        VolcanoPlanner planner = (VolcanoPlanner) input.getCluster().getPlanner();
        RelSet set = planner.getSet(input);
        return set.subsets;
    }
}
