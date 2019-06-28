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

package com.hazelcast.internal.query.physical;

import java.util.ArrayList;
import java.util.List;

/**
 * Base physical plan.
 */
public class PhysicalPlan {
    private final List<PhysicalNode> nodes = new ArrayList<>();

    public PhysicalPlan() {
        // No-op.
    }

    public List<PhysicalNode> getNodes() {
        return nodes;
    }

    public void addNode(PhysicalNode node) {
        nodes.add(node);
    }
}
