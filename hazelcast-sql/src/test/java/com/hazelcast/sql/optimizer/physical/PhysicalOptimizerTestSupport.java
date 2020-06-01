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

package com.hazelcast.sql.optimizer.physical;

import com.hazelcast.sql.impl.calcite.opt.physical.PhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.RootPhysicalRel;
import com.hazelcast.sql.optimizer.OptimizerTestSupport;
import org.apache.calcite.rel.RelNode;

import static junit.framework.TestCase.assertEquals;

/**
 * Utility methods for physical optimizer tests.
 */
public class PhysicalOptimizerTestSupport extends OptimizerTestSupport {
    @Override
    protected final boolean isOptimizePhysical() {
        return true;
    }

    /**
     * Perform physical optimization.
     *
     * @param sql SQL.
     * @return Input of the root node.
     */
    protected RelNode optimizePhysical(String sql) {
        PhysicalRel rel = optimize(sql).getPhysical();

        RootPhysicalRel root = assertRoot(rel);

        return root.getInput();
    }

    protected static RootPhysicalRel assertRoot(RelNode node) {
        return assertClass(node, RootPhysicalRel.class);
    }

    @SuppressWarnings("unchecked")
    protected static <T> T assertClass(RelNode rel, Class<? extends PhysicalRel> expClass) {
        assertEquals(expClass, rel.getClass());

        return (T) rel;
    }
}
