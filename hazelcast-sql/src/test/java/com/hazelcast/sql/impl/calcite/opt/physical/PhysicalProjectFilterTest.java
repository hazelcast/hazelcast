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

package com.hazelcast.sql.impl.calcite.opt.physical;

import com.hazelcast.sql.impl.calcite.opt.OptimizerTestSupport;
import org.junit.Test;

/**
 * Tests for physical optimizer project/filter stuff.
 */
public class PhysicalProjectFilterTest extends OptimizerTestSupport {
    /**
     * Before: Project2(expression) <- Project1 <- Scan
     * After : Project2 <- Scan(Project2)
     */
    @Test
    public void testProjectExpressionProjectIntoScan() {
        assertPlan(
            optimizePhysical("SELECT f1 + f2, f3 FROM (SELECT f1, f2, f3, f4 FROM p)"),
            plan(
                planRow(0, RootPhysicalRel.class, "", 100d),
                planRow(1, ProjectPhysicalRel.class, "EXPR$0=[+($0, $1)], f3=[$2]", 100d),
                planRow(2, MapScanPhysicalRel.class, "table=[[hazelcast, p[projects=[0, 1, 2]]]]", 100d)
            )
        );
    }
}
