/*
 * Copyright 2021 Hazelcast Inc.
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

package com.hazelcast.jet.sql.impl;

import com.hazelcast.instance.impl.HazelcastInstanceProxy;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.sql.impl.SqlServiceImpl;
import com.hazelcast.sql.impl.optimizer.SqlOptimizer;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class OptimizerInstantiationTest extends SqlTestSupport {

    @BeforeClass
    public static void setUpClass() {
        initialize(1, null);
    }

    @Test
    public void testCreate() {
        SqlServiceImpl service = ((HazelcastInstanceProxy) instance()).getOriginal().node.getNodeEngine().getSqlService();

        SqlOptimizer optimizer = service.getOptimizer();

        assertNotNull(optimizer);
        assertEquals(CalciteSqlOptimizer.class, optimizer.getClass());
    }
}
