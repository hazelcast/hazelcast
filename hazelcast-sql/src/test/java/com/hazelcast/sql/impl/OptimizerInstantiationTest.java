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

package com.hazelcast.sql.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastInstanceProxy;
import com.hazelcast.sql.impl.calcite.CalciteSqlOptimizer;
import com.hazelcast.sql.impl.optimizer.SqlOptimizer;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class OptimizerInstantiationTest extends SqlTestSupport {

    private static final TestHazelcastInstanceFactory FACTORY = new TestHazelcastInstanceFactory(1);

    @AfterClass
    public static void afterClass() {
        FACTORY.shutdownAll();
    }

    @Test
    public void testCreate() {
        HazelcastInstance instance = FACTORY.newHazelcastInstance();

        SqlServiceImpl service = ((HazelcastInstanceProxy) instance).getOriginal().node.getNodeEngine().getSqlService();

        SqlOptimizer optimizer = service.getOptimizer();

        assertNotNull(optimizer);
        assertEquals(CalciteSqlOptimizer.class, optimizer.getClass());
    }
}
