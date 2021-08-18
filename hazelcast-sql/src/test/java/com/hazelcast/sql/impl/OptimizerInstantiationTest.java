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

package com.hazelcast.sql.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastInstanceProxy;
import com.hazelcast.jet.sql.impl.CalciteSqlOptimizer;
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
