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

package com.hazelcast.sql.impl.calcite.schema;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.util.ImmutableBitSet;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HazelcastTableStatisticTest {
    @Test
    public void testStatistic() {
        HazelcastTableStatistic statistic = new HazelcastTableStatistic(100L);

        assertEquals(100d, statistic.getRowCount(), 0.0d);

        assertTrue(statistic.getKeys().isEmpty());
        assertFalse(statistic.isKey(ImmutableBitSet.builder().set(0).build()));

        assertTrue(statistic.getReferentialConstraints().isEmpty());

        assertTrue(statistic.getCollations().isEmpty());

        assertEquals(RelDistributionTraitDef.INSTANCE.getDefault(), statistic.getDistribution());
    }

    @Test
    public void testToString() {
        HazelcastTableStatistic statistic = new HazelcastTableStatistic(100L);

        assertEquals("HazelcastTableStatistic{rowCount=100}", statistic.toString());
    }
}
