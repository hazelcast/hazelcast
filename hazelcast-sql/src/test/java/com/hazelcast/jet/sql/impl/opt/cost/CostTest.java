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

package com.hazelcast.jet.sql.impl.opt.cost;

import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.calcite.plan.RelOptUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CostTest extends SqlTestSupport {

    @Test
    public void testCostFactory() {
        CostFactory factory = CostFactory.INSTANCE;

        assertSame(Cost.INFINITY, factory.makeInfiniteCost());
        assertSame(Cost.HUGE, factory.makeHugeCost());
        assertSame(Cost.TINY, factory.makeTinyCost());
        assertSame(Cost.ZERO, factory.makeZeroCost());

        Cost cost = factory.makeCost(1.0d, 2.0d, 3.0d);

        assertEquals(1.0d, cost.getRowsInternal(), 0.0d);
        assertEquals(2.0d, cost.getCpuInternal(), 0.0d);
        assertEquals(3.0d, cost.getNetworkInternal(), 0.0d);
    }

    @Test
    public void testIsInfinite() {
        CostFactory factory = CostFactory.INSTANCE;

        assertFalse(factory.makeCost(1.0, 2.0d, 3.0d).isInfinite());

        assertTrue(factory.makeCost(1.0, Double.POSITIVE_INFINITY, 3.0d).isInfinite());
        assertTrue(factory.makeCost(1.0, 2.0d, Double.POSITIVE_INFINITY).isInfinite());
        assertTrue(factory.makeCost(1.0, Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY).isInfinite());
        assertTrue(factory.makeInfiniteCost().isInfinite());
    }

    @Test
    public void testEpsilon() {
        CostFactory factory = CostFactory.INSTANCE;

        Cost cost1 = factory.makeCost(1.0d, 2.0d, 3.0d);
        Cost cost2 = factory.makeCost(1.0d, 2.0d + RelOptUtil.EPSILON / 2, 3.0d);
        Cost cost3 = factory.makeCost(1.0d, 2.0d + RelOptUtil.EPSILON * 2, 3.0d);

        assertTrue(cost1.isEqWithEpsilon(cost2));
        assertFalse(cost1.isEqWithEpsilon(cost3));

        assertTrue(cost2.isEqWithEpsilon(cost1));
        assertFalse(cost2.isEqWithEpsilon(cost3));

        assertFalse(cost3.isEqWithEpsilon(cost1));
        assertFalse(cost3.isEqWithEpsilon(cost2));

        Cost infiniteCost = factory.makeInfiniteCost();

        assertFalse(cost1.isEqWithEpsilon(infiniteCost));
        assertFalse(infiniteCost.isEqWithEpsilon(cost1));
        assertFalse(infiniteCost.isEqWithEpsilon(infiniteCost));
    }

    @Test
    public void testLessThan() {
        CostFactory factory = CostFactory.INSTANCE;

        Cost cost = factory.makeCost(1.0d, 2.0d, 3.0d);
        Cost sameCost = factory.makeCost(1.0d, 2.0d, 3.0d);

        assertFalse(cost.isLt(cost));
        assertFalse(cost.isLt(sameCost));
        assertTrue(cost.isLe(cost));
        assertTrue(cost.isLe(sameCost));

        Cost biggerCost = factory.makeCost(4.0d, 5.0d, 6.0d);

        assertTrue(cost.isLt(biggerCost));
        assertTrue(cost.isLe(biggerCost));
        assertFalse(biggerCost.isLt(cost));
        assertFalse(biggerCost.isLe(cost));

        Cost infiniteCost = factory.makeInfiniteCost();

        assertFalse(infiniteCost.isLt(infiniteCost));
        assertTrue(infiniteCost.isLe(infiniteCost));
    }

    @Test
    public void testPlus() {
        CostFactory factory = CostFactory.INSTANCE;

        Cost firstCost = factory.makeCost(1.0d, 2.0d, 3.0d);
        Cost secondCost = factory.makeCost(4.0d, 5.0d, 6.0d);
        Cost infiniteCost = factory.makeInfiniteCost();

        assertEquals(factory.makeCost(5.0d, 7.0d, 9.0d), firstCost.plus(secondCost));
        assertEquals(infiniteCost, firstCost.plus(infiniteCost));
    }

    @Test
    public void testMultiply() {
        CostFactory factory = CostFactory.INSTANCE;

        Cost originalCost = factory.makeCost(1.0d, 2.0d, 3.0d);
        Cost multipliedCost = factory.makeCost(3.0d, 6.0d, 9.0d);
        assertEquals(multipliedCost, originalCost.multiplyBy(3.0d));

        Cost infiniteCost = factory.makeInfiniteCost();
        assertEquals(infiniteCost, infiniteCost.multiplyBy(3.0d));
    }

    @Test
    public void testEquals() {
        CostFactory factory = CostFactory.INSTANCE;

        checkEquals(factory.makeCost(1.0d, 2.0d, 3.0d), factory.makeCost(1.0d, 2.0d, 3.0d), true);

        checkEquals(factory.makeCost(1.0d, 2.0d, 3.0d), factory.makeCost(10.0d, 2.0d, 3.0d), false);

        checkEquals(factory.makeCost(1.0d, 2.0d, 3.0d), factory.makeCost(1.0d, 10.0d, 3.0d), false);
        checkEquals(factory.makeCost(1.0d, 2.0d, 3.0d), factory.makeCost(1.0d, Double.POSITIVE_INFINITY, 3.0d), false);

        checkEquals(factory.makeCost(1.0d, 2.0d, 3.0d), factory.makeCost(1.0d, 2.0d, 10.0d), false);
        checkEquals(factory.makeCost(1.0d, 2.0d, 3.0d), factory.makeCost(1.0d, 2.0d, Double.POSITIVE_INFINITY), false);
    }
}
